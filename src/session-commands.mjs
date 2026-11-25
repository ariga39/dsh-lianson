/** Conflict-safe delegation session creation and idempotent prompt submission. */

import { randomUUID } from 'node:crypto'
import { realpath, stat } from 'node:fs/promises'
import { LiaisonError, remoteCodeOf } from './errors.mjs'
import { normalizeSessionTitle } from './title.mjs'

function normalizeModel(model) {
  if (model === null || typeof model !== 'object') throw new TypeError('model selection is required')
  if (typeof model.provider !== 'string' || model.provider === '') throw new TypeError('model.provider is required')
  if (typeof model.model !== 'string' || model.model === '') throw new TypeError('model.model is required')
  return {
    provider: model.provider,
    model: model.model,
    ...(model.reasoningEffort === undefined ? {} : { reasoningEffort: model.reasoningEffort }),
  }
}

function sameModel(left, right) {
  return left.provider === right.provider
    && left.model === right.model
    && (left.reasoningEffort ?? null) === (right.reasoningEffort ?? null)
}

const HISTORY_PAGE_MESSAGES = 500
const HISTORY_MAX_PAGES = 100

function eventList(page) {
  return page.records.map(record => record.event)
}

/** Delegation operations over the sliced Host Remote adapter. */
export class SessionCommands {
  #registry
  #adapter
  #historyPageMessages
  #historyMaxPages
  #createChains = new Map()

  /**
   * @param {{ registry: import('./delegation-registry.mjs').DelegationRegistry, adapter: object, historyPageMessages?: number, historyMaxPages?: number }} options - persistence and Host seam.
   */
  constructor({
    registry,
    adapter,
    historyPageMessages = HISTORY_PAGE_MESSAGES,
    historyMaxPages = HISTORY_MAX_PAGES,
  }) {
    this.#registry = registry
    this.#adapter = adapter
    this.#historyPageMessages = historyPageMessages
    this.#historyMaxPages = historyMaxPages
  }

  /**
   * Create or resume one delegation session and bind it to an explicit model.
   * Calls for the same delegation are serialized so a late host result can
   * never overtake a ready transition.
   * @param {object} request - delegation identity, directory, title, model, and optional IPC replay identity.
   * @returns the ready ledger record.
   */
  async createDelegation(request) {
    const delegationId = request?.delegationId
    if (typeof delegationId !== 'string' || delegationId === '') {
      return this.#createDelegationUnlocked(request)
    }
    const previous = this.#createChains.get(delegationId) ?? Promise.resolve()
    const run = previous.then(
      () => this.#createDelegationUnlocked(request),
      () => this.#createDelegationUnlocked(request),
    )
    const tail = run.then(() => undefined, () => undefined)
    this.#createChains.set(delegationId, tail)
    try {
      return await run
    } finally {
      if (this.#createChains.get(delegationId) === tail) this.#createChains.delete(delegationId)
    }
  }

  async #createDelegationUnlocked({ delegationId, cwd, title, model, agentPreset, ipcRequestId, ipcFingerprint }) {
    if (typeof delegationId !== 'string' || delegationId === '') throw new TypeError('delegationId is required')
    if (typeof title !== 'string' || title.trim() === '') throw new TypeError('title is required')
    const normalizedTitle = normalizeSessionTitle(title)
    if (normalizedTitle === '') {
      throw new LiaisonError(
        'delegation/title-invalid',
        'session title must contain visible characters',
        { title },
      )
    }
    const expectedModel = normalizeModel(model)
    const canonicalCwd = await this.#canonicalDirectory(cwd)

    const catalog = await this.#adapter.modelCatalog()
    this.#requireAdvertised(catalog, expectedModel)

    const workspace = await this.#adapter.ensureWorkspace(canonicalCwd)
    if (typeof workspace?.workspaceId !== 'string') {
      throw new LiaisonError(
        'delegation/workspace-unavailable',
        `workspace.create returned no workspace id for "${canonicalCwd}"`,
        { cwd: canonicalCwd },
      )
    }

    const record = await this.#registry.beginCreate({
      delegationId,
      cwd: canonicalCwd,
      workspaceId: workspace.workspaceId,
      sessionId: `session-${randomUUID()}`,
      title: normalizedTitle,
      agentPreset,
      expectedModel,
    })

    let created
    try {
      created = await this.#adapter.createSession({
        workspaceId: record.workspaceId,
        sessionId: record.sessionId,
        ...(record.agentPreset === undefined ? {} : { agentPreset: record.agentPreset }),
      })
    } catch (error) {
      throw await this.#mapCreateFailure(record, error)
    }

    if (created?.sessionId !== record.sessionId) {
      await this.#registry.markConflict(record.delegationId, {
        upstreamCode: 'session/substituted',
        message: `Host returned session "${String(created?.sessionId)}" instead of "${record.sessionId}"`,
        substitutedSessionId: created?.sessionId,
      })
      throw new LiaisonError(
        'delegation/session-substituted',
        `Host substituted session "${String(created?.sessionId)}" for requested "${record.sessionId}"; refusing to bind`,
        {
          delegationId: record.delegationId,
          requestedSessionId: record.sessionId,
          returnedSessionId: created?.sessionId,
          workspaceId: record.workspaceId,
        },
      )
    }

    try {
      await this.#adapter.renameSession({ sessionId: record.sessionId, title: record.title })
    } catch (error) {
      throw new LiaisonError(
        'delegation/rename-failed',
        `session "${record.sessionId}" was created but could not be renamed: ${error?.message ?? String(error)}`,
        { delegationId: record.delegationId, sessionId: record.sessionId },
        { cause: error },
      )
    }

    let selected
    try {
      selected = normalizeModel((await this.#adapter.selectModel({
        sessionId: record.sessionId,
        ...expectedModel,
      })).selected)
    } catch (error) {
      throw new LiaisonError(
        'delegation/model-selection-failed',
        `session "${record.sessionId}" could not select ${expectedModel.provider}/${expectedModel.model}: ${error?.message ?? String(error)}`,
        { delegationId: record.delegationId, sessionId: record.sessionId, expectedModel },
        { cause: error },
      )
    }
    if (!sameModel(selected, expectedModel)) {
      await this.#registry.markConflict(record.delegationId, {
        upstreamCode: 'delegation/model-selection-mismatch',
        message: `Host selected ${selected.provider}/${selected.model} instead of ${expectedModel.provider}/${expectedModel.model}`,
      })
      throw new LiaisonError(
        'delegation/model-mismatch',
        `Host selected ${selected.provider}/${selected.model} instead of the requested ${expectedModel.provider}/${expectedModel.model}`,
        { delegationId: record.delegationId, sessionId: record.sessionId, expectedModel, selected },
      )
    }

    return this.#registry.markReady(record.delegationId, {
      ...(ipcRequestId === undefined ? {} : { ipcRequestId }),
      ...(ipcFingerprint === undefined ? {} : { ipcFingerprint }),
    })
  }

  /**
   * Submit one prompt with a stable request id.
   * @param {{ delegationId: string, content: string, requestId?: string }} request - target and prompt.
   * @returns acceptance evidence, never a completion claim.
   */
  async submitPrompt({ delegationId, content, requestId }) {
    const record = await this.#requireReady(delegationId)
    if (typeof content !== 'string' || content.trim() === '') throw new TypeError('content is required')
    const stableRequestId = requestId ?? `request-${randomUUID()}`
    const before = await this.#adapter.sessionState({ sessionId: record.sessionId })
    const accepted = await this.#adapter.prompt({
      requestId: stableRequestId,
      sessionId: record.sessionId,
      mode: 'queue',
      content: [{ type: 'text', text: content }],
    })
    if (accepted?.accepted !== true) {
      throw new LiaisonError(
        'delegation/prompt-rejected',
        `Host did not accept prompt "${stableRequestId}"`,
        { delegationId, sessionId: record.sessionId, requestId: stableRequestId },
      )
    }
    return {
      delegationId,
      sessionId: record.sessionId,
      requestId: stableRequestId,
      accepted: true,
      beforeCursor: before.cursor,
      completed: false,
    }
  }

  /**
   * Verify the persisted request-header model for one submitted request.
   * @param {{ delegationId: string, requestId?: string }} request - delegation and optional exact request window.
   * @returns the verified route and event position.
   */
  async verifyRequestModel({ delegationId, requestId }) {
    const record = await this.#requireReady(delegationId)
    let header
    let turnEndSeq
    if (requestId === undefined) {
      header = await this.#latestRequestHeader(record.sessionId)
    } else {
      const window = await this.#requestHeaderWindow(record.sessionId, requestId)
      header = window.header
      turnEndSeq = window.turnEnd.seq
    }
    if (header === undefined) {
      throw new LiaisonError(
        'delegation/request-header-missing',
        `session "${record.sessionId}" has no persisted request/header yet`,
        { delegationId, sessionId: record.sessionId, requestId },
      )
    }

    const config = header.data.header.config
    const expected = record.expectedModel
    const matches = config.provider === expected.provider
      && config.model === expected.model
      && (expected.reasoningEffort === undefined || config.reasoningEffort === expected.reasoningEffort)
    if (!matches) {
      throw new LiaisonError(
        'delegation/model-mismatch',
        `request header used ${String(config.provider)}/${String(config.model)} instead of `
          + `${expected.provider}/${expected.model}`,
        {
          delegationId,
          sessionId: record.sessionId,
          requestId,
          expectedModel: expected,
          actual: {
            provider: config.provider,
            model: config.model,
            ...(config.reasoningEffort === undefined ? {} : { reasoningEffort: config.reasoningEffort }),
          },
        },
      )
    }

    const verified = {
      provider: config.provider,
      model: config.model,
      ...(expected.reasoningEffort === undefined ? {} : { reasoningEffort: config.reasoningEffort }),
    }
    await this.#registry.setVerifiedModel(delegationId, verified)
    return {
      delegationId,
      sessionId: record.sessionId,
      ...(requestId === undefined ? {} : { requestId }),
      ...verified,
      seq: header.seq,
      ...(turnEndSeq === undefined ? {} : { turnEndSeq }),
    }
  }

  /**
   * Read the latest persisted request/header when no exact request id scopes
   * the lookup, paginating past a page that contains no header event.
   */
  async #latestRequestHeader(sessionId) {
    const address = { kind: 'session', sessionId }
    let beforeSeq
    for (let index = 0; index < this.#historyMaxPages; index += 1) {
      const page = await this.#adapter.sessionEvents({
        address,
        maxMessages: this.#historyPageMessages,
        ...(beforeSeq === undefined ? {} : { beforeSeq }),
      })
      const events = eventList(page)
      if (events.length === 0) return undefined
      const header = events.findLast(event => event.type === 'request/header')
      if (header !== undefined) return header
      if (page.hasMore !== true) return undefined
      beforeSeq = events[0].seq
    }
    return undefined
  }

  /**
   * Locate the requested turn, pin history at its turn/end, and walk backwards
   * to the latest request/header at or before that cut. An unchanged header is
   * inherited from an earlier turn; a later turn's header is never eligible.
   */
  async #requestHeaderWindow(sessionId, requestId) {
    const address = { kind: 'session', sessionId }
    const pages = []
    let beforeSeq
    let user
    let turnEnd
    for (let index = 0; index < this.#historyMaxPages; index += 1) {
      const page = await this.#adapter.sessionEvents({
        address,
        maxMessages: this.#historyPageMessages,
        ...(beforeSeq === undefined ? {} : { beforeSeq }),
      })
      const events = eventList(page)
      if (events.length === 0) break
      pages.unshift(events)
      const all = pages.flat()
      user = all.find(event =>
        event.type === 'user/message' && event.data.source?.rpcId === requestId)
      if (user !== undefined) {
        turnEnd = all.find(event => event.type === 'turn/end' && event.seq > user.seq)
        if (turnEnd !== undefined) break
      }
      if (page.hasMore !== true) break
      beforeSeq = events[0].seq
    }
    if (user === undefined) {
      throw new LiaisonError(
        'delegation/request-not-found',
        `request "${requestId}" is not persisted in session "${sessionId}"`,
        { sessionId, requestId },
      )
    }
    if (turnEnd === undefined) {
      throw new LiaisonError(
        'delegation/request-window-incomplete',
        `request "${requestId}" has no completed turn to pin in session "${sessionId}"`,
        { sessionId, requestId, requestSeq: user.seq },
      )
    }

    const pinned = []
    let pinnedBefore
    let header
    for (let index = 0; index < this.#historyMaxPages; index += 1) {
      const page = await this.#adapter.sessionEvents({
        address,
        throughSeq: turnEnd.seq,
        maxMessages: this.#historyPageMessages,
        ...(pinnedBefore === undefined ? {} : { beforeSeq: pinnedBefore }),
      })
      const events = eventList(page)
      if (events.length === 0) break
      pinned.unshift(events)
      header = pinned.flat().findLast(event =>
        event.type === 'request/header' && event.seq <= turnEnd.seq)
      if (header !== undefined) break
      if (page.hasMore !== true) break
      pinnedBefore = events[0].seq
    }
    if (header === undefined) {
      throw new LiaisonError(
        'delegation/request-header-missing',
        `session "${sessionId}" has no request/header at or before the requested turn end`,
        { sessionId, requestId, turnEndSeq: turnEnd.seq },
      )
    }
    return { header, user, turnEnd }
  }

  async #canonicalDirectory(cwd) {
    if (typeof cwd !== 'string' || cwd === '') throw new TypeError('cwd is required')
    let canonical
    try {
      canonical = await realpath(cwd)
    } catch (error) {
      throw new LiaisonError(
        'delegation/invalid-cwd',
        `working directory "${cwd}" does not exist`,
        { cwd },
        { cause: error },
      )
    }
    const information = await stat(canonical)
    if (!information.isDirectory()) {
      throw new LiaisonError(
        'delegation/invalid-cwd',
        `working directory "${cwd}" is not a directory`,
        { cwd },
      )
    }
    return canonical
  }

  #requireAdvertised(catalog, model) {
    const routable = new Set(catalog?.routableProviders ?? [])
    const group = (catalog?.groups ?? []).find(candidate => candidate.id === model.provider)
    const entry = group?.models.find(candidate => candidate.id === model.model)
    if (!routable.has(model.provider) || entry === undefined) {
      throw new LiaisonError(
        'delegation/model-unavailable',
        `model ${model.provider}/${model.model} is not advertised by the Host`,
        { provider: model.provider, model: model.model },
      )
    }
    if (model.reasoningEffort !== undefined
      && !(entry.reasoning?.efforts ?? []).some(effort => effort.id === model.reasoningEffort)) {
      throw new LiaisonError(
        'delegation/model-unavailable',
        `reasoning effort "${model.reasoningEffort}" is not advertised for ${model.provider}/${model.model}`,
        { provider: model.provider, model: model.model, reasoningEffort: model.reasoningEffort },
      )
    }
  }

  async #mapCreateFailure(record, error) {
    const code = remoteCodeOf(error)
    if (code === 'session/workspace-attach-failed') {
      const details = error?.details ?? {}
      const sessionId = details.sessionId ?? record.sessionId
      const workspaceId = details.workspaceId ?? record.workspaceId
      await this.#registry.markAttachFailed(record.delegationId, {
        sessionId,
        workspaceId,
        message: error?.message,
      })
      return new LiaisonError(
        'delegation/attach-failed',
        `session "${sessionId}" was created but could not attach to workspace "${workspaceId}"`,
        { delegationId: record.delegationId, sessionId, workspaceId, upstreamCode: code },
        { cause: error },
      )
    }
    if (code === 'session/conflict' || code === 'session/agent-busy') {
      await this.#registry.markConflict(record.delegationId, {
        upstreamCode: code,
        message: error?.message,
      })
      return new LiaisonError(
        'delegation/conflict',
        `session "${record.sessionId}" cannot be claimed: ${error?.message ?? code}`,
        { delegationId: record.delegationId, sessionId: record.sessionId, upstreamCode: code, upstreamDetails: error?.details },
        { cause: error },
      )
    }
    return new LiaisonError(
      'delegation/create-failed',
      `session.create for delegation "${record.delegationId}" did not complete: ${error?.message ?? String(error)}`,
      {
        delegationId: record.delegationId,
        sessionId: record.sessionId,
        workspaceId: record.workspaceId,
        transient: error?.transient === true,
        causeCode: code,
      },
      { cause: error },
    )
  }

  async #requireReady(delegationId) {
    const record = await this.#registry.get(delegationId)
    if (record === undefined) {
      throw new LiaisonError(
        'delegation/not-found',
        `delegation "${delegationId}" is not in the ledger`,
        { delegationId },
      )
    }
    if (record.state !== 'ready') {
      throw new LiaisonError(
        'delegation/not-ready',
        `delegation "${delegationId}" is ${record.state}, not ready`,
        { delegationId, state: record.state, sessionId: record.sessionId },
      )
    }
    return record
  }
}

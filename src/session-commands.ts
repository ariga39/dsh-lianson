import { createHash, randomUUID } from 'node:crypto'
import { resolve } from 'node:path'
import type { ClientRemote } from '@deepseek-ai/dsh-api-gateway/client'
import type {
  SessionEventEntry,
  SessionQueuedItem,
  SessionRequestId,
  SessionState,
  SessionWireEvent,
} from '@deepseek-ai/dsh-api-session-controller/types'
import { brandString } from '@deepseek-ai/dsh-brand'
import { normalizeSessionTitle } from '@deepseek-ai/dsh-session-title'
import type { Deadline } from './deadline.ts'
import { LiaisonError } from './errors.ts'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import type { RemoteFailure, RemoteResult } from '@deepseek-ai/dsh-typert-protocol'
import {
  DelegationRegistry,
  DelegationRegistryError,
  type DelegationRecord,
  type ModelTarget,
  type PromptKeyBinding,
  type PromptReceipt,
} from './delegation-registry.ts'
import { DelegationError, guardWorkspace, type DelegationErrorCode } from './workspace-guard.ts'

/** Verified Host metadata without credentials. */
export interface VerifiedHostContext {
  readonly stateDir: string
  readonly dshHome: string
  readonly cwd: string
  readonly hostPid: number
  readonly origin: string
  readonly port: number
}

/** Minimal typed Remote subset needed by SessionCommands. */
export interface SessionCommandsRemote {
  readonly workspace: Pick<ClientRemote['workspace'], 'create'>
  readonly session: Pick<
    ClientRemote['session'],
    'modelCatalog' | 'list' | 'create' | 'rename' | 'selectModel' | 'prompt' | 'state' | 'events'
  >
}

/**
 * Single-attempt, deadline-bounded operator access.
 * Production supplies this from HostManager; tests may adapt the real Host operator.
 */
export interface HostOperatorAccess {
  hostContext(deadline?: Deadline): Promise<VerifiedHostContext>
  withOperator<T>(
    operation: (remote: SessionCommandsRemote, signal: AbortSignal) => Promise<RemoteResult<T>>,
    timeoutOrDeadline?: number | Deadline,
  ): Promise<RemoteResult<T>>
}

/** Input for one write-ahead delegation creation. */
export interface CreateDelegationCommand {
  readonly delegationId?: string
  readonly title: string
  readonly provider: string
  readonly model: string
  readonly reasoningEffort?: string
  readonly agentPreset?: string
  readonly cwd: string
  readonly ipcRequestId?: string
  readonly deadline?: Deadline
}

/** Result of a create command, distinguishing a replay from a new Host mutation. */
export interface CreateDelegationCommandResult {
  readonly delegation: DelegationRecord
  readonly replayed: boolean
}

/** Input for one idempotent prompt submission. */
export interface PromptCommand {
  readonly delegationId: string
  readonly content: string
  readonly requestId?: string
  readonly ipcRequestId?: string
  readonly deadline?: Deadline
}

/** Acceptance receipt; completion belongs to later output workflows. */
export interface PromptCommandResult {
  readonly accepted: true
  readonly completed: false
  readonly replayed: boolean
  readonly delegationId: string
  readonly sessionId: string
  readonly requestId: string
  readonly beforeCursor: number
}

/** Input for turn-scoped model verification. */
export interface VerifyModelCommand {
  readonly delegationId: string
  readonly requestId?: string
  readonly ipcRequestId?: string
  readonly deadline?: Deadline
}

/** Verified model evidence for one requested turn. */
export interface VerifyModelResult {
  readonly delegationId: string
  readonly sessionId: string
  readonly requestId?: string
  readonly turnSeq: number
  readonly expectedModel: ModelTarget
  readonly observedModel: ModelTarget
  readonly verified: true
}

/** Lookup by exactly one delegation or session identity. */
export interface DelegationLookup {
  readonly delegationId?: string
  readonly sessionId?: string
}

/** Construction options for one Session command service. */
export interface SessionCommandsOptions {
  readonly registry: DelegationRegistry
  readonly access: HostOperatorAccess | (() => HostOperatorAccess)
  readonly ledgerDir: string
  readonly titleMaxBytes?: number
  readonly timeoutMs?: number
}

interface ModelSelection {
  readonly provider: string
  readonly model: string
  readonly reasoningEffort?: string
}

const DEFAULT_TITLE_MAX_BYTES = 120
const DEFAULT_TIMEOUT_MS = 15_000
const EVENTS_PAGE_MESSAGES = 400
const MAX_EVENT_PAGES = 32
const EVIDENCE_TYPES = new Set(['assistant/attempt', 'assistant/message', 'tool/call', 'tool/result'])

/**
 * Delegation create/prompt/verify operations over one durable registry and one
 * single-attempt operator access seam.
 */
export class SessionCommands {
  private readonly registry: DelegationRegistry
  private readonly access: HostOperatorAccess | (() => HostOperatorAccess)
  private readonly ledgerDir: string
  private readonly titleMaxBytes: number
  private readonly timeoutMs: number
  private readonly flights = new Map<string, Promise<unknown>>()

  /**
   * @param options - durable registry, Host access, ledger directory, and bounded defaults.
   */
  constructor(options: SessionCommandsOptions) {
    this.registry = options.registry
    this.access = options.access
    this.ledgerDir = options.ledgerDir
    this.titleMaxBytes = options.titleMaxBytes ?? DEFAULT_TITLE_MAX_BYTES
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  }

  /**
   * Create or replay one delegated Session and its workspace binding.
   * @param command - identity, model, title, cwd, and optional IPC replay key.
   * @returns the durable record and whether no Host mutation was needed.
   */
  async create(command: CreateDelegationCommand): Promise<CreateDelegationCommandResult> {
    const fingerprint = createFingerprint(command)
    const requestedId = command.delegationId
      ?? (command.ipcRequestId === undefined
        ? `delegation-${randomUUID()}`
        : `delegation-${digest(command.ipcRequestId)}`)
    const keys = [
      `create:${requestedId}:${fingerprint}`,
      ...command.ipcRequestId === undefined ? [] : [`create-ipc:${command.ipcRequestId}`],
    ]

    return await this.flight(keys, async () => {
      await this.ownWriter(command.deadline)
      assertDeadlineLive(command.deadline)
      const existing = await this.lookupCreate(requestedId, command.delegationId, fingerprint, command.ipcRequestId)
      if (existing !== undefined && existing.state === 'ready') {
        await this.assertRecordedWorkspace(existing, command.deadline)
        return { delegation: existing, replayed: true }
      }

      assertDeadlineLive(command.deadline)
      const context = await this.host().hostContext(command.deadline)
      const cwd = await guardWorkspace({
        cwd: command.cwd,
        stateDir: context.stateDir,
        dshHome: context.dshHome,
        ledgerDir: this.ledgerDir,
      })
      const title = normalizeSessionTitle(command.title, this.titleMaxBytes)
      if (title === '') {
        throw new DelegationError('DELEGATION_TITLE_CONFLICT', 'the requested title is empty after normalization', {
          title: command.title,
        })
      }
      const expectedModel = modelTarget({
        provider: command.provider,
        model: command.model,
        ...(command.reasoningEffort === undefined ? {} : { reasoningEffort: command.reasoningEffort }),
      })
      await this.validateModel(expectedModel)
      await this.validateLiveTitle(title, cwd, existing?.sessionId)

      const delegationId = existing?.delegationId ?? requestedId
      const sessionId = existing?.state === 'creating' && existing.createFingerprint === fingerprint
        ? existing.sessionId
        : `session-${randomUUID()}`
      assertDeadlineLive(command.deadline)
      const begun = await this.registry.beginCreate({
        delegationId,
        sessionId,
        title,
        cwd,
        expectedModel,
        createFingerprint: fingerprint,
        ...(command.agentPreset === undefined ? {} : { agentPreset: command.agentPreset }),
        ...(command.ipcRequestId === undefined ? {} : { createIpcId: command.ipcRequestId }),
      })
      if (begun.record.state === 'ready') {
        return { delegation: begun.record, replayed: true }
      }

      assertDeadlineLive(command.deadline)
      const workspace = await this.callRemote(
        remote => remote.workspace.create({ path: begun.record.cwd }),
        'WORKSPACE_INVALID_CWD',
        command.deadline,
      )
      const workspaceId = String(workspace.workspace.workspaceId)
      assertDeadlineLive(command.deadline)
      const created = await this.host().withOperator(
        remote => remote.session.create({
          workspaceId: workspace.workspace.workspaceId,
          sessionId: brandString<SessionId>(begun.record.sessionId),
          ...(begun.record.agentPreset === undefined ? {} : { agentPreset: begun.record.agentPreset }),
        }),
        command.deadline ?? this.timeoutMs,
      )
      if (!created.ok) {
        const failure = await this.handleCreateFailure(begun.record, created.error)
        throw failure
      }
      if (String(created.value.sessionId) !== begun.record.sessionId) {
        await this.registry.transition(begun.record.delegationId, { state: 'conflict' })
        throw new DelegationError(
          'DELEGATION_SESSION_SUBSTITUTED',
          `Host created ${String(created.value.sessionId)} instead of ${begun.record.sessionId}`,
          {
            delegationId: begun.record.delegationId,
            requestedSessionId: begun.record.sessionId,
            returnedSessionId: String(created.value.sessionId),
          },
        )
      }

      const renamed = await this.callRemote(
        remote => remote.session.rename({
          sessionId: brandString<SessionId>(begun.record.sessionId),
          title: begun.record.title,
        }),
        'DELEGATION_TITLE_CONFLICT',
        undefined,
      )
      if (renamed.title !== begun.record.title) {
        throw new DelegationError(
          'DELEGATION_TITLE_CONFLICT',
          `Host normalized ${begun.record.title} to ${renamed.title}`,
          { delegationId: begun.record.delegationId, requestedTitle: begun.record.title, acceptedTitle: renamed.title },
        )
      }

      const selected = await this.callRemote(
        remote => remote.session.selectModel({
          sessionId: brandString<SessionId>(begun.record.sessionId),
          provider: begun.record.expectedModel.provider,
          model: begun.record.expectedModel.model,
          ...(begun.record.expectedModel.reasoningEffort === undefined
            ? {}
            : { reasoningEffort: begun.record.expectedModel.reasoningEffort }),
        }),
        'DELEGATION_MODEL_UNAVAILABLE',
        undefined,
      )
      if (!sameModel(selected.selected, begun.record.expectedModel)) {
        throw new DelegationError(
          'DELEGATION_MODEL_MISMATCH',
          `Host selected a different model than requested`,
          {
            delegationId: begun.record.delegationId,
            expectedProvider: begun.record.expectedModel.provider,
            expectedModel: begun.record.expectedModel.model,
            observedProvider: selected.selected.provider,
            observedModel: selected.selected.model,
          },
        )
      }

      const ready = await this.registry.transition(begun.record.delegationId, {
        state: 'ready',
        workspaceId,
      })
      return { delegation: ready, replayed: false }
    })
  }

  /**
   * Submit or replay one prompt under its stable request identity.
   * @param command - delegation, content, and optional caller replay identities.
   * @returns acceptance with the original output cursor.
   */
  async prompt(command: PromptCommand): Promise<PromptCommandResult> {
    const requestId = command.requestId
      ?? (command.ipcRequestId === undefined ? `request-${randomUUID()}` : `request-${digest(command.ipcRequestId)}`)
    const contentHash = hash(command.content)
    const keys = [
      `prompt:${command.delegationId}:${requestId}:${contentHash}`,
      ...command.ipcRequestId === undefined ? [] : [`prompt-ipc:${command.ipcRequestId}`],
    ]

    return await this.flight(keys, async () => {
      await this.ownWriter(command.deadline)
      assertDeadlineLive(command.deadline)
      const record = await this.registry.get(command.delegationId)
      if (record.state !== 'ready') {
        throw new DelegationError(
          'DELEGATION_SESSION_CONFLICT',
          `delegation ${record.delegationId} is ${record.state}, not ready`,
          { delegationId: record.delegationId, state: record.state },
        )
      }
      const binding = await this.findPromptBinding(command.ipcRequestId)
      if (binding !== undefined) {
        this.assertPromptBinding(binding, command.ipcRequestId ?? '', record.delegationId, requestId, contentHash)
      }
      const existing = await this.registry.promptReceipt(record.delegationId, requestId)
      if (existing !== undefined) {
        if (existing.contentHash !== contentHash) {
          throw new DelegationRegistryError(
            'DELEGATION_PROMPT_CONFLICT',
            `request ${requestId} already carries different content`,
            { delegationId: record.delegationId, requestId },
          )
        }
        if (command.ipcRequestId !== undefined && binding === undefined) {
          await this.registry.beginPrompt(record.delegationId, {
            requestId,
            contentHash,
            beforeCursor: existing.beforeCursor,
            ipcRequestId: command.ipcRequestId,
          })
        }
        if (existing.accepted) return promptResult(record, existing, true)
        await this.assertRecordedWorkspace(record, command.deadline)
        return await this.resolvePendingPrompt(record, requestId, command.content, existing, command.deadline)
      }
      if (binding !== undefined) {
        throw new DelegationRegistryError(
          'DELEGATION_PROMPT_CONFLICT',
          `prompt key ${command.ipcRequestId ?? ''} is bound without a matching receipt`,
          { delegationId: record.delegationId, requestId, ipcRequestId: command.ipcRequestId ?? '' },
        )
      }

      await this.assertRecordedWorkspace(record, command.deadline)
      const state = await this.callRemote(
        (remote, signal) => remote.session.state({ sessionId: brandString<SessionId>(record.sessionId) }, signal),
        'DELEGATION_SESSION_CONFLICT',
        command.deadline,
      )
      assertDeadlineLive(command.deadline)
      const admission = await this.registry.beginPrompt(record.delegationId, {
        requestId,
        contentHash,
        beforeCursor: state.cursor,
        ...(command.ipcRequestId === undefined ? {} : { ipcRequestId: command.ipcRequestId }),
      })
      if (admission.receipt.accepted) return promptResult(record, admission.receipt, true)
      await this.dispatchPrompt(record, requestId, command.content, command.deadline)
      const accepted = await this.registry.acceptPrompt(record.delegationId, requestId)
      return promptResult(record, accepted, false)
    })
  }

  /**
   * Verify the exact model used by one requested turn.
   * @param command - delegation, turn request id, or optional IPC replay key.
   * @returns the observed model and monotonic turn position.
   */
  async verifyModel(command: VerifyModelCommand): Promise<VerifyModelResult> {
    await this.ownWriter(command.deadline)
    assertDeadlineLive(command.deadline)
    const record = await this.registry.get(command.delegationId)
    const requestId = command.requestId
      ?? (command.ipcRequestId === undefined ? undefined : `request-${digest(command.ipcRequestId)}`)
    const state = await this.callRemote(
      (remote, signal) => remote.session.state({ sessionId: brandString<SessionId>(record.sessionId) }, signal),
      'DELEGATION_SESSION_CONFLICT',
      command.deadline,
    )
    const events = await this.readEvents(record, state.cursor, command.deadline)
    const target = locateTarget(events, requestId)
    if (target === undefined) {
      throw new DelegationError(
        'DELEGATION_MODEL_EVIDENCE_MISSING',
        requestId === undefined
          ? `delegation ${record.delegationId} has no user turn`
          : `delegation ${record.delegationId} has no user turn for ${requestId}`,
        { delegationId: record.delegationId, ...(requestId === undefined ? {} : { requestId }) },
      )
    }

    const turnStart = [...events].reverse().find(event => event.type === 'turn/start' && event.seq <= target.seq)
    const turnEnd = events.find(event => event.type === 'turn/end' && event.seq >= target.seq)
    const windowStart = turnStart?.seq ?? target.seq
    const windowEnd = turnEnd?.seq ?? events.at(-1)?.seq ?? target.seq
    const evidence = events.filter(event => event.seq >= windowStart
      && event.seq <= windowEnd
      && EVIDENCE_TYPES.has(event.type))
    if (evidence.length === 0) {
      throw new DelegationError(
        'DELEGATION_MODEL_EVIDENCE_MISSING',
        `turn at ${String(windowStart)} contains no model-request evidence`,
        { delegationId: record.delegationId, turnSeq: windowStart, ...(requestId === undefined ? {} : { requestId }) },
      )
    }

    const headers = events.filter(event => event.type === 'request/header')
    const inTurn = headers.filter(event => event.seq >= windowStart && event.seq <= windowEnd)
    const firstInTurn = inTurn[0]?.seq
    const inheritedIsEffective = inTurn.length === 0
      || evidence.some(event => firstInTurn !== undefined && event.seq < firstInTurn)
    const inherited = inheritedIsEffective
      ? headers.filter(event => event.seq < windowStart).at(-1)
      : undefined
    if (inheritedIsEffective && inherited === undefined) {
      throw new DelegationError(
        'DELEGATION_MODEL_EVIDENCE_MISSING',
        `turn at ${String(windowStart)} has no effective request header`,
        { delegationId: record.delegationId, turnSeq: windowStart, ...(requestId === undefined ? {} : { requestId }) },
      )
    }
    const applicable = inherited === undefined ? inTurn : [inherited, ...inTurn]
    const effective = applicable.map(headerConfig)
    if (effective.length === 0 || effective.some(config => config === undefined)) {
      throw new DelegationError(
        'DELEGATION_MODEL_EVIDENCE_MISSING',
        `turn at ${String(windowStart)} has no effective request header`,
        { delegationId: record.delegationId, turnSeq: windowStart, ...(requestId === undefined ? {} : { requestId }) },
      )
    }
    const configured = effective.filter((config): config is ModelSelection => config !== undefined)
    const divergent = configured.find(config => !sameModel(config, record.expectedModel))
    if (divergent !== undefined) {
      throw new DelegationError(
        'DELEGATION_MODEL_MISMATCH',
        `turn at ${String(windowStart)} used ${divergent.provider}/${divergent.model}, not the expected model`,
        {
          delegationId: record.delegationId,
          turnSeq: windowStart,
          expectedProvider: record.expectedModel.provider,
          expectedModel: record.expectedModel.model,
          observedProvider: divergent.provider,
          observedModel: divergent.model,
        },
      )
    }

    const observed = configured.at(-1)
    if (observed === undefined) {
      throw new DelegationError(
        'DELEGATION_MODEL_EVIDENCE_MISSING',
        `turn at ${String(windowStart)} has no effective request header`,
        { delegationId: record.delegationId, turnSeq: windowStart, ...(requestId === undefined ? {} : { requestId }) },
      )
    }
    assertDeadlineLive(command.deadline)
    await this.registry.recordVerifiedModel(record.delegationId, {
      model: observed,
      turnSeq: windowStart,
    })
    return {
      delegationId: record.delegationId,
      sessionId: record.sessionId,
      ...(requestId === undefined ? {} : { requestId }),
      turnSeq: windowStart,
      expectedModel: record.expectedModel,
      observedModel: observed,
      verified: true,
    }
  }

  /**
   * Read one delegation by id or session id.
   * @param lookup - exactly one delegation identity or Session identity.
   * @returns the durable delegation record.
   */
  async get(lookup: DelegationLookup): Promise<DelegationRecord> {
    if (lookup.delegationId !== undefined && lookup.sessionId === undefined) {
      return await this.registry.get(lookup.delegationId)
    }
    if (lookup.sessionId !== undefined && lookup.delegationId === undefined) {
      const record = (await this.registry.list()).find(item => item.sessionId === lookup.sessionId)
      if (record !== undefined) return record
      throw new DelegationRegistryError(
        'DELEGATION_NOT_FOUND',
        `session ${lookup.sessionId} is not a delegation`,
        { sessionId: lookup.sessionId },
      )
    }
    throw new DelegationRegistryError(
      'DELEGATION_NOT_FOUND',
      'provide exactly one of delegationId or sessionId',
      {},
    )
  }

  /**
   * List every durable delegation.
   * @returns read-only durable records.
   */
  async list(): Promise<readonly DelegationRecord[]> {
    return await this.registry.list()
  }

  private async assertRecordedWorkspace(record: DelegationRecord, deadline?: Deadline): Promise<void> {
    assertDeadlineLive(deadline)
    const context = await this.host().hostContext(deadline)
    await guardWorkspace({
      cwd: record.cwd,
      stateDir: context.stateDir,
      dshHome: context.dshHome,
      ledgerDir: this.ledgerDir,
    })
  }

  private async findPromptBinding(ipcRequestId: string | undefined): Promise<PromptKeyBinding | undefined> {
    if (ipcRequestId === undefined) return undefined
    for (const record of await this.registry.list()) {
      const binding = record.promptKeys !== undefined && Object.hasOwn(record.promptKeys, ipcRequestId)
        ? record.promptKeys[ipcRequestId]
        : undefined
      if (binding !== undefined) return binding
    }
    return undefined
  }

  private assertPromptBinding(
    binding: PromptKeyBinding,
    ipcRequestId: string,
    delegationId: string,
    requestId: string,
    contentHash: string,
  ): void {
    if (binding.delegationId === delegationId
      && binding.requestId === requestId
      && binding.contentHash === contentHash) return
    throw new DelegationRegistryError(
      'DELEGATION_PROMPT_CONFLICT',
      `prompt key ${ipcRequestId} is already bound to a different delegation or request`,
      {
        ipcRequestId,
        requestedDelegationId: delegationId,
        boundDelegationId: binding.delegationId,
        requestedRequestId: requestId,
        boundRequestId: binding.requestId,
        contentMatches: binding.contentHash === contentHash,
      },
    )
  }

  private async ownWriter(deadline?: Deadline): Promise<void> {
    assertDeadlineLive(deadline)
    const waitMs = deadline === undefined ? undefined : Math.max(0, deadline.remaining())
    try {
      await this.registry.ensureWriter(waitMs === undefined ? {} : { waitMs })
    } catch (error) {
      if (deadline?.expired() === true
        && error instanceof DelegationRegistryError
        && error.code === 'DELEGATION_LEDGER_READONLY') {
        throw new LiaisonError('IPC_TIMEOUT', 'the request deadline expired while waiting for the delegation ledger')
      }
      throw error
    }
  }

  private host(): HostOperatorAccess {
    return typeof this.access === 'function' ? this.access() : this.access
  }

  private async lookupCreate(
    delegationId: string,
    explicitDelegationId: string | undefined,
    fingerprint: string,
    ipcRequestId: string | undefined,
  ): Promise<DelegationRecord | undefined> {
    const records = await this.registry.list()
    const byId = records.find(record => record.delegationId === delegationId)
    const byIpc = ipcRequestId === undefined
      ? undefined
      : records.find(record => record.createIpcId === ipcRequestId)
    if (explicitDelegationId !== undefined
      && byIpc !== undefined
      && byIpc.delegationId !== explicitDelegationId) {
      throw new DelegationRegistryError(
        'DELEGATION_REPLAY_CONFLICT',
        `replay key ${ipcRequestId ?? ''} is already bound to ${byIpc.delegationId}`,
        {
          requestedDelegationId: explicitDelegationId,
          boundDelegationId: byIpc.delegationId,
          ipcRequestId: ipcRequestId ?? '',
        },
      )
    }
    for (const record of [byId, byIpc]) {
      if (record === undefined) continue
      if (record.createFingerprint !== fingerprint) {
        throw new DelegationRegistryError(
          'DELEGATION_REPLAY_CONFLICT',
          `delegation ${record.delegationId} was created with different inputs`,
          { delegationId: record.delegationId, state: record.state },
        )
      }
      if (record.state === 'ready') return record
    }
    return byId ?? byIpc
  }

  private async validateModel(expected: ModelTarget): Promise<void> {
    const catalog = await this.callRemote(
      remote => remote.session.modelCatalog(),
      'DELEGATION_MODEL_UNAVAILABLE',
    )
    const route = catalog.groups
      .find(group => group.id === expected.provider)
      ?.models.find(model => model.id === expected.model)
    if (route === undefined) {
      throw new DelegationError(
        'DELEGATION_MODEL_UNAVAILABLE',
        `Host does not advertise ${expected.provider}/${expected.model}`,
        { provider: expected.provider, model: expected.model },
      )
    }
    if (expected.reasoningEffort !== undefined
      && !route.reasoning?.efforts.some(effort => effort.id === expected.reasoningEffort)) {
      throw new DelegationError(
        'DELEGATION_MODEL_UNAVAILABLE',
        `Host does not advertise ${expected.reasoningEffort} for ${expected.provider}/${expected.model}`,
        {
          provider: expected.provider,
          model: expected.model,
          reasoningEffort: expected.reasoningEffort,
        },
      )
    }
  }

  private async validateLiveTitle(title: string, cwd: string, ignoreSessionId: string | undefined): Promise<void> {
    const sessions = await this.callRemote(
      (remote, signal) => remote.session.list({}, signal),
      'DELEGATION_SESSION_CONFLICT',
    )
    const collision = sessions.items.find((summary) => {
      if (String(summary.sessionId) === ignoreSessionId) return false
      if (summary.cwd === undefined || resolve(summary.cwd) !== cwd) return false
      return summary.projections?.values.title === title
    })
    if (collision !== undefined) {
      throw new DelegationError(
        'DELEGATION_TITLE_CONFLICT',
        `title ${title} already belongs to live session ${String(collision.sessionId)} in ${cwd}`,
        {
          title,
          cwd,
          sessionId: String(collision.sessionId),
        },
      )
    }
  }

  private async handleCreateFailure(record: DelegationRecord, failure: RemoteFailure): Promise<DelegationError> {
    if (failure.code === 'session/workspace-attach-failed') {
      const attached = detailText(failure, 'sessionId')
      if (attached !== record.sessionId) {
        await this.registry.transition(record.delegationId, { state: 'conflict' })
        return new DelegationError(
          'DELEGATION_SESSION_SUBSTITUTED',
          `workspace attach failed for ${String(attached)}, not ${record.sessionId}`,
          {
            delegationId: record.delegationId,
            requestedSessionId: record.sessionId,
            returnedSessionId: attached ?? null,
          },
        )
      }
      await this.registry.transition(record.delegationId, { state: 'attach-failed' })
      return new DelegationError(
        'DELEGATION_WORKSPACE_ATTACH_FAILED',
        `session ${record.sessionId} could not attach to its workspace`,
        { delegationId: record.delegationId, sessionId: record.sessionId },
      )
    }
    if (failure.code === 'session/conflict' || failure.code === 'session/agent-busy') {
      await this.registry.transition(record.delegationId, { state: 'conflict' })
      return this.delegationError(failure, 'DELEGATION_SESSION_CONFLICT')
    }
    return this.delegationError(failure, 'DELEGATION_SESSION_CONFLICT')
  }

  private async resolvePendingPrompt(
    record: DelegationRecord,
    requestId: string,
    content: string,
    receipt: PromptReceipt,
    deadline?: Deadline,
  ): Promise<PromptCommandResult> {
    let state: SessionState
    try {
      const observed = await this.host().withOperator(
        (remote, signal) => remote.session.state({
          sessionId: brandString<SessionId>(record.sessionId),
        }, signal),
        deadline ?? this.timeoutMs,
      )
      if (!observed.ok) {
        if (observed.error.code === 'session/not-found') {
          throw new DelegationError(
            'DELEGATION_REQUEST_ID_EXPIRED',
            `session ${record.sessionId} is not found or has expired`,
            { delegationId: record.delegationId, requestId, sessionId: record.sessionId },
          )
        }
        throw new DelegationError(
          'DELEGATION_REQUEST_ID_UNCERTAIN',
          `cannot verify request ${requestId}: ${observed.error.message}`,
          { delegationId: record.delegationId, requestId },
        )
      }
      state = observed.value
    } catch (error) {
      if (error instanceof DelegationError) throw error
      throw new DelegationError(
        'DELEGATION_REQUEST_ID_UNCERTAIN',
        `cannot verify request ${requestId} after a lost Host response`,
        { delegationId: record.delegationId, requestId },
      )
    }

    if (queueHasRequest(state.queue, requestId)) {
      const accepted = await this.registry.acceptPrompt(record.delegationId, requestId)
      return promptResult(record, accepted, true)
    }

    let events: readonly SessionWireEvent[]
    try {
      events = await this.readEvents(record, state.cursor, deadline)
    } catch {
      throw new DelegationError(
        'DELEGATION_REQUEST_ID_UNCERTAIN',
        `cannot read the Host log for ${requestId}`,
        { delegationId: record.delegationId, requestId },
      )
    }
    const persisted = events.some(event => event.seq > receipt.beforeCursor && requestIdOf(event) === requestId)
    if (persisted) {
      const accepted = await this.registry.acceptPrompt(record.delegationId, requestId)
      return promptResult(record, accepted, true)
    }

    await this.dispatchPrompt(record, requestId, content, deadline)
    const accepted = await this.registry.acceptPrompt(record.delegationId, requestId)
    return promptResult(record, accepted, true)
  }

  private async dispatchPrompt(
    record: DelegationRecord,
    requestId: string,
    content: string,
    deadline?: Deadline,
  ): Promise<void> {
    assertDeadlineLive(deadline)
    const result = await this.host().withOperator(
      (remote, signal) => remote.session.prompt({
        requestId: brandString<SessionRequestId>(requestId),
        sessionId: brandString<SessionId>(record.sessionId),
        mode: 'queue',
        content: [{ type: 'text', text: content }],
      }, signal),
      deadline ?? this.timeoutMs,
    )
    if (!result.ok) throw this.delegationError(result.error, 'DELEGATION_SESSION_CONFLICT')
  }

  private async readEvents(
    record: DelegationRecord,
    throughSeq: number,
    deadline?: Deadline,
  ): Promise<readonly SessionWireEvent[]> {
    const events = new Map<number, SessionWireEvent>()
    const through = throughSeq
    let before: number | undefined
    for (let page = 0; page < MAX_EVENT_PAGES; page += 1) {
      const value = await this.callRemote(
        (remote, signal) => remote.session.events({
          address: { kind: 'session', sessionId: brandString<SessionId>(record.sessionId) },
          throughSeq: through,
          ...(before === undefined ? {} : { beforeSeq: before }),
          maxMessages: EVENTS_PAGE_MESSAGES,
        }, signal),
        'DELEGATION_SESSION_CONFLICT',
        deadline,
      )
      for (const entry of value.records) events.set(entry.event.seq, entry.event)
      if (!value.hasMore) break
      const earliest = earliestSeq(value.records)
      if (earliest === undefined || earliest <= 0) break
      before = earliest
    }
    return [...events.values()].sort((left, right) => left.seq - right.seq)
  }

  private async callRemote<T>(
    operation: (remote: SessionCommandsRemote, signal: AbortSignal) => Promise<RemoteResult<T>>,
    fallback: DelegationErrorCode,
    deadline?: Deadline,
  ): Promise<T> {
    const result = await this.host().withOperator(operation, deadline ?? this.timeoutMs)
    if (!result.ok) throw this.delegationError(result.error, fallback)
    return result.value
  }

  private delegationError(failure: RemoteFailure, fallback: DelegationErrorCode): DelegationError {
    const details: Record<string, string | number | boolean | null> = {
      remoteCode: failure.code,
    }
    const source = objectOf(failure.details) ?? {}
    for (const [key, value] of Object.entries(source)) {
      if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean' || value === null) {
        details[key] = value
      }
    }
    return new DelegationError(fallback, failure.message, details)
  }

  private async flight<T>(keys: readonly string[], operation: () => Promise<T>): Promise<T> {
    for (const key of keys) {
      const running = this.flights.get(key)
      if (running !== undefined) return await running as T
    }
    const run = operation()
    for (const key of keys) this.flights.set(key, run)
    try {
      return await run
    } finally {
      for (const key of keys) {
        if (this.flights.get(key) === run) this.flights.delete(key)
      }
    }
  }
}

function assertDeadlineLive(deadline?: Deadline): void {
  if (deadline?.expired() === true) {
    throw new LiaisonError('IPC_TIMEOUT', 'the request deadline expired before it could run')
  }
}

function promptResult(record: DelegationRecord, receipt: PromptReceipt, replayed: boolean): PromptCommandResult {
  return {
    accepted: true,
    completed: false,
    replayed,
    delegationId: record.delegationId,
    sessionId: record.sessionId,
    requestId: receipt.requestId,
    beforeCursor: receipt.beforeCursor,
  }
}

function createFingerprint(command: CreateDelegationCommand): string {
  return hash(JSON.stringify({
    title: command.title,
    cwd: resolve(command.cwd),
    provider: command.provider,
    model: command.model,
    reasoningEffort: command.reasoningEffort ?? null,
    agentPreset: command.agentPreset ?? null,
  }))
}

function modelTarget(model: ModelSelection): ModelTarget {
  return model.reasoningEffort === undefined
    ? { provider: model.provider, model: model.model }
    : { provider: model.provider, model: model.model, reasoningEffort: model.reasoningEffort }
}

function sameModel(left: ModelSelection, right: ModelTarget): boolean {
  if (left.provider !== right.provider || left.model !== right.model) return false
  if (right.reasoningEffort === undefined) return true
  return left.reasoningEffort === right.reasoningEffort
}

function locateTarget(events: readonly SessionWireEvent[], requestId: string | undefined): SessionWireEvent | undefined {
  const users = events.filter(event => event.type === 'user/message')
    .filter(event => requestIdOf(event) !== undefined || requestId === undefined)
  if (requestId !== undefined) return users.find(event => requestIdOf(event) === requestId)
  return users.at(-1)
}

function requestIdOf(event: SessionWireEvent): string | undefined {
  if (event.type !== 'user/message') return undefined
  const source = objectOf(objectOf(event.data)?.['source'])
  const rpcId = source?.['rpcId']
  return typeof rpcId === 'string' ? rpcId : undefined
}

function headerConfig(event: SessionWireEvent): ModelSelection | undefined {
  if (event.type !== 'request/header') return undefined
  const header = objectOf(objectOf(event.data)?.['header'])
  const config = objectOf(header?.['config'])
  const provider = config?.['provider']
  const model = config?.['model']
  if (typeof provider !== 'string' || typeof model !== 'string') return undefined
  const reasoningEffort = config?.['reasoningEffort']
  return typeof reasoningEffort === 'string'
    ? { provider, model, reasoningEffort }
    : { provider, model }
}

function queueHasRequest(queue: readonly SessionQueuedItem[] | null, requestId: string): boolean {
  return queue?.some(item => item.rpcId !== undefined && String(item.rpcId) === requestId) === true
}

function earliestSeq(records: readonly SessionEventEntry[]): number | undefined {
  return records.reduce<number | undefined>((earliest, entry) =>
    earliest === undefined || entry.event.seq < earliest ? entry.event.seq : earliest, undefined)
}

function detailText(failure: RemoteFailure, field: string): string | undefined {
  const details = objectOf(failure.details) ?? {}
  const value = details[field]
  return typeof value === 'string' ? value : undefined
}

function objectOf(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

function digest(value: string): string {
  return hash(value).slice(0, 32)
}

function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex')
}

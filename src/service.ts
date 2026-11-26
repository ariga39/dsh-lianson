/** Delegation, prompt, and model-verification business rules. */

import { createHash, randomUUID } from 'node:crypto'
import { normalizeSessionTitle } from '@deepseek-ai/dsh-session-title'
import { LiaisonError } from './errors.ts'
import type { DelegationRecord, Ledger, ModelVerification } from './ledger.ts'
import type { DshRemote, RemoteSource } from './remote.ts'
import { assertTrustedCwd } from './trust.ts'
import type { TrustPolicy } from './trust.ts'

/** Harness-wide accepted title byte budget (`session-title.maxTitleBytes`). */
export const TITLE_MAX_BYTES = 80

/** One replay-aware delegation creation result. */
export interface CreateResult {
  readonly delegation: DelegationRecord
  readonly replayed: boolean
}

/** One replay-aware prompt receipt result. */
export interface PromptResult {
  readonly delegationId: string
  readonly requestId: string
  readonly beforeCursor: number
  readonly accepted: true
}

/** One truthful model verification report. */
export interface VerifyModelResult {
  readonly delegationId: string
  readonly requestId: string | null
  readonly verification: ModelVerification
  readonly expected: { readonly provider: string; readonly model: string } | null
  readonly observed: { readonly provider: string; readonly model: string } | null
  readonly evidence: { readonly userSeq?: number; readonly headerSeq?: number } | null
}

/** Clock and identity seams kept explicit so unit tests stay deterministic. */
export interface ServiceOptions {
  readonly ledger: Ledger
  readonly remote: RemoteSource
  readonly trust: TrustPolicy
  readonly now?: () => number
  readonly newId?: () => string
}

/** The command behavior behind every HTTP route. */
export class DelegationService {
  private readonly ledger: Ledger
  private readonly remote: RemoteSource
  private readonly trust: TrustPolicy
  private readonly now: () => number
  private readonly newId: () => string

  constructor(options: ServiceOptions) {
    this.ledger = options.ledger
    this.remote = options.remote
    this.trust = options.trust
    this.now = options.now ?? Date.now
    this.newId = options.newId ?? randomUUID
  }

  /** @returns stored delegation metadata for `get`. */
  get(delegationId: string): DelegationRecord {
    const record = this.ledger.findDelegation(delegationId)
    if (record === undefined) throw new LiaisonError('not-found', `delegation ${delegationId} not found`, { delegationId })
    return record
  }

  /** @returns every stored delegation in creation order. */
  list(): readonly DelegationRecord[] {
    return this.ledger.listDelegations()
  }

  /**
   * Create a delegation, persisting its minted Session id before any remote
   * `workspace.create`/`session.create` call. Replaying the same
   * `delegationId` reuses that Session when parameters match.
   * @param input - validated create request.
   * @returns the ready record and whether this call replayed an existing row.
   */
  async create(input: {
    readonly delegationId: string
    readonly cwd: string
    readonly title?: string | undefined
    readonly model?: string | undefined
  }): Promise<CreateResult> {
    const cwd = assertTrustedCwd(input.cwd, this.trust)
    const existing = this.ledger.findDelegation(input.delegationId)
    if (existing !== undefined) {
      const title = input.title === undefined ? existing.title : this.normalizeTitle(input.title)
      const model = input.model === undefined ? existing.model : canonicalModel(input.model)
      const mismatched = mismatches(existing, { cwd, title, model })
      if (mismatched.length > 0) {
        throw new LiaisonError('conflict', `delegation ${input.delegationId} already exists with different parameters`, {
          delegationId: input.delegationId,
          mismatched,
        })
      }
      // A ready replay is a ledger read: it stays available while dsh is offline.
      if (existing.status === 'ready') return { delegation: existing, replayed: true }
      return { delegation: await this.completeCreate(this.remote.requireRemote(), existing), replayed: true }
    }

    const remote = this.remote.requireRemote()
    const title = this.normalizeTitle(input.title ?? input.delegationId)
    const catalog = await remote.modelCatalog()
    const selection = resolveModel(input.model, catalog)
    const model = `${selection.provider}/${selection.model}`
    const timestamp = this.now()
    const record: DelegationRecord = {
      delegationId: input.delegationId,
      sessionId: `session-${this.newId()}`,
      cwd,
      title,
      model,
      modelVerification: 'indeterminate',
      status: 'creating',
      createdAt: timestamp,
      updatedAt: timestamp,
    }
    try {
      this.ledger.insertDelegation(record)
    } catch (error) {
      // A concurrent replay may have inserted the same business key first.
      const raced = this.ledger.findDelegation(input.delegationId)
      if (raced === undefined || !(error instanceof LiaisonError) || error.code !== 'conflict') throw error
      const mismatched = mismatches(raced, { cwd, title, model })
      if (mismatched.length > 0) throw error
      if (raced.status === 'ready') return { delegation: raced, replayed: true }
      return { delegation: await this.completeCreate(remote, raced), replayed: true }
    }
    return { delegation: await this.completeCreate(remote, record), replayed: false }
  }

  /**
   * Persist a prompt receipt before the remote prompt call. A retry with the
   * same content reuses the original cursor and relies on Host-native
   * `requestId` deduplication; different content is a conflict.
   * @param input - validated prompt request.
   * @returns the durable pre-prompt cursor and acceptance receipt.
   */
  async prompt(input: {
    readonly delegationId: string
    readonly requestId?: string | undefined
    readonly text: string
  }): Promise<PromptResult> {
    const delegation = this.get(input.delegationId)
    if (delegation.status !== 'ready') {
      throw new LiaisonError('conflict', `delegation ${input.delegationId} is not ready`, { delegationId: input.delegationId })
    }
    if (input.text.trim().length === 0) {
      throw new LiaisonError('usage', 'prompt text must include non-whitespace content')
    }
    const requestId = input.requestId ?? `liaison-${this.newId()}`
    const promptHash = hashPrompt(input.text)
    const remote = this.remote.requireRemote()

    let receipt = this.ledger.findReceipt(input.delegationId, requestId)
    if (receipt !== undefined && receipt.promptHash !== promptHash) {
      throw new LiaisonError('conflict', `request ${requestId} was already used with different content`, {
        delegationId: input.delegationId,
        requestId,
      })
    }
    if (receipt === undefined) {
      const beforeCursor = await remote.beforeCursor(delegation.sessionId)
      const candidate = {
        delegationId: input.delegationId,
        requestId,
        beforeCursor,
        promptHash,
        createdAt: this.now(),
      }
      try {
        this.ledger.insertReceipt(candidate)
        receipt = candidate
      } catch (error) {
        if (!(error instanceof LiaisonError) || error.code !== 'conflict') throw error
        const raced = this.ledger.findReceipt(input.delegationId, requestId)
        if (raced === undefined) throw error
        if (raced.promptHash !== promptHash) {
          throw new LiaisonError('conflict', `request ${requestId} was already used with different content`, {
            delegationId: input.delegationId,
            requestId,
          })
        }
        receipt = raced
      }
    }

    await remote.prompt(delegation.sessionId, requestId, input.text)
    this.ledger.touchDelegation(input.delegationId, this.now())
    return {
      delegationId: input.delegationId,
      requestId,
      beforeCursor: receipt.beforeCursor,
      accepted: true,
    }
  }

  /**
   * Inspect the persisted `request/header` for the requested turn (or the
   * latest turn when no request id is supplied) and compare its model route.
   * @param input - delegation and optional exact prompt request id.
   * @returns a truthful `proven`, `mismatch`, or `indeterminate` verdict.
   */
  async verifyModel(input: { readonly delegationId: string; readonly requestId?: string | undefined }): Promise<VerifyModelResult> {
    const delegation = this.get(input.delegationId)
    const remote = this.remote.requireRemote()
    const page = await remote.readEvents(delegation.sessionId)
    const events = [...page.events].sort((left, right) => left.seq - right.seq)
    const expected = parseModel(delegation.model)

    const user = input.requestId === undefined
      ? undefined
      : events.findLast(event => event.type === 'user/message' && requestIdOf(event.data) === input.requestId)
    if (input.requestId !== undefined && user === undefined) {
      return this.finishVerification(delegation, input.requestId ?? null, 'indeterminate', expected, null, null)
    }
    const header = user === undefined
      ? events.findLast(event => event.type === 'request/header')
      : events.find(event => event.type === 'request/header' && event.seq > user.seq)
    const observed = header === undefined ? null : modelOfHeader(header.data)
    if (header === undefined || observed === null || expected === null) {
      return this.finishVerification(
        delegation,
        input.requestId ?? null,
        'indeterminate',
        expected,
        observed,
        header === undefined ? null : { ...(user === undefined ? {} : { userSeq: user.seq }), headerSeq: header.seq },
      )
    }
    const verification: ModelVerification = observed.provider === expected.provider && observed.model === expected.model
      ? 'proven'
      : 'mismatch'
    return this.finishVerification(delegation, input.requestId ?? null, verification, expected, observed, {
      ...(user === undefined ? {} : { userSeq: user.seq }),
      headerSeq: header.seq,
    })
  }

  private finishVerification(
    delegation: DelegationRecord,
    requestId: string | null,
    verification: ModelVerification,
    expected: { provider: string; model: string } | null,
    observed: { provider: string; model: string } | null,
    evidence: { userSeq?: number; headerSeq?: number } | null,
  ): VerifyModelResult {
    this.ledger.setModelVerification(delegation.delegationId, verification, this.now())
    return { delegationId: delegation.delegationId, requestId, verification, expected, observed, evidence }
  }

  private async completeCreate(remote: DshRemote, record: DelegationRecord): Promise<DelegationRecord> {
    const workspaceId = await remote.createWorkspace(record.cwd)
    const returnedId = await remote.createSession({ workspaceId, sessionId: record.sessionId })
    if (returnedId !== record.sessionId) {
      throw new LiaisonError('conflict', 'Host returned a different Session id than the one persisted', {
        delegationId: record.delegationId,
        expected: record.sessionId,
        returned: returnedId,
      })
    }
    const selection = parseModel(record.model)
    if (selection === null) throw new LiaisonError('internal', `stored model route is invalid: ${record.model}`)
    await remote.renameSession(record.sessionId, record.title)
    await remote.selectModel(record.sessionId, selection)
    const updatedAt = this.now()
    this.ledger.markReady(record.delegationId, updatedAt)
    return { ...record, status: 'ready', updatedAt }
  }

  private normalizeTitle(input: string): string {
    const title = normalizeSessionTitle(input, TITLE_MAX_BYTES)
    if (title.length === 0) throw new LiaisonError('usage', 'title is empty after normalization')
    return title
  }
}

/**
 * Resolve a requested `provider/model` route against the Host catalog; an
 * omitted route uses the catalog default.
 * @param requested - caller route, or undefined.
 * @param catalog - Host model catalog.
 * @returns the available route.
 */
export function resolveModel(
  requested: string | undefined,
  catalog: { readonly default: { readonly provider: string; readonly model: string }; readonly groups: readonly { readonly id: string; readonly models: readonly { readonly id: string }[] }[] },
): { provider: string; model: string } {
  const selection = requested === undefined ? { provider: catalog.default.provider, model: catalog.default.model } : parseModel(requested)
  if (selection === null) throw new LiaisonError('usage', 'model must be written as provider/model', { model: requested })
  if (!catalog.groups.some(group => group.id === selection.provider && group.models.some(model => model.id === selection.model))) {
    throw new LiaisonError('model-unavailable', `model ${selection.provider}/${selection.model} is not in the Host catalog`, {
      provider: selection.provider,
      model: selection.model,
    })
  }
  return selection
}

/** Canonicalize a caller-supplied model route or fail as usage. */
function canonicalModel(value: string): string {
  const parsed = parseModel(value)
  if (parsed === null) throw new LiaisonError('usage', 'model must be written as provider/model', { model: value })
  return `${parsed.provider}/${parsed.model}`
}

/** Split `provider/model` at the first slash. */
export function parseModel(value: string): { provider: string; model: string } | null {
  const slash = value.indexOf('/')
  if (slash <= 0 || slash === value.length - 1) return null
  return { provider: value.slice(0, slash), model: value.slice(slash + 1) }
}

function hashPrompt(text: string): string {
  return createHash('sha256').update(text, 'utf8').digest('hex')
}

function mismatches(
  existing: DelegationRecord,
  candidate: { readonly cwd: string; readonly title: string; readonly model: string },
): string[] {
  const fields: string[] = []
  if (existing.cwd !== candidate.cwd) fields.push('cwd')
  if (existing.title !== candidate.title) fields.push('title')
  if (existing.model !== candidate.model) fields.push('model')
  return fields
}

function requestIdOf(data: unknown): string | undefined {
  const source = recordField(data, 'source')
  return stringField(recordField(source, 'rpcId'))
}

function modelOfHeader(data: unknown): { provider: string; model: string } | null {
  const header = recordField(data, 'header')
  const config = recordField(header, 'config')
  const provider = stringField(recordField(config, 'provider'))
  const model = stringField(recordField(config, 'model'))
  return provider === undefined || model === undefined ? null : { provider, model }
}

function recordField(value: unknown, key: string): unknown {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined
  return Object.getOwnPropertyDescriptor(value, key)?.value
}

function stringField(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

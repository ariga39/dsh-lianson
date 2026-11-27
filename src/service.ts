/** Delegation, prompt, and model-verification business rules. */

import { createHash, randomUUID } from 'node:crypto'
import type {
  SessionHistoryRecord, SessionWireEvent, SessionWaitValue,
} from '@deepseek-ai/dsh-api-session-controller/types'
import type { AskUserQuestionAnswerItem, AskUserQuestionItem } from '@deepseek-ai/dsh-user-questions/types'
import { normalizeSessionTitle } from '@deepseek-ai/dsh-session-title'
import { DEFAULT_STOP_TIMEOUT_MS, DEFAULT_WAIT_TIMEOUT_MS } from './budgets.ts'
import { LiaisonError } from './errors.ts'
import { EventReader } from './events.ts'
import type { EventRange } from './events.ts'
import { InteractionBroker } from './interactions.ts'
import type {
  ContinuedQuestionView, InteractionAnswerInput, InteractionView,
} from './interactions.ts'
import type { DelegationRecord, Ledger, ModelVerification } from './ledger.ts'
import type {
  DshRemote, RemoteProjections, RemoteQueueAction, RemoteSessionState, RemoteSource,
} from './remote.ts'
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

/** Persisted events supporting one verification verdict. */
export interface VerifyEvidence {
  readonly turn?: number
  readonly turnStartSeq?: number
  readonly turnEndSeq?: number
  readonly userSeq?: number
  readonly headerSeq?: number
  /** `turn` means the header was logged inside the target turn; `carried` means it predates it. */
  readonly headerSource?: 'turn' | 'carried'
  /** Assistant settlement proving the carried header's turn actually dispatched a request. */
  readonly executionSeq?: number
  /** Whether the selected header belongs to a fork-inherited prefix. */
  readonly inherited?: boolean
}

/** One truthful model verification report. */
export interface VerifyModelResult {
  readonly delegationId: string
  readonly requestId: string | null
  readonly verification: ModelVerification
  readonly expected: { readonly provider: string; readonly model: string } | null
  readonly observed: { readonly provider: string; readonly model: string } | null
  readonly evidence: VerifyEvidence | null
}

/** Caller-visible wait state. */
export type WaitStatus = 'completed' | 'input-required' | 'running' | 'queued' | 'timeout' | 'indeterminate'

/** One truthful segmented wait report. */
export interface WaitResult {
  readonly delegationId: string
  readonly sessionId: string
  readonly afterSeq: number
  readonly cursor: number
  readonly reason: 'idle' | 'input-required' | 'timeout' | 'indeterminate'
  readonly status: WaitStatus
  readonly state: RemoteSessionState | null
  readonly slices: number
  readonly output: { readonly records: readonly SessionHistoryRecord[]; readonly truncated: boolean }
  readonly subagentCatalog: readonly unknown[] | null
  readonly indeterminate: boolean
  readonly indeterminateReason: string | null
}

/** One contiguous event read plus the Session's direct subagent catalog. */
export interface EventsResult extends EventRange {
  readonly delegationId: string
  readonly throughSeq: number
  readonly subagentCatalog: readonly unknown[] | null
}

/** One accepted prompt delivered in steering mode. */
export interface SteerResult {
  readonly delegationId: string
  readonly sessionId: string
  readonly requestId: string
  readonly mode: 'steer'
  readonly accepted: true
}

/** One accepted pending-queue mutation. */
export interface QueueResult {
  readonly delegationId: string
  readonly sessionId: string
  readonly itemId: string
  readonly action: RemoteQueueAction['kind']
  readonly accepted: true
}

/** One time-bounded stop outcome. */
export interface StopResult {
  readonly delegationId: string
  readonly sessionId: string
  readonly outcome: 'stopped' | 'running' | 'unknown'
  readonly stopped: boolean
  readonly quiescent: boolean | null
  readonly budgetExpired: boolean
  readonly clearedQueue: number
  readonly cancelled: boolean
  readonly state: RemoteSessionState | null
  /** Queue item ids still known or believed pending when the budget expired. */
  readonly residualQueue: readonly string[]
  readonly errors: readonly string[]
}

/** One rework prompt submitted to the original Session. */
export interface ReworkResult extends PromptResult {
  readonly sessionId: string
  readonly unarchived: true
}

/** Pending and continued interactions visible to the CLI. */
export interface InteractionListResult {
  readonly interactions: readonly (InteractionView | ContinuedQuestionView)[]
}

/** One open or closed turn window scoped by its durable turn number. */
interface TurnWindow {
  readonly turn: number
  readonly startSeq: number
  readonly endSeq?: number
  readonly inherited: boolean
}

/** One selected request header and the execution proof required for carried headers. */
interface HeaderChoice {
  readonly header: SessionWireEvent
  readonly source: 'turn' | 'carried'
  readonly execution?: SessionWireEvent
}

/** Clock and identity seams kept explicit so unit tests stay deterministic. */
export interface ServiceOptions {
  readonly ledger: Ledger
  readonly remote: RemoteSource
  readonly trust: TrustPolicy
  readonly now?: () => number
  readonly newId?: () => string
  /** Monotonic clock for waits and stop budgets; defaults to `Date.now`. */
  readonly clock?: () => number
  /** Abortable or test-injected sleep used between subagent/stop polls. */
  readonly sleep?: (ms: number) => Promise<void>
  /** Pending interaction buffer shared with the loopback event bridge. */
  readonly interactions?: InteractionBroker
  /** Delay between subagent and stop polls. */
  readonly pollIntervalMs?: number
}

/** The command behavior behind every HTTP route. */
export class DelegationService {
  private readonly ledger: Ledger
  private readonly remote: RemoteSource
  private readonly trust: TrustPolicy
  private readonly now: () => number
  private readonly newId: () => string
  private readonly clock: () => number
  private readonly sleep: (ms: number) => Promise<void>
  private readonly interactions: InteractionBroker
  private readonly pollIntervalMs: number
  /** One serialized create chain per delegation id; unrelated ids stay independent. */
  private readonly createChains = new Map<string, Promise<void>>()

  constructor(options: ServiceOptions) {
    this.ledger = options.ledger
    this.remote = options.remote
    this.trust = options.trust
    this.now = options.now ?? Date.now
    this.newId = options.newId ?? randomUUID
    this.clock = options.clock ?? Date.now
    this.sleep = options.sleep ?? (ms => new Promise(resolve => setTimeout(resolve, ms)))
    this.pollIntervalMs = options.pollIntervalMs ?? 50
    this.interactions = options.interactions ?? new InteractionBroker({
      bindingOfSession: sessionId => this.ledger.findDelegationBySession(sessionId)?.delegationId,
      now: this.now,
      newId: this.newId,
    })
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
  create(input: {
    readonly delegationId: string
    readonly cwd: string
    readonly title?: string | undefined
    readonly model?: string | undefined
  }): Promise<CreateResult> {
    const previous = this.createChains.get(input.delegationId) ?? Promise.resolve()
    const run = previous.then(
      () => this.createOnce(input),
      () => this.createOnce(input),
    )
    const tail = run.then(() => undefined, () => undefined)
    this.createChains.set(input.delegationId, tail)
    void tail.then(() => {
      if (this.createChains.get(input.delegationId) === tail) this.createChains.delete(input.delegationId)
    })
    return run
  }

  private async createOnce(input: {
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
      return { delegation: await this.completeCreate(await this.remote.requireRemote(), existing), replayed: true }
    }

    const remote = await this.remote.requireRemote()
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
    const remote = await this.remote.requireRemote()

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
   * Wait across Host windows until the bound Session needs input or has
   * completed its turn, joining directly-attributable background subagents.
   * @param input - delegation, output cursor, and wait bounds.
   * @returns the stopping reason, last state, output, and truthful subagent status.
   */
  async wait(input: {
    readonly delegationId: string
    readonly afterSeq?: number | undefined
    readonly timeoutMs?: number | undefined
    readonly sliceTimeoutMs?: number | undefined
    readonly maxMessages?: number | undefined
  }): Promise<WaitResult> {
    const delegation = this.readyDelegation(input.delegationId)
    const afterSeq = input.afterSeq ?? -1
    const timeoutMs = input.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS
    const sliceTimeoutMs = Math.min(input.sliceTimeoutMs ?? 30_000, 30_000)
    const maxMessages = input.maxMessages ?? 500
    assertWaitBounds(afterSeq, timeoutMs, sliceTimeoutMs, maxMessages)

    const deadline = this.clock() + timeoutMs
    const handshake = AbortSignal.timeout(timeoutMs)
    let remote: DshRemote
    try {
      remote = await withinBudget(this.remote.requireRemote(), handshake)
    } catch (error) {
      if (handshake.aborted || this.clock() >= deadline) {
        return waitResult(
          delegation, afterSeq, afterSeq, 'timeout', 'timeout', null, 0,
          new Map<number, SessionHistoryRecord>(), false, null, null,
        )
      }
      throw error
    }
    if (handshake.aborted || this.clock() >= deadline) {
      return waitResult(
        delegation, afterSeq, afterSeq, 'timeout', 'timeout', null, 0,
        new Map<number, SessionHistoryRecord>(), false, null, null,
      )
    }

    let cursor = afterSeq
    let lastState: RemoteSessionState | undefined
    let subagentCatalog: readonly unknown[] | null = null
    let childRunning = false
    let childQueued = false
    let slices = 0
    const collected = new Map<number, SessionHistoryRecord>()
    let truncated = false

    for (;;) {
      const remaining = deadline - this.clock()
      if (remaining <= 0) break
      let value: SessionWaitValue
      try {
        value = await remote.wait(delegation.sessionId, {
          afterSeq: cursor,
          timeoutMs: Math.max(1, Math.min(sliceTimeoutMs, remaining)),
          maxMessages,
        })
      } catch (error) {
        if (!isWaitTimeout(error)) throw error
        if (this.clock() >= deadline) break
        await this.sleep(Math.max(1, Math.min(this.pollIntervalMs, deadline - this.clock())))
        continue
      }
      slices += 1
      for (const record of value.output.records) collected.set(record.event.seq, record)
      truncated ||= value.output.truncated
      lastState = value.state
      cursor = Math.max(cursor, value.state.cursor)

      if (value.reason === 'input-required' || value.state.pendingInput.length > 0) {
        return waitResult(delegation, afterSeq, cursor, 'input-required', 'input-required', lastState, slices, collected, truncated, subagentCatalog, null)
      }
      if (value.state.running) {
        if (value.reason === 'timeout') continue
        // A running Session cannot stop a wait; keep polling within the budget.
        continue
      }
      if ((value.state.queue?.length ?? 0) > 0) {
        if (this.clock() >= deadline) break
        await this.sleep(Math.max(1, Math.min(this.pollIntervalMs, deadline - this.clock())))
        continue
      }
      if (value.reason === 'idle') {
        const inspection = await this.inspectSubagents(remote, delegation.sessionId)
        subagentCatalog = inspection.catalog
        childRunning = inspection.running > 0
        childQueued = inspection.queued > 0
        if (inspection.indeterminateReason !== null) {
          return waitResult(delegation, afterSeq, cursor, 'indeterminate', 'indeterminate', lastState, slices, collected, truncated, subagentCatalog, inspection.indeterminateReason)
        }
        if (childRunning || childQueued) {
          if (this.clock() >= deadline) break
          await this.sleep(Math.max(1, Math.min(this.pollIntervalMs, deadline - this.clock())))
          continue
        }
        return waitResult(delegation, afterSeq, cursor, 'idle', 'completed', lastState, slices, collected, truncated, subagentCatalog, null)
      }
      // A window timeout with neither a running turn nor queued work still
      // leaves the caller's overall deadline in charge. A fixture that answers
      // immediately must not spin: yield briefly before the next window.
      await this.sleep(Math.max(1, Math.min(this.pollIntervalMs, deadline - this.clock())))
    }

    const status = lastState === undefined
      ? 'timeout'
      : lastState.running || childRunning
        ? 'running'
        : (lastState.queue?.length ?? 0) > 0 || childQueued ? 'queued' : 'timeout'
    return waitResult(delegation, afterSeq, cursor, 'timeout', status, lastState ?? null, slices, collected, truncated, subagentCatalog, null)
  }

  /**
   * Read a contiguous event range and the Session's direct subagent catalog.
   * @param input - delegation, range, and page budget.
   * @returns every event in the range plus catalog metadata.
   */
  async events(input: {
    readonly delegationId: string
    readonly afterSeq?: number | undefined
    readonly throughSeq?: number | undefined
    readonly pageSize?: number | undefined
  }): Promise<EventsResult> {
    const delegation = this.readyDelegation(input.delegationId)
    const remote = await this.remote.requireRemote()
    const reader = new EventReader(remote)
    const range = await reader.readRange(delegation.sessionId, {
      ...(input.afterSeq === undefined ? {} : { afterSeq: input.afterSeq }),
      ...(input.throughSeq === undefined ? {} : { throughSeq: input.throughSeq }),
      ...(input.pageSize === undefined ? {} : { pageSize: input.pageSize }),
      requireContiguous: true,
    })
    const catalog = catalogOf(await readProjectionsOrNull(remote, delegation.sessionId))
    return {
      ...range,
      delegationId: delegation.delegationId,
      throughSeq: range.pinnedCursor,
      subagentCatalog: catalog,
    }
  }

  /**
   * Admit one steering prompt into the running turn.
   * @param input - delegation, request identity, and text.
   * @returns the accepted steering receipt.
   */
  async steer(input: {
    readonly delegationId: string
    readonly requestId?: string | undefined
    readonly text: string
  }): Promise<SteerResult> {
    const delegation = this.readyDelegation(input.delegationId)
    if (input.text.trim().length === 0) throw new LiaisonError('usage', 'steer text must include non-whitespace content')
    const remote = await this.remote.requireRemote()
    const requestId = input.requestId ?? `liaison-${this.newId()}`
    await remote.prompt(delegation.sessionId, requestId, input.text, 'steer')
    this.ledger.touchDelegation(delegation.delegationId, this.now())
    return {
      delegationId: delegation.delegationId,
      sessionId: delegation.sessionId,
      requestId,
      mode: 'steer',
      accepted: true,
    }
  }

  /**
   * Mutate one still-pending queue item.
   * @param input - delegation, item id, and action.
   * @returns the accepted mutation.
   */
  async queue(input: {
    readonly delegationId: string
    readonly itemId: string
    readonly action: RemoteQueueAction
  }): Promise<QueueResult> {
    const delegation = this.readyDelegation(input.delegationId)
    if (input.action.kind === 'edit' && input.action.text.trim().length === 0) {
      throw new LiaisonError('usage', 'queue edit text must include non-whitespace content')
    }
    const remote = await this.remote.requireRemote()
    await remote.updateQueue(delegation.sessionId, input.itemId, input.action)
    return {
      delegationId: delegation.delegationId,
      sessionId: delegation.sessionId,
      itemId: input.itemId,
      action: input.action.kind,
      accepted: true,
    }
  }

  /**
   * Clear queued input, cancel the active turn, and poll quiet state inside one
   * overall budget. Budget expiry reports the last known state without claiming
   * the turn stopped.
   * @param input - delegation and optional overall budget.
   * @returns the known or unknown stop outcome.
   */
  async stop(input: { readonly delegationId: string; readonly timeoutMs?: number | undefined }): Promise<StopResult> {
    const delegation = this.readyDelegation(input.delegationId)
    const timeoutMs = input.timeoutMs ?? DEFAULT_STOP_TIMEOUT_MS
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
      throw new LiaisonError('usage', 'timeoutMs must be a positive safe integer', { timeoutMs })
    }
    const deadline = this.clock() + timeoutMs
    const budget = AbortSignal.timeout(timeoutMs)
    const expired = (): boolean => budget.aborted || this.clock() >= deadline
    const lastKnown: { state: RemoteSessionState | null } = { state: null }
    let clearedQueue = 0
    let cancelled = false
    let interrupted = false
    const errors: string[] = []
    let residual = new Set<string>()

    const result = (budgetExpired: boolean): StopResult => {
      const knownQuiet = lastKnown.state !== null && !lastKnown.state.running && residual.size === 0
      const quiescent = interrupted
        ? null
        : lastKnown.state === null ? null : budgetExpired && knownQuiet ? null : knownQuiet
      return {
        delegationId: delegation.delegationId,
        sessionId: delegation.sessionId,
        outcome: quiescent === null ? 'unknown' : quiescent ? 'stopped' : 'running',
        stopped: !budgetExpired && quiescent === true,
        quiescent,
        budgetExpired,
        clearedQueue,
        cancelled,
        state: lastKnown.state,
        residualQueue: [...residual],
        errors,
      }
    }

    let remote: DshRemote
    try {
      remote = await withinBudget(this.remote.requireRemote(), budget)
    } catch (error) {
      if (expired()) return result(true)
      throw error
    }
    if (expired()) return result(true)

    const read = async (): Promise<boolean> => {
      if (expired()) return false
      try {
        const current = await withinBudget(remote.readState(delegation.sessionId), budget)
        lastKnown.state = current
        residual = new Set((current.queue ?? []).map(item => String(item.id)))
        return true
      } catch (error) {
        if (expired()) interrupted = true
        else errors.push(messageOf(error))
        return false
      }
    }

    const drain = async (): Promise<boolean> => {
      if (!await read()) return false
      let mutated = false
      for (const item of lastKnown.state?.queue ?? []) {
        // Every mutation is a new action: never start one after the budget.
        if (expired()) return false
        mutated = true
        try {
          await withinBudget(
            remote.updateQueue(delegation.sessionId, item.id, { kind: 'remove' }),
            budget,
          )
          clearedQueue += 1
          residual.delete(String(item.id))
        } catch (error) {
          if (expired()) {
            interrupted = true
            return false
          }
          if (isNotFound(error)) residual.delete(String(item.id))
          else errors.push(messageOf(error))
        }
      }
      // A removal can race with dispatch: only a read taken after the last
      // mutation may decide that no queued work remains.
      return mutated ? read() : true
    }

    if (!await drain()) return result(true)

    if (expired()) return result(true)
    try {
      await withinBudget(remote.cancel(delegation.sessionId), budget)
      cancelled = true
    } catch (error) {
      // A cold Session has no live Agent to cancel; quiet state still counts.
      if (expired()) interrupted = true
      else if (isNotFound(error)) cancelled = true
      else errors.push(messageOf(error))
    }
    if (expired()) return result(true)

    for (;;) {
      const remaining = deadline - this.clock()
      if (remaining <= 0 || budget.aborted) break
      await this.sleep(Math.max(1, Math.min(this.pollIntervalMs, remaining)))
      if (expired()) break
      if (!await drain()) break
      if (!interrupted && lastKnown.state !== null && !lastKnown.state.running && residual.size === 0) return result(false)
    }

    return result(true)
  }

  /**
   * Restore an archived delegation Session and submit follow-up work to the
   * same durable `sessionId`.
   * @param input - delegation, request identity, and prompt text.
   * @returns the prompt receipt and the original Session identity.
   */
  async rework(input: {
    readonly delegationId: string
    readonly requestId?: string | undefined
    readonly text: string
  }): Promise<ReworkResult> {
    const delegation = this.readyDelegation(input.delegationId)
    const remote = await this.remote.requireRemote()
    await remote.unarchiveSession(delegation.sessionId)
    const result = await this.prompt(input)
    return { ...result, sessionId: delegation.sessionId, unarchived: true }
  }

  /**
   * List buffered interactions and directly-observable continued questions.
   * @param input - optional delegation filter.
   * @returns pending and continued interactions.
   */
  async interactionList(input: { readonly delegationId?: string | undefined }): Promise<InteractionListResult> {
    const pending: (InteractionView | ContinuedQuestionView)[] = [...this.interactions.list(input.delegationId)]
    let remote: DshRemote
    try {
      remote = await this.remote.requireRemote()
    } catch {
      return { interactions: pending }
    }
    const delegations = input.delegationId === undefined
      ? this.ledger.listDelegations().filter(record => record.status === 'ready')
      : [this.readyDelegation(input.delegationId)]
    for (const delegation of delegations) {
      for (const question of await continuedQuestionsOf(remote, delegation.sessionId)) {
        if (pending.some(item => item.kind === 'question' && item.callId === question.callId)) continue
        pending.push({
          id: `continued-${question.callId}`,
          delegationId: delegation.delegationId,
          sessionId: delegation.sessionId,
          kind: 'question',
          state: 'continued',
          createdAt: 0,
          callId: question.callId,
          questions: question.questions,
        })
      }
    }
    return { interactions: pending }
  }

  /**
   * Approve one buffered approval.
   * @param interactionId - pending interaction identity.
   * @returns the settled approval.
   */
  interactionApprove(interactionId: string): { readonly interaction: InteractionView; readonly outcome: 'allowed-once'; readonly accepted: true } {
    const interaction = this.interactions.approve(interactionId)
    return { interaction, outcome: 'allowed-once', accepted: true }
  }

  /**
   * Reject one buffered approval.
   * @param interactionId - pending interaction identity.
   * @returns the settled rejection.
   */
  interactionReject(interactionId: string): { readonly interaction: InteractionView; readonly outcome: 'rejected'; readonly accepted: true } {
    const interaction = this.interactions.reject(interactionId)
    return { interaction, outcome: 'rejected', accepted: true }
  }

  /**
   * Answer a buffered question, falling back to the public `userQuestions.answer`
   * RPC for a timed question that outlived its foreground wait.
   * @param input - pending identity or call id plus the answer batch.
   * @returns the accepted answer route.
   */
  async interactionAnswer(input: InteractionAnswerInput): Promise<{
    readonly interactionId: string | null
    readonly delegationId: string
    readonly sessionId: string
    readonly callId: string | null
    readonly accepted: true
    readonly via: 'pending' | 'continued'
    readonly answers: readonly AskUserQuestionAnswerItem[]
  }> {
    const pending = this.interactions.answer(input)
    if (pending !== undefined) {
      return {
        interactionId: pending.id,
        delegationId: pending.delegationId,
        sessionId: pending.sessionId,
        callId: pending.callId ?? null,
        accepted: true,
        via: 'pending',
        answers: input.answers,
      }
    }
    if (input.callId === undefined || input.delegationId === undefined) {
      throw new LiaisonError('usage', 'a continued question requires --delegation-id and --call-id', {
        callId: input.callId ?? null,
        delegationId: input.delegationId ?? null,
      })
    }
    const delegation = this.readyDelegation(input.delegationId)
    const remote = await this.remote.requireRemote()
    const accepted = await remote.answerUserQuestion(delegation.sessionId, input.callId, { answers: [...input.answers] })
    if (!accepted) {
      throw new LiaisonError('not-found', `question ${input.callId} is no longer answerable`, {
        delegationId: delegation.delegationId,
        callId: input.callId,
      })
    }
    return {
      interactionId: null,
      delegationId: delegation.delegationId,
      sessionId: delegation.sessionId,
      callId: input.callId,
      accepted: true,
      via: 'continued',
      answers: input.answers,
    }
  }

  /**
   * Inspect the persisted `request/header` for the requested turn (or the
   * latest turn when no request id is supplied) and compare its model route.
   * A request-scoped check is historical and leaves the stored delegation
   * summary untouched. Only an unscoped check records its verdict for the
   * latest turn it observes.
   * @param input - delegation and optional exact prompt request id.
   * @returns a truthful `proven`, `mismatch`, or `indeterminate` verdict.
   */
  async verifyModel(input: { readonly delegationId: string; readonly requestId?: string | undefined }): Promise<VerifyModelResult> {
    const delegation = this.get(input.delegationId)
    const remote = await this.remote.requireRemote()
    const range = await new EventReader(remote).readRange(delegation.sessionId, { requireContiguous: false })
    const events = [...range.events].sort((left, right) => left.seq - right.seq)
    const expected = parseModel(delegation.model)
    const inheritedCut = inheritedCutOf(events)

    let user: SessionWireEvent | undefined
    let turn: TurnWindow | undefined
    if (input.requestId !== undefined) {
      user = events.findLast(event => event.type === 'user/message' && requestIdOf(event.data) === input.requestId)
      if (user === undefined) {
        return this.finishVerification(delegation, input.requestId, 'indeterminate', expected, null, null)
      }
      turn = turnContaining(events, user.seq, inheritedCut)
      if (turn === undefined) {
        return this.finishVerification(delegation, input.requestId, 'indeterminate', expected, null, { userSeq: user.seq })
      }
    } else {
      turn = latestTurn(events, inheritedCut)
      if (turn === undefined) {
        return this.finishVerification(delegation, null, 'indeterminate', expected, null, null)
      }
      user = lastUserInTurn(events, turn)
    }

    const anchor = user?.seq ?? turn.startSeq
    const base: VerifyEvidence = {
      turn: turn.turn,
      turnStartSeq: turn.startSeq,
      ...(turn.endSeq === undefined ? {} : { turnEndSeq: turn.endSeq }),
      ...(user === undefined ? {} : { userSeq: user.seq }),
    }
    const choice = headerChoiceFor(events, turn, anchor, user !== undefined)
    if (choice === undefined) {
      return this.finishVerification(delegation, input.requestId ?? null, 'indeterminate', expected, null, base)
    }
    const observed = modelOfHeader(choice.header.data)
    const evidence: VerifyEvidence = {
      ...base,
      headerSeq: choice.header.seq,
      headerSource: choice.source,
      inherited: choice.header.seq < inheritedCut,
      ...(choice.execution === undefined ? {} : { executionSeq: choice.execution.seq }),
    }
    // A persisted header alone is not execution evidence: a request can be
    // cancelled or fail before dispatch. Every source (direct, carried, or
    // inherited) needs an assistant settlement in this target step.
    if (choice.execution === undefined) {
      return this.finishVerification(delegation, input.requestId ?? null, 'indeterminate', expected, observed, evidence)
    }
    if (observed === null || expected === null) {
      return this.finishVerification(delegation, input.requestId ?? null, 'indeterminate', expected, observed, evidence)
    }
    const verification: ModelVerification = observed.provider === expected.provider && observed.model === expected.model
      ? 'proven'
      : 'mismatch'
    return this.finishVerification(delegation, input.requestId ?? null, verification, expected, observed, evidence)
  }

  private finishVerification(
    delegation: DelegationRecord,
    requestId: string | null,
    verification: ModelVerification,
    expected: { provider: string; model: string } | null,
    observed: { provider: string; model: string } | null,
    evidence: VerifyEvidence | null,
  ): VerifyModelResult {
    // `null` identifies the unscoped check of the latest turn. Scoped checks
    // stay historical and must not overwrite what `get`/`list` report.
    if (requestId === null) {
      this.ledger.setModelVerification(delegation.delegationId, verification, this.now())
    }
    return { delegationId: delegation.delegationId, requestId, verification, expected, observed, evidence }
  }

  private readyDelegation(delegationId: string): DelegationRecord {
    const delegation = this.get(delegationId)
    if (delegation.status !== 'ready') {
      throw new LiaisonError('conflict', `delegation ${delegationId} is not ready`, { delegationId })
    }
    return delegation
  }

  /**
   * Attribute idle to the root only when every direct child is observable and
   * finished. External or unknown catalog rows and unreadable local children
   * make the result indeterminate instead of claiming completion.
   */
  private async inspectSubagents(remote: DshRemote, sessionId: string): Promise<{
    readonly catalog: readonly unknown[] | null
    readonly running: number
    readonly queued: number
    readonly indeterminateReason: string | null
  }> {
    let value: RemoteProjections
    try {
      value = await remote.readProjections(sessionId)
    } catch {
      return { catalog: null, running: 0, queued: 0, indeterminateReason: 'subagent-status-unavailable' }
    }
    const catalog = catalogOf(value)
    // A missing catalog is not a known-empty catalog: completion would claim
    // more than this read can prove.
    if (catalog === null) return { catalog: null, running: 0, queued: 0, indeterminateReason: 'subagent-catalog-unavailable' }
    let running = 0
    let queued = 0
    for (const row of catalog) {
      const mode = recordField(row, 'mode')
      const id = stringField(recordField(row, 'id'))
      if (mode === 'external') return { catalog, running, queued, indeterminateReason: 'subagent-external-status-unavailable' }
      if (mode === 'unknown') return { catalog, running, queued, indeterminateReason: 'subagent-unknown-status-unavailable' }
      if (id === undefined) return { catalog, running, queued, indeterminateReason: 'subagent-status-unavailable' }
      let child: RemoteSessionState
      try {
        child = await remote.readState(id)
      } catch {
        return { catalog, running, queued, indeterminateReason: 'subagent-status-unavailable' }
      }
      if (child.pendingInput.length > 0) return { catalog, running, queued, indeterminateReason: 'subagent-input-required' }
      if (child.running) running += 1
      if ((child.queue?.length ?? 0) > 0) queued += 1
    }
    return { catalog, running, queued, indeterminateReason: null }
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

function waitResult(
  delegation: DelegationRecord,
  afterSeq: number,
  cursor: number,
  reason: WaitResult['reason'],
  status: WaitStatus,
  state: RemoteSessionState | null,
  slices: number,
  collected: ReadonlyMap<number, SessionHistoryRecord>,
  truncated: boolean,
  subagentCatalog: readonly unknown[] | null,
  indeterminateReason: string | null,
): WaitResult {
  return {
    delegationId: delegation.delegationId,
    sessionId: delegation.sessionId,
    afterSeq,
    cursor,
    reason,
    status,
    state,
    slices,
    output: {
      records: [...collected.values()].sort((left, right) => left.event.seq - right.event.seq),
      truncated,
    },
    subagentCatalog,
    indeterminate: indeterminateReason !== null,
    indeterminateReason,
  }
}

function assertWaitBounds(afterSeq: number, timeoutMs: number, sliceTimeoutMs: number, maxMessages: number): void {
  if (!Number.isSafeInteger(afterSeq) || afterSeq < -1) {
    throw new LiaisonError('usage', 'afterSeq must be an integer greater than or equal to -1', { afterSeq })
  }
  for (const [name, value] of [['timeoutMs', timeoutMs], ['sliceTimeoutMs', sliceTimeoutMs], ['maxMessages', maxMessages]] as const) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new LiaisonError('usage', `${name} must be a positive safe integer`, { [name]: value })
    }
  }
}

function catalogOf(value: RemoteProjections): readonly unknown[] | null {
  if (value === null || !('values' in value)) return null
  const catalog = recordField(value.values, 'subagentCatalog')
  return Array.isArray(catalog) ? catalog : null
}

async function readProjectionsOrNull(remote: DshRemote, sessionId: string): Promise<RemoteProjections> {
  try {
    return await remote.readProjections(sessionId)
  } catch {
    return null
  }
}

async function continuedQuestionsOf(remote: DshRemote, sessionId: string): Promise<readonly { readonly callId: string; readonly questions: readonly AskUserQuestionItem[] }[]> {
  const value = await readProjectionsOrNull(remote, sessionId)
  if (value === null || !('values' in value)) return []
  const view = recordField(value.values, 'userQuestions')
  const active = recordField(view, 'active')
  if (!Array.isArray(active)) return []
  const result: { callId: string; questions: readonly AskUserQuestionItem[] }[] = []
  for (const row of active) {
    if (recordField(row, 'state') !== 'continued') continue
    const callId = stringField(recordField(row, 'callId'))
    const questions = recordField(row, 'questions')
    if (callId === undefined || !Array.isArray(questions)) continue
    result.push({ callId, questions: questions as readonly AskUserQuestionItem[] })
  }
  return result
}

/** Race one Remote await against the command's overall abort signal. */
function withinBudget<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(budgetError(signal))
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => { reject(budgetError(signal)) }
    signal.addEventListener('abort', onAbort, { once: true })
    work.then(
      (value) => {
        signal.removeEventListener('abort', onAbort)
        resolve(value)
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort)
        reject(error instanceof Error ? error : new LiaisonError('internal', String(error)))
      },
    )
  })
}

function budgetError(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new LiaisonError('dsh-unavailable', 'command budget expired')
}

function isWaitTimeout(error: unknown): boolean {
  return error instanceof LiaisonError && (
    error.code === 'dsh-unavailable' && error.message.includes('wait-timeout')
    || error.details?.remoteCode === 'session/wait-timeout'
  )
}

function isNotFound(error: unknown): boolean {
  return error instanceof LiaisonError && error.code === 'not-found'
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** Last tagged fork marker: events before its seq are inherited from the parent. */
function inheritedCutOf(events: readonly SessionWireEvent[]): number {
  const marker = events.findLast(event => event.type === 'session/end-seed' && recordField(event.data, 'inherited') === true)
  return marker?.seq ?? -1
}

/** The turn a user message was appended inside, plus that turn's end when committed. */
function turnContaining(events: readonly SessionWireEvent[], userSeq: number, inheritedCut: number): TurnWindow | undefined {
  let current: { turn: number; startSeq: number } | undefined
  for (const event of events) {
    if (event.seq > userSeq) break
    if (event.type === 'turn/start') {
      const turn = numberField(recordField(event.data, 'turn'))
      if (turn !== undefined) current = { turn, startSeq: event.seq }
    } else if (event.type === 'turn/end' && current !== undefined
      && numberField(recordField(event.data, 'turn')) === current.turn) {
      current = undefined
    }
  }
  if (current === undefined) return undefined
  const endSeq = events.find(event => event.seq > userSeq && event.type === 'turn/end'
    && numberField(recordField(event.data, 'turn')) === current.turn)?.seq
  return {
    turn: current.turn,
    startSeq: current.startSeq,
    ...(endSeq === undefined ? {} : { endSeq }),
    inherited: current.startSeq < inheritedCut,
  }
}

/** The most recently opened turn, open or closed. */
function latestTurn(events: readonly SessionWireEvent[], inheritedCut: number): TurnWindow | undefined {
  const start = events.findLast(event => event.type === 'turn/start' && numberField(recordField(event.data, 'turn')) !== undefined)
  if (start === undefined) return undefined
  const turn = numberField(recordField(start.data, 'turn'))
  /* v8 ignore next -- the predicate above already requires a numeric turn */
  if (turn === undefined) return undefined
  const endSeq = events.find(event => event.seq > start.seq && event.type === 'turn/end'
    && numberField(recordField(event.data, 'turn')) === turn)?.seq
  return {
    turn,
    startSeq: start.seq,
    ...(endSeq === undefined ? {} : { endSeq }),
    inherited: start.seq < inheritedCut,
  }
}

/** Last user message in one turn window, used when no request id selects one. */
function lastUserInTurn(events: readonly SessionWireEvent[], turn: TurnWindow): SessionWireEvent | undefined {
  return events.findLast(event => event.type === 'user/message' && event.seq > turn.startSeq
    && (turn.endSeq === undefined || event.seq < turn.endSeq))
}

/**
 * Select the target step's header. A header logged after the anchoring user
 * message and before the next step belongs to that message's step. Otherwise
 * the step reused the previous header unchanged, and only an assistant
 * settlement inside that step proves the reused configuration was dispatched.
 */
function headerChoiceFor(
  events: readonly SessionWireEvent[],
  turn: TurnWindow,
  anchor: number,
  hasAnchorUser: boolean,
): HeaderChoice | undefined {
  const step = hasAnchorUser ? stepContaining(events, turn, anchor) : undefined
  const upperBound = step?.endSeq ?? turn.endSeq
  const execution = events.find(event => event.type === 'assistant/message'
    && hasAssistantContent(event.data)
    && event.seq > anchor
    && numberField(recordField(event.data, 'turn')) === turn.turn
    && (step === undefined || numberField(recordField(event.data, 'step')) === step.step)
    && (upperBound === undefined || event.seq < upperBound))
  const inStep = events.find(event => event.type === 'request/header' && event.seq > anchor
    && (upperBound === undefined || event.seq < upperBound))
  if (inStep !== undefined) {
    return { header: inStep, source: 'turn', ...(execution === undefined ? {} : { execution }) }
  }
  const carried = events.findLast(event => event.type === 'request/header' && event.seq < anchor)
  if (carried === undefined) return undefined
  return { header: carried, source: 'carried', ...(execution === undefined ? {} : { execution }) }
}

/** The step a user message was appended inside, bounded by the next step or turn end. */
function stepContaining(
  events: readonly SessionWireEvent[],
  turn: TurnWindow,
  userSeq: number,
): { step: number; endSeq?: number } | undefined {
  const start = events.findLast(event => event.type === 'step/start'
    && event.seq > turn.startSeq
    && event.seq < userSeq
    && numberField(recordField(event.data, 'turn')) === turn.turn)
  if (start === undefined) return undefined
  const step = numberField(recordField(start.data, 'step'))
  if (step === undefined) return undefined
  const next = events.find(event => event.seq > userSeq && (
    (event.type === 'step/start' && numberField(recordField(event.data, 'turn')) === turn.turn)
    || (event.type === 'turn/end' && numberField(recordField(event.data, 'turn')) === turn.turn)))
  return { step, ...(next === undefined ? {} : { endSeq: next.seq }) }
}

/**
 * Whether a committed assistant message carries actual model-produced
 * content. Failed attempts settle as `assistant/attempt`; even a terminal
 * finish/error chunk there is not output, so partial turns stay indeterminate.
 */
function hasAssistantContent(data: unknown): boolean {
  const content = recordField(recordField(data, 'message'), 'content')
  return Array.isArray(content) && content.length > 0
}

function numberField(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : undefined
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

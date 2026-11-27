/**
 * In-memory buffer for forwarded approval and user-question waterfalls.
 *
 * Only bound delegation sessions are offered to this broker. A forwarded
 * request stays pending until a `dsh-liaison` command settles it, or until the
 * Gateway/browser cancels the delivery; a request from any other session is
 * left to the next Gateway listener.
 */

import { randomUUID } from 'node:crypto'
import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-api-remotes/client'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import type { ApprovalOutcome, ApprovalRequestEvent } from '@deepseek-ai/dsh-user-approval/types'
import type {
  AskUserQuestionAnswer, AskUserQuestionAnswerItem, AskUserQuestionItem, AskUserQuestionRequestEvent,
} from '@deepseek-ai/dsh-user-questions/types'
import type { TypertClientContextAdapter } from '@deepseek-ai/dsh-typert-protocol'
import { LiaisonError } from './errors.ts'

/** Client-projected approval request: the Agent becomes a scoped Context. */
type ClientApprovalRequest = Omit<ApprovalRequestEvent, 'agent'> & { readonly agent?: unknown }

/** Client-projected question request: the Agent becomes a scoped Context. */
type ClientQuestionRequest = Omit<AskUserQuestionRequestEvent, 'agent'> & { readonly agent?: unknown }

/** One pending interaction as exposed to the CLI. */
export interface InteractionView {
  readonly id: string
  readonly delegationId: string
  readonly sessionId: string
  readonly kind: 'approval' | 'question'
  readonly state: 'pending'
  readonly createdAt: number
  readonly toolName?: string
  readonly callId?: string
  readonly reason?: string
  readonly questions?: readonly AskUserQuestionItem[]
  readonly timed?: boolean
}

/** One settled question pulled from a Session projection. */
export interface ContinuedQuestionView {
  readonly id: string
  readonly delegationId: string
  readonly sessionId: string
  readonly kind: 'question'
  readonly state: 'continued'
  readonly createdAt: number
  readonly callId: string
  readonly questions: readonly AskUserQuestionItem[]
}

/** Broker dependencies. */
export interface InteractionBrokerOptions {
  readonly bindingOfSession: (sessionId: string) => string | undefined
  readonly now?: () => number
  readonly newId?: () => string
}

interface BaseEntry {
  readonly view: InteractionView
  finish: () => void
  cancel: (reason: unknown) => void
}

interface ApprovalEntry extends BaseEntry {
  readonly kind: 'approval'
  readonly settle: (outcome: ApprovalOutcome) => void
}

interface QuestionEntry extends BaseEntry {
  readonly kind: 'question'
  readonly settle: (answer: AskUserQuestionAnswer) => void
}

type Entry = ApprovalEntry | QuestionEntry

/** One answer batch supplied through the CLI. */
export interface InteractionAnswerInput {
  readonly interactionId?: string | undefined
  readonly delegationId?: string | undefined
  readonly callId?: string | undefined
  readonly answers: readonly AskUserQuestionAnswerItem[]
}

/** Buffers pending interactions and settles them on operator command. */
export class InteractionBroker {
  private readonly entries = new Map<string, Entry>()
  private readonly now: () => number
  private readonly newId: () => string
  private readonly bindingOfSession: (sessionId: string) => string | undefined

  /** @param options - durable binding lookup and deterministic seams. */
  constructor(options: InteractionBrokerOptions) {
    this.bindingOfSession = options.bindingOfSession
    this.now = options.now ?? Date.now
    this.newId = options.newId ?? randomUUID
  }

  /**
   * @returns every pending interaction, newest last, optionally for one delegation.
   */
  list(delegationId?: string): readonly InteractionView[] {
    const views = [...this.entries.values()]
      .map(entry => entry.view)
      .filter(view => delegationId === undefined || view.delegationId === delegationId)
    return views.sort((left, right) => left.createdAt - right.createdAt)
  }

  /** @returns the pending entry owned by one Gateway delivery identity. */
  has(interactionId: string): boolean {
    return this.entries.has(interactionId)
  }

  /**
   * Buffer one approval request and wait for an operator decision.
   * @param sessionId - bound Session receiving the request.
   * @param request - forwarded approval waterfall request.
   * @returns the decision, or a rejection when the delivery is cancelled.
   */
  offerApproval(sessionId: string, request: ClientApprovalRequest): Promise<ApprovalOutcome> {
    const view = this.baseView(sessionId, 'approval', {
      ...(request.toolName === undefined ? {} : { toolName: request.toolName }),
      ...(request.callId === undefined ? {} : { callId: String(request.callId) }),
      ...(request.reason === undefined ? {} : { reason: request.reason }),
    })
    if (view === undefined) return Promise.reject(unbound(sessionId))
    const deferred = Promise.withResolvers<ApprovalOutcome>()
    const entry: ApprovalEntry = {
      kind: 'approval',
      view,
      settle: outcome => { deferred.resolve(outcome) },
      finish: () => {},
      cancel: reason => { deferred.reject(reason) },
    }
    this.attach(entry, request.signal)
    return deferred.promise
  }

  /**
   * Buffer one user-questions request and wait for a structured answer.
   * @param sessionId - bound Session receiving the question.
   * @param request - forwarded question waterfall request.
   * @returns the answer, or a rejection when the delivery is cancelled.
   */
  offerQuestion(sessionId: string, request: ClientQuestionRequest): Promise<AskUserQuestionAnswer> {
    const view = this.baseView(sessionId, 'question', {
      ...(request.wait?.callId === undefined ? {} : { callId: String(request.wait.callId) }),
      ...(request.wait?.timed === undefined ? {} : { timed: request.wait.timed }),
      questions: request.questions,
    })
    if (view === undefined) return Promise.reject(unbound(sessionId))
    const deferred = Promise.withResolvers<AskUserQuestionAnswer>()
    const entry: QuestionEntry = {
      kind: 'question',
      view,
      settle: answer => { deferred.resolve(answer) },
      finish: () => {},
      cancel: reason => { deferred.reject(reason) },
    }
    this.attach(entry, request.signal)
    return deferred.promise
  }

  /**
   * Approve one pending approval.
   * @param interactionId - id shown by `interaction list`.
   * @returns the settled view.
   */
  approve(interactionId: string): InteractionView {
    return this.settleApproval(interactionId, 'allowed-once')
  }

  /**
   * Reject one pending approval.
   * @param interactionId - id shown by `interaction list`.
   * @returns the settled view.
   */
  reject(interactionId: string): InteractionView {
    return this.settleApproval(interactionId, 'rejected')
  }

  /**
   * Answer one pending question. When no buffered entry owns the call id, the
   * caller may settle a continued timed question through the public
   * `userQuestions.answer` RPC instead.
   * @param input - interaction id or call id plus a complete answer batch.
   * @returns the settled view, or undefined when it is not buffered.
   */
  answer(input: InteractionAnswerInput): InteractionView | undefined {
    const entry = this.findAnswerEntry(input)
    if (entry === undefined) {
      if (input.interactionId !== undefined) {
        throw new LiaisonError('not-found', `interaction ${input.interactionId} is no longer pending`, {
          interactionId: input.interactionId,
        })
      }
      return undefined
    }
    if (entry.kind !== 'question') {
      throw new LiaisonError('conflict', `interaction ${entry.view.id} is an approval, not a question`, {
        interactionId: entry.view.id,
      })
    }
    const questions = entry.view.questions ?? []
    validateAnswers(questions, input.answers)
    this.finish(entry)
    entry.settle({ answers: [...input.answers] })
    return entry.view
  }

  /** Reject every pending request; used when the loopback connection is disposed. */
  clear(reason: unknown = new LiaisonError('dsh-unavailable', 'loopback connection disposed')): void {
    for (const entry of this.entries.values()) {
      this.finish(entry)
      entry.cancel(reason)
    }
  }

  private settleApproval(interactionId: string, outcome: ApprovalOutcome): InteractionView {
    const entry = this.entries.get(interactionId)
    if (entry === undefined) {
      throw new LiaisonError('not-found', `interaction ${interactionId} is no longer pending`, { interactionId })
    }
    if (entry.kind !== 'approval') {
      throw new LiaisonError('conflict', `interaction ${interactionId} is a question, not an approval`, { interactionId })
    }
    this.finish(entry)
    entry.settle(outcome)
    return entry.view
  }

  private findAnswerEntry(input: InteractionAnswerInput): Entry | undefined {
    if (input.interactionId !== undefined) return this.entries.get(input.interactionId)
    if (input.callId === undefined) {
      throw new LiaisonError('usage', 'interaction answer requires --interaction-id or --call-id')
    }
    return [...this.entries.values()].find(entry => entry.kind === 'question'
      && entry.view.callId === input.callId
      && (input.delegationId === undefined || entry.view.delegationId === input.delegationId))
  }

  private baseView(
    sessionId: string,
    kind: InteractionView['kind'],
    extra: Pick<InteractionView, 'toolName' | 'callId' | 'reason' | 'questions' | 'timed'>,
  ): InteractionView | undefined {
    const delegationId = this.bindingOfSession(sessionId)
    if (delegationId === undefined) return undefined
    return {
      id: `interaction-${this.newId()}`,
      delegationId,
      sessionId,
      kind,
      state: 'pending',
      createdAt: this.now(),
      ...extra,
    }
  }

  /** Register one entry and tie its lifetime to the Gateway delivery signal. */
  private attach(entry: Entry, signal: AbortSignal | undefined): void {
    const reject = entry.cancel
    const onAbort = (): void => {
      if (!this.entries.has(entry.view.id)) return
      this.finish(entry)
      reject(signal?.reason ?? new LiaisonError('conflict', `interaction ${entry.view.id} was cancelled`))
    }
    entry.finish = () => {
      this.entries.delete(entry.view.id)
      signal?.removeEventListener('abort', onAbort)
    }
    entry.cancel = (reason: unknown) => {
      this.finish(entry)
      reject(reason)
    }
    if (signal?.aborted === true) {
      reject(signal.reason)
      return
    }
    signal?.addEventListener('abort', onAbort, { once: true })
    this.entries.set(entry.view.id, entry)
  }

  private finish(entry: Entry): void {
    this.entries.delete(entry.view.id)
    entry.finish()
  }
}

/** Validate the answer batch against the exact question list. */
export function validateAnswers(
  questions: readonly AskUserQuestionItem[],
  answers: readonly AskUserQuestionAnswerItem[],
): void {
  const ids = new Set(answers.map(answer => answer.id))
  if (ids.size !== answers.length
    || questions.length !== answers.length
    || !questions.every(question => ids.has(question.id))) {
    throw new LiaisonError('usage', `the answer batch must name each of its ${String(questions.length)} questions exactly once`, {
      expected: questions.map(question => question.id),
      actual: answers.map(answer => answer.id),
    })
  }
}

/** One bound Session Context registry for Gateway event delivery. */
export interface InteractionContexts {
  readonly adapter: TypertClientContextAdapter<SessionId>
  sessionOf(ctx: Context): SessionId | undefined
}

/**
 * Create per-Session child contexts lazily for bound delegations and return
 * the Gateway client-context adapter. Unbound identities resolve to undefined,
 * which makes Gateway continue to its next listener.
 * @param root - isolated loopback Gateway context.
 * @param bindingOfSession - durable delegation lookup.
 * @returns the adapter and reverse Context lookup.
 */
export function createInteractionContexts(
  root: Context,
  bindingOfSession: (sessionId: string) => string | undefined,
): InteractionContexts {
  const scopes = new Map<string, Context>()
  const sessions = new WeakMap<Context, SessionId>()
  const adapter: TypertClientContextAdapter<SessionId> = {
    identity: ctx => sessions.get(ctx),
    resolve: id => {
      const sessionId = String(id)
      if (bindingOfSession(sessionId) === undefined) return undefined
      const existing = scopes.get(sessionId)
      if (existing !== undefined) return existing
      // A no-op child plugin gives the Session-scoped dispatch point a lifetime
      // tied to the root Gateway context.
      const fiber = root.plugin(() => {})
      const ctx = fiber.ctx
      scopes.set(sessionId, ctx)
      sessions.set(ctx, id)
      return ctx
    },
  }
  return { adapter, sessionOf: ctx => sessions.get(ctx) }
}

/**
 * Register the Agent context adapter and forwarded-event listeners on one
 * loopback Gateway client.
 * @param root - isolated loopback Gateway context.
 * @param broker - pending interaction buffer.
 * @param bindingOfSession - durable delegation lookup.
 */
export function installInteractionContexts(
  root: Context,
  broker: InteractionBroker,
  bindingOfSession: (sessionId: string) => string | undefined,
): void {
  const contexts = createInteractionContexts(root, bindingOfSession)
  root.typert.contexts.registerClient('agent', contexts.adapter)
  root.effect(
    () => root.remote.$on('approval/request', function (request, next) {
      const sessionId = contexts.sessionOf(this)
      return sessionId === undefined ? next() : broker.offerApproval(String(sessionId), request)
    }),
    'dsh-liaison: approval requests',
  )
  root.effect(
    () => root.remote.$on('user-questions/request', function (request, next) {
      const sessionId = contexts.sessionOf(this)
      return sessionId === undefined ? next() : broker.offerQuestion(String(sessionId), request)
    }),
    'dsh-liaison: user questions',
  )
}

function unbound(sessionId: string): LiaisonError {
  return new LiaisonError('not-found', `session ${sessionId} is not bound to a delegation`, { sessionId })
}

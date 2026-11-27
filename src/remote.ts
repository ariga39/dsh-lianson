/** Remote operations the delegation service needs from the loopback Gateway client. */

import type {
  ModelCatalog, SessionProjectionsValue, SessionState, SessionWaitRequest, SessionWaitValue, SessionWireEvent,
} from '@deepseek-ai/dsh-api-session-controller/types'
import type { AskUserQuestionAnswer } from '@deepseek-ai/dsh-user-questions/types'

/** Visible model catalog subset consumed by `create`. */
export type RemoteModelCatalog = Pick<ModelCatalog, 'default' | 'groups' | 'failures'>

/** One requested model route. */
export interface RemoteModelSelection {
  readonly provider: string
  readonly model: string
}

/** One page of durable Session events. */
export interface RemoteEventPage {
  readonly events: readonly SessionWireEvent[]
  readonly cursor: number
  /** Omitted by lightweight fixtures; the reader treats omission as a final page. */
  readonly hasMore?: boolean
}

/** One backwards history-page request. */
export interface RemoteEventPageRequest {
  readonly throughSeq?: number
  readonly beforeSeq?: number
  readonly maxMessages?: number
}

/** One pending queue mutation supplied by the CLI. */
export type RemoteQueueAction =
  | { readonly kind: 'edit'; readonly text: string }
  | { readonly kind: 'remove' }
  | { readonly kind: 'steer' }

/** One requested wait window. */
export type RemoteWaitRequest = Omit<SessionWaitRequest, 'sessionId'>

/** Passive Session state read through the public Remote surface. */
export type RemoteSessionState = SessionState

/** Exact or cached projection values for one Session. */
export type RemoteProjections = SessionProjectionsValue

/** Connected Host operations; every method rejects with a structured failure. */
export interface DshRemote {
  /** Host origin, never carrying credentials. */
  readonly origin: string
  modelCatalog(): Promise<RemoteModelCatalog>
  createWorkspace(path: string): Promise<string>
  createSession(input: { readonly workspaceId: string; readonly sessionId: string }): Promise<string>
  renameSession(sessionId: string, title: string): Promise<void>
  selectModel(sessionId: string, selection: RemoteModelSelection): Promise<void>
  /** Latest durable cursor before a prompt is admitted. */
  beforeCursor(sessionId: string): Promise<number>
  prompt(sessionId: string, requestId: string, text: string, mode?: 'queue' | 'steer'): Promise<void>
  /** Read one Host history page. */
  readEventPage(sessionId: string, request: RemoteEventPageRequest): Promise<RemoteEventPage>
  /** Passive execution, queue, and pending-input snapshot. */
  readState(sessionId: string): Promise<RemoteSessionState>
  /** One Host wait window, never cancelling execution. */
  wait(sessionId: string, request: RemoteWaitRequest): Promise<SessionWaitValue>
  /** Exact projections or migration hints for one Session. */
  readProjections(sessionId: string): Promise<RemoteProjections>
  /** Mutate one still-pending queue item. */
  updateQueue(sessionId: string, itemId: string, action: RemoteQueueAction): Promise<void>
  /** Cancel the active turn while retaining pending inbox work. */
  cancel(sessionId: string): Promise<void>
  /** Restore an archived Session before submitting follow-up work. */
  unarchiveSession(sessionId: string): Promise<void>
  /** Answer a timed question that outlived its foreground wait. */
  answerUserQuestion(sessionId: string, callId: string, answer: AskUserQuestionAnswer): Promise<boolean>
  dispose(): Promise<void>
}

/** Loopback lifecycle as reported by `status`. */
export type LoopbackState = 'idle' | 'connecting' | 'connected' | 'unavailable' | 'disposed'

/** Status snapshot shared with the CLI. */
export interface LoopbackStatus {
  readonly state: LoopbackState
  readonly origin?: string
  readonly lastError?: string
}

/** Supplies the connected remote to the delegation service. */
export interface RemoteSource {
  status(): LoopbackStatus
  /** @throws {LiaisonError} `dsh-unavailable` until the loopback client connects. */
  requireRemote(): DshRemote
  dispose(): Promise<void>
}

/** Remote operations the delegation service needs from the loopback Gateway client. */

import type { ModelCatalog, SessionWireEvent } from '@deepseek-ai/dsh-api-session-controller/types'

/** Visible model catalog subset consumed by `create`. */
export type RemoteModelCatalog = Pick<ModelCatalog, 'default' | 'groups' | 'failures'>

/** One requested model route. */
export interface RemoteModelSelection {
  readonly provider: string
  readonly model: string
}

/** One page of recent durable Session events. */
export interface RemoteEventPage {
  readonly events: readonly SessionWireEvent[]
  readonly cursor: number
}

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
  prompt(sessionId: string, requestId: string, text: string): Promise<void>
  /** Recent durable events, paged backwards far enough to inspect the last turns. */
  readEvents(sessionId: string): Promise<RemoteEventPage>
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

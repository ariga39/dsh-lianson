import { randomUUID } from 'node:crypto'
import { LIAISON_ERROR_CODES, LiaisonError, type LiaisonErrorDetails, type WireError } from './errors.ts'
import type { HostInspection, HostSummary, PingOutcome, StopOutcome } from './host-manager.ts'

/** Methods accepted by the local daemon. */
export const IPC_METHODS = ['host.start', 'host.status', 'host.ping', 'host.stop'] as const

/** One daemon method. */
export type IpcMethod = (typeof IPC_METHODS)[number]

/** Request/result correlation envelope. */
export interface IpcRequest {
  readonly id: string
  readonly method: IpcMethod
  readonly params: Record<string, unknown>
  /** Caller wall-clock deadline; the daemon converts it to a monotonic budget. */
  readonly deadline: number
}

/** Successful daemon reply. */
export interface IpcSuccess {
  readonly id: string
  readonly ok: true
  readonly result: unknown
}

/** Failed daemon reply. */
export interface IpcFailure {
  readonly id: string
  readonly ok: false
  readonly error: WireError
}

/** Either daemon reply. */
export type IpcResponse = IpcSuccess | IpcFailure

/** Result shapes by method. */
export interface IpcResultMap {
  readonly 'host.start': HostSummary & { readonly daemonPid: number }
  readonly 'host.status': (HostInspection | HostSummary) & { readonly daemonPid: number | null }
  readonly 'host.ping': PingOutcome & { readonly daemonPid: number }
  readonly 'host.stop': StopOutcome & { readonly daemonPid: number | null }
}

/** Parameters by method. */
export interface IpcParamsMap {
  readonly 'host.start': {
    readonly cwd?: string
    readonly port?: number
    readonly patchPaths?: readonly string[]
  }
  readonly 'host.status': { readonly ensure?: boolean }
  readonly 'host.ping': Record<string, never>
  readonly 'host.stop': { readonly keepHost?: boolean }
}

/** Create one correlated request with one caller wall deadline. */
export function makeRequest<M extends IpcMethod>(method: M, params: IpcParamsMap[M], timeoutMs: number): IpcRequest {
  return { id: randomUUID(), method, params: { ...params }, deadline: Date.now() + timeoutMs }
}

/**
 * Parse one NDJSON request line.
 * @param line - complete line without its newline.
 * @returns the validated request.
 */
export function parseIpcRequest(line: string): IpcRequest {
  let parsed: unknown
  try {
    parsed = JSON.parse(line)
  } catch (error) {
    throw new LiaisonError('IPC_PROTOCOL', 'the daemon request is not valid JSON', { cause: error })
  }
  const record = recordOf(parsed)
  if (record === undefined) throw new LiaisonError('IPC_PROTOCOL', 'the daemon request must be an object')
  const id = record.id
  const method = record.method
  if (typeof id !== 'string' || id === '') throw new LiaisonError('IPC_PROTOCOL', 'the daemon request has no id')
  if (!isIpcMethod(method)) throw new LiaisonError('IPC_PROTOCOL', 'the daemon request has an unknown method')
  const params = record.params === undefined ? {} : recordOf(record.params)
  if (params === undefined) throw new LiaisonError('IPC_PROTOCOL', 'the daemon request params must be an object')
  const deadline = record.deadline
  if (typeof deadline !== 'number' || !Number.isSafeInteger(deadline) || deadline <= 0) {
    throw new LiaisonError('IPC_PROTOCOL', 'the daemon request deadline must be a positive epoch millisecond')
  }
  return { id, method, params, deadline }
}

/**
 * Parse one NDJSON reply line.
 * @param line - complete line without its newline.
 * @returns the validated reply.
 */
export function parseIpcResponse(line: string): IpcResponse {
  let parsed: unknown
  try {
    parsed = JSON.parse(line)
  } catch (error) {
    throw new LiaisonError('IPC_PROTOCOL', 'the daemon reply is not valid JSON', { cause: error })
  }
  const record = recordOf(parsed)
  if (record === undefined) throw new LiaisonError('IPC_PROTOCOL', 'the daemon reply must be an object')
  const id = record.id
  const ok = record.ok
  if (typeof id !== 'string' || typeof ok !== 'boolean') {
    throw new LiaisonError('IPC_PROTOCOL', 'the daemon reply has no correlated outcome')
  }
  if (ok) return { id, ok: true, result: record.result }
  const error = recordOf(record.error)
  const code = error?.code
  const message = error?.message
  if (!isErrorCode(code) || typeof message !== 'string') {
    throw new LiaisonError('IPC_PROTOCOL', 'the daemon reply has a malformed error')
  }
  const details = error?.details === undefined ? undefined : recordOf(error.details)
  if (details === undefined) return { id, ok: false, error: { code, message } }
  return {
    id,
    ok: false,
    error: { code, message, details: details as LiaisonErrorDetails },
  }
}

/** Whether a parsed method is accepted. */
export function isIpcMethod(value: unknown): value is IpcMethod {
  return typeof value === 'string' && (IPC_METHODS as readonly string[]).includes(value)
}

function isErrorCode(value: unknown): value is WireError['code'] {
  return typeof value === 'string' && (LIAISON_ERROR_CODES as readonly string[]).includes(value)
}

function recordOf(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

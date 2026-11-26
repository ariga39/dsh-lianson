import { createConnection, type Socket } from 'node:net'
import { LiaisonError } from './errors.ts'
import { isMissing } from './fs-safe.ts'
import {
  makeRequest,
  parseIpcResponse,
  type IpcMethod,
  type IpcParamsMap,
  type IpcResultMap,
} from './ipc-protocol.ts'

/**
 * Send one correlated request over the local Unix socket.
 * @param socketPath - daemon socket path.
 * @param method - accepted daemon method.
 * @param params - method parameters.
 * @param timeoutMs - complete connect/write/reply deadline.
 * @returns the typed result.
 */
export async function callDaemon<M extends IpcMethod>(
  socketPath: string,
  method: M,
  params: IpcParamsMap[M],
  timeoutMs: number,
): Promise<IpcResultMap[M]> {
  const request = makeRequest(method, params)
  const line = `${JSON.stringify(request)}\n`
  return await new Promise<IpcResultMap[M]>((resolve, reject) => {
    let settled = false
    let buffer = ''
    const socket: Socket = createConnection(socketPath)
    const finish = (outcome: { readonly value: IpcResultMap[M] } | { readonly error: Error }): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      socket.destroy()
      if ('value' in outcome) resolve(outcome.value)
      else reject(outcome.error)
    }
    const timer = setTimeout(() => {
      finish({ error: new LiaisonError('IPC_TIMEOUT', `the daemon did not reply within ${String(timeoutMs)}ms`) })
    }, timeoutMs)
    timer.unref()
    socket.setEncoding('utf8')
    socket.once('connect', () => { socket.write(line) })
    socket.on('data', (chunk: string) => {
      buffer += chunk
      const newline = buffer.indexOf('\n')
      if (newline === -1) return
      const reply = buffer.slice(0, newline)
      try {
        const response = parseIpcResponse(reply)
        if (response.id !== request.id) {
          finish({ error: new LiaisonError('IPC_PROTOCOL', 'the daemon reply did not match the request') })
          return
        }
        if (response.ok) {
          finish({ value: response.result as IpcResultMap[M] })
          return
        }
        finish({
          error: response.error.details === undefined
            ? new LiaisonError(response.error.code, response.error.message)
            : new LiaisonError(response.error.code, response.error.message, { details: response.error.details }),
        })
      } catch (error) {
        finish({ error: error instanceof Error ? error : new Error(String(error)) })
      }
    })
    socket.once('error', (error: NodeJS.ErrnoException) => {
      finish({
        error: isMissing(error) || error.code === 'ECONNREFUSED'
          ? new LiaisonError('IPC_UNAVAILABLE', 'the daemon is not running', { cause: error })
          : new LiaisonError('IPC_PROTOCOL', error.message, { cause: error }),
      })
    })
    socket.once('close', () => {
      if (!settled) finish({ error: new LiaisonError('IPC_PROTOCOL', 'the daemon closed without a reply') })
    })
  })
}

/**
 * Whether a failure means no daemon answered.
 * @param error - thrown value.
 * @returns true for IPC_UNAVAILABLE.
 */
export function isDaemonUnavailable(error: unknown): boolean {
  return error instanceof LiaisonError && error.code === 'IPC_UNAVAILABLE'
}

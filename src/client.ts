/** Stateless CLI transport to the plugin's Unix-domain HTTP socket. */

import { request } from 'node:http'
import type { IncomingMessage } from 'node:http'
import { dirname } from 'node:path'
import { LiaisonError } from './errors.ts'
import type { RoutePath, ServerEnvelope } from './protocol.ts'
import { inspectPrivateRuntimeDirectory } from './uds-server.ts'

/** Maximum response body read from the plugin. */
const MAX_RESPONSE_BYTES = 8 * 1024 * 1024

/**
 * Send one command document to the plugin.
 * @param socketPath - absolute Unix-domain socket path.
 * @param route - command route.
 * @param body - JSON request.
 * @param timeoutMs - overall request budget.
 * @returns the successful envelope value.
 * @throws {LiaisonError} `plugin-offline` when the socket cannot be reached.
 */
export async function sendCommand(
  socketPath: string,
  route: RoutePath,
  body: unknown,
  timeoutMs = 30_000,
): Promise<unknown> {
  await assertPrivateSocketDirectory(socketPath)
  const payload = JSON.stringify(body)
  const envelope = await new Promise<ServerEnvelope>((resolve, reject) => {
    const req = request({
      socketPath,
      path: route,
      method: 'POST',
      headers: {
        'content-type': 'application/json; charset=utf-8',
        'content-length': String(Buffer.byteLength(payload)),
      },
    }, (response) => {
      void readEnvelope(response).then(resolve, reject)
    })
    req.setTimeout(timeoutMs, () => {
      req.destroy(new LiaisonError('plugin-offline', 'dsh-liaison plugin did not answer in time'))
    })
    req.on('error', (error: NodeJS.ErrnoException) => {
      if (error.code === 'ENOENT' || error.code === 'ECONNREFUSED' || error.code === 'ECONNRESET' || error.code === 'EPIPE') {
        reject(new LiaisonError('plugin-offline', 'dsh-liaison plugin is not running on this socket', { socketPath }))
        return
      }
      reject(error instanceof LiaisonError ? error : new LiaisonError('plugin-offline', error.message))
    })
    req.end(payload)
  })
  if (envelope.ok) return envelope.value
  throw new LiaisonError(
    codeOf(envelope.error.code),
    envelope.error.message,
    envelope.error.details,
  )
}

/**
 * Fail closed before placing a command on the socket when its runtime
 * directory is unsafe. A missing directory or socket stays an offline read;
 * this check never creates paths.
 */
async function assertPrivateSocketDirectory(socketPath: string): Promise<void> {
  const directory = dirname(socketPath)
  try {
    await inspectPrivateRuntimeDirectory(directory)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      throw new LiaisonError('plugin-offline', 'dsh-liaison runtime directory is missing', { socketPath })
    }
    if (error instanceof LiaisonError) throw error
    throw new LiaisonError('unsafe-runtime-dir', `cannot verify runtime directory ${directory}`, {
      directory,
      reason: error instanceof Error ? error.message : String(error),
    })
  }
}

function codeOf(code: string): LiaisonError['code'] {
  switch (code) {
    case 'usage':
    case 'not-found':
    case 'conflict':
    case 'unsafe-cwd':
    case 'invalid-cwd':
    case 'model-unavailable':
    case 'dsh-unavailable':
    case 'plugin-offline':
    case 'socket-in-use':
    case 'unsafe-runtime-dir':
    case 'ledger':
    case 'protocol':
    case 'internal':
      return code
    default:
      return 'internal'
  }
}

async function readEnvelope(response: IncomingMessage): Promise<ServerEnvelope> {
  const chunks: Uint8Array[] = []
  let total = 0
  for await (const chunk of response) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    total += buffer.byteLength
    if (total > MAX_RESPONSE_BYTES) throw new LiaisonError('protocol', 'plugin response is too large')
    chunks.push(buffer)
  }
  let value: unknown
  try {
    value = JSON.parse(Buffer.concat(chunks).toString('utf8'))
  } catch {
    throw new LiaisonError('protocol', `plugin returned non-JSON (HTTP ${String(response.statusCode ?? 0)})`)
  }
  if (value === null || typeof value !== 'object' || !('ok' in value)) {
    throw new LiaisonError('protocol', 'plugin returned an invalid envelope')
  }
  return value as ServerEnvelope
}

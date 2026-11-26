/** In-process Cordis plugin serving the liaison UDS endpoint inside `dsh web`. */
import { closeSync, openSync } from 'node:fs'
import { chmod, lstat, mkdir, stat, unlink } from 'node:fs/promises'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { dirname } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-cmdline'
import { resolveDshHome } from '@deepseek-ai/dsh-home-paths'
import type { WebEntryUrl } from '@deepseek-ai/dsh-host-webserver'
import { tryLockExclusive } from '@deepseek-ai/node-addon-system/flock'
import {
  ErrorResponseSchema,
  HealthResponseSchema,
  LIAISON_SERVICE,
  PingRequestSchema,
  PingResponseSchema,
  type ErrorResponse,
  type HealthResponse,
  type PingRequest,
  type PingResponse,
} from './contract.ts'
import { connectLoopback, type LoopbackOperator } from './loopback.ts'
import { resolveLiaisonLockPath, resolveLiaisonSocketPath } from './socket-path.ts'

/** Stable Cordis plugin name. */
export const name = 'dsh-liaison'

const MAX_REQUEST_BODY_BYTES = 64 * 1024
const SOCKET_TIMEOUT_MS = 10_000
const LOOPBACK_HOSTS = new Set(['127.0.0.1', '::1', '[::1]', 'localhost'])

/** Test hooks; production never mutates them. */
export const internals: {
  connectLoopback: typeof connectLoopback
} = { connectLoopback }

interface SocketIdentity {
  readonly dev: number
  readonly ino: number
}

class HttpError extends Error {
  readonly status: number
  readonly code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.status = status
    this.code = code
  }
}

/**
 * Mount the liaison endpoint and its loopback answering operator.
 * @param ctx - Host plugin context.
 */
export async function apply(ctx: Context): Promise<void> {
  const startedAt = new Date().toISOString()
  const dshHome = resolveDshHome()
  const socketPath = resolveLiaisonSocketPath()
  const lockPath = resolveLiaisonLockPath(socketPath)
  const pid = process.pid

  let loopback: LoopbackOperator | undefined
  let origin: string | null = null
  let server: Server | undefined
  let lockFd: number | undefined
  let startingLoopback = false
  let disposed = false
  let socketIdentity: SocketIdentity | undefined

  const entry = Promise.withResolvers<string>()
  // The authenticated URL is captured in memory only; it is never written to disk.
  // Entry listeners append synchronously, so read the completed list afterwards.
  ctx.on('webserver/entry-url', (_baseUrl, entries: WebEntryUrl[]) => {
    queueMicrotask(() => {
      if (disposed || origin !== null) return
      const selected = selectEntry(entries)
      if (selected === undefined) return
      origin = new URL(selected).origin
      entry.resolve(selected)
    })
  })

  await ensureDirectory(socketPath)
  lockFd = openSync(lockPath, 'a+', 0o600)
  try {
    await tryLockExclusive(lockFd)
  } catch (error) {
    closeSync(lockFd)
    throw new Error(
      `dsh-liaison: another instance owns ${socketPath} (${errorMessage(error)})`,
      { cause: error },
    )
  }

  try {
    await removeStaleSocket(socketPath)
    const httpServer = createServer((request, response) => {
      void handleRequest(request, response).catch((error: unknown) => {
        respondFailure(response, error)
      })
    })
    await listen(httpServer, socketPath)
    const bound = await stat(socketPath)
    socketIdentity = { dev: bound.dev, ino: bound.ino }
    await chmod(socketPath, 0o600)
    httpServer.on('error', (error) => {
      console.error(`dsh-liaison: socket error: ${errorMessage(error)}`)
    })
    server = httpServer
  } catch (error) {
    await removeOwnedSocket(socketPath, socketIdentity)
    closeSync(lockFd)
    throw error
  }

  const startLoopback = async (): Promise<void> => {
    if (disposed || startingLoopback || loopback !== undefined) return
    startingLoopback = true
    try {
      const launchUrl = await entry.promise
      if (disposed) return
      const next = await internals.connectLoopback(launchUrl)
      if (disposed) {
        await next.close()
        return
      }
      loopback = next
    } catch (error) {
      if (!disposed) console.warn(`dsh-liaison: loopback operator unavailable: ${redact(errorMessage(error))}`)
    } finally {
      startingLoopback = false
    }
  }

  const appReady = ctx.get('appReady')
  const offReady = appReady?.onReady(() => { void startLoopback() })
  if (appReady === undefined) void startLoopback()

  ctx.effect(() => async () => {
    disposed = true
    offReady?.()
    await loopback?.close().catch(() => undefined)
    loopback = undefined
    if (server !== undefined) {
      await closeServer(server).catch(() => undefined)
      server = undefined
    }
    await removeOwnedSocket(socketPath, socketIdentity)
    if (lockFd !== undefined) {
      closeSync(lockFd)
      lockFd = undefined
    }
  }, 'dsh-liaison: socket lifetime')

  async function handleRequest(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const target = new URL(request.url ?? '/', 'http://dsh-liaison.invalid')
    if (request.method === 'GET' && target.pathname === '/health') {
      const health: HealthResponse = HealthResponseSchema.parse({
        ok: true,
        service: LIAISON_SERVICE,
        pid,
        dshHome,
        origin,
        socket: socketPath,
        startedAt,
        loopback: {
          connected: loopback?.isConnected() ?? false,
          generation: loopback?.generation() ?? null,
          reauthentications: loopback?.reauthentications() ?? 0,
        },
      })
      respondJson(response, 200, health)
      return
    }
    if (request.method === 'POST' && target.pathname === '/ping') {
      let input: PingRequest
      try {
        input = PingRequestSchema.parse(JSON.parse(await readBody(request)) as unknown)
      } catch (error) {
        throw new HttpError(400, 'invalid-request', errorMessage(error))
      }
      if (!(await waitForLoopback(SOCKET_TIMEOUT_MS))) {
        throw new HttpError(503, 'loopback-unavailable', 'the loopback operator is not connected')
      }
      const operator = loopback
      if (operator === undefined) {
        throw new HttpError(503, 'loopback-unavailable', 'the loopback operator is not connected')
      }
      const models = await withTimeout(operator.modelCount(), SOCKET_TIMEOUT_MS, 'loopback RPC')
      const generation = operator.generation()
      if (generation === null) {
        throw new HttpError(503, 'loopback-unavailable', 'the loopback operator lost its generation')
      }
      const pong: PingResponse = PingResponseSchema.parse({
        ok: true,
        pong: true,
        nonce: input.nonce ?? null,
        generation,
        reauthentications: operator.reauthentications(),
        models,
      })
      respondJson(response, 200, pong)
      return
    }
    throw new HttpError(404, 'not-found', `no route for ${request.method ?? 'GET'} ${target.pathname}`)
  }

  async function waitForLoopback(timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      if (loopback?.isConnected() === true) return true
      await delay(50)
    }
    return loopback?.isConnected() === true
  }
}

function selectEntry(entries: readonly WebEntryUrl[]): string | undefined {
  for (const entry of entries) {
    try {
      const url = new URL(entry.url)
      if (!LOOPBACK_HOSTS.has(url.hostname)) continue
      if (url.searchParams.get('token') === null) continue
      return entry.url
    } catch {
      // Ignore malformed entries contributed by other packages.
    }
  }
  return undefined
}

async function ensureDirectory(socketPath: string): Promise<void> {
  const directory = dirname(socketPath)
  const custom = (process.env.DSH_LIAISON_SOCKET?.trim() ?? '') !== ''
  let existed = true
  try {
    const stats = await lstat(directory)
    if (!stats.isDirectory() || stats.isSymbolicLink()) {
      throw new Error(`dsh-liaison: socket parent ${directory} must be a real directory`)
    }
  } catch (error) {
    if (!isErrno(error, 'ENOENT')) throw error
    existed = false
  }
  if (!existed) await mkdir(directory, { recursive: true, mode: 0o700 })
  const current = await lstat(directory)
  if (current.isSymbolicLink() || !current.isDirectory()) {
    throw new Error(`dsh-liaison: socket parent ${directory} must be a real directory`)
  }
  if (!custom) await chmod(directory, 0o700)
  const mode = (await stat(directory)).mode & 0o777
  if ((mode & 0o077) !== 0) {
    throw new Error(`dsh-liaison: socket parent ${directory} must be private (0700), found ${mode.toString(8)}`)
  }
}

async function removeStaleSocket(socketPath: string): Promise<void> {
  let stats
  try {
    stats = await lstat(socketPath)
  } catch (error) {
    if (isErrno(error, 'ENOENT')) return
    throw error
  }
  if (!stats.isSocket()) {
    throw new Error(`dsh-liaison: refusing to remove non-socket path ${socketPath}`)
  }
  await unlink(socketPath)
}

async function removeOwnedSocket(socketPath: string, identity: SocketIdentity | undefined): Promise<void> {
  if (identity === undefined) return
  let stats
  try {
    stats = await lstat(socketPath)
  } catch (error) {
    if (isErrno(error, 'ENOENT')) return
    throw error
  }
  if (stats.isSocket() && stats.dev === identity.dev && stats.ino === identity.ino) await unlink(socketPath)
}

async function listen(server: Server, socketPath: string): Promise<void> {
  await new Promise<void>((resolvePromise, reject) => {
    const onError = (error: Error): void => {
      server.off('listening', onListening)
      reject(error)
    }
    const onListening = (): void => {
      server.off('error', onError)
      resolvePromise()
    }
    server.once('error', onError)
    server.once('listening', onListening)
    server.listen(socketPath)
  })
}

async function closeServer(server: Server): Promise<void> {
  if (!server.listening) return
  server.closeAllConnections()
  await new Promise<void>((resolvePromise, reject) => {
    server.close((error) => {
      if (error !== undefined) reject(error)
      else resolvePromise()
    })
  })
}

async function readBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of request) {
    const buffer = Buffer.from(chunk as Buffer)
    size += buffer.length
    if (size > MAX_REQUEST_BODY_BYTES) {
      throw new HttpError(413, 'payload-too-large', 'request body exceeds the liaison limit')
    }
    chunks.push(buffer)
  }
  return Buffer.concat(chunks).toString('utf8')
}

function respondJson(response: ServerResponse, status: number, payload: unknown): void {
  response.writeHead(status, {
    'content-type': 'application/json',
    'cache-control': 'no-store',
  })
  response.end(`${JSON.stringify(payload)}\n`)
}

function respondFailure(response: ServerResponse, error: unknown): void {
  if (response.headersSent || response.writableEnded) {
    response.destroy()
    return
  }
  const failure: ErrorResponse = {
    ok: false,
    error: {
      code: error instanceof HttpError ? error.code : 'internal-error',
      message: redact(errorMessage(error)),
    },
  }
  respondJson(response, error instanceof HttpError ? error.status : 500, ErrorResponseSchema.parse(failure))
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${label} timed out after ${String(timeoutMs)}ms`))
        }, timeoutMs)
      }),
    ])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function redact(message: string): string {
  return message
    .replace(/([?&\s])token=[^\s&)]+/giu, '$1token=<redacted>')
    .replace(/\b(cookie|set-cookie)\s*:\s*[^\n]+/giu, '$1: <redacted>')
}

function isErrno(error: unknown, code: string): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === code
}

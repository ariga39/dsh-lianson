import { Context } from '@deepseek-ai/cordis'
import { apply as gatewayApply, inject as gatewayInject, type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client'
import type {} from '@deepseek-ai/dsh-api-remotes/types'
import sessionRemote from '@deepseek-ai/dsh-api-session-controller/remote'
import workspaceRemote from '@deepseek-ai/dsh-api-workspace-controller/remote'
import {
  installConnection,
  type ConnectionHandle,
  type ConnectionWebSocket,
  type ConnectionWebSocketFactory,
} from '@deepseek-ai/dsh-client-connection/client'
import commandsRemote from '@deepseek-ai/dsh-commands/remote'
import permissionPresetsRemote from '@deepseek-ai/dsh-permission-presets/remote'
import type { RemoteResult } from '@deepseek-ai/dsh-typert-protocol'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import userQuestionsRemote from '@deepseek-ai/dsh-user-questions/remote'
import WebSocket from 'ws'
import { LiaisonError } from './errors.ts'

/** One authenticated Host target. */
export interface OperatorTarget {
  readonly origin: string
  readonly launchToken: string
}

/** Injection and timing overrides for one operator connection. */
export interface OperatorConnectionOptions {
  readonly fetch?: typeof fetch
  readonly createWebSocket?: ConnectionWebSocketFactory
  readonly reconnectBaseMs?: number
  readonly reconnectMaxMs?: number
  readonly generationTimeoutMs?: number
  /** Per-attempt `/connect` deadline; defaults to 10s. */
  readonly connectTimeoutMs?: number
  readonly onCookie?: (cookie: string) => void
  readonly onWebSocket?: (socket: ConnectionWebSocket, url: string) => void
}

interface OpenedConnection {
  readonly ctx: Context
  readonly remote: ClientRemote
  readonly connection: ConnectionHandle
  readonly cookie: string
  readonly lost: AbortController
  readonly dispose: () => Promise<void>
}

interface ReadyWaiter {
  readonly resolve: () => void
  readonly reject: (error: Error) => void
  readonly timer: NodeJS.Timeout
}

const DEFAULT_RECONNECT_BASE_MS = 100
const DEFAULT_RECONNECT_MAX_MS = 2_000
const DEFAULT_GENERATION_TIMEOUT_MS = 15_000
const DEFAULT_CONNECT_TIMEOUT_MS = 10_000
const DISPOSE_TIMEOUT_MS = 5_000

/**
 * Keep one answering operator connection per Host. Every physical loss disposes
 * the old generation before a fresh `/connect` exchange, so a revoked v2 cookie
 * is never reused and two mux sockets never collide.
 */
export class OperatorConnection {
  private readonly options: OperatorConnectionOptions
  private readonly readyWaiters: ReadyWaiter[] = []
  private target: OperatorTarget | undefined
  private current: OpenedConnection | undefined
  private loopAbort: AbortController | undefined
  private loopDone: Promise<void> | undefined
  private closing = false
  private closingTask: Promise<void> | undefined
  private lastError: Error | undefined
  private changes: Promise<void> = Promise.resolve()
  private readonly foreground = new Set<AbortController>()

  /**
   * @param options - transport overrides and bound reconnect timing.
   */
  constructor(options: OperatorConnectionOptions = {}) {
    this.options = options
  }

  /**
   * Whether the active target is exactly this Host.
   * @param target - origin and launch token.
   * @returns true when the same Host is already targeted.
   */
  matches(target: OperatorTarget): boolean {
    return this.target !== undefined
      && this.target.origin === target.origin
      && this.target.launchToken === target.launchToken
  }

  /**
   * Replace the target, detaching the prior reconnect loop before waiting.
   * @param target - origin and launch token.
   * @param timeoutMs - optional foreground readiness deadline.
   */
  async setHost(target: OperatorTarget, timeoutMs?: number): Promise<void> {
    await this.serializeChange(async () => {
      if (this.closing) throw new LiaisonError('OPERATOR_UNAVAILABLE', 'operator connection is closed')
      await this.detach()
      this.target = target
      this.startLoop()
      if (timeoutMs !== undefined) await this.waitReady(timeoutMs)
    })
  }

  /**
   * Wait for an established generation.
   * @param timeoutMs - foreground deadline.
   */
  async waitReady(timeoutMs: number): Promise<void> {
    if (this.closing) throw new LiaisonError('OPERATOR_UNAVAILABLE', 'operator connection is closed')
    if (this.current !== undefined) return
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.removeWaiter(waiter)
        const details = this.lastError === undefined ? {} : { lastError: this.lastError.message }
        reject(new LiaisonError('OPERATOR_UNAVAILABLE', 'the operator connection did not become ready in time', { details }))
      }, timeoutMs)
      const waiter: ReadyWaiter = { resolve, reject, timer }
      this.readyWaiters.push(waiter)
    })
  }

  /**
   * Run one foreground RPC against the active generation under a deadline.
   * Background retries are never cancelled by the deadline.
   * @param operation - callback receiving the typed Remote surface and its abort signal.
   * @param timeoutMs - foreground deadline.
   * @returns the callback result.
   */
  async withClient<T>(operation: (remote: ClientRemote, signal: AbortSignal) => Promise<RemoteResult<T>>, timeoutMs: number): Promise<RemoteResult<T>> {
    await this.waitReady(timeoutMs)
    if (this.closing) throw new LiaisonError('OPERATOR_UNAVAILABLE', 'operator connection is closed')
    const current = this.current
    if (current === undefined) throw new LiaisonError('OPERATOR_UNAVAILABLE', 'the operator connection is not ready')
    const controller = new AbortController()
    if (this.closing) throw new LiaisonError('OPERATOR_UNAVAILABLE', 'operator connection is closed')
    this.foreground.add(controller)
    const timer = setTimeout(() => { controller.abort() }, timeoutMs)
    try {
      return await Promise.race([
        operation(current.remote, controller.signal),
        abortOutcome(controller.signal, timeoutMs),
      ])
    } finally {
      clearTimeout(timer)
      this.foreground.delete(controller)
    }
  }

  /** Stop reconnecting, close the current socket, and reject pending waits. */
  async close(): Promise<void> {
    if (this.closingTask !== undefined) {
      await this.closingTask
      return
    }
    this.closing = true
    this.target = undefined
    const failure = new LiaisonError('OPERATOR_UNAVAILABLE', 'operator connection closed')
    for (const waiter of this.readyWaiters.splice(0)) {
      clearTimeout(waiter.timer)
      waiter.reject(failure)
    }
    for (const controller of this.foreground) controller.abort(failure)
    this.foreground.clear()
    this.closingTask = this.serializeChange(async () => {
      await this.detach()
    })
    await this.closingTask
  }

  private serializeChange<T>(operation: () => Promise<T>): Promise<T> {
    const start = this.changes
    let release: () => void = () => {}
    this.changes = new Promise<void>((resolve) => { release = resolve })
    return start.then(operation).finally(release)
  }

  private startLoop(): void {
    const target = this.target
    if (target === undefined || this.closing) return
    if (this.loopAbort !== undefined) throw new Error('operator connection already has an active loop')
    const abort = new AbortController()
    this.loopAbort = abort
    this.loopDone = this.runLoop(target, abort.signal).catch((error: unknown) => {
      this.lastError = error instanceof Error ? error : new Error(String(error))
    })
  }

  private async runLoop(target: OperatorTarget, signal: AbortSignal): Promise<void> {
    let attempt = 0
    while (!signal.aborted && !this.closing) {
      let opened: OpenedConnection | undefined
      try {
        opened = await this.open(target, signal)
        if (signal.aborted || this.closing) return
        this.current = opened
        this.lastError = undefined
        this.notifyReady()
        await this.waitForLoss(opened, signal)
      } catch (error) {
        if (!signal.aborted) this.lastError = error instanceof Error ? error : new Error(String(error))
      } finally {
        if (opened !== undefined) {
          if (this.current === opened) this.current = undefined
          await opened.dispose()
        }
      }
      if (signal.aborted || this.closing) return
      attempt += 1
      await sleep(this.backoff(attempt), signal)
    }
  }

  private async open(target: OperatorTarget, signal: AbortSignal): Promise<OpenedConnection> {
    const { cookie, baseUrl } = await exchangeOperatorCredential(
      target,
      this.options.fetch,
      signal,
      this.connectTimeoutMs(),
    )
    this.options.onCookie?.(cookie)
    const ctx = new Context()
    const lost = new AbortController()
    const baseFetch = this.options.fetch ?? globalThis.fetch
    const socketFactory = this.options.createWebSocket
    const createWebSocket = (url: string): ConnectionWebSocket => {
      const socket = socketFactory === undefined
        ? new WebSocket(url, { headers: { cookie } })
        : socketFactory(url)
      socket.addEventListener('close', () => { lost.abort() }, { once: true })
      socket.addEventListener('error', () => { lost.abort() }, { once: true })
      this.options.onWebSocket?.(socket, url)
      return socket
    }
    try {
      installConnection(ctx, {
        interactions: 'answer',
        transport: {
          streamBaseUrl: baseUrl,
          fetch: async (input, init) => {
            const headers = new Headers(init.headers)
            headers.set('cookie', cookie)
            const url = input instanceof URL ? input : new URL(input, baseUrl)
            const response = await baseFetch(url, { ...init, headers })
            if (response.status === 401 && !lost.signal.aborted) {
              // The authenticated lease was revoked. Start a fresh exchange,
              // but never replay this RPC (it may be a mutation).
              lost.abort(new LiaisonError('OPERATOR_UNAVAILABLE', 'operator credential was rejected'))
            }
            return response
          },
          createWebSocket,
        },
      })
      await ctx.plugin(TypertRegistry)
      await ctx.plugin({ apply: gatewayApply, inject: gatewayInject })
      for (const contribution of [
        sessionRemote,
        workspaceRemote,
        commandsRemote,
        permissionPresetsRemote,
        userQuestionsRemote,
      ]) {
        await ctx.remote.$mount(contribution)
      }
      const connection = ctx.get('connection') as ConnectionHandle
      await waitForGeneration(connection, this.generationTimeoutMs(), signal, lost.signal)
      const opened: OpenedConnection = {
        ctx,
        remote: ctx.remote,
        connection,
        cookie,
        lost,
        dispose: async () => {
          await disposeContext(ctx)
        },
      }
      return opened
    } catch (error) {
      await disposeContext(ctx)
      throw error
    }
  }

  private async waitForLoss(opened: OpenedConnection, signal: AbortSignal): Promise<void> {
    await new Promise<void>((resolve) => {
      let settled = false
      const finish = (): void => {
        if (settled) return
        settled = true
        unsubscribe()
        opened.lost.signal.removeEventListener('abort', finish)
        signal.removeEventListener('abort', finish)
        resolve()
      }
      const unsubscribe = opened.connection.generation.subscribe(() => {
        if (opened.connection.generation.getSnapshot() === undefined) finish()
      })
      opened.lost.signal.addEventListener('abort', finish, { once: true })
      signal.addEventListener('abort', finish, { once: true })
      if (opened.connection.generation.getSnapshot() === undefined || opened.lost.signal.aborted || signal.aborted) finish()
    })
  }

  private notifyReady(): void {
    for (const waiter of this.readyWaiters.splice(0)) {
      clearTimeout(waiter.timer)
      waiter.resolve()
    }
  }

  private removeWaiter(waiter: ReadyWaiter): void {
    const index = this.readyWaiters.indexOf(waiter)
    if (index !== -1) this.readyWaiters.splice(index, 1)
  }

  private async detach(): Promise<void> {
    const abort = this.loopAbort
    const done = this.loopDone
    this.loopAbort = undefined
    this.loopDone = undefined
    abort?.abort()
    // Every started loop is awaited: changes are serialized, so this loop is
    // the only one and cannot be abandoned by a shorter timeout.
    if (done !== undefined) await done
  }

  private backoff(attempt: number): number {
    const base = this.options.reconnectBaseMs ?? DEFAULT_RECONNECT_BASE_MS
    const max = this.options.reconnectMaxMs ?? DEFAULT_RECONNECT_MAX_MS
    const cap = Math.min(max, base * 2 ** Math.max(0, attempt - 1))
    return cap / 2 + Math.random() * (cap / 2)
  }

  private generationTimeoutMs(): number {
    return this.options.generationTimeoutMs ?? DEFAULT_GENERATION_TIMEOUT_MS
  }

  private connectTimeoutMs(): number {
    return this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS
  }
}

/**
 * Exchange the Host launch token for one independent answering credential.
 * @param target - Host origin and launch token.
 * @param fetchImpl - fetch implementation, default global fetch.
 * @param signal - cancellation for shutdown/replacement.
 * @returns cookie and Host base URL.
 */
export async function exchangeOperatorCredential(
  target: OperatorTarget,
  fetchImpl: typeof fetch = globalThis.fetch,
  signal?: AbortSignal,
  timeoutMs = DEFAULT_CONNECT_TIMEOUT_MS,
): Promise<{ readonly cookie: string; readonly baseUrl: string }> {
  const connect = new URL('/connect', target.origin)
  connect.searchParams.set('token', target.launchToken)
  connect.searchParams.set('interactions', 'answer')
  const attempt = new AbortController()
  const forwardAbort = (): void => {
    attempt.abort(signal?.reason instanceof Error
      ? signal.reason
      : new LiaisonError('OPERATOR_UNAVAILABLE', 'operator /connect was aborted'))
  }
  if (signal?.aborted === true) forwardAbort()
  else signal?.addEventListener('abort', forwardAbort, { once: true })
  let timer: NodeJS.Timeout | undefined
  const timedOut = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      const error = new LiaisonError('OPERATOR_UNAVAILABLE', `operator /connect did not answer within ${String(timeoutMs)}ms`)
      attempt.abort(error)
      reject(error)
    }, timeoutMs)
  })
  const aborted = new Promise<never>((_resolve, reject) => {
    attempt.signal.addEventListener('abort', () => {
      reject(attempt.signal.reason instanceof Error
        ? attempt.signal.reason
        : new LiaisonError('OPERATOR_UNAVAILABLE', 'operator /connect was aborted'))
    }, { once: true })
  })
  try {
    const response = await Promise.race([
      fetchImpl(connect, { redirect: 'manual', signal: attempt.signal }),
      timedOut,
      aborted,
    ])
    if (response.status !== 302) {
      throw new LiaisonError('OPERATOR_UNAVAILABLE', `operator /connect returned HTTP ${String(response.status)}`)
    }
    const setCookie = response.headers.get('set-cookie')
    const cookie = setCookie?.split(';', 1)[0]
    if (cookie === undefined || cookie === '') {
      throw new LiaisonError('OPERATOR_UNAVAILABLE', 'operator /connect returned no credential')
    }
    return { cookie, baseUrl: new URL('/', target.origin).href }
  } finally {
    if (timer !== undefined) clearTimeout(timer)
    signal?.removeEventListener('abort', forwardAbort)
  }
}

async function waitForGeneration(
  connection: ConnectionHandle,
  timeoutMs: number,
  signal: AbortSignal,
  lost?: AbortSignal,
): Promise<void> {
  if (connection.generation.getSnapshot() !== undefined) return
  if (lost?.aborted === true) {
    throw new LiaisonError('OPERATOR_UNAVAILABLE', 'the operator transport closed before generation readiness')
  }
  await new Promise<void>((resolve, reject) => {
    let settled = false
    const finish = (error?: Error): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      unsubscribe()
      signal.removeEventListener('abort', onAbort)
      lost?.removeEventListener('abort', onLost)
      if (error === undefined) resolve()
      else reject(error)
    }
    const onAbort = (): void => { finish(new Error('operator connection was aborted')) }
    const onLost = (): void => {
      finish(new LiaisonError('OPERATOR_UNAVAILABLE', 'the operator transport closed before generation readiness'))
    }
    const unsubscribe = connection.generation.subscribe(() => {
      if (connection.generation.getSnapshot() !== undefined) finish()
    })
    const timer = setTimeout(() => {
      finish(new LiaisonError('OPERATOR_UNAVAILABLE', 'the operator generation was not ready in time'))
    }, timeoutMs)
    signal.addEventListener('abort', onAbort, { once: true })
    lost?.addEventListener('abort', onLost, { once: true })
    if (connection.generation.getSnapshot() !== undefined || lost?.aborted === true) finish()
  })
}

async function disposeContext(ctx: Context): Promise<void> {
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, DISPOSE_TIMEOUT_MS)
    void ctx.fiber.dispose().then(
      () => { clearTimeout(timer); resolve() },
      () => { clearTimeout(timer); resolve() },
    )
  })
}

function abortOutcome(signal: AbortSignal, timeoutMs: number): Promise<never> {
  return new Promise((_resolve, reject) => {
    const fail = (): void => {
      signal.removeEventListener('abort', fail)
      const reason: unknown = signal.reason
      reject(reason instanceof Error
        ? reason
        : new LiaisonError('OPERATOR_UNAVAILABLE', `the foreground operator call did not finish within ${String(timeoutMs)}ms`))
    }
    if (signal.aborted) fail()
    else signal.addEventListener('abort', fail, { once: true })
  })
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    let settled = false
    const finish = (): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      signal.removeEventListener('abort', finish)
      resolve()
    }
    const timer = setTimeout(finish, ms)
    signal.addEventListener('abort', finish, { once: true })
  })
}

/**
 * Loopback lifecycle: collect the authenticated entry URL, exchange it for a
 * cookie, connect the Gateway client, and retry cleanly when `dsh` is not
 * ready. Timers and in-flight connects are owned by this object.
 */

import { LiaisonError } from './errors.ts'
import type { DshRemote, LoopbackState, LoopbackStatus } from './remote.ts'
import { selectAuthenticatedEntry } from './dsh-remote.ts'
import type { EntryUrl } from './dsh-remote.ts'

/** Behaviors injected so unit tests never touch a live Host. */
export interface LoopbackOptions {
  /** Collect current entry URLs for the advertised base URL. */
  readonly collectEntries: () => readonly EntryUrl[]
  /** Exchange a token entry for an in-memory cookie and origin. */
  readonly exchange: (entryUrl: string, signal: AbortSignal) => Promise<{ origin: string; cookie: string }>
  /** Connect the Gateway client; abort must release the connection. */
  readonly connect: (origin: string, cookie: string, signal: AbortSignal) => Promise<DshRemote>
  /** Retry delays in milliseconds; the last entry repeats. */
  readonly retryDelaysMs?: readonly number[]
  /** Maximum connect attempts; defaults to one for every retry delay. */
  readonly maxAttempts?: number
  /** Observes every state transition, for diagnostics and tests. */
  readonly onStatus?: (status: LoopbackStatus) => void
  /** Abortable sleep; defaults to a real timer. */
  readonly sleep?: (ms: number, signal: AbortSignal) => Promise<void>
}

const DEFAULT_RETRY_DELAYS = [250, 500, 1_000, 2_000, 5_000] as const

/** One serialized loopback connect loop. */
export class LoopbackClient {
  private currentStatus: LoopbackStatus = { state: 'idle' }
  private remote: DshRemote | undefined
  private readonly abort = new AbortController()
  private running: Promise<void> | undefined
  private disposed = false

  private readonly options: LoopbackOptions

  constructor(options: LoopbackOptions) {
    this.options = options
  }

  /** @returns the latest lifecycle status. */
  status(): LoopbackStatus {
    return this.currentStatus
  }

  /** Begin connecting without blocking plugin activation. */
  start(): void {
    if (this.running !== undefined || this.disposed) return
    this.running = this.runTracked()
  }

  /**
   * Join the active connect round, or start a fresh lazy round when none is
   * running. This keeps the first command after Host readiness from failing
   * while the plugin's asynchronous authentication handshake is still in
   * flight.
   * @returns the connected remote.
   * @throws {LiaisonError} `dsh-unavailable` when the round settles unconnected.
   */
  async requireRemote(): Promise<DshRemote> {
    if (this.remote !== undefined) return this.remote
    if (this.disposed) throw unavailable(this.currentStatus)
    if (this.running === undefined) this.start()
    await this.running
    if (this.remote !== undefined) return this.remote
    throw unavailable(this.currentStatus)
  }

  /**
   * Abort retries and in-flight connects, then release the connected remote.
   * Idempotent.
   */
  async dispose(): Promise<void> {
    if (this.disposed) return
    this.disposed = true
    this.abort.abort()
    await this.running?.catch(() => undefined)
    const remote = this.remote
    this.remote = undefined
    this.publish({ state: 'disposed' })
    if (remote !== undefined) await remote.dispose().catch(() => undefined)
  }

  private async runTracked(): Promise<void> {
    try {
      await this.run()
    } finally {
      this.running = undefined
    }
  }

  private async run(): Promise<void> {
    const delays = this.options.retryDelaysMs ?? DEFAULT_RETRY_DELAYS
    const maxAttempts = this.options.maxAttempts ?? Math.max(delays.length, 1)
    let lastError = this.currentStatus.lastError ?? 'dsh is not ready'
    for (let attempt = 0; attempt < maxAttempts && !this.disposed; attempt += 1) {
      this.publish({ state: 'connecting', ...(attempt === 0 ? {} : { lastError }) })
      try {
        const entryUrl = this.entryUrl()
        if (entryUrl === undefined) throw new LiaisonError('dsh-unavailable', 'dsh exposes no authenticated entry URL')
        const { origin, cookie } = await this.options.exchange(entryUrl, this.abort.signal)
        if (this.disposed) return
        const remote = await this.options.connect(origin, cookie, this.abort.signal)
        if (this.disposed) {
          await remote.dispose()
          return
        }
        this.remote = remote
        this.publish({ state: 'connected', origin })
        return
      } catch (error) {
        if (this.disposed || this.abort.signal.aborted) return
        lastError = error instanceof Error ? error.message : String(error)
      }
      this.publish({ state: 'unavailable', lastError })
      if (attempt + 1 >= maxAttempts) return
      const delay = delays[Math.min(attempt, delays.length - 1)] ?? 0
      try {
        await (this.options.sleep ?? sleep)(delay, this.abort.signal)
      } catch {
        return
      }
    }
  }

  private entryUrl(): string | undefined {
    const entries = this.options.collectEntries()
    return selectAuthenticatedEntry(entries)
  }

  private publish(status: LoopbackStatus): void {
    this.currentStatus = status
    this.options.onStatus?.(status)
  }
}

function unavailable(status: LoopbackStatus): LiaisonError {
  return new LiaisonError('dsh-unavailable', status.lastError ?? 'dsh loopback is not connected', { state: status.state })
}

/** Abortable timer owned by the loopback lifetime. */
export function sleep(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(new LiaisonError('dsh-unavailable', 'loopback disposed'))
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    const onAbort = (): void => {
      clearTimeout(timer)
      reject(new LiaisonError('dsh-unavailable', 'loopback disposed'))
    }
    signal.addEventListener('abort', onAbort, { once: true })
  })
}

export type { LoopbackState }

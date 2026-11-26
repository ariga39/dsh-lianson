/** Answering operator connection with single-flight Context/cookie replacement on transport loss. */

import { LiaisonError } from './errors.mjs'
import { redactSecrets } from './redaction.mjs'
import { assertLoopbackOrigin } from './host-manager.mjs'
import { loadUpstream } from './upstream.mjs'

function sleep(ms, signal) {
  return new Promise(resolve => {
    if (signal?.aborted === true) {
      resolve()
      return
    }
    let timer
    const done = () => {
      if (timer !== undefined) clearTimeout(timer)
      signal?.removeEventListener('abort', abort)
      resolve()
    }
    const abort = () => done()
    timer = setTimeout(done, ms)
    signal?.addEventListener('abort', abort, { once: true })
  })
}

function unauthorized(error) {
  const message = error instanceof Error ? error.message : String(error)
  return /HTTP 401\b/u.test(message) || error?.status === 401
}

function sameSocketAuthority(target, origin) {
  let candidate
  let expected
  try {
    candidate = new URL(target)
    expected = new URL(origin)
  } catch {
    return false
  }
  const protocol = expected.protocol === 'https:' ? 'wss:' : 'ws:'
  return (candidate.protocol === protocol || candidate.protocol === expected.protocol)
    && candidate.username === '' && candidate.password === ''
    && candidate.hostname === expected.hostname
    && candidate.port === expected.port
}

/**
 * One answering operator over a managed Host.
 *
 * A physical `/api/remote.mux` close revokes the v2 cookie on the Host, so a
 * reconnect first disposes the old Context and then fetches a fresh cookie
 * from `/connect` before installing a replacement Context. Concurrent drops
 * share a single reconnect promise.
 */
export class OperatorConnection {
  constructor({
    host = null,
    hostManager = null,
    upstream = null,
    createWebSocket,
    fetch: fetchImpl = globalThis.fetch,
    reconnectDelayMs = 0,
    reconnectMaxDelayMs = 1_000,
    readyTimeoutMs = 30_000,
    foregroundWaitMs = 10_000,
  } = {}) {
    this.host = host
    this.hostManager = hostManager
    this.upstream = upstream
    this.createWebSocket = createWebSocket
    this.fetchImpl = fetchImpl
    this.reconnectDelayMs = reconnectDelayMs
    this.reconnectMaxDelayMs = reconnectMaxDelayMs
    this.readyTimeoutMs = readyTimeoutMs
    this.foregroundWaitMs = foregroundWaitMs

    this.generation = undefined
    this.opening = undefined
    this.reconnectPromise = undefined
    this.stopping = false
    this.lifecycle = new AbortController()
    this.connectedGeneration = 0
    this.connectCount = 0
    this.reconnectCount = 0
    this.lastError = undefined
    this.hooks = new Set()
  }

  /** Register a Context hook replayed on every initial connection and reconnect. */
  onContextReady(hook) {
    this.hooks.add(hook)
    return () => { this.hooks.delete(hook) }
  }

  /**
   * Replace the Host handle. A different pid or origin invalidates the current
   * Context; a rotated token on the same Host is picked up on the next reconnect.
   */
  async setHost(host) {
    const changed = this.host !== null
      && (this.host.pid !== host.pid || this.host.origin !== host.origin)
    this.host = host
    if (changed) {
      if (this.reconnectPromise !== undefined) await this.reconnectPromise.catch(() => {})
      await this.#disposeCurrent()
    }
  }

  async #getUpstream() {
    if (this.upstream === null) this.upstream = await loadUpstream()
    return this.upstream
  }

  async #currentHost() {
    let host = this.host
    if (this.hostManager !== null) {
      const current = await this.hostManager.readOwnedHost()
      if (current === null) throw new LiaisonError('HOST_UNAVAILABLE', 'Managed Host record is missing')
      if (confirmOrigin(host, current)) throw new LiaisonError('HOST_ORIGIN_CHANGED',
        'Managed Host moved to a different loopback origin; refusing an automatic cross-origin reconnect')
      host = current
      this.host = current
    }
    if (host === null) throw new LiaisonError('HOST_UNAVAILABLE', 'No managed Host is available')
    assertLoopbackOrigin(host.origin)
    return host
  }

  async ensureConnected({ timeoutMs = this.foregroundWaitMs } = {}) {
    if (this.stopping) throw new LiaisonError('OPERATOR_STOPPED', 'Operator connection is closed')
    if (this.reconnectPromise !== undefined) {
      await this.#waitForReconnect(this.reconnectPromise, timeoutMs)
      if (this.generation?.ready === true) return
    }
    if (this.generation?.ready === true) return
    await this.#waitForReconnect(
      this.#scheduleReconnect(new LiaisonError('OPERATOR_DISCONNECTED', 'Operator is not connected')),
      timeoutMs,
    )
  }

  async #waitForReconnect(promise, timeoutMs) {
    if (timeoutMs === undefined || timeoutMs <= 0) return promise
    let timer
    let abort
    try {
      return await Promise.race([
        promise,
        new Promise((_resolve, reject) => {
          timer = setTimeout(() => {
            reject(new LiaisonError('OPERATOR_RECONNECTING',
              'Operator is still reconnecting in the background'))
          }, timeoutMs)
          timer.unref?.()
          abort = () => reject(new LiaisonError('OPERATOR_STOPPED', 'Operator connection is closed'))
          this.lifecycle.signal.addEventListener('abort', abort, { once: true })
        }),
      ])
    } finally {
      if (timer !== undefined) clearTimeout(timer)
      if (abort !== undefined) this.lifecycle.signal.removeEventListener('abort', abort)
    }
  }

  /**
   * Run one RPC against the active Context. No mutation is retried after it
   * starts: a 401 schedules recovery for the next caller, never a blind replay.
   */
  async withClient(fn) {
    await this.ensureConnected()
    const generation = this.generation
    if (generation === undefined) throw new LiaisonError('OPERATOR_DISCONNECTED', 'Operator has no Context')
    try {
      return await fn(generation.ctx.remote, generation.ctx)
    } catch (error) {
      if (unauthorized(error)) {
        this.#onTransportFailure(generation,
          new LiaisonError('UNAUTHORIZED', 'Host rejected the active credential after transport loss'))
      }
      throw error
    }
  }

  snapshot() {
    const generation = this.generation
    const connected = generation?.ready === true && generation.disposed !== true
    return {
      connected,
      generation: this.connectedGeneration,
      connectCount: this.connectCount,
      reconnectCount: this.reconnectCount,
      hostPid: this.host?.pid ?? null,
      origin: this.host?.origin ?? null,
      lastError: this.lastError === undefined ? null : {
        code: typeof this.lastError.code === 'string' ? this.lastError.code : 'CONNECTION_ERROR',
        message: redactSecrets(this.lastError instanceof Error ? this.lastError.message : String(this.lastError),
          this.#secrets()),
      },
    }
  }

  #secrets() {
    return [this.host?.launchToken, this.generation?.cookie].filter(secret => typeof secret === 'string')
  }

  #scheduleReconnect(reason) {
    if (this.stopping) return Promise.reject(new LiaisonError('OPERATOR_STOPPED', 'Operator connection is closed'))
    if (this.reconnectPromise !== undefined) return this.reconnectPromise
    this.lastError = reason
    const promise = this.#runReconnect(reason)
    this.reconnectPromise = promise
    const clear = () => {
      if (this.reconnectPromise === promise) this.reconnectPromise = undefined
    }
    promise.then(clear, clear)
    // Keep a background drop from surfacing as an unhandled rejection.
    void promise.catch(() => {})
    return promise
  }

  async #runReconnect(reason) {
    const previous = this.generation
    this.generation = undefined
    if (previous !== undefined) {
      previous.disposed = true
      await previous.ctx.fiber.dispose().catch(() => {})
    }

    let attempt = 0
    let delay = previous === undefined ? 0 : Math.max(0, this.reconnectDelayMs)
    while (true) {
      if (this.stopping || this.lifecycle.signal.aborted) {
        throw new LiaisonError('OPERATOR_STOPPED', 'Operator connection is closed')
      }
      attempt += 1
      if (delay > 0) {
        await sleep(delay + Math.floor(Math.random() * Math.min(50, delay / 2)), this.lifecycle.signal)
      }
      if (this.stopping || this.lifecycle.signal.aborted) {
        throw new LiaisonError('OPERATOR_STOPPED', 'Operator connection is closed')
      }
      try {
        const host = await this.#currentHost()
        const generation = await this.#openGeneration(host)
        this.generation = generation
        this.connectedGeneration += 1
        this.connectCount += 1
        if (this.connectCount > 1) this.reconnectCount += 1
        this.lastError = undefined
        return
      } catch (error) {
        this.lastError = error
        const code = typeof error?.code === 'string' ? error.code : ''
        if (code === 'HOST_ORIGIN_CHANGED' || code === 'HOST_ORIGIN_UNSAFE'
          || code === 'OPERATOR_STOPPED') throw error
        // Recovery stays active in the background with a capped backoff until it
        // succeeds or close() cancels it, including the first daemon attach.
        const base = Math.max(50, this.reconnectDelayMs)
        delay = Math.min(this.reconnectMaxDelayMs, base * 2 ** Math.min(attempt - 1, 10))
      }
    }
  }

  async #openGeneration(host) {
    if (this.stopping || this.lifecycle.signal.aborted) {
      throw new LiaisonError('OPERATOR_STOPPED', 'Operator connection is closed')
    }
    const upstream = await this.#getUpstream()
    const generation = {
      ctx: undefined,
      cookie: undefined,
      ready: false,
      disposed: false,
      failed: undefined,
      host,
    }
    this.opening = generation
    try {
      const ctx = new upstream.Context()
      generation.ctx = ctx
      const cookie = await this.#exchange(host)
      generation.cookie = cookie

      const transport = {
        streamBaseUrl: `${host.origin}/`,
        fetch: (input, init = {}) => this.#transportFetch(generation, host, cookie, input, init),
        createWebSocket: target => this.#createSocket(upstream, generation, host, cookie, target),
      }
      upstream.installConnection(ctx, { interactions: 'answer', transport })
      await ctx.plugin(upstream.TypertRegistry)
      await ctx.plugin({ apply: upstream.gateway.apply, inject: upstream.gateway.inject })
      for (const contribution of upstream.remotes) await ctx.remote.$mount(contribution)
      for (const hook of this.hooks) await hook(ctx)
      await this.#waitReady(generation)
      generation.ready = true
      return generation
    } catch (error) {
      await this.#disposeGeneration(generation)
      throw error
    } finally {
      if (this.opening === generation) this.opening = undefined
    }
  }

  async #waitReady(generation) {
    const deadline = Date.now() + this.readyTimeoutMs
    while (Date.now() < deadline) {
      if (this.stopping || this.lifecycle.signal.aborted) {
        throw new LiaisonError('OPERATOR_STOPPED', 'Operator connection is closed')
      }
      if (generation.disposed) throw new LiaisonError('CONNECTION_DISPOSED', 'Connection generation was disposed')
      if (generation.failed !== undefined) throw generation.failed
      if (generation.ctx?.get('connection')?.generation.getSnapshot() !== undefined) return
      await sleep(50, this.lifecycle.signal)
    }
    throw new LiaisonError('CONNECTION_TIMEOUT', 'Operator connection did not become ready')
  }

  async #exchange(host) {
    const origin = assertLoopbackOrigin(host.origin)
    if (typeof host.launchToken !== 'string' || host.launchToken === '') {
      throw new LiaisonError('CONNECT_FAILED', 'Host record has no launch token')
    }
    const url = new URL('/connect', `${origin}/`)
    url.searchParams.set('token', host.launchToken)
    url.searchParams.set('interactions', 'answer')
    let response
    try {
      response = await this.fetchImpl(url, {
        method: 'GET',
        redirect: 'manual',
        headers: { accept: 'application/json' },
        signal: AbortSignal.any([AbortSignal.timeout(15_000), this.lifecycle.signal]),
      })
    } catch (error) {
      if (this.stopping || this.lifecycle.signal.aborted) {
        throw new LiaisonError('OPERATOR_STOPPED', 'Operator connection is closed')
      }
      throw new LiaisonError('CONNECT_FAILED', 'Could not reach the Host /connect endpoint', { cause: String(error?.code ?? '') })
    }
    const rawCookies = typeof response.headers.getSetCookie === 'function'
      ? response.headers.getSetCookie()
      : []
    await response.arrayBuffer().catch(() => {})
    if (response.status !== 302 || rawCookies.length === 0) {
      throw new LiaisonError('CONNECT_FAILED', `Host /connect returned HTTP ${String(response.status)}`)
    }
    const cookie = rawCookies[0].split(';', 1)[0].trim()
    if (!/^dsh-auth-[A-Za-z0-9_-]+=v2\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/u.test(cookie)) {
      throw new LiaisonError('CONNECT_FAILED', 'Host /connect returned an invalid v2 cookie')
    }
    return cookie
  }

  async #transportFetch(generation, host, cookie, input, init) {
    if (generation.disposed || this.generation !== generation && this.opening !== generation) {
      throw new LiaisonError('CONNECTION_DISPOSED', 'Connection generation is no longer active')
    }
    let url
    try { url = new URL(String(input), `${host.origin}/`) } catch {
      throw new LiaisonError('ORIGIN_MISMATCH', 'RPC target is not a valid URL')
    }
    if (url.origin !== assertLoopbackOrigin(host.origin)) {
      throw new LiaisonError('ORIGIN_MISMATCH', 'RPC target escaped the managed Host origin')
    }
    const headers = new Headers(init.headers)
    headers.set('cookie', cookie)
    const response = await this.fetchImpl(url, { ...init, headers, redirect: 'manual' })
    if (response.status === 401 || response.status === 409) {
      this.#onTransportFailure(generation, new LiaisonError('UNAUTHORIZED',
        `Host transport returned HTTP ${String(response.status)}`))
    }
    return response
  }

  #createSocket(upstream, generation, host, cookie, target) {
    if (!sameSocketAuthority(target, host.origin)) {
      throw new LiaisonError('ORIGIN_MISMATCH', 'WebSocket target escaped the managed Host origin')
    }
    const create = this.createWebSocket
      ?? ((url, options) => new upstream.WebSocket(url, options))
    const socket = create(target, { headers: { cookie } })
    const failed = (error) => this.#onTransportFailure(generation, error)
    socket.addEventListener('close', () => {
      failed(new LiaisonError('SOCKET_CLOSED', 'Host WebSocket closed unexpectedly'))
    }, { once: true })
    if (typeof socket.on === 'function') {
      socket.on('unexpected-response', (_request, response) => {
        const status = response.statusCode
        response.resume?.()
        if (status === 401 || status === 409) {
          failed(new LiaisonError('SOCKET_REJECTED', `Host WebSocket rejected with HTTP ${String(status)}`))
        }
      })
    }
    return socket
  }

  #onTransportFailure(generation, error) {
    if (generation.disposed || this.stopping) return
    generation.failed ??= error
    this.lastError = error
    if (this.generation === generation && generation.ready) {
      this.#scheduleReconnect(error)
    }
  }

  async #disposeGeneration(generation) {
    if (generation === undefined || generation.disposed) return
    generation.disposed = true
    if (generation.ctx !== undefined) await generation.ctx.fiber.dispose().catch(() => {})
  }

  async #disposeCurrent() {
    const generation = this.generation
    this.generation = undefined
    await this.#disposeGeneration(generation)
  }

  async close() {
    this.stopping = true
    this.lifecycle.abort(new LiaisonError('OPERATOR_STOPPED', 'Operator connection is closed'))
    if (this.reconnectPromise !== undefined) await this.reconnectPromise.catch(() => {})
    await this.#disposeCurrent()
    await this.#disposeGeneration(this.opening)
    this.opening = undefined
  }
}

function confirmOrigin(previous, current) {
  return previous !== null && previous.origin !== current.origin
}

import { writeFile } from 'node:fs/promises'
import type { Duplex } from 'node:stream'
import { setTimeout as delay } from 'node:timers/promises'
import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-host-webserver'

interface FixtureConfig {
  readonly evidencePath: string
}

interface Handshake {
  readonly cookie: string
  readonly status: number
  readonly authorized: boolean
}

interface Evidence {
  readonly before: { readonly status: number; readonly authorized: boolean }
  readonly oldCookieStatus: number
  readonly fresh: { readonly status: number; readonly authorized: boolean }
  readonly cookieRotated: boolean
  readonly mux409Count: number
}

interface WritableSocket {
  write: (...args: unknown[]) => unknown
  end: (...args: unknown[]) => unknown
}

const MUX_PATH = '/api/remote.mux'
const PROBE_PATH = '/api/session/modelCatalog'

/**
 * Test-only fixture: observe the real plugin mux handshake and credentials in
 * memory, destroy upgraded sockets on SIGUSR2, and write status-only evidence.
 */
export function apply(ctx: Context, config: FixtureConfig): void {
  let before: Handshake | undefined
  let fresh: Handshake | undefined
  let oldCookieStatus: number | undefined
  let mux409Count = 0
  let origin: string | undefined
  let published = false

  const publish = (): void => {
    if (published || before === undefined || fresh === undefined || oldCookieStatus === undefined) return
    published = true
    const evidence: Evidence = {
      before: { status: before.status, authorized: before.authorized },
      oldCookieStatus,
      fresh: { status: fresh.status, authorized: fresh.authorized },
      cookieRotated: before.cookie !== fresh.cookie,
      mux409Count,
    }
    void writeFile(config.evidencePath, `${JSON.stringify(evidence)}\n`, 'utf8')
  }

  const observe = (cookie: string, status: number, authorized: boolean): void => {
    if (before === undefined) {
      if (status === 101 && authorized) before = { cookie, status, authorized }
      return
    }
    if (status === 409) mux409Count += 1
    if (status === 101 && authorized && cookie !== before.cookie) fresh = { cookie, status, authorized }
    publish()
  }

  const drop = (): void => {
    const owner = ctx.get('webServer') as unknown as { upgradedSockets?: ReadonlySet<{ destroy(): void }> } | undefined
    for (const socket of owner?.upgradedSockets ?? []) socket.destroy()
    const oldCookie = before?.cookie
    if (oldCookie === undefined || origin === undefined) return
    void (async () => {
      const deadline = Date.now() + 10_000
      let status = 0
      while (Date.now() < deadline) {
        status = await probe(origin, oldCookie)
        if (status === 401) break
        await delay(50)
      }
      oldCookieStatus = status
      publish()
    })()
  }

  ctx.on('webserver/upgrade', async (request, socket, _head, peer, next) => {
    const path = request.url?.split('?', 1)[0]
    if (path !== MUX_PATH) {
      await next()
      return
    }
    const cookie = headerValue(request.headers.cookie)
    if (origin === undefined && request.headers.host !== undefined) origin = `http://${request.headers.host}`
    if (cookie !== undefined) instrument(socket, status => { observe(cookie, status, peer !== undefined) })
    await next()
  }, { prepend: true })

  process.on('SIGUSR2', drop)
  ctx.effect(() => () => {
    process.off('SIGUSR2', drop)
  }, 'mux-observer fixture')
}

function instrument(socket: Duplex, report: (status: number) => void): void {
  const writable = socket as unknown as WritableSocket
  let reported = false
  const record = (chunk: unknown): void => {
    if (reported) return
    const status = statusOf(chunk)
    if (status === undefined) return
    reported = true
    report(status)
  }
  const write = writable.write.bind(writable)
  writable.write = (...args: unknown[]) => {
    record(args[0])
    return write(...args)
  }
  const end = writable.end.bind(writable)
  writable.end = (...args: unknown[]) => {
    record(args[0])
    return end(...args)
  }
}

function statusOf(chunk: unknown): number | undefined {
  const text = typeof chunk === 'string'
    ? chunk
    : Buffer.isBuffer(chunk)
      ? chunk.toString('latin1')
      : chunk instanceof Uint8Array
        ? Buffer.from(chunk).toString('latin1')
        : ''
  const match = /^HTTP\/1\.1 (\d{3})/u.exec(text)
  return match?.[1] === undefined ? undefined : Number(match[1])
}

function headerValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value.join('; ') : value
}

async function probe(origin: string, cookie: string): Promise<number> {
  try {
    const response = await fetch(new URL(PROBE_PATH, origin), {
      method: 'POST',
      headers: { cookie, 'content-type': 'application/json' },
      body: '{}',
      signal: AbortSignal.timeout(5_000),
    })
    await response.body?.cancel()
    return response.status
  } catch {
    return 0
  }
}

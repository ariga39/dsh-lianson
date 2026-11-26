import { randomUUID } from 'node:crypto'
import { chmod, rename, rm, writeFile } from 'node:fs/promises'
import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-cmdline'
import type { WebEntryUrl } from '@deepseek-ai/dsh-host-webserver'

/** One successful or explicit startup observation. */
export interface ObserverReadyRecord {
  readonly ok: boolean
  readonly launchId: string
  readonly pid: number
  readonly origin?: string
  readonly port?: number
  readonly launchToken?: string
  readonly reason?: string
}

/** Configuration in the secret-free `--patch` insert row. */
export interface ObserverConfig {
  readonly readyFile: string
  readonly launchId: string
}

/** Stable plugin name; the patch row id is also unique. */
export const name = 'dsh-liaison-startup-observer'
/** Host services required before the observer can publish. */
export const inject = ['webServer', 'appReady'] as const

const observed = new WeakSet<Context>()

/**
 * Publish the bound loopback origin and authenticated entry token after the
 * launcher commits successful startup. Failed or aborted startup never runs it.
 * @param ctx - Host plugin context.
 * @param config - one-shot ready file and launch identity.
 */
export function apply(ctx: Context, config: ObserverConfig): void {
  if (observed.has(ctx)) return
  observed.add(ctx)
  const ready = ctx.get('appReady')
  if (ready === undefined) throw new Error('dsh-liaison-startup-observer: appReady is unavailable')
  ready.onReady(() => {
    void publish(ctx, config)
  })
}

async function publish(ctx: Context, config: ObserverConfig): Promise<void> {
  try {
    const webServer = ctx.get('webServer')
    if (webServer === undefined) throw new Error('webServer is unavailable after readiness')
    const origin = webServer.getOrigin()
    const entries: WebEntryUrl[] = []
    ctx.emit(webServer, 'webserver/entry-url', `${origin}/`, entries)
    const candidate = entries.find((entry) => {
      let url: URL
      try {
        url = new URL(entry.url)
      } catch {
        return false
      }
      return url.origin === origin && url.pathname === '/'
        && url.searchParams.getAll('token').length === 1
        && (url.searchParams.get('token') ?? '') !== ''
    })
    if (candidate === undefined) {
      await writeRecord(config, { ok: false, launchId: config.launchId, pid: process.pid, reason: 'HOST_AUTH_UNAVAILABLE' })
      return
    }
    const token = new URL(candidate.url).searchParams.get('token')
    const port = webServer.port
    if (token === null || token === '' || port === undefined || !Number.isSafeInteger(port) || port < 1 || port > 65_535) {
      await writeRecord(config, { ok: false, launchId: config.launchId, pid: process.pid, reason: 'HOST_AUTH_UNAVAILABLE' })
      return
    }
    await writeRecord(config, {
      ok: true,
      launchId: config.launchId,
      pid: process.pid,
      origin,
      port,
      launchToken: token,
    })
  } catch {
    await writeRecord(config, {
      ok: false,
      launchId: config.launchId,
      pid: process.pid,
      reason: 'HOST_OBSERVER_FAILED',
    }).catch(() => undefined)
  }
}

async function writeRecord(config: ObserverConfig, record: ObserverReadyRecord): Promise<void> {
  const temporary = `${config.readyFile}.${String(process.pid)}.${randomUUID()}.tmp`
  try {
    await writeFile(temporary, JSON.stringify(record), { encoding: 'utf8', mode: 0o600 })
    await chmod(temporary, 0o600)
    await rename(temporary, config.readyFile)
  } catch (error) {
    await rm(temporary, { force: true }).catch(() => undefined)
    throw error
  }
}

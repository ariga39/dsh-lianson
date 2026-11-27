/** In-process Cordis plugin entry for `dsh-liaison`. */

import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-host-webserver'
import type {} from '@deepseek-ai/dsh-sandbox-policy'
import type { WebEntryUrl } from '@deepseek-ai/dsh-host-webserver'
import { startLiaison } from './start.ts'

/** Stable Cordis plugin name. */
export const name = 'dsh-liaison'

/** The plugin needs the Host Web listener to authenticate and advertise itself. */
export const inject = ['webServer']

/**
 * Start the liaison socket and loopback connection, and dispose both with the
 * plugin fiber.
 * @param ctx - Host plugin context.
 */
export async function apply(ctx: Context): Promise<void> {
  const runtime = await startLiaison({
    env: process.env,
    origin: () => {
      try {
        return ctx.webServer.getOrigin()
      } catch {
        return undefined
      }
    },
    collectEntries: (baseUrl) => {
      const entries: WebEntryUrl[] = []
      ctx.emit(ctx.webServer, 'webserver/entry-url', baseUrl, entries)
      return entries
    },
    sandboxPolicy: () => ctx.get('sandboxPolicy')?.resolve(),
    logger: ctx.logger,
  })
  ctx.effect(() => () => runtime.dispose(), 'dsh-liaison: socket, loopback, and ledger')
}

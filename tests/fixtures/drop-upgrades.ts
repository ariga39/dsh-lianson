import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-host-webserver'

interface UpgradeOwner {
  readonly upgradedSockets?: ReadonlySet<{ destroy(): void }>
}

/**
 * Test-only fixture: destroy every upgraded WebSocket when the Host receives
 * SIGUSR2, modeling an external `/api/remote.mux` loss without product hooks.
 */
export function apply(ctx: Context): void {
  const drop = (): void => {
    const owner = ctx.get('webServer') as unknown as UpgradeOwner | undefined
    for (const socket of owner?.upgradedSockets ?? []) socket.destroy()
  }
  process.on('SIGUSR2', drop)
  ctx.effect(() => () => {
    process.off('SIGUSR2', drop)
  }, 'drop-upgrades fixture')
}

/** Stable user-only socket location shared by the plugin and the CLI. */
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'

/** Environment override used by tests and operators to name the exact socket. */
export const LIAISON_SOCKET_ENV = 'DSH_LIAISON_SOCKET'
/** Directory name under the runtime or state root. */
export const LIAISON_DIR_NAME = 'dsh-liaison'
/** Stable socket file name. */
export const LIAISON_SOCKET_NAME = 'dsh-liaison.sock'

/**
 * Resolve the liaison socket path.
 *
 * Precedence: an explicit `DSH_LIAISON_SOCKET`, then `$XDG_RUNTIME_DIR`,
 * then `$HOME/.local/state`. Blank values are treated as unset.
 * @param env - environment mapping to read.
 * @returns the absolute socket path.
 */
export function resolveLiaisonSocketPath(env: NodeJS.ProcessEnv = process.env): string {
  const override = env[LIAISON_SOCKET_ENV]?.trim()
  if (override !== undefined && override !== '') return resolve(override)
  const runtime = env.XDG_RUNTIME_DIR?.trim()
  if (runtime !== undefined && runtime !== '') {
    return join(resolve(runtime), LIAISON_DIR_NAME, LIAISON_SOCKET_NAME)
  }
  const home = env.HOME?.trim()
  const homeRoot = home === undefined || home === '' ? homedir() : home
  return join(resolve(homeRoot), '.local', 'state', LIAISON_DIR_NAME, LIAISON_SOCKET_NAME)
}

/**
 * Derive the lifetime lock file path for one socket.
 * @param socketPath - absolute socket path.
 * @returns the lock file path, a sibling that is never unlinked.
 */
export function resolveLiaisonLockPath(socketPath: string): string {
  return `${socketPath}.lock`
}

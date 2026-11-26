import { homedir } from 'node:os'
import { join, resolve } from 'node:path'

/** Absolute locations owned by one dsh-liaison state directory. */
export interface StatePaths {
  readonly stateDir: string
  readonly socket: string
  readonly daemonLock: string
  readonly hostLock: string
  readonly hostRecord: string
  readonly observerPatch: string
  readonly readyFile: string
}

/**
 * Resolve the single-user state directory.
 * Precedence: `DSH_LIAISON_STATE_DIR`, `$XDG_RUNTIME_DIR/dsh-liaison`, `$HOME/.local/state/dsh-liaison`.
 * @param env - environment mapping to read.
 * @returns an absolute state-directory path.
 */
export function resolveStateDir(env: NodeJS.ProcessEnv = process.env): string {
  const override = env.DSH_LIAISON_STATE_DIR
  if (override !== undefined && override.trim() !== '') return resolve(override)
  const runtime = env.XDG_RUNTIME_DIR
  if (runtime !== undefined && runtime.trim() !== '') return join(resolve(runtime), 'dsh-liaison')
  const home = env.HOME
  return join(home !== undefined && home.trim() !== '' ? resolve(home) : homedir(), '.local', 'state', 'dsh-liaison')
}

/**
 * Name the files inside one state directory.
 * @param stateDir - absolute state directory.
 * @returns every lifecycle path.
 */
export function statePaths(stateDir: string): StatePaths {
  return {
    stateDir,
    socket: join(stateDir, 'daemon.sock'),
    daemonLock: join(stateDir, 'daemon.lock'),
    hostLock: join(stateDir, 'host.lock'),
    hostRecord: join(stateDir, 'host.json'),
    observerPatch: join(stateDir, 'host-observer.patch.json'),
    readyFile: join(stateDir, 'host-ready.json'),
  }
}

/** Filesystem locations shared by the in-process plugin and the stateless CLI. */

import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { resolveDshHome } from '@deepseek-ai/dsh-home-paths'
import { LiaisonError } from './errors.ts'

/** Maximum Unix-domain socket path length accepted on Linux/macOS. */
export const MAX_SOCKET_PATH_BYTES = 100

/** Resolved locations for one `DSH_HOME` and runtime directory. */
export interface LiaisonPaths {
  /** Exact DeepSeek Harness home, from `resolveDshHome()`. */
  readonly dshHome: string
  /** Directory owning the socket, its lock file, and no user data. */
  readonly runtimeDir: string
  /** `0600` Unix-domain socket served by the plugin. */
  readonly socketPath: string
  /** Lifetime kernel `flock` path guarding the socket. */
  readonly lockPath: string
  /** SQLite ledger shared by the plugin and offline CLI reads. */
  readonly ledgerPath: string
}

/**
 * Resolve every path from the caller's environment. A non-empty runtime-dir
 * override wins, then `$XDG_RUNTIME_DIR/dsh-liaison`, then a per-user temp
 * directory. The ledger lives under the Harness data category.
 * @param env - environment mapping; defaults to `process.env`.
 * @returns absolute, normalized liaison paths.
 */
export function resolveLiaisonPaths(env: NodeJS.ProcessEnv = process.env): LiaisonPaths {
  const dshHome = resolveDshHome(env.DSH_HOME, env)
  const override = nonEmpty(env.DSH_LIAISON_RUNTIME_DIR)
  const xdg = nonEmpty(env.XDG_RUNTIME_DIR)
  const runtimeDir = override
    ?? (xdg === undefined ? join(tmpdir(), `dsh-liaison-${String(process.getuid?.() ?? 0)}`) : join(xdg, 'dsh-liaison'))
  const socketPath = join(runtimeDir, 'dsh-liaison.sock')
  const bytes = Buffer.byteLength(socketPath)
  if (bytes > MAX_SOCKET_PATH_BYTES) {
    throw new LiaisonError('usage', `Unix-domain socket path is ${String(bytes)} bytes; the limit is ${String(MAX_SOCKET_PATH_BYTES)}`, {
      socketPath,
    })
  }
  const ledgerPath = nonEmpty(env.DSH_LIAISON_LEDGER) ?? join(dshHome, 'data', 'dsh-liaison', 'ledger.sqlite3')
  return {
    dshHome,
    runtimeDir,
    socketPath,
    lockPath: `${socketPath}.lock`,
    ledgerPath,
  }
}

function nonEmpty(value: string | undefined): string | undefined {
  return value === undefined || value.trim().length === 0 ? undefined : value
}

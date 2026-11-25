/** Pinned upstream kernel flock used as one lifetime ledger writer lock. */

import { chmod, open } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const REPO_ROOT = fileURLToPath(new URL('..', import.meta.url))
const FLOCK_ANCHOR = join(REPO_ROOT, 'deepseek-harness/native/system/packages/entry/package.json')
let cachedFlock

async function loadFlock() {
  if (cachedFlock !== undefined) return cachedFlock
  try {
    const resolved = createRequire(FLOCK_ANCHOR).resolve('@deepseek-ai/node-addon-system/flock')
    cachedFlock = await import(pathToFileURL(resolved).href)
  } catch {
    cachedFlock = null
  }
  return cachedFlock
}

/**
 * Try one nonblocking lifetime writer lock on a persistent lock file.
 * @param {string} path - lock path; the file is never unlinked.
 * @returns {Promise<{acquired: boolean, release: () => Promise<void>}>} ownership and release.
 *   A busy or unavailable kernel lock is reported as not acquired so callers can
 *   serve fresh read-only state instead of risking a stale write.
 */
export async function tryAcquireKernelWriterLock(path) {
  const flock = await loadFlock()
  if (flock === null) return { acquired: false, release: async () => {} }
  let handle
  try {
    handle = await open(path, 'a+', 0o600)
    await chmod(path, 0o600).catch(() => {})
    try {
      await flock.tryLockExclusive(handle.fd)
    } catch (error) {
      if (error?.code !== 'EAGAIN' && error?.code !== 'EWOULDBLOCK') {
        return { acquired: false, release: async () => {} }
      }
      await handle.close().catch(() => {})
      return { acquired: false, release: async () => {} }
    }
    let released = false
    return {
      acquired: true,
      release: async () => {
        if (released) return
        released = true
        await handle.close().catch(() => {})
      },
    }
  } catch {
    await handle?.close().catch(() => {})
    return { acquired: false, release: async () => {} }
  }
}

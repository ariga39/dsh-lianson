import { open, stat, type FileHandle } from 'node:fs/promises'
import { tryLockExclusive } from '@deepseek-ai/node-addon-system/flock'

/** One acquired kernel lock; closing the descriptor releases it. */
export interface HeldLock {
  readonly path: string
  release(): Promise<void>
}

/** Options for a bounded lock acquisition attempt. */
export interface AcquireLockOptions {
  /** Total time to keep retrying contention, in milliseconds. Defaults to no waiting. */
  readonly waitMs?: number
  /** Delay between contention attempts, in milliseconds. Defaults to 25. */
  readonly retryMs?: number
}

/**
 * Acquire an exclusive flock on a stable lock file without ever unlinking it.
 * The locked descriptor is verified against the path so a stale inode cannot be
 * mistaken for the current lock.
 * @param path - absolute lock-file path.
 * @param options - bounded contention wait.
 * @returns the held lock, or undefined when contention outlives the wait.
 */
export async function acquireLock(path: string, options: AcquireLockOptions = {}): Promise<HeldLock | undefined> {
  const deadline = Date.now() + (options.waitMs ?? 0)
  const retryMs = options.retryMs ?? 25
  while (true) {
    const handle = await open(path, 'w', 0o600)
    try {
      try {
        await tryLockExclusive(handle.fd)
      } catch (error) {
        if (!isContention(error)) throw error
        await handle.close()
        if (Date.now() >= deadline) return undefined
        await pause(retryMs)
        continue
      }
      const held = await handle.stat({ bigint: true })
      const current = await stat(path, { bigint: true }).catch((error: unknown) => {
        if ((error as NodeJS.ErrnoException | null)?.code === 'ENOENT') return undefined
        throw error
      })
      if (current !== undefined && current.ino === held.ino && current.dev === held.dev) {
        return heldLock(path, handle)
      }
    } catch (error) {
      await handle.close()
      throw error
    }
    await handle.close()
    if (Date.now() >= deadline) return undefined
    await pause(retryMs)
  }
}

function heldLock(path: string, handle: FileHandle): HeldLock {
  let released = false
  return {
    path,
    async release(): Promise<void> {
      if (released) return
      released = true
      await handle.close()
    },
  }
}

function isContention(error: unknown): boolean {
  const failure = error as NodeJS.ErrnoException | null
  return failure?.code === 'EAGAIN' || failure?.code === 'EWOULDBLOCK'
}

async function pause(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms)
  })
}

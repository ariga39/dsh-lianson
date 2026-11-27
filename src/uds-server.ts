/**
 * Single-owner Unix-domain HTTP listener. A lifetime non-blocking kernel
 * `flock` on a sibling lock file decides which process may bind the socket;
 * the socket itself is created with mode `0600` so other UIDs cannot connect.
 */

import { chmod, lstat, mkdir, open, rm } from 'node:fs/promises'
import type { FileHandle } from 'node:fs/promises'
import { createServer } from 'node:http'
import type { IncomingMessage, Server, ServerResponse } from 'node:http'
import { dirname } from 'node:path'
import { tryLockExclusive } from '@deepseek-ai/node-addon-system/flock'
import { LiaisonError } from './errors.ts'

/** Request listener owned by the liaison HTTP router. */
export type UdsRequestHandler = (request: IncomingMessage, response: ServerResponse) => Promise<void>

/** One bound socket with its lock and cleanup state. */
export class UdsServer {
  private readonly socketPath: string
  private readonly lockPath: string
  private readonly handler: UdsRequestHandler
  private server: Server | undefined
  private lock: FileHandle | undefined
  private started = false
  private stopping: Promise<void> | undefined

  /**
   * @param socketPath - absolute Unix-domain socket path.
   * @param lockPath - sibling lock file path; never the socket itself.
   * @param handler - owns each HTTP response.
   */
  constructor(
    socketPath: string,
    lockPath: string,
    handler: UdsRequestHandler,
  ) {
    this.socketPath = socketPath
    this.lockPath = lockPath
    this.handler = handler
  }

  /**
   * Acquire the lifetime lock, remove a stale socket, bind, and restrict the
   * socket mode. A second live owner fails with `socket-in-use`.
   * @returns when the listener accepts connections.
   */
  async start(): Promise<void> {
    if (this.started) return
    const socketDir = dirname(this.socketPath)
    const lockDir = dirname(this.lockPath)
    const socketDirIdentity = await ensurePrivateRuntimeDirectory(socketDir)
    if (lockDir !== socketDir) await ensurePrivateRuntimeDirectory(lockDir)
    const lock = await open(this.lockPath, 'a+', 0o600)
    try {
      await tryLockExclusive(lock.fd)
    } catch (error) {
      await lock.close()
      if (isLockContention(error)) {
        throw new LiaisonError('socket-in-use', 'another dsh-liaison plugin owns this socket', { socketPath: this.socketPath })
      }
      throw error
    }
    this.lock = lock
    let server: Server | undefined
    try {
      await removeStaleSocket(this.socketPath)
      server = createServer((request, response) => {
        void this.handler(request, response).catch(() => {
          if (!response.headersSent) response.writeHead(500, { 'content-type': 'application/json; charset=utf-8' })
          if (!response.writableEnded) response.end(JSON.stringify({ ok: false, error: { code: 'internal', message: 'unhandled request failure' } }))
        })
      })
      const listening = server
      await new Promise<void>((resolve, reject) => {
        const onError = (error: Error): void => {
          listening.off('listening', onListening)
          reject(error)
        }
        const onListening = (): void => {
          listening.off('error', onError)
          resolve()
        }
        listening.once('error', onError)
        listening.once('listening', onListening)
        listening.listen(this.socketPath)
      })
      listening.on('error', () => {
        // A later listener error cannot be answered through a request; the
        // plugin reports loopback/health separately. Keep the process alive.
      })
      const after = await inspectPrivateRuntimeDirectory(socketDir)
      if (after.dev !== socketDirIdentity.dev || after.ino !== socketDirIdentity.ino) {
        throw new LiaisonError('unsafe-runtime-dir', 'runtime directory was replaced while binding the socket', { directory: socketDir })
      }
      await chmod(this.socketPath, 0o600)
      this.server = listening
      this.started = true
    } catch (error) {
      try {
        if (server !== undefined) {
          try {
            server.closeAllConnections()
            // A listener that bound unlinks its own socket; one that never
            // listened owns no path and must not touch a pre-existing entry.
            await closeIfListening(server)
          } catch {
            // Startup already failed; preserve that failure instead of a cleanup error.
          }
        }
      } finally {
        await this.releaseLock().catch(() => undefined)
      }
      throw error
    }
  }

  /**
   * Stop the listener, remove the socket path, and release the kernel lock.
   * Idempotent and safe after a failed start.
   * @returns when every owned resource is released.
   */
  stop(): Promise<void> {
    if (this.stopping !== undefined) return this.stopping
    this.stopping = this.stopOnce()
    return this.stopping
  }

  private async stopOnce(): Promise<void> {
    const server = this.server
    this.server = undefined
    this.started = false
    try {
      if (server !== undefined) {
        server.closeAllConnections()
        // Node unlinks the bound Unix socket when its listener closes. Doing
        // another unlink here could delete a replacement we never owned.
        await closeIfListening(server)
      }
    } finally {
      await this.releaseLock().catch(() => undefined)
    }
  }

  private async releaseLock(): Promise<void> {
    const lock = this.lock
    this.lock = undefined
    if (lock !== undefined) await lock.close()
  }
}

/** One runtime directory's identity, stable across socket binding. */
interface DirectoryIdentity {
  readonly dev: bigint
  readonly ino: bigint
}

/**
 * Create the runtime directory when absent and prove it is a real, private
 * directory owned by this uid. The predictable fallback under `/tmp` may be
 * pre-created, so ownership, mode, and symlink status are checked, not assumed.
 * @param directory - runtime directory that will hold the socket and lock.
 * @returns the directory's device/inode identity for the post-bind check.
 */
export async function ensurePrivateRuntimeDirectory(directory: string): Promise<DirectoryIdentity> {
  try {
    return await inspectPrivateRuntimeDirectory(directory)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      throw error instanceof LiaisonError
        ? error
        : new LiaisonError('unsafe-runtime-dir', `cannot inspect runtime directory ${directory}`, {
          directory,
          reason: error instanceof Error ? error.message : String(error),
        })
    }
  }
  try {
    await mkdir(directory, { recursive: true, mode: 0o700 })
  } catch (error) {
    throw new LiaisonError('unsafe-runtime-dir', `cannot create private runtime directory ${directory}`, {
      directory,
      reason: error instanceof Error ? error.message : String(error),
    })
  }
  return inspectPrivateRuntimeDirectory(directory)
}

/**
 * Inspect one runtime directory without following a final symlink.
 * @param directory - directory that must hold the socket.
 * @returns its device/inode identity.
 * @throws {LiaisonError} `unsafe-runtime-dir`.
 */
export async function inspectPrivateRuntimeDirectory(directory: string): Promise<DirectoryIdentity> {
  const info = await lstat(directory, { bigint: true })
  const uid = process.getuid?.()
  if (uid === undefined) {
    throw new LiaisonError('unsafe-runtime-dir', 'cannot verify runtime directory ownership on this platform', { directory })
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw new LiaisonError('unsafe-runtime-dir', `runtime directory is not a real directory: ${directory}`, { directory })
  }
  if (info.uid !== BigInt(uid)) {
    throw new LiaisonError('unsafe-runtime-dir', `runtime directory is not owned by uid ${String(uid)}: ${directory}`, { directory })
  }
  const mode = info.mode & 0o777n
  if ((mode & 0o700n) !== 0o700n || (mode & 0o077n) !== 0n) {
    throw new LiaisonError('unsafe-runtime-dir', `runtime directory mode is not private: ${directory}`, {
      directory,
      mode: mode.toString(8),
    })
  }
  return { dev: info.dev, ino: info.ino }
}

/** Close a listening server and wait for its path cleanup; a non-listening server owns no path. */
function closeIfListening(server: Server): Promise<void> {
  if (!server.listening) return Promise.resolve()
  return new Promise<void>((resolve) => { server.close(() => { resolve() }) })
}

/** Whether a `tryLockExclusive` failure means another descriptor owns the lock. */
function isLockContention(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | null)?.code
  return code === 'EAGAIN' || code === 'EWOULDBLOCK'
}

/** Remove a stale socket file, refusing to delete any other object at the path. */
async function removeStaleSocket(socketPath: string): Promise<void> {
  try {
    const info = await lstat(socketPath)
    if (!info.isSocket()) {
      throw new LiaisonError('socket-in-use', `refusing to replace a non-socket at ${socketPath}`, { socketPath })
    }
    await rm(socketPath)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return
    throw error
  }
}

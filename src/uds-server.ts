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
    await mkdir(dirname(this.socketPath), { recursive: true, mode: 0o700 })
    await mkdir(dirname(this.lockPath), { recursive: true, mode: 0o700 })
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
    try {
      await removeStaleSocket(this.socketPath)
      const server = createServer((request, response) => {
        void this.handler(request, response).catch(() => {
          if (!response.headersSent) response.writeHead(500, { 'content-type': 'application/json; charset=utf-8' })
          if (!response.writableEnded) response.end(JSON.stringify({ ok: false, error: { code: 'internal', message: 'unhandled request failure' } }))
        })
      })
      await new Promise<void>((resolve, reject) => {
        const onError = (error: Error): void => {
          server.off('listening', onListening)
          reject(error)
        }
        const onListening = (): void => {
          server.off('error', onError)
          resolve()
        }
        server.once('error', onError)
        server.once('listening', onListening)
        server.listen(this.socketPath)
      })
      server.on('error', () => {
        // A later listener error cannot be answered through a request; the
        // plugin reports loopback/health separately. Keep the process alive.
      })
      await chmod(this.socketPath, 0o600)
      this.server = server
      this.started = true
    } catch (error) {
      await this.releaseLock()
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
    if (server !== undefined) {
      server.closeAllConnections()
      await new Promise<void>((resolve) => {
        server.close(() => { resolve() })
      })
    }
    await rm(this.socketPath, { force: true })
    await this.releaseLock()
  }

  private async releaseLock(): Promise<void> {
    const lock = this.lock
    this.lock = undefined
    if (lock !== undefined) await lock.close()
  }
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

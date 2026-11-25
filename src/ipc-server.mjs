/** Unix-only NDJSON daemon socket. No TCP listener is ever created. */

import { createServer } from 'node:net'
import { chmod, lstat, unlink } from 'node:fs/promises'
import { join } from 'node:path'
import { createLock, ensurePrivateDir, resolveStateDir } from './host-manager.mjs'
import { probe } from './ipc-client.mjs'
import { failure, requestFingerprint } from './ipc-protocol.mjs'
import { redactSecrets } from './redaction.mjs'

const MAX_REQUEST_BYTES = 1_000_000
const DAEMON_LOCK_TIMEOUT_MS = 30_000

function listen(server, socketPath) {
  return new Promise((resolve, reject) => {
    const failed = error => { server.off('listening', ready); reject(error) }
    const ready = () => { server.off('error', failed); resolve() }
    server.once('error', failed)
    server.once('listening', ready)
    server.listen(socketPath)
  })
}

/**
 * Start or reuse the single daemon socket under the private state directory.
 * @param {{stateDir?: string, socketPath?: string, dispatch: Function, onShutdown?: Function}} options
 */
export async function startIpcServer({
  stateDir = resolveStateDir(),
  socketPath = join(stateDir, 'daemon.sock'),
  dispatch,
  onShutdown,
}) {
  await ensurePrivateDir(stateDir)
  const lock = await createLock(join(stateDir, 'daemon.lock'))
  const completedIds = new Map()
  const pendingIds = new Map()
  const MAX_COMPLETED_IDS = 4_096
  const REPLAYABLE_COMMANDS = new Set(['delegation.create', 'delegation.prompt'])

  // Only successful create/prompt mutations are replayable after a lost socket
  // response. Failed calls stay retryable (the write-ahead ledger and Host
  // request id make an identical retry safe), while reads such as get/list/
  // verifyModel always execute live even when they reuse an id.
  async function dispatchIdempotent(request, command) {
    const id = typeof request?.id === 'string' && request.id !== '' ? request.id : undefined
    if (id === undefined || !REPLAYABLE_COMMANDS.has(command)) return dispatch(request)
    const fingerprint = requestFingerprint(command, request?.params)
    const completed = completedIds.get(id)
    if (completed !== undefined) {
      if (completed.fingerprint !== fingerprint) {
        return {
          envelope: failure(command, {
            code: 'IPC_ID_CONFLICT',
            message: `Request id "${id}" was already used with different command parameters`,
          }, id),
        }
      }
      return completed.outcome
    }
    const pending = pendingIds.get(id)
    if (pending !== undefined) {
      if (pending.fingerprint !== fingerprint) {
        return {
          envelope: failure(command, {
            code: 'IPC_ID_CONFLICT',
            message: `Request id "${id}" is already in flight with different command parameters`,
          }, id),
        }
      }
      return pending.promise
    }
    const promise = (async () => {
      const outcome = await dispatch(request)
      const envelope = outcome?.envelope ?? outcome
      if (envelope?.ok === true) {
        completedIds.set(id, { fingerprint, outcome })
        if (completedIds.size > MAX_COMPLETED_IDS) {
          completedIds.delete(completedIds.keys().next().value)
        }
      }
      return outcome
    })()
    pendingIds.set(id, { fingerprint, promise })
    try {
      return await promise
    } finally {
      pendingIds.delete(id)
    }
  }

  let release
  try {
    // The winning daemon holds this lock for its whole lifetime. A second
    // daemon therefore cannot mistake a busy first socket for a stale one.
    release = await lock.acquire({ timeoutMs: DAEMON_LOCK_TIMEOUT_MS })
  } catch (error) {
    if (error?.code === 'HOST_LOCK_TIMEOUT' && await probe(socketPath, 2_000)) {
      return { reused: true, socketPath, close: async () => {} }
    }
    throw error
  }
  let released = false
  const releaseLock = async () => {
    if (released) return
    released = true
    await release()
  }
  let retainLock = false
  try {
    if (await probe(socketPath, 1_000)) {
      await releaseLock()
      return { reused: true, socketPath, close: async () => {} }
    }
    try {
      const existing = await lstat(socketPath)
      if (!existing.isSocket()) {
        throw Object.assign(new Error('A non-socket file occupies the daemon socket path'), {
          code: 'DAEMON_SOCKET_CONFLICT',
        })
      }
      await unlink(socketPath)
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error
    }

    const connections = new Set()
    const server = createServer(socket => {
      connections.add(socket)
      socket.once('close', () => { connections.delete(socket) })
      let buffer = ''
      let handled = false
      socket.setEncoding('utf8')
      socket.on('error', () => {})
      socket.on('data', chunk => {
        buffer += chunk
        if (buffer.length > MAX_REQUEST_BYTES) {
          socket.destroy()
          return
        }
        const newline = buffer.indexOf('\n')
        if (newline === -1 || handled) return
        handled = true
        const requestText = buffer.slice(0, newline)
        // Each CLI command owns its own connection. Host mutation ordering is
        // enforced by the daemon layer, so unrelated reads never wait here.
        void (async () => {
          let request
          try { request = JSON.parse(requestText) } catch {
            socket.end(`${JSON.stringify(failure(null, {
              code: 'IPC_BAD_REQUEST',
              message: 'Request is not valid JSON',
            }))}\n`)
            return
          }
          const command = typeof request?.command === 'string' ? request.command : null
          let outcome
          try {
            outcome = await dispatchIdempotent({ command, params: request?.params, id: request?.id }, command)
          } catch (error) {
            outcome = {
              envelope: failure(command, {
                code: typeof error?.code === 'string' ? error.code : 'INTERNAL_ERROR',
                message: redactSecrets(error instanceof Error ? error.message : String(error)),
              }),
            }
          }
          const envelope = outcome?.envelope ?? outcome
          const responseLine = `${JSON.stringify(envelope)}\n`
          if (outcome?.shutdown === true) {
            await withdraw()
            socket.end(responseLine, () => { setImmediate(() => { void onShutdown?.() }) })
          } else {
            socket.end(responseLine)
          }
        })().catch(error => {
          socket.destroy(error)
        })
      })
    })

    let listening = false
    const previousUmask = process.umask(0o177)
    try {
      await listen(server, socketPath)
      listening = true
    } catch (error) {
      if (error?.code === 'EADDRINUSE' && await probe(socketPath, 1_000)) {
        await releaseLock()
        return { reused: true, socketPath, close: async () => {} }
      }
      throw error
    } finally {
      process.umask(previousUmask)
    }
    if (!listening) throw new Error('daemon socket did not listen')
    await chmod(socketPath, 0o600)
    const identity = await lstat(socketPath)
    let closed = false
    let withdrawn = false
    let closeComplete = Promise.resolve()
    const withdraw = async () => {
      if (withdrawn) return
      withdrawn = true
      if (server.listening) {
        closeComplete = new Promise(resolve => { server.close(() => resolve()) })
      }
      try {
        const current = await lstat(socketPath)
        if (current.ino === identity.ino) await unlink(socketPath)
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error
      }
    }
    retainLock = true

    return {
      reused: false,
      socketPath,
      server,
      withdraw,
      close: async () => {
        if (closed) return
        closed = true
        try {
          await withdraw()
          for (const socket of connections) socket.destroy()
          await closeComplete
        } finally {
          await releaseLock()
        }
      },
    }
  } finally {
    if (!retainLock) await releaseLock()
  }
}

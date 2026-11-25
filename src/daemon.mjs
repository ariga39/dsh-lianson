/** User-only local daemon: one managed Host plus one answering operator. */

import { HostManager, resolveStateDir } from './host-manager.mjs'
import { OperatorConnection } from './operator-connection.mjs'
import { startIpcServer } from './ipc-server.mjs'
import { IPC_COMMANDS, failure, success } from './ipc-protocol.mjs'
import { errorEnvelope, LiaisonError } from './errors.mjs'
import { redactSecrets } from './redaction.mjs'
import { join } from 'node:path'

const stateDir = resolveStateDir()
const socketPath = join(stateDir, 'daemon.sock')
const manager = new HostManager({ stateDir })
const operator = new OperatorConnection({ hostManager: manager })
let host = null
let server
let shuttingDown = false
let gate = Promise.resolve()

function redact(value) {
  return redactSecrets(value, [host?.launchToken].filter(secret => typeof secret === 'string'))
}

function exclusive(fn) {
  const run = gate.then(fn, fn)
  gate = run.then(() => {}, () => {})
  return run
}

function statusData() {
  const snapshot = operator.snapshot()
  return {
    daemonPid: process.pid,
    hostPid: host?.pid ?? null,
    origin: host?.origin ?? null,
    port: host?.port ?? null,
    dshHome: host?.dshHome ?? null,
    connected: snapshot.connected,
    generation: snapshot.generation,
    connectCount: snapshot.connectCount,
    reconnectCount: snapshot.reconnectCount,
    socketPath,
    lastError: snapshot.lastError,
  }
}

async function refreshHost() {
  try {
    const current = await manager.readOwnedHost()
    if (current === null) {
      host = null
      return null
    }
    host = current
    await operator.setHost(current)
    if (!operator.snapshot().connected) void operator.ensureConnected().catch(() => {})
    return current
  } catch {
    host = null
    return null
  }
}

async function ensureHost(params) {
  const next = await manager.ensureHost(params)
  host = next
  await operator.setHost(next)
  return next
}

async function dispatch(request) {
  const { command, params = {}, id } = request
  const key = typeof command === 'string' ? command.replace(/ /gu, '.') : command
  if (key === null || !IPC_COMMANDS.has(key)) {
    return { envelope: failure(command, {
      code: 'UNKNOWN_COMMAND',
      message: `Unknown command: ${String(command)}`,
    }, id) }
  }

  try {
    if (key === 'host.start') {
      return await exclusive(async () => {
        await ensureHost(params)
        await operator.ensureConnected()
        return { envelope: success(command, statusData(), id) }
      })
    }
    if (key === 'host.status') {
      if (params.ensure === true) {
        return await exclusive(async () => {
          await ensureHost(params)
          await operator.ensureConnected()
          return { envelope: success(command, statusData(), id) }
        })
      }
      await refreshHost()
      return { envelope: success(command, statusData(), id) }
    }
    if (key === 'host.ping') {
      if (host === null) await exclusive(async () => { await ensureHost(params) })
      await operator.ensureConnected()
      const result = await operator.withClient(async remote => {
        const response = await remote.session.modelCatalog()
        if (response?.ok !== true) {
          throw new LiaisonError('RPC_FAILED', 'session/modelCatalog failed',
            response?.error === undefined ? {} : { remote: response.error })
        }
        return { modelGroups: response.value.groups.length }
      })
      return {
        envelope: success(command, { ...statusData(), rpc: 'session/modelCatalog', ...result }, id),
      }
    }
    if (key === 'host.stop') {
      return await exclusive(async () => {
        const keep = params.stopHost === false
        const previous = host
        const stopped = keep
          ? { stopped: false, reason: 'kept', pid: previous?.pid ?? null }
          : await manager.stopHost()
        await operator.close()
        host = null
        return {
          envelope: success(command, {
            daemonPid: process.pid,
            hostPid: stopped.pid ?? previous?.pid ?? null,
            stoppedHost: stopped.stopped,
            stopReason: stopped.reason,
          }, id),
          shutdown: true,
        }
      })
    }
    return { envelope: failure(command, {
      code: 'UNKNOWN_COMMAND',
      message: `Unknown command: ${String(command)}`,
    }, id) }
  } catch (error) {
    return { envelope: failure(command, errorEnvelope(error, redact), id) }
  }
}

async function shutdown(code = 0) {
  if (shuttingDown) return
  shuttingDown = true
  await operator.close().catch(() => {})
  await server?.close().catch(() => {})
  process.exit(code)
}

async function main() {
  server = await startIpcServer({
    stateDir,
    socketPath,
    dispatch,
    onShutdown: () => { void shutdown(0) },
  })
  if (server.reused === true) {
    process.exit(0)
    return
  }

  process.on('SIGTERM', () => { void shutdown(0) })
  process.on('SIGINT', () => { void shutdown(0) })
  process.on('unhandledRejection', error => {
    process.stderr.write(`[dsh-liaison] ${redact(error instanceof Error ? error.message : String(error))}\n`)
  })

  try {
    const existing = await manager.readOwnedHost()
    if (existing !== null) {
      host = existing
      await operator.setHost(existing)
      void operator.ensureConnected().catch(() => {})
    }
  } catch (error) {
    process.stderr.write(`[dsh-liaison] ${redact(error instanceof Error ? error.message : String(error))}\n`)
  }
}

main().catch(error => {
  process.stderr.write(`[dsh-liaison] ${redact(error instanceof Error ? error.message : String(error))}\n`)
  process.exit(1)
})

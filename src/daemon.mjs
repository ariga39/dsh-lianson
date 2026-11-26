/** User-only local daemon: one managed Host plus one answering operator. */

import { HostManager, hostStopSucceeded, resolveDelegationStateDir, resolveStateDir } from './host-manager.mjs'
import { OperatorConnection } from './operator-connection.mjs'
import { startIpcServer } from './ipc-server.mjs'
import { IPC_COMMANDS, failure, requestFingerprint, stablePromptRequestId, success } from './ipc-protocol.mjs'
import { errorEnvelope, LiaisonError } from './errors.mjs'
import { redactSecrets } from './redaction.mjs'
import { DelegationRegistry } from './delegation-registry.mjs'
import { DelegationService } from './delegation-service.mjs'
import { createOperatorAdapter } from './operator-adapter.mjs'
import { join } from 'node:path'

const stateDir = resolveStateDir()
const delegationStateDir = resolveDelegationStateDir()
const socketPath = join(stateDir, 'daemon.sock')
const manager = new HostManager({ stateDir })
const operator = new OperatorConnection({ hostManager: manager })
const delegationRegistry = new DelegationRegistry({ filePath: join(delegationStateDir, 'delegations.json') })
const delegations = new DelegationService({
  registry: delegationRegistry,
  adapter: createOperatorAdapter(operator),
})
let host = null
let server
let shuttingDown = false
let closing = false
let gate = Promise.resolve()
let resolveInitialized
const initialized = new Promise(resolve => { resolveInitialized = resolve })

function assertOpen() {
  if (closing) throw new LiaisonError('DAEMON_CLOSING', 'Daemon is closing')
}

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
  assertOpen()
  const next = await manager.ensureHost(params)
  // A stop may have started while the Host lock was held. The stop path will
  // reclaim this launch under the same lock, but this request must not adopt it.
  assertOpen()
  host = next
  await operator.setHost(next)
  return next
}

/** Start or attach one Host for a delegation mutation, then wait for the operator. */
async function ensureDelegationHost(params) {
  await exclusive(async () => {
    if (host === null) await ensureHost(params?.host ?? {})
    await operator.ensureConnected()
  })
}

/** Require the already-owned Host; never spawn a Host that would not own the session. */
async function requireDelegationHost() {
  if (host === null) {
    await refreshHost()
    if (host === null) {
      throw new LiaisonError(
        'HOST_UNAVAILABLE',
        'No managed dsh web Host is available; run "dsh-liaison host start" first',
      )
    }
  }
  await operator.ensureConnected()
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
  if (closing && key !== 'host.stop') {
    return { envelope: failure(command, {
      code: 'DAEMON_CLOSING',
      message: 'Daemon is closing',
    }, id) }
  }

  try {
    if (key.startsWith('delegation.')) await initialized
    if (key === 'host.start') {
      return await exclusive(async () => {
        assertOpen()
        await ensureHost(params)
        assertOpen()
        await operator.ensureConnected()
        return { envelope: success(command, statusData(), id) }
      })
    }
    if (key === 'host.status') {
      if (params.ensure === true) {
        return await exclusive(async () => {
          assertOpen()
          await ensureHost(params)
          assertOpen()
          await operator.ensureConnected()
          return { envelope: success(command, statusData(), id) }
        })
      }
      await refreshHost()
      return { envelope: success(command, statusData(), id) }
    }
    if (key === 'host.ping') {
      if (host === null) {
        await exclusive(async () => {
          assertOpen()
          await ensureHost(params)
        })
      }
      assertOpen()
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
    if (key === 'delegation.create') {
      const ipcRequestId = typeof id === 'string' && id !== '' ? id : undefined
      const ipcFingerprint = requestFingerprint(command, params)
      if (ipcRequestId !== undefined) {
        const replay = await delegations.createReplay(ipcRequestId, ipcFingerprint)
        if (replay !== undefined) return { envelope: success(command, replay, id) }
      }
      await ensureDelegationHost(params)
      const created = await delegations.create({
        ...params,
        ...(ipcRequestId === undefined ? {} : { ipcRequestId, ipcFingerprint }),
      })
      return { envelope: success(command, created, id) }
    }
    if (key === 'delegation.get') {
      return { envelope: success(command, await delegations.get(params), id) }
    }
    if (key === 'delegation.list') {
      return { envelope: success(command, await delegations.list(), id) }
    }
    if (key === 'delegation.prompt') {
      const ipcRequestId = typeof id === 'string' && id !== '' ? id : undefined
      const ipcFingerprint = requestFingerprint(command, params)
      if (ipcRequestId !== undefined) {
        const replay = await delegations.promptReplay(ipcRequestId, ipcFingerprint)
        if (replay !== undefined) return { envelope: success(command, replay, id) }
      }
      await requireDelegationHost()
      const requestId = stablePromptRequestId(params.requestId, id)
      const promptParams = requestId === undefined ? params : { ...params, requestId }
      const result = await delegations.prompt({
        ...promptParams,
        ...(ipcRequestId === undefined ? {} : { ipcRequestId, ipcFingerprint }),
      })
      return { envelope: success(command, result, id) }
    }
    if (key === 'delegation.verifyModel') {
      await requireDelegationHost()
      const requestId = stablePromptRequestId(params.requestId, id)
      const verifyParams = requestId === undefined ? params : { ...params, requestId }
      return { envelope: success(command, await delegations.verifyModel(verifyParams), id) }
    }
    if (key === 'host.stop') {
      if (closing) {
        return { envelope: failure(command, {
          code: 'DAEMON_CLOSING',
          message: 'Daemon is already closing',
        }, id) }
      }
      closing = true
      const keep = params.stopHost === false
      const previous = host
      try {
        // Close first: this aborts any persistent background reconnect before
        // the stop path waits on process or filesystem state.
        await operator.close()
        const stopped = keep
          ? await manager.inspectHost({ probe: false }).catch(() => null).then(hostInfo => ({
            stopped: false,
            reason: 'kept',
            pid: hostInfo?.pid ?? previous?.pid ?? null,
            retained: false,
          }))
          : await manager.stopHost()
        if (!keep && !hostStopSucceeded(stopped)) {
          throw new LiaisonError('HOST_STOP_INCOMPLETE', 'Host stop did not complete', {
            pid: stopped.pid ?? previous?.pid ?? null,
            reason: stopped.reason,
            retained: stopped.retained === true,
          })
        }
        host = null
        return {
          envelope: success(command, {
            daemonPid: process.pid,
            hostPid: stopped.pid ?? previous?.pid ?? null,
            stoppedHost: stopped.stopped,
            stopReason: stopped.reason,
            stopRetained: stopped.retained === true,
          }, id),
          shutdown: true,
        }
      } catch (error) {
        // The operator is already closed. Exit rather than leaving an unusable
        // daemon; the Host record and its ownership lock remain for a successor.
        return {
          envelope: failure(command, errorEnvelope(error, redact), id),
          shutdown: true,
          preserveOwnership: true,
        }
      }
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
  await delegationRegistry.close().catch(() => {})
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
  try {
    await delegationRegistry.acquireWriter()
  } finally {
    resolveInitialized()
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

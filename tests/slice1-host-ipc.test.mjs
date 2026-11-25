import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { test } from 'node:test'
import { loadUpstream } from '../src/upstream.mjs'
import { HostManager, inspectProcess } from '../src/host-manager.mjs'
import { OperatorConnection } from '../src/operator-connection.mjs'
import { startScriptedMessagesServer } from './support/scripted-messages-server.mjs'
import { createKeylessEnvironment } from './support/keyless-environment.mjs'
import { createTransportFixture } from './support/transport-fixture.mjs'
import {
  listeningTcpSockets,
  rawRpcStatus,
  runCli,
  sleep,
  waitFor,
  waitForExit,
} from './support/cli-harness.mjs'

function isLoopback(origin) {
  const hostname = new URL(origin).hostname.replace(/^\[|\]$/gu, '')
  return hostname === 'localhost' || hostname === '::1' || /^127\./u.test(hostname)
}

async function readModelCatalog(remote) {
  const result = await remote.session.modelCatalog()
  assert.equal(result.ok, true, JSON.stringify(result.error ?? {}))
  return result.value
}

test('slice1: forced mux drop revokes the old cookie and single-flight /connect re-exchanges it', { timeout: 180_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-reconnect-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  const manager = new HostManager({
    stateDir,
    dshHome: fixtureEnvironment.dshHome,
    cwd: fixtureEnvironment.cwd,
    patches: [fixtureEnvironment.overlayPath],
    port: 0,
    env: fixtureEnvironment.env,
  })
  let host
  let operator
  t.after(async () => {
    await operator?.close().catch(() => {})
    await manager?.stopHost().catch(() => {})
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  host = await manager.ensureHost()
  assert.equal(isLoopback(host.origin), true, 'managed Host origin must be loopback')

  const upstream = await loadUpstream()
  const transport = createTransportFixture(upstream.WebSocket)
  operator = new OperatorConnection({
    hostManager: manager,
    host,
    upstream,
    createWebSocket: transport.createWebSocket,
    reconnectDelayMs: 100,
  })
  await operator.ensureConnected()
  const before = operator.snapshot()
  assert.equal(before.connected, true)
  assert.equal(before.connectCount, 1)
  assert.equal(before.reconnectCount, 0)

  await readModelCatalog(await operator.withClient(remote => remote))
  const oldCookie = transport.cookies.at(-1)
  assert.match(oldCookie, /^dsh-auth-/u, 'the answering client must use the v2 API cookie')

  const dropped = transport.forceDropMux()
  assert.equal(dropped, 1, 'one physical /api/remote.mux socket must exist')
  await waitFor(() => operator.snapshot().connected === false,
    { timeoutMs: 5_000, label: 'operator to observe the mux drop' })

  const [first, second] = await Promise.all([
    operator.withClient(readModelCatalog),
    operator.withClient(readModelCatalog),
  ])
  assert.ok(first.groups.length > 0)
  assert.ok(second.groups.length > 0)

  const after = operator.snapshot()
  assert.equal(after.connected, true)
  assert.equal(after.reconnectCount, 1, 'two waiters must share one reconnect')
  assert.equal(after.connectCount, before.connectCount + 1, 'single-flight reconnect must create exactly one Context')
  assert.ok(after.generation > before.generation)

  const newCookie = transport.cookies.at(-1)
  assert.notEqual(newCookie, oldCookie, 'reconnect must use a freshly exchanged cookie')
  assert.equal(await rawRpcStatus(host.origin, oldCookie), 401,
    'closing the mux must revoke the old v2 cookie')
  assert.equal(await rawRpcStatus(host.origin, newCookie), 200)

  assert.equal(transport.unexpectedStatuses.includes(409), false, 'no 409 collision may occur')
  await readModelCatalog(await operator.withClient(remote => remote))
})

test('slice1: daemon keeps state private, exposes no TCP listener, and redacts credentials', { timeout: 180_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-daemon-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  const env = { ...fixtureEnvironment.env, DSH_LIAISON_STATE_DIR: stateDir }
  const previousUmask = process.umask(0o022)
  let started
  t.after(async () => {
    process.umask(previousUmask)
    if (started?.envelope?.ok === true) {
      await runCli(['host', 'stop'], { cwd: fixtureEnvironment.cwd, env, timeoutMs: 60_000 }).catch(() => {})
    }
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  started = await runCli([
    'host', 'start',
    '--workspace', fixtureEnvironment.cwd,
    '--dsh-home', fixtureEnvironment.dshHome,
    '--patch', fixtureEnvironment.overlayPath,
  ], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(started.code, 0, started.stderr)
  assert.equal(started.envelope?.ok, true, started.stdout)
  assert.equal(started.envelope.command, 'host start')
  const startedData = started.envelope.data
  assert.ok(Number.isInteger(startedData.daemonPid))
  assert.ok(Number.isInteger(startedData.hostPid))
  assert.equal(isLoopback(startedData.origin), true)
  assert.equal(startedData.socketPath, join(stateDir, 'daemon.sock'))
  assert.equal(startedData.connected, true)
  assert.equal(startedData.dshHome, resolve(fixtureEnvironment.dshHome))

  const stateDirMode = (await stat(stateDir)).mode & 0o777
  const socketMode = (await stat(startedData.socketPath)).mode & 0o777
  const recordPath = join(stateDir, 'host.json')
  const recordMode = (await stat(recordPath)).mode & 0o777
  assert.equal(stateDirMode, 0o700, 'state directory must be user-only')
  assert.equal(socketMode, 0o600, 'Unix socket must be user-only')
  assert.equal(recordMode, 0o600, 'credential file must be user-only')

  const record = JSON.parse(await readFile(recordPath, 'utf8'))
  assert.equal(record.pid, startedData.hostPid)
  assert.equal(record.dshHome, resolve(fixtureEnvironment.dshHome))
  assert.equal(typeof record.launchToken, 'string')
  assert.ok(record.launchToken.length >= 32)

  assert.deepEqual(await listeningTcpSockets(startedData.daemonPid), [],
    'the daemon itself must not open any TCP listener')

  const status = await runCli(['host', 'status'], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(status.code, 0, status.stderr)
  assert.equal(status.envelope?.ok, true)
  assert.equal(status.envelope.command, 'host status')
  assert.equal(status.envelope.data.hostPid, startedData.hostPid)
  assert.equal(status.envelope.data.daemonPid, startedData.daemonPid)

  const ping = await runCli(['host', 'ping'], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(ping.code, 0, ping.stderr)
  assert.equal(ping.envelope?.ok, true)
  assert.equal(ping.envelope.command, 'host ping')
  assert.equal(ping.envelope.data.rpc, 'session/modelCatalog')
  assert.ok(ping.envelope.data.modelGroups > 0)

  const combined = [started.stdout, started.stderr, status.stdout, status.stderr, ping.stdout, ping.stderr].join('\n')
  assert.equal(combined.includes(record.launchToken), false, 'CLI output must not contain the launch token')
  assert.equal(/token=/u.test(combined), false, 'CLI output must not contain token query strings')
  assert.equal(/dsh-auth-/u.test(combined), false, 'CLI output must not contain v2 cookies')
  assert.equal(/\bv2\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/u.test(combined), false)

  const lockPath = join(stateDir, 'daemon.lock')
  const daemonLock = JSON.parse(await readFile(lockPath, 'utf8'))
  assert.equal(daemonLock.pid, startedData.daemonPid,
    'the daemon must still own its startup lock while serving')

  const stopped = await runCli(['host', 'stop'], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(stopped.code, 0, stopped.stderr)
  assert.equal(stopped.envelope?.ok, true, stopped.stdout)
  assert.equal(stopped.envelope.data.stoppedHost, true)
  await waitForExit(startedData.daemonPid)
  await waitForExit(startedData.hostPid)
  await waitFor(async () => {
    try { await stat(lockPath); return false } catch (error) { if (error?.code === 'ENOENT') return true; throw error }
  }, { label: 'daemon lock release' })
  await assert.rejects(() => stat(startedData.socketPath), error => error?.code === 'ENOENT')
  started = undefined
})

test('slice1: concurrent CLI starts share one Host and a stale socket is reclaimed after daemon loss', { timeout: 240_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-attach-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  const env = { ...fixtureEnvironment.env, DSH_LIAISON_STATE_DIR: stateDir }
  const args = [
    'host', 'start',
    '--workspace', fixtureEnvironment.cwd,
    '--dsh-home', fixtureEnvironment.dshHome,
    '--patch', fixtureEnvironment.overlayPath,
  ]
  let started
  t.after(async () => {
    if (started?.envelope?.ok === true) {
      await runCli(['host', 'stop'], { cwd: fixtureEnvironment.cwd, env, timeoutMs: 60_000 }).catch(() => {})
    } else {
      try {
        const record = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8'))
        process.kill(record.pid, 'SIGKILL')
      } catch {}
    }
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  const [left, right] = await Promise.all([
    runCli(args, { cwd: fixtureEnvironment.cwd, env }),
    runCli(args, { cwd: fixtureEnvironment.cwd, env }),
  ])
  started = left.envelope?.ok === true ? left : right
  assert.equal(left.code, 0, left.stderr)
  assert.equal(right.code, 0, right.stderr)
  assert.equal(left.envelope?.ok, true, left.stdout)
  assert.equal(right.envelope?.ok, true, right.stdout)
  assert.equal(left.envelope.data.hostPid, right.envelope.data.hostPid, 'only one dsh web may be spawned')
  assert.equal(left.envelope.data.daemonPid, right.envelope.data.daemonPid, 'only one daemon may own the socket')

  const hostPid = left.envelope.data.hostPid
  const oldDaemonPid = left.envelope.data.daemonPid
  process.kill(oldDaemonPid, 'SIGKILL')
  await waitForExit(oldDaemonPid)
  process.kill(hostPid, 0)
  assert.equal((await stat(join(stateDir, 'daemon.sock'))).mode & 0o777, 0o600)

  const status = await runCli(['host', 'status'], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(status.code, 0, status.stderr)
  assert.equal(status.envelope?.ok, true, status.stdout)
  assert.notEqual(status.envelope.data.daemonPid, oldDaemonPid, 'a fresh daemon must replace the killed one')
  assert.equal(status.envelope.data.hostPid, hostPid, 'the fresh daemon must attach to the same dsh web')
  assert.equal((await stat(join(stateDir, 'daemon.sock'))).mode & 0o777, 0o600)
})

test('slice1: host stop after daemon loss stops a verified surviving Host unless keep-host', { timeout: 240_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-stop-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  const env = { ...fixtureEnvironment.env, DSH_LIAISON_STATE_DIR: stateDir }
  const args = [
    'host', 'start',
    '--workspace', fixtureEnvironment.cwd,
    '--dsh-home', fixtureEnvironment.dshHome,
    '--patch', fixtureEnvironment.overlayPath,
  ]
  t.after(async () => {
    await new HostManager({ stateDir, env: process.env }).stopHost().catch(() => {})
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  const first = await runCli(args, { cwd: fixtureEnvironment.cwd, env })
  assert.equal(first.code, 0, first.stderr)
  assert.equal(first.envelope?.ok, true, first.stdout)
  const firstHostPid = first.envelope.data.hostPid
  process.kill(first.envelope.data.daemonPid, 'SIGKILL')
  await waitForExit(first.envelope.data.daemonPid)

  const stopped = await runCli(['host', 'stop'], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(stopped.code, 0, stopped.stderr)
  assert.equal(stopped.envelope?.ok, true, stopped.stdout)
  assert.equal(stopped.envelope.command, 'host stop')
  assert.equal(stopped.envelope.data.daemonPid, null)
  assert.equal(stopped.envelope.data.hostPid, firstHostPid)
  assert.equal(stopped.envelope.data.stoppedHost, true,
    'a daemon-less stop must still stop the verified surviving Host')
  await waitForExit(firstHostPid)
  await assert.rejects(() => stat(join(stateDir, 'host.json')), error => error?.code === 'ENOENT')

  const second = await runCli(args, { cwd: fixtureEnvironment.cwd, env })
  assert.equal(second.code, 0, second.stderr)
  assert.equal(second.envelope?.ok, true, second.stdout)
  const secondHostPid = second.envelope.data.hostPid
  process.kill(second.envelope.data.daemonPid, 'SIGKILL')
  await waitForExit(second.envelope.data.daemonPid)

  const kept = await runCli(['host', 'stop', '--keep-host'], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(kept.code, 0, kept.stderr)
  assert.equal(kept.envelope?.ok, true, kept.stdout)
  assert.equal(kept.envelope.data.stoppedHost, false)
  assert.equal(kept.envelope.data.stopReason, 'kept')
  assert.equal(kept.envelope.data.hostPid, secondHostPid)
  process.kill(secondHostPid, 0)
  await sleep(100)
  assert.equal(process.kill(secondHostPid, 0), true, '--keep-host must leave dsh web running')
})

test('slice1: reconnect keeps retrying past five failures and close cancels the retry loop', { timeout: 180_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-retry-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  const manager = new HostManager({
    stateDir,
    dshHome: fixtureEnvironment.dshHome,
    cwd: fixtureEnvironment.cwd,
    patches: [fixtureEnvironment.overlayPath],
    port: 0,
    env: fixtureEnvironment.env,
  })
  let operator
  t.after(async () => {
    await operator?.close().catch(() => {})
    await manager.stopHost().catch(() => {})
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  const host = await manager.ensureHost()
  const upstream = await loadUpstream()
  const transport = createTransportFixture(upstream.WebSocket)
  operator = new OperatorConnection({
    hostManager: manager,
    host,
    upstream,
    createWebSocket: transport.createWebSocket,
    reconnectDelayMs: 10,
    reconnectMaxDelayMs: 50,
  })
  await operator.ensureConnected()
  await readModelCatalog(await operator.withClient(remote => remote))

  const realFetch = operator.fetchImpl
  let connectFailures = 0
  let failureLimit = 7
  operator.fetchImpl = (input, init) => {
    if (String(input).includes('/connect') && connectFailures < failureLimit) {
      connectFailures += 1
      return Promise.reject(new Error('simulated /connect outage'))
    }
    return realFetch(input, init)
  }

  assert.equal(transport.forceDropMux(), 1)
  await waitFor(() => operator.snapshot().connected === true && connectFailures >= 7, {
    timeoutMs: 30_000,
    label: 'recovery after more than five failed /connect exchanges',
  })
  assert.equal(connectFailures, 7, 'the retry loop must survive the old five-attempt bound')
  assert.equal(operator.snapshot().reconnectCount, 1)
  await readModelCatalog(await operator.withClient(remote => remote))

  connectFailures = 0
  failureLimit = Number.POSITIVE_INFINITY
  operator.reconnectDelayMs = 5_000
  operator.reconnectMaxDelayMs = 5_000
  assert.equal(transport.forceDropMux(), 1)
  await waitFor(() => operator.snapshot().connected === false && connectFailures >= 1, {
    timeoutMs: 15_000,
    label: 'the persistent retry loop to make another attempt',
  })
  const attemptsBeforeClose = connectFailures
  const closeStarted = Date.now()
  await operator.close()
  const closeElapsed = Date.now() - closeStarted
  assert.ok(closeElapsed < 2_000, `close must cancel the backoff wait (took ${String(closeElapsed)}ms)`)
  await sleep(300)
  assert.equal(connectFailures, attemptsBeforeClose, 'no retry may run after close')
  assert.equal(operator.snapshot().connected, false)
})

test('slice1: stale unverified Host records are never signalled and foreign origins are rejected', { timeout: 60_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-ownership-'))
  const stateDir = join(root, 'state')
  await mkdir(stateDir, { recursive: true, mode: 0o700 })
  await chmod(stateDir, 0o700)
  const sleeper = spawn(process.execPath, ['-e', 'setTimeout(() => {}, 60_000)'], { stdio: 'ignore' })
  t.after(async () => {
    if (sleeper.exitCode === null) sleeper.kill('SIGKILL')
    await rm(root, { recursive: true, force: true })
  })
  await waitFor(() => sleeper.pid !== undefined && sleeper.exitCode === null, { label: 'sleeper start' })
  const exitedChild = spawn(process.execPath, ['-e', ''], { stdio: 'ignore' })
  await new Promise(resolve => exitedChild.once('exit', resolve))
  const deadPid = exitedChild.pid

  const deadInspection = await inspectProcess({ pid: deadPid, marker: randomUUID() })
  assert.equal(deadInspection.ok, false)
  assert.equal(deadInspection.alive, false)
  assert.equal(deadInspection.reason, 'dead')
  const hiddenProcfs = await inspectProcess(
    { pid: sleeper.pid, marker: randomUUID() },
    { procRoot: join(root, 'no-procfs') },
  )
  assert.equal(hiddenProcfs.ok, false)
  assert.equal(hiddenProcfs.alive, true, 'a live pid is not dead merely because procfs is unavailable')
  assert.equal(hiddenProcfs.reason, 'unverified')
  assert.equal(hiddenProcfs.unsupported, true)
  const hiddenDead = await inspectProcess(
    { pid: deadPid, marker: randomUUID() },
    { procRoot: join(root, 'no-procfs') },
  )
  assert.equal(hiddenDead.reason, 'dead', 'an OS-confirmed gone pid is still dead without procfs')

  const manager = new HostManager({ stateDir, env: process.env })
  const recordPath = join(stateDir, 'host.json')
  await writeFile(recordPath, JSON.stringify({
    version: 1,
    pid: sleeper.pid,
    origin: 'http://127.0.0.1:9',
    port: 9,
    launchToken: 'not-a-real-token-but-long-enough',
    dshHome: join(root, 'home'),
    startedAt: new Date().toISOString(),
    marker: randomUUID(),
  }), { mode: 0o600 })

  const stopped = await manager.stopHost()
  assert.equal(stopped.stopped, false)
  assert.equal(stopped.reason, 'unverified')
  process.kill(sleeper.pid, 0)
  assert.equal(sleeper.exitCode, null, 'an unverified pid must never receive a signal')

  await writeFile(recordPath, JSON.stringify({
    version: 1,
    pid: sleeper.pid,
    origin: 'http://198.51.100.10:1234',
    port: 1234,
    launchToken: 'not-a-real-token-but-long-enough',
    dshHome: join(root, 'home'),
    startedAt: new Date().toISOString(),
    marker: randomUUID(),
  }), { mode: 0o600 })

  await assert.rejects(
    () => manager.ensureHost({ dshHome: join(root, 'home'), cwd: root, patches: [] }),
    error => error?.code === 'HOST_ORIGIN_UNSAFE',
  )
  process.kill(sleeper.pid, 0)
  await sleep(50)
  assert.equal(sleeper.exitCode, null, 'a foreign origin must not trigger process control')
})

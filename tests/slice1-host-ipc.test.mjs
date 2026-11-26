import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { createConnection } from 'node:net'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { test } from 'node:test'
import { loadUpstream } from '../src/upstream.mjs'
import {
  DRAIN_HOST_MARKER_ENV,
  DRAIN_MARKER_ENV,
  DSH_BIN,
  HOST_MARKER_ENV,
  HostManager,
  createLock,
  findProcessesByMarker,
  inspectProcess,
  parseDrainPid,
  parseLaunchAnnouncement,
  processStartToken,
  readCompleteLines,
} from '../src/host-manager.mjs'
import { startIpcServer } from '../src/ipc-server.mjs'
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

const DRAIN_BIN = join(import.meta.dirname, '../src/host-drain.mjs')

function isLoopback(origin) {
  const hostname = new URL(origin).hostname.replace(/^\[|\]$/gu, '')
  return hostname === 'localhost' || hostname === '::1' || /^127\./u.test(hostname)
}

async function readModelCatalog(remote) {
  const result = await remote.session.modelCatalog()
  assert.equal(result.ok, true, JSON.stringify(result.error ?? {}))
  return result.value
}

test('slice1: readiness accepts only a complete announcement line across stdout chunks', () => {
  let buffer = ''
  const feed = chunk => {
    const read = readCompleteLines(buffer, chunk)
    buffer = read.buffer
    return read.lines
  }

  assert.deepEqual(feed('DSH_DRAIN_PID 12'), [])
  let lines = feed('34\n')
  assert.deepEqual(lines.map(parseDrainPid), [1234])

  assert.deepEqual(feed('dsh web: http://127.0.0.1:412'), [])
  assert.deepEqual(feed('34/?token=abc'), [])
  assert.deepEqual(feed('\n').map(parseLaunchAnnouncement), ['http://127.0.0.1:41234/?token=abc'])
  assert.deepEqual(feed('prefix dsh web: http://127.0.0.1:1/?token=x\n').map(parseLaunchAnnouncement), [undefined])
  assert.equal(parseLaunchAnnouncement('dsh web: http://127.0.0.1:1/?token=x'), 'http://127.0.0.1:1/?token=x')
})

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
  await assert.rejects(() => stat(startedData.socketPath), error => error?.code === 'ENOENT')
  const releasedLock = await createLock(lockPath)
  const releaseLock = await releasedLock.acquire({ timeoutMs: 2_000 })
  await releaseLock()

  const restarted = await runCli([
    'host', 'start',
    '--workspace', fixtureEnvironment.cwd,
    '--dsh-home', fixtureEnvironment.dshHome,
    '--patch', fixtureEnvironment.overlayPath,
  ], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(restarted.code, 0, restarted.stderr)
  assert.equal(restarted.envelope?.ok, true, restarted.stdout)
  assert.notEqual(restarted.envelope.data.daemonPid, startedData.daemonPid)
  assert.equal(restarted.envelope.data.connected, true)
  const stoppedAgain = await runCli(['host', 'stop'], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(stoppedAgain.code, 0, stoppedAgain.stderr)
  assert.equal(stoppedAgain.envelope?.ok, true, stoppedAgain.stdout)
  await waitForExit(restarted.envelope.data.daemonPid)
  await waitForExit(restarted.envelope.data.hostPid)
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
  process.kill(-second.envelope.data.daemonPid, 'SIGKILL')
  await waitForExit(second.envelope.data.daemonPid)
  await sleep(100)
  assert.equal(process.kill(secondHostPid, 0), true,
    'a detached Host must survive a daemon process-group signal')

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
  operator.reconnectDelayMs = 100
  operator.reconnectMaxDelayMs = 100
  operator.foregroundWaitMs = 100
  assert.equal(transport.forceDropMux(), 1)
  await waitFor(() => operator.snapshot().connected === false && connectFailures >= 1, {
    timeoutMs: 15_000,
    label: 'the persistent retry loop to make another attempt',
  })

  const foregroundStarted = Date.now()
  await assert.rejects(
    () => operator.withClient(remote => remote.session.modelCatalog()),
    error => error?.code === 'OPERATOR_RECONNECTING',
  )
  const foregroundElapsed = Date.now() - foregroundStarted
  assert.ok(foregroundElapsed < 2_000, 'a foreground RPC must not wait forever on a persistent retry')
  const failuresAtTimeout = connectFailures
  await waitFor(() => connectFailures > failuresAtTimeout, {
    timeoutMs: 3_000,
    label: 'background retries to continue after the foreground timeout',
  })

  operator.reconnectDelayMs = 5_000
  operator.reconnectMaxDelayMs = 5_000
  const failuresBeforeLongWait = connectFailures
  await waitFor(() => connectFailures > failuresBeforeLongWait, {
    timeoutMs: 15_000,
    label: 'the retry loop to enter its long backoff wait',
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

test('slice1: initial /connect outage keeps retrying after a bounded foreground wait', { timeout: 120_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-initial-retry-'))
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
  operator = new OperatorConnection({
    hostManager: manager,
    host,
    upstream,
    foregroundWaitMs: 100,
    reconnectDelayMs: 10,
    reconnectMaxDelayMs: 50,
  })
  const realFetch = operator.fetchImpl
  let connectFailures = 0
  operator.fetchImpl = (input, init) => {
    if (String(input).includes('/connect') && connectFailures < 7) {
      connectFailures += 1
      return Promise.reject(new Error('simulated initial /connect outage'))
    }
    return realFetch(input, init)
  }

  const started = Date.now()
  await assert.rejects(
    () => operator.ensureConnected(),
    error => error?.code === 'OPERATOR_RECONNECTING',
  )
  assert.ok(Date.now() - started < 2_000)
  await waitFor(() => operator.snapshot().connected === true && connectFailures >= 7, {
    timeoutMs: 30_000,
    label: 'initial background reconnect to recover',
  })
  assert.equal(operator.snapshot().connectCount, 1)
  assert.equal(operator.snapshot().reconnectCount, 0)
  await readModelCatalog(await operator.withClient(remote => remote))
})

test('slice1: kernel flock enforces exclusive ownership and releases on process death', { timeout: 60_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-flock-'))
  const lockPath = join(root, 'shared.lock')
  let holder
  t.after(async () => {
    if (holder?.exitCode === null) holder.kill('SIGKILL')
    await rm(root, { recursive: true, force: true })
  })

  const first = await createLock(lockPath)
  const releaseFirst = await first.acquire({ timeoutMs: 1_000 })
  const second = await createLock(lockPath)
  await assert.rejects(
    () => second.acquire({ timeoutMs: 0 }),
    error => error?.code === 'HOST_LOCK_TIMEOUT',
  )
  await releaseFirst()
  const releaseSecond = await second.acquire({ timeoutMs: 1_000 })
  await releaseSecond()

  holder = spawn(process.execPath, [join(import.meta.dirname, 'support/flock-holder.mjs'), lockPath], {
    stdio: ['ignore', 'pipe', 'ignore'],
  })
  await new Promise((resolve, reject) => {
    holder.stdout.once('data', resolve)
    holder.once('error', reject)
  })
  assert.equal(holder.exitCode, null)
  await waitFor(async () => {
    const contender = await createLock(lockPath)
    try {
      const release = await contender.acquire({ timeoutMs: 0 })
      await release()
      return false
    } catch (error) {
      if (error?.code === 'HOST_LOCK_TIMEOUT') return true
      throw error
    }
  }, { timeoutMs: 2_000, intervalMs: 20, label: 'OS-level holder exclusion' })
  const third = await createLock(lockPath)
  await assert.rejects(
    () => third.acquire({ timeoutMs: 0 }),
    error => error?.code === 'HOST_LOCK_TIMEOUT',
  )

  // Kill the holder and acquire before reaping it. The kernel releases the
  // flock when the owner's descriptors close at exit.
  holder.kill('SIGKILL')
  const releaseThird = await third.acquire({ timeoutMs: 5_000 })
  await releaseThird()
  await waitForExit(holder.pid)
})

test('slice1: drainer keeps consuming Host output after the readiness pipe closes', { timeout: 60_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-drain-'))
  const donePath = join(root, 'writer.done')
  const script = [
    "const fs = require('node:fs')",
    "const data = 'x'.repeat(1024)",
    'for (let i = 0; i < 20000; i += 1) fs.writeSync(1, data)',
    `fs.writeFileSync(${JSON.stringify(donePath)}, 'done')`,
    'setTimeout(() => {}, 60000)',
  ].join(';')
  const drainer = spawn(process.execPath, [DRAIN_BIN], {
    env: {
      ...process.env,
      DSH_LIAISON_DRAIN_ARGS: JSON.stringify(['-e', script]),
      DSH_LIAISON_DRAIN_CWD: root,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let output = ''
  let hostPid
  t.after(async () => {
    if (hostPid !== undefined) {
      try { process.kill(hostPid, 'SIGKILL') } catch {}
    }
    if (drainer.exitCode === null) drainer.kill('SIGKILL')
    await rm(root, { recursive: true, force: true })
  })
  drainer.stdout.setEncoding('utf8')
  drainer.stdout.on('data', chunk => { output += chunk })
  await waitFor(() => {
    const match = /DSH_DRAIN_PID (\d+)/u.exec(output)
    if (match?.[1] === undefined) return false
    hostPid = Number(match[1])
    return true
  }, { label: 'drainer to report its Host pid' })

  drainer.stdout.destroy()
  drainer.stderr.destroy()
  await waitFor(async () => {
    try { return (await stat(donePath)).isFile() } catch { return false }
  }, { timeoutMs: 15_000, label: 'the writer to finish after relay close' })
  assert.equal(process.kill(hostPid, 0), true)
  process.kill(hostPid, 'SIGKILL')
  await waitForExit(drainer.pid)
})

test('slice1: write-ahead launch marker terminates a verified orphan before spawning replacement', { timeout: 180_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-writeahead-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  await mkdir(stateDir, { recursive: true, mode: 0o700 })
  const marker = randomUUID()
  const orphan = spawn(process.execPath, ['-e', 'setTimeout(() => {}, 60_000)', DSH_BIN, 'web'], {
    env: { ...process.env, DSH_LIAISON_HOST_MARKER: marker },
    stdio: 'ignore',
  })
  const manager = new HostManager({ stateDir })
  t.after(async () => {
    if (orphan.exitCode === null) orphan.kill('SIGKILL')
    await manager.stopHost().catch(() => {})
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })
  await waitFor(() => orphan.pid !== undefined && orphan.exitCode === null, { label: 'orphan start' })
  await writeFile(join(stateDir, 'host.json'), JSON.stringify({
    version: 1,
    state: 'launching',
    marker,
    drainerMarker: randomUUID(),
    dshHome: fixtureEnvironment.dshHome,
    cwd: fixtureEnvironment.cwd,
    requestedPort: 0,
    startedAt: new Date().toISOString(),
  }), { mode: 0o600 })

  const host = await manager.ensureHost({
    dshHome: fixtureEnvironment.dshHome,
    cwd: fixtureEnvironment.cwd,
    patches: [fixtureEnvironment.overlayPath],
    env: fixtureEnvironment.env,
  })
  await waitForExit(orphan.pid)
  assert.notEqual(host.pid, orphan.pid, 'the orphan must not be adopted as the ready Host')
  const record = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8'))
  assert.equal(record.state, 'ready')
  assert.equal(record.pid, host.pid)
  assert.equal(record.marker, host.marker)
})

test('slice1: starting record with pid identity is terminated before replacement', { timeout: 180_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-starting-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  await mkdir(stateDir, { recursive: true, mode: 0o700 })
  const marker = randomUUID()
  const orphan = spawn(process.execPath, ['-e', 'setTimeout(() => {}, 60_000)', DSH_BIN, 'web'], {
    env: { ...process.env, DSH_LIAISON_HOST_MARKER: marker },
    stdio: 'ignore',
  })
  const manager = new HostManager({ stateDir })
  t.after(async () => {
    if (orphan.exitCode === null) orphan.kill('SIGKILL')
    await manager.stopHost().catch(() => {})
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })
  await waitFor(() => orphan.pid !== undefined && orphan.exitCode === null, { label: 'starting orphan start' })
  await writeFile(join(stateDir, 'host.json'), JSON.stringify({
    version: 1,
    state: 'starting',
    pid: orphan.pid,
    procStart: await processStartToken(orphan.pid),
    marker,
    drainerMarker: randomUUID(),
    dshHome: fixtureEnvironment.dshHome,
    cwd: fixtureEnvironment.cwd,
    requestedPort: 0,
    startedAt: new Date().toISOString(),
  }), { mode: 0o600 })

  const host = await manager.ensureHost({
    dshHome: fixtureEnvironment.dshHome,
    cwd: fixtureEnvironment.cwd,
    patches: [fixtureEnvironment.overlayPath],
    env: fixtureEnvironment.env,
  })
  await waitForExit(orphan.pid)
  assert.notEqual(host.pid, orphan.pid, 'the starting orphan must not become the ready Host')
  const record = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8'))
  assert.equal(record.state, 'ready')
  assert.equal(record.pid, host.pid)
})

test('slice1: real drainer and Host crash before pid recording stops the launcher first and replaces both', { timeout: 180_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-real-crash-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  await mkdir(stateDir, { recursive: true, mode: 0o700 })
  const marker = randomUUID()
  const drainerMarker = randomUUID()
  const args = [
    DSH_BIN, 'web',
    '--patch', fixtureEnvironment.overlayPath,
    '--no-open', '--host', '127.0.0.1', '--port', '0',
  ]
  const drainerEnv = {
    ...fixtureEnvironment.env,
    DSH_LIAISON_DRAIN_ARGS: JSON.stringify(args),
    DSH_LIAISON_DRAIN_CWD: fixtureEnvironment.cwd,
    DSH_LIAISON_DRAIN_MARKER: drainerMarker,
    DSH_LIAISON_DRAIN_HOST_MARKER: marker,
  }
  delete drainerEnv[HOST_MARKER_ENV]
  await writeFile(join(stateDir, 'host.json'), JSON.stringify({
    version: 1,
    state: 'launching',
    marker,
    drainerMarker,
    dshHome: fixtureEnvironment.dshHome,
    cwd: fixtureEnvironment.cwd,
    requestedPort: 0,
    startedAt: new Date().toISOString(),
  }), { mode: 0o600 })
  const drainer = spawn(process.execPath, [DRAIN_BIN], {
    cwd: fixtureEnvironment.cwd,
    env: drainerEnv,
    detached: true,
    stdio: 'ignore',
  })
  const manager = new HostManager({ stateDir })
  let hostPid
  t.after(async () => {
    if (hostPid !== undefined) {
      try { process.kill(hostPid, 'SIGKILL') } catch {}
    }
    if (drainer.exitCode === null) drainer.kill('SIGKILL')
    await manager.stopHost().catch(() => {})
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  const hostMatches = await waitFor(async () => {
    const found = await findProcessesByMarker(marker, {
      markerEnv: HOST_MARKER_ENV,
      cmdlineIncludes: [DSH_BIN, 'web'],
    })
    return found.matches.length === 1 ? found.matches : false
  }, { timeoutMs: 30_000, label: 'real Host to spawn under the drainer' })
  hostPid = hostMatches[0].pid
  const drainerMatches = await findProcessesByMarker(drainerMarker, {
    markerEnv: DRAIN_MARKER_ENV,
    cmdlineIncludes: [DRAIN_BIN],
  })
  assert.equal(drainerMatches.matches.length, 1)
  assert.equal(drainerMatches.matches[0].pid, drainer.pid)
  assert.notEqual(hostPid, drainer.pid)

  const replacement = await manager.ensureHost({
    dshHome: fixtureEnvironment.dshHome,
    cwd: fixtureEnvironment.cwd,
    patches: [fixtureEnvironment.overlayPath],
    env: fixtureEnvironment.env,
  })
  await waitForExit(drainer.pid)
  await waitForExit(hostPid)
  assert.notEqual(replacement.pid, hostPid)
  const record = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8'))
  assert.equal(record.state, 'ready')
  assert.equal(record.pid, replacement.pid)
})

test('slice1: spawn fails before side effects when process identity is unsupported', { timeout: 60_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-unsupported-'))
  const stateDir = join(root, 'state')
  const manager = new HostManager({
    stateDir,
    procRoot: join(root, 'missing-procfs'),
  })
  t.after(async () => { await rm(root, { recursive: true, force: true }) })

  await assert.rejects(
    () => manager.ensureHost({ dshHome: join(root, 'home'), cwd: root, patches: [] }),
    error => error?.code === 'HOST_IDENTITY_UNSUPPORTED',
  )
  await assert.rejects(() => stat(join(stateDir, 'host.json')), error => error?.code === 'ENOENT')
})

test('slice1: losing process identity during stop retains the record and reports unverified', { timeout: 60_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-stop-identity-'))
  const stateDir = join(root, 'state')
  await mkdir(stateDir, { recursive: true, mode: 0o700 })
  const marker = randomUUID()
  const readyPath = join(root, 'stubborn.ready')
  const stubborn = spawn(process.execPath, [
    '-e',
    `process.on('SIGTERM', () => {}); require('node:fs').writeFileSync(${JSON.stringify(readyPath)}, 'ready'); setTimeout(() => {}, 60_000)`,
  ], {
    env: { ...process.env, DSH_LIAISON_HOST_MARKER: marker },
    stdio: 'ignore',
  })
  const manager = new HostManager({ stateDir })
  t.after(async () => {
    manager.procRoot = '/proc'
    if (stubborn.exitCode === null) stubborn.kill('SIGKILL')
    await manager.stopHost().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })
  await waitFor(async () => {
    try { return (await stat(readyPath)).isFile() } catch { return false }
  }, { label: 'stubborn child to install its SIGTERM handler' })
  const recordPath = join(stateDir, 'host.json')
  await writeFile(recordPath, JSON.stringify({
    version: 1,
    state: 'ready',
    pid: stubborn.pid,
    procStart: await processStartToken(stubborn.pid),
    marker,
    origin: 'http://127.0.0.1:9',
    port: 9,
    launchToken: 'not-a-real-token-but-long-enough',
    dshHome: join(root, 'home'),
    cwd: root,
    startedAt: new Date().toISOString(),
  }), { mode: 0o600 })

  const hide = setTimeout(() => { manager.procRoot = join(root, 'missing-procfs') }, 300)
  const outcome = await manager.stopHost({ timeoutMs: 1_200, killTimeoutMs: 500 })
  clearTimeout(hide)
  assert.equal(outcome.stopped, false)
  assert.equal(outcome.reason, 'unverified')
  assert.equal(outcome.retained, true)
  assert.equal((await stat(recordPath)).isFile(), true, 'an unverified stop must retain the record')
  assert.equal(process.kill(stubborn.pid, 0), true)

  manager.procRoot = '/proc'
  const cleaned = await manager.stopHost({ timeoutMs: 100, killTimeoutMs: 1_000 })
  assert.equal(cleaned.stopped, true)
  await waitForExit(stubborn.pid)
})

test('slice1: daemon socket withdraws before stop ack and destroys accepted sockets on shutdown', { timeout: 60_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-ipc-close-'))
  const stateDir = join(root, 'state')
  const server = await startIpcServer({
    stateDir,
    dispatch: async ({ command }) => ({ envelope: { ok: true, command, data: {} } }),
    onShutdown: () => {},
  })
  t.after(async () => {
    await server.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  const first = createConnection(server.socketPath)
  const second = createConnection(server.socketPath)
  first.on('error', () => {})
  second.on('error', () => {})
  const firstClosed = new Promise(resolve => first.once('close', resolve))
  const secondClosed = new Promise(resolve => second.once('close', resolve))
  await Promise.all([once(first, 'connect'), once(second, 'connect')])
  await server.withdraw()
  await assert.rejects(() => stat(server.socketPath), error => error?.code === 'ENOENT')
  await server.close()
  await Promise.all([firstClosed, secondClosed])
})

test('slice1: invalid cwd is reported without an unhandled child error or stale launch record', { timeout: 60_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-badcwd-'))
  const stateDir = join(root, 'state')
  const manager = new HostManager({ stateDir })
  t.after(async () => { await rm(root, { recursive: true, force: true }) })

  await assert.rejects(
    () => manager.ensureHost({
      dshHome: join(root, 'home'),
      cwd: join(root, 'missing-workspace'),
      patches: [],
    }),
    error => error?.code === 'HOST_SPAWN_FAILED',
  )
  await assert.rejects(() => stat(join(stateDir, 'host.json')), error => error?.code === 'ENOENT')
})

test('slice1: fresh host stop without a state directory is truthful success', { timeout: 60_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-fresh-stop-'))
  const stateDir = join(root, 'never-created')
  const env = { ...process.env, DSH_LIAISON_STATE_DIR: stateDir }
  t.after(async () => { await rm(root, { recursive: true, force: true }) })

  const stopped = await runCli(['host', 'stop'], { cwd: root, env })
  assert.equal(stopped.code, 0, stopped.stderr)
  assert.equal(stopped.envelope?.ok, true, stopped.stdout)
  assert.equal(stopped.envelope.command, 'host stop')
  assert.equal(stopped.envelope.data.daemonPid, null)
  assert.equal(stopped.envelope.data.hostPid, null)
  assert.equal(stopped.envelope.data.stoppedHost, false)
  assert.equal(stopped.envelope.data.stopReason, 'missing')
  await assert.rejects(() => stat(stateDir), error => error?.code === 'ENOENT')
})

test('slice1: unverified stop is a nonzero failure with retained pid details', { timeout: 60_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-unverified-stop-'))
  const stateDir = join(root, 'state')
  await mkdir(stateDir, { recursive: true, mode: 0o700 })
  const sleeper = spawn(process.execPath, ['-e', 'setTimeout(() => {}, 60_000)'], { stdio: 'ignore' })
  t.after(async () => {
    if (sleeper.exitCode === null) sleeper.kill('SIGKILL')
    await rm(root, { recursive: true, force: true })
  })
  await waitFor(() => sleeper.pid !== undefined && sleeper.exitCode === null, { label: 'sleeper start' })
  const recordPath = join(stateDir, 'host.json')
  await writeFile(recordPath, JSON.stringify({
    version: 1,
    state: 'ready',
    pid: sleeper.pid,
    marker: randomUUID(),
    origin: 'http://127.0.0.1:9',
    port: 9,
    launchToken: 'not-a-real-token-but-long-enough',
    dshHome: join(root, 'home'),
    cwd: root,
    startedAt: new Date().toISOString(),
  }), { mode: 0o600 })
  const env = { ...process.env, DSH_LIAISON_STATE_DIR: stateDir }
  const stopped = await runCli(['host', 'stop'], { cwd: root, env })
  assert.equal(stopped.code, 1)
  assert.equal(stopped.envelope?.ok, false)
  assert.equal(stopped.envelope.error.code, 'HOST_STOP_INCOMPLETE')
  assert.equal(stopped.envelope.error.details.reason, 'unverified')
  assert.equal(stopped.envelope.error.details.pid, sleeper.pid)
  assert.equal(stopped.envelope.error.details.retained, true)
  assert.equal((await stat(recordPath)).isFile(), true)
  assert.equal(process.kill(sleeper.pid, 0), true)
})

test('slice1: failed stop after operator close exits safely and preserves Host ownership', { timeout: 120_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-stop-fail-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  const env = { ...fixtureEnvironment.env, DSH_LIAISON_STATE_DIR: stateDir }
  let hostPid
  t.after(async () => {
    if (hostPid !== undefined) {
      try { process.kill(hostPid, 'SIGKILL') } catch {}
    }
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  const started = await runCli([
    'host', 'start',
    '--workspace', fixtureEnvironment.cwd,
    '--dsh-home', fixtureEnvironment.dshHome,
    '--patch', fixtureEnvironment.overlayPath,
  ], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(started.code, 0, started.stderr)
  assert.equal(started.envelope?.ok, true, started.stdout)
  hostPid = started.envelope.data.hostPid
  const recordPath = join(stateDir, 'host.json')
  const record = JSON.parse(await readFile(recordPath, 'utf8'))
  await writeFile(recordPath, JSON.stringify({
    ...record,
    origin: 'http://198.51.100.10:1234',
    port: 1234,
  }), { mode: 0o600 })

  const stopped = await runCli(['host', 'stop'], { cwd: fixtureEnvironment.cwd, env })
  assert.equal(stopped.code, 1)
  assert.equal(stopped.envelope?.ok, false)
  assert.equal(stopped.envelope.error.code, 'HOST_ORIGIN_UNSAFE')
  await waitForExit(started.envelope.data.daemonPid)
  assert.equal(process.kill(hostPid, 0), true, 'the Host must survive a failed stop')
  const retained = JSON.parse(await readFile(recordPath, 'utf8'))
  assert.equal(retained.origin, 'http://198.51.100.10:1234')
})

test('slice1: a start racing stop cannot leave a Host after stop success', { timeout: 180_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-race-'))
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
    await new HostManager({ stateDir }).stopHost().catch(() => {})
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  const first = await runCli(args, { cwd: fixtureEnvironment.cwd, env })
  assert.equal(first.code, 0, first.stderr)
  assert.equal(first.envelope?.ok, true, first.stdout)
  process.kill(first.envelope.data.hostPid, 'SIGKILL')
  await waitForExit(first.envelope.data.hostPid)

  const startPromise = runCli(args, { cwd: fixtureEnvironment.cwd, env })
  await sleep(300)
  const stopPromise = runCli(['host', 'stop'], { cwd: fixtureEnvironment.cwd, env })
  const [startResult, stopResult] = await Promise.all([startPromise, stopPromise])
  assert.equal(stopResult.code, 0, stopResult.stderr)
  assert.equal(stopResult.envelope?.ok, true, stopResult.stdout)
  if (startResult.envelope?.ok === true) {
    await waitForExit(startResult.envelope.data.hostPid)
  } else {
    assert.equal(startResult.envelope?.error?.code, 'DAEMON_CLOSING')
  }
  await waitFor(async () => {
    try { await stat(join(stateDir, 'host.json')); return false } catch (error) {
      if (error?.code === 'ENOENT') return true
      throw error
    }
  }, { timeoutMs: 10_000, label: 'no Host record after stop success' })
})

test('slice1: start waits through a daemon-less stop ownership deadline', { timeout: 240_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice1-start-during-stop-'))
  const mock = await startScriptedMessagesServer()
  const fixtureEnvironment = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  await mkdir(stateDir, { recursive: true, mode: 0o700 })
  const env = { ...fixtureEnvironment.env, DSH_LIAISON_STATE_DIR: stateDir }
  const marker = randomUUID()
  const readyPath = join(root, 'slow-stop.ready')
  const stubborn = spawn(process.execPath, [
    '-e',
    `process.on('SIGTERM', () => {}); require('node:fs').writeFileSync(${JSON.stringify(readyPath)}, 'ready'); setTimeout(() => {}, 60_000)`,
  ], {
    env: { ...process.env, DSH_LIAISON_HOST_MARKER: marker },
    stdio: 'ignore',
  })
  t.after(async () => {
    if (stubborn.exitCode === null) stubborn.kill('SIGKILL')
    await new HostManager({ stateDir }).stopHost().catch(() => {})
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })
  await waitFor(async () => {
    try { return (await stat(readyPath)).isFile() } catch { return false }
  }, { label: 'slow stop child readiness' })
  await writeFile(join(stateDir, 'host.json'), JSON.stringify({
    version: 1,
    state: 'ready',
    pid: stubborn.pid,
    procStart: await processStartToken(stubborn.pid),
    marker,
    origin: 'http://127.0.0.1:9',
    port: 9,
    launchToken: 'not-a-real-token-but-long-enough',
    dshHome: fixtureEnvironment.dshHome,
    cwd: fixtureEnvironment.cwd,
    startedAt: new Date().toISOString(),
  }), { mode: 0o600 })

  const stopPromise = runCli(['host', 'stop'], { cwd: fixtureEnvironment.cwd, env })
  await sleep(300)
  const startPromise = runCli([
    'host', 'start',
    '--workspace', fixtureEnvironment.cwd,
    '--dsh-home', fixtureEnvironment.dshHome,
    '--patch', fixtureEnvironment.overlayPath,
  ], { cwd: fixtureEnvironment.cwd, env })
  const [stopped, started] = await Promise.all([stopPromise, startPromise])
  assert.equal(stopped.code, 0, stopped.stderr)
  assert.equal(stopped.envelope?.ok, true, stopped.stdout)
  assert.equal(stopped.envelope.data.stoppedHost, true)
  assert.equal(started.code, 0, started.stderr)
  assert.equal(started.envelope?.ok, true, started.stdout)
  assert.notEqual(started.envelope.data.hostPid, stubborn.pid)
  await waitForExit(stubborn.pid)
  await runCli(['host', 'stop'], { cwd: fixtureEnvironment.cwd, env })
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
    state: 'ready',
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
  assert.equal(stopped.retained, true)
  assert.equal((await stat(recordPath)).isFile(), true, 'the unverified record must be retained')
  process.kill(sleeper.pid, 0)
  assert.equal(sleeper.exitCode, null, 'an unverified pid must never receive a signal')

  await writeFile(recordPath, JSON.stringify({
    version: 1,
    state: 'ready',
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

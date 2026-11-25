import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { test } from 'node:test'
import { loadUpstream } from '../src/upstream.mjs'
import { HostManager } from '../src/host-manager.mjs'
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

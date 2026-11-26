import { strict as assert } from 'node:assert'
import { spawn, spawnSync } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readdir, readFile, readlink, rm, stat, writeFile } from 'node:fs/promises'
import { createConnection, createServer, type Socket } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import type { ChildProcess } from 'node:child_process'
import { createDeadline } from '../src/deadline.ts'
import { LiaisonError } from '../src/errors.ts'
import { acquireLock } from '../src/flock.ts'
import { HostManager } from '../src/host-manager.ts'
import { callDaemon } from '../src/ipc-client.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))
const binPath = join(projectRoot, 'bin', 'dsh-liaison.ts')

interface CliResult {
  readonly status: number | null
  readonly stdout: string
  readonly stderr: string
  readonly json: unknown
}

interface StartedHost {
  readonly daemonPid: number
  readonly hostPid: number
  readonly origin: string
  readonly port: number
}

await test('lifecycle: attach, status, ping, stop, keep-host, daemon loss, and concurrency', { timeout: 300_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-lifecycle-'))
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env = cliEnv(root, dshHome, stateDir)
  const cleanupPids = new Set<number>()
  try {
    await mkdir(workspace, { recursive: true })
    const first = startHost(env, workspace)
    cleanupPids.add(first.daemonPid)
    cleanupPids.add(first.hostPid)

    const second = startHost(env, workspace)
    assert.equal(second.hostPid, first.hostPid, 'a second start must attach to the existing Host')
    assert.equal(second.daemonPid, first.daemonPid, 'a second start must reuse the daemon')

    const statusRun = runCli(env, workspace, ['host', 'status'])
    assert.equal(statusRun.status, 0, statusRun.stderr)
    const status = objectOf(statusRun.json)
    assert.equal(status.ok, true)
    assert.equal(status.state, 'ready')
    assert.equal(status.hostPid, first.hostPid)
    assert.equal(status.daemonPid, first.daemonPid)
    assert.equal(status.identity, 'alive-verified')
    assert.equal(status.reachable, true)
    assert.ok(!statusRun.stdout.includes('launchToken'))
    assert.ok(!statusRun.stdout.includes('dsh-auth-'))

    const pingRun = runCli(env, workspace, ['host', 'ping'])
    assert.equal(pingRun.status, 0, pingRun.stderr)
    const ping = objectOf(pingRun.json)
    assert.equal(ping.ok, true)
    assert.equal(ping.hostPid, first.hostPid)
    assert.equal(ping.daemonPid, first.daemonPid)
    assert.equal(typeof ping.sessions, 'number')
    assert.ok(!pingRun.stdout.includes('launchToken'))
    assert.ok(!pingRun.stdout.includes('dsh-auth-'))

    await assertNoTcpPorts(first.daemonPid)

    const stopRun = runCli(env, workspace, ['host', 'stop'])
    assert.equal(stopRun.status, 0, stopRun.stderr)
    const stopped = objectOf(stopRun.json)
    assert.equal(stopped.ok, true)
    assert.equal(stopped.hostPid, first.hostPid)
    assert.equal(stopped.stoppedHost, true)
    assert.equal(stopped.stopReason, 'stopped')
    await assertGone(first.hostPid)
    await assertGone(first.daemonPid)
    cleanupPids.delete(first.hostPid)
    cleanupPids.delete(first.daemonPid)
    assert.equal(existsSync(join(stateDir, 'daemon.sock')), false)
    assert.equal(existsSync(join(stateDir, 'host.json')), false)
    assert.equal(existsSync(join(stateDir, 'host-observer.patch.json')), false)

    // An immediate normal start after a successful stop must wait for the
    // withdrawing daemon instead of being rejected by a half-dead socket.
    const immediate = startHost(env, workspace)
    cleanupPids.add(immediate.daemonPid)
    cleanupPids.add(immediate.hostPid)
    const immediateStop = runCli(env, workspace, ['host', 'stop'])
    assert.equal(immediateStop.status, 0, immediateStop.stderr)
    await assertGone(immediate.hostPid)
    await assertGone(immediate.daemonPid)
    cleanupPids.delete(immediate.hostPid)
    cleanupPids.delete(immediate.daemonPid)

    // A daemon created by an earlier ping must still honor a later start's
    // explicit port instead of reusing its own launch defaults.
    const pingWithoutHost = runCli(env, workspace, ['host', 'ping'])
    assert.notEqual(pingWithoutHost.status, 0)
    const explicitPort = await freePort()
    const explicit = startHost(env, workspace, explicitPort)
    cleanupPids.add(explicit.daemonPid)
    cleanupPids.add(explicit.hostPid)
    assert.equal(explicit.port, explicitPort, 'host start must honor --port when the daemon already exists')
    const explicitStop = runCli(env, workspace, ['host', 'stop'])
    assert.equal(explicitStop.status, 0, explicitStop.stderr)
    await assertGone(explicit.hostPid)
    await assertGone(explicit.daemonPid)
    cleanupPids.delete(explicit.hostPid)
    cleanupPids.delete(explicit.daemonPid)

    const missingState = join(root, 'never-created')
    const missingRun = runCli(cliEnv(root, dshHome, missingState), workspace, ['host', 'stop'])
    assert.equal(missingRun.status, 0, missingRun.stderr)
    assert.equal(objectOf(missingRun.json).stopReason, 'absent')
    assert.equal(existsSync(missingState), false, 'a missing-state stop must not create the directory')

    const kept = startHost(env, workspace)
    cleanupPids.add(kept.daemonPid)
    cleanupPids.add(kept.hostPid)
    const keepRun = runCli(env, workspace, ['host', 'stop', '--keep-host'])
    assert.equal(keepRun.status, 0, keepRun.stderr)
    const keptStop = objectOf(keepRun.json)
    assert.equal(keptStop.stoppedHost, false)
    assert.equal(keptStop.stopReason, 'kept')
    await assertGone(kept.daemonPid)
    cleanupPids.delete(kept.daemonPid)
    assert.doesNotThrow(() => { process.kill(kept.hostPid, 0) }, 'keep-host must leave the Host alive')
    assert.equal(existsSync(join(stateDir, 'host.json')), true)

    const reattached = startHost(env, workspace)
    assert.equal(reattached.hostPid, kept.hostPid, 'a new daemon must attach to the kept Host')
    assert.notEqual(reattached.daemonPid, kept.daemonPid)
    cleanupPids.add(reattached.daemonPid)

    process.kill(reattached.daemonPid, 'SIGKILL')
    await assertGone(reattached.daemonPid)
    cleanupPids.delete(reattached.daemonPid)
    assert.doesNotThrow(() => { process.kill(reattached.hostPid, 0) }, 'daemon loss must not kill the detached Host')
    const afterKillStart = startHost(env, workspace)
    assert.equal(afterKillStart.hostPid, reattached.hostPid, 'a fresh daemon must reattach the surviving Host')
    assert.notEqual(afterKillStart.daemonPid, reattached.daemonPid)
    cleanupPids.add(afterKillStart.daemonPid)
    const afterKill = runCli(env, workspace, ['host', 'stop'])
    assert.equal(afterKill.status, 0, afterKill.stderr)
    assert.equal(objectOf(afterKill.json).stoppedHost, true)
    await assertGone(reattached.hostPid)
    cleanupPids.delete(reattached.hostPid)
    await assertGone(afterKillStart.daemonPid)
    cleanupPids.delete(afterKillStart.daemonPid)

    const concurrentDir = join(root, 'concurrent-state')
    const concurrentEnv = cliEnv(root, join(root, 'concurrent-dsh'), concurrentDir)
    const concurrent = await Promise.all([
      spawnCli(concurrentEnv, workspace, ['host', 'start']),
      spawnCli(concurrentEnv, workspace, ['host', 'start']),
    ])
    for (const result of concurrent) assert.equal(result.status, 0, result.stderr)
    const started = concurrent.map(result => startedHost(result.json))
    const hostPids = new Set(started.map(host => host.hostPid))
    const daemonPids = new Set(started.map(host => host.daemonPid))
    assert.equal(hostPids.size, 1, 'concurrent starts must share one Host')
    assert.equal(daemonPids.size, 1, 'concurrent starts must share one daemon')
    for (const pid of [...hostPids, ...daemonPids]) cleanupPids.add(pid)
    const concurrentStop = runCli(concurrentEnv, workspace, ['host', 'stop'])
    assert.equal(concurrentStop.status, 0, concurrentStop.stderr)
    for (const pid of [...hostPids, ...daemonPids]) {
      await assertGone(pid)
      cleanupPids.delete(pid)
    }

    // Establish one live daemon, start a normal CLI launch, wait until its
    // launching record exists, then queue a second start and the stop on that
    // same original socket. No independent CLI arrival order is guessed.
    const raceDir = join(root, 'race-state')
    const raceEnv = cliEnv(root, join(root, 'race-dsh'), raceDir)
    const primePing = runCli(raceEnv, workspace, ['host', 'ping'])
    assert.notEqual(primePing.status, 0, 'ping primes a daemon without a Host')
    const primeStatus = runCli(raceEnv, workspace, ['host', 'status'])
    assert.equal(primeStatus.status, 0, primeStatus.stderr)
    const primeDaemon = objectOf(primeStatus.json).daemonPid
    assert.equal(typeof primeDaemon, 'number')
    cleanupPids.add(primeDaemon as number)

    const firstStart = spawnCli(raceEnv, workspace, ['host', 'start'])
    await waitForHostRecord(raceDir, record => record.state === 'launching')
    const session = openIpcSession(join(raceDir, 'daemon.sock'))
    try {
      const queuedStart = session.call('host.start', {})
      const stop = session.call('host.stop', { keepHost: false })
      const outcomes = await Promise.allSettled([queuedStart.promise, stop.promise, firstStart])
      const [queuedOutcome, stopOutcome, firstOutcome] = outcomes
      assert.equal(firstOutcome.status, 'fulfilled')
      assert.equal(stopOutcome.status, 'fulfilled')
      assert.equal(queuedOutcome.status, 'fulfilled')
      const first = firstOutcome.value
      assert.equal(first.status, 0, first.stderr)
      const firstHost = startedHost(first.json)
      assert.equal(firstHost.daemonPid, primeDaemon, 'the ordered start must use the original daemon')
      cleanupPids.add(firstHost.hostPid)
      cleanupPids.add(firstHost.daemonPid)

      const queued = queuedOutcome.value
      if (queued.ok) {
        const queuedHost = objectOf(queued.result)
        assert.equal(queuedHost.hostPid, firstHost.hostPid, 'an admitted queued start must bind the same Host')
      } else {
        assert.equal(queued.error?.code, 'DAEMON_STOPPING', 'a queued start must observe the stop fence')
      }
      const stopReply = stopOutcome.value
      assert.equal(stopReply.ok, true, `stop on the original daemon failed: ${stopReply.error?.message ?? ''}`)
      assert.equal(objectOf(stopReply.result).stoppedHost, true)

      await assertGone(firstHost.hostPid)
      await assertGone(firstHost.daemonPid)
      cleanupPids.delete(firstHost.hostPid)
      cleanupPids.delete(firstHost.daemonPid)
    } finally {
      session.close()
    }
    await assertGone(primeDaemon as number)
    cleanupPids.delete(primeDaemon as number)
    assert.equal(existsSync(join(raceDir, 'host.json')), false, 'a successful stop must not leave a ready Host record')
    assert.equal(existsSync(join(raceDir, 'daemon.sock')), false, 'a successful stop must withdraw the daemon socket')
  } finally {
    runCli(env, workspace, ['host', 'stop'])
    for (const pid of cleanupPids) {
      try { process.kill(pid, 'SIGKILL') } catch { /* already gone */ }
    }
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('expired start and stop waiting on host.lock leave no action or fence', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-lock-expiry-'))
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env = cliEnv(root, dshHome, stateDir)
  const cleanupPids = new Set<number>()
  let held: Awaited<ReturnType<typeof acquireLock>> | undefined
  try {
    await mkdir(workspace, { recursive: true })
    const prime = runCli(env, workspace, ['host', 'ping'])
    assert.notEqual(prime.status, 0, 'ping primes a daemon without a Host')
    const status = runCli(env, workspace, ['host', 'status'])
    assert.equal(status.status, 0, status.stderr)
    const daemonPid = objectOf(status.json).daemonPid
    assert.equal(typeof daemonPid, 'number')
    cleanupPids.add(daemonPid as number)
    held = await acquireLock(join(stateDir, 'host.lock'), { waitMs: 0 })
    assert.ok(held !== undefined, 'the test must hold the kernel host lock')
    const socketPath = join(stateDir, 'daemon.sock')
    const queuedStart = callDaemon(socketPath, 'host.start', {}, 250)
    const queuedStop = callDaemon(socketPath, 'host.stop', { keepHost: false }, 250)
    const outcomes = await Promise.allSettled([queuedStart, queuedStop])
    for (const outcome of outcomes) {
      assert.equal(outcome.status, 'rejected')
      if (outcome.status === 'rejected') {
        assert.ok(outcome.reason instanceof LiaisonError)
        assert.equal(outcome.reason.code, 'IPC_TIMEOUT')
        assert.match(outcome.reason.message, /outcome may be unknown|deadline expired/u)
      }
    }
    assert.equal(existsSync(join(stateDir, 'host.json')), false, 'an expired start must not create ownership')
    assert.notEqual(await procState(daemonPid as number), undefined, 'the daemon must remain alive')
    assert.notEqual(await procState(daemonPid as number), 'Z')
    assert.equal(existsSync(socketPath), true, 'the expired stop must not withdraw the daemon')

    await held.release()
    held = undefined
    const fresh = startHost(env, workspace)
    assert.equal(fresh.daemonPid, daemonPid, 'the original daemon must remain usable')
    cleanupPids.add(fresh.hostPid)
    cleanupPids.add(fresh.daemonPid)
    const stopped = runCli(env, workspace, ['host', 'stop'])
    assert.equal(stopped.status, 0, stopped.stderr)
    await assertGone(fresh.hostPid)
    await assertGone(fresh.daemonPid)
    cleanupPids.delete(fresh.hostPid)
    cleanupPids.delete(fresh.daemonPid)
    cleanupPids.delete(daemonPid as number)
  } finally {
    await held?.release()
    runCli(env, workspace, ['host', 'stop'])
    for (const pid of cleanupPids) {
      try { process.kill(pid, 'SIGKILL') } catch { /* already gone */ }
    }
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('expired keep-host waiting on host.lock leaves daemon and attachment usable', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-keep-lock-expiry-'))
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env = cliEnv(root, dshHome, stateDir)
  const cleanupPids = new Set<number>()
  let held: Awaited<ReturnType<typeof acquireLock>> | undefined
  try {
    await mkdir(workspace, { recursive: true })
    const first = startHost(env, workspace)
    cleanupPids.add(first.daemonPid)
    cleanupPids.add(first.hostPid)
    held = await acquireLock(join(stateDir, 'host.lock'), { waitMs: 0 })
    assert.ok(held !== undefined)
    const socketPath = join(stateDir, 'daemon.sock')
    const expiredKeep = callDaemon(socketPath, 'host.stop', { keepHost: true }, 250)
    await assert.rejects(expiredKeep, (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'IPC_TIMEOUT')
      return true
    })
    assert.notEqual(await procState(first.daemonPid), undefined)
    assert.notEqual(await procState(first.daemonPid), 'Z')
    assert.equal(existsSync(socketPath), true)

    await held.release()
    held = undefined
    const fresh = startHost(env, workspace)
    assert.equal(fresh.hostPid, first.hostPid, 'the expired keep-host must not fence or replace the Host')
    assert.equal(fresh.daemonPid, first.daemonPid)
    const stopped = runCli(env, workspace, ['host', 'stop'])
    assert.equal(stopped.status, 0, stopped.stderr)
    await assertGone(first.hostPid)
    await assertGone(first.daemonPid)
    cleanupPids.delete(first.hostPid)
    cleanupPids.delete(first.daemonPid)
  } finally {
    await held?.release()
    runCli(env, workspace, ['host', 'stop'])
    for (const pid of cleanupPids) {
      try { process.kill(pid, 'SIGKILL') } catch { /* already gone */ }
    }
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('a spawn committed before expiry keeps its readiness and ownership', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-spawn-commit-'))
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env = cliEnv(root, dshHome, stateDir)
  const manager = new HostManager({
    stateDir,
    projectRoot,
    cwd: workspace,
    env,
    port: 0,
    startTimeoutMs: 120_000,
    stopGraceMs: 10_000,
    killGraceMs: 2_000,
  })
  try {
    await mkdir(workspace, { recursive: true })
    const summary = await manager.start({}, createDeadline(100))
    assert.equal(summary.state, 'ready')
    const record = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as Record<string, unknown>
    assert.equal(record.state, 'ready')
    assert.equal(record.hostPid, summary.hostPid)
    assert.doesNotThrow(() => { process.kill(summary.hostPid, 0) })
    const stopped = await manager.stop()
    assert.equal(stopped.stoppedHost, true)
    await assertGone(summary.hostPid)
  } finally {
    await manager.stop().catch(() => undefined)
    await manager.close().catch(() => undefined)
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('expired queued start and stop cannot act later; a fresh request still works', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-expired-queue-'))
  const workspaceA = join(root, 'workspace-a')
  const workspaceB = join(root, 'workspace-b')
  const workspaceC = join(root, 'workspace-c')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env = cliEnv(root, dshHome, stateDir)
  const cleanupPids = new Set<number>()
  try {
    await mkdir(workspaceA, { recursive: true })
    await mkdir(workspaceB, { recursive: true })
    await mkdir(workspaceC, { recursive: true })
    const prime = runCli(env, workspaceA, ['host', 'ping'])
    assert.notEqual(prime.status, 0, 'ping primes a daemon without a Host')
    const primeStatus = runCli(env, workspaceA, ['host', 'status'])
    assert.equal(primeStatus.status, 0, primeStatus.stderr)
    const daemonPid = objectOf(primeStatus.json).daemonPid
    assert.equal(typeof daemonPid, 'number')
    cleanupPids.add(daemonPid as number)

    const firstStart = spawnCli(env, workspaceA, ['host', 'start'])
    await waitForHostRecord(stateDir, record => record.state === 'launching')
    const socketPath = join(stateDir, 'daemon.sock')
    const portB = await freePort()
    const patchB = await writePatch(root)
    const queuedStart = callDaemon(socketPath, 'host.start', {
      cwd: workspaceB,
      port: portB,
      patchPaths: [patchB],
    }, 120)
    const queuedStop = callDaemon(socketPath, 'host.stop', { keepHost: false }, 120)
    const queuedOutcomes = Promise.allSettled([queuedStart, queuedStop])
    await sleep(250)
    const first = await firstStart
    assert.equal(first.status, 0, first.stderr)
    const firstHost = startedHost(first.json)
    cleanupPids.add(firstHost.hostPid)
    cleanupPids.add(firstHost.daemonPid)
    const outcomes = await queuedOutcomes
    for (const outcome of outcomes) {
      assert.equal(outcome.status, 'rejected')
      if (outcome.status === 'rejected') {
        assert.ok(outcome.reason instanceof LiaisonError)
        assert.equal(outcome.reason.code, 'IPC_TIMEOUT')
      }
    }

    const record = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as Record<string, unknown>
    assert.equal(record.state, 'ready')
    assert.equal(record.hostPid, firstHost.hostPid)
    assert.equal(record.cwd, workspaceA, 'the expired start must not apply its cwd')
    const cmdline = record.expectedCmdline
    assert.ok(Array.isArray(cmdline))
    const portIndex = cmdline.indexOf('--port')
    assert.ok(portIndex >= 0)
    assert.equal(cmdline[portIndex + 1], '0', 'the expired start must not apply its port')
    assert.ok(!cmdline.includes(patchB), 'the expired start must not apply its patch')
    assert.notEqual(await procState(firstHost.daemonPid), undefined, 'an expired stop must not withdraw the daemon')
    assert.notEqual(await procState(firstHost.daemonPid), 'Z')
    assert.equal(existsSync(socketPath), true, 'an expired stop must not set the closing fence')

    const fresh = startHost(env, workspaceC)
    assert.equal(fresh.hostPid, firstHost.hostPid, 'a fresh request must still attach the live Host')
    assert.equal(fresh.daemonPid, firstHost.daemonPid)
    cleanupPids.add(fresh.daemonPid)

    const stopped = runCli(env, workspaceC, ['host', 'stop'])
    assert.equal(stopped.status, 0, stopped.stderr)
    await assertGone(firstHost.hostPid)
    await assertGone(firstHost.daemonPid)
    cleanupPids.delete(firstHost.hostPid)
    cleanupPids.delete(firstHost.daemonPid)
    cleanupPids.delete(daemonPid as number)
  } finally {
    runCli(env, workspaceA, ['host', 'stop'])
    for (const pid of cleanupPids) {
      try { process.kill(pid, 'SIGKILL') } catch { /* already gone */ }
    }
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('relaunch applies this start overrides; a live attach keeps the existing Host options', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-overrides-'))
  const workspaceA = join(root, 'workspace-a')
  const workspaceB = join(root, 'workspace-b')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env = cliEnv(root, dshHome, stateDir)
  const cleanupPids = new Set<number>()
  try {
    await mkdir(workspaceA, { recursive: true })
    await mkdir(workspaceB, { recursive: true })
    const first = startHost(env, workspaceA)
    cleanupPids.add(first.daemonPid)
    cleanupPids.add(first.hostPid)
    process.kill(first.hostPid, 'SIGKILL')
    await assertGone(first.hostPid)
    cleanupPids.delete(first.hostPid)
    assert.notEqual(await procState(first.daemonPid), undefined, 'the daemon must survive Host loss')
    assert.notEqual(await procState(first.daemonPid), 'Z')

    const portB = await freePort()
    const patchB = await writePatch(root)
    const relaunchRun = runCli(env, workspaceB, [
      'host', 'start', '--port', String(portB), '--patch', patchB,
    ])
    assert.equal(relaunchRun.status, 0, relaunchRun.stderr)
    const relaunch = startedHost(relaunchRun.json)
    cleanupPids.add(relaunch.hostPid)
    cleanupPids.add(relaunch.daemonPid)
    assert.equal(relaunch.daemonPid, first.daemonPid, 'the existing daemon must perform the relaunch')
    assert.notEqual(relaunch.hostPid, first.hostPid)

    const relaunchedRecord = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as Record<string, unknown>
    assert.equal(relaunchedRecord.cwd, workspaceB)
    assert.equal(relaunchedRecord.port, portB)
    const cmdline = relaunchedRecord.expectedCmdline
    assert.ok(Array.isArray(cmdline))
    assert.ok(cmdline.includes(patchB), 'the relaunch command must carry this invocation patch')
    assert.ok(cmdline.includes(String(portB)), 'the relaunch command must carry this invocation port')

    const ping = runCli(env, workspaceB, ['host', 'ping'])
    assert.equal(ping.status, 0, ping.stderr)
    assert.equal(objectOf(ping.json).hostPid, relaunch.hostPid)

    const portC = await freePort()
    const attachRun = runCli(env, workspaceA, ['host', 'start', '--port', String(portC)])
    assert.equal(attachRun.status, 0, attachRun.stderr)
    const attached = startedHost(attachRun.json)
    assert.equal(attached.hostPid, relaunch.hostPid, 'a live attach must not relaunch')
    assert.equal(attached.daemonPid, first.daemonPid)
    const attachedRecord = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as Record<string, unknown>
    assert.equal(attachedRecord.cwd, workspaceB, 'live attach must not rewrite cwd')
    assert.equal(attachedRecord.port, portB, 'live attach must not rewrite port')
    assert.deepEqual(attachedRecord.expectedCmdline, relaunchedRecord.expectedCmdline)

    const stopped = runCli(env, workspaceA, ['host', 'stop'])
    assert.equal(stopped.status, 0, stopped.stderr)
    await assertGone(relaunch.hostPid)
    await assertGone(relaunch.daemonPid)
    cleanupPids.delete(relaunch.hostPid)
    cleanupPids.delete(relaunch.daemonPid)
  } finally {
    runCli(env, workspaceA, ['host', 'stop'])
    for (const pid of cleanupPids) {
      try { process.kill(pid, 'SIGKILL') } catch { /* already gone */ }
    }
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('keep-host fences a delayed operator attach', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-keep-fence-'))
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env = cliEnv(root, dshHome, stateDir)
  const manager = new HostManager({
    stateDir,
    projectRoot,
    cwd: workspace,
    env,
    port: 0,
    startTimeoutMs: 120_000,
    stopGraceMs: 10_000,
    killGraceMs: 2_000,
  })
  try {
    await mkdir(workspace, { recursive: true })
    await manager.start()
    const record = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as Record<string, unknown>
    const hostPid = record.hostPid
    assert.equal(typeof hostPid, 'number')
    const kept = await manager.stop({ keepHost: true })
    assert.equal(kept.stopReason, 'kept')
    assert.equal(kept.stoppedHost, false)
    assert.doesNotThrow(() => { process.kill(hostPid as number, 0) }, 'keep-host leaves the Host alive')
    assert.equal(existsSync(join(stateDir, 'host.json')), true)

    await assert.rejects(manager.ping(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'OPERATOR_UNAVAILABLE')
      return true
    })
    assert.doesNotThrow(() => { process.kill(hostPid as number, 0) }, 'the fenced ping must not stop the Host')

    const stopped = await manager.stop()
    assert.equal(stopped.stoppedHost, true)
    await assertGone(hostPid as number)
    assert.equal(existsSync(join(stateDir, 'host.json')), false)
  } finally {
    await manager.close().catch(() => undefined)
    await manager.stop().catch(() => undefined)
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('permissions and redaction: private state, one Unix socket, no TCP listener, no credential output', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-permissions-'))
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env = cliEnv(root, dshHome, stateDir)
  const previousUmask = process.umask(0o022)
  let host: StartedHost | undefined
  try {
    await mkdir(workspace, { recursive: true })
    const startRun = runCli(env, workspace, ['host', 'start', '--patch', await writePatch(root)])
    assert.equal(startRun.status, 0, startRun.stderr)
    host = startedHost(startRun.json)
    const record = objectOf(JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as unknown)
    const secret = record.launchToken
    assert.equal(typeof secret, 'string')
    assert.ok(!startRun.stdout.includes(secret as string))
    assert.ok(!startRun.stderr.includes(secret as string))

    assert.equal((await stat(stateDir)).mode & 0o777, 0o700)
    assert.equal((await stat(join(stateDir, 'daemon.sock'))).mode & 0o777, 0o600)
    assert.equal((await stat(join(stateDir, 'host.json'))).mode & 0o777, 0o600)
    assert.equal((await stat(join(stateDir, 'host-observer.patch.json'))).mode & 0o777, 0o600)
    assert.equal((await stat(join(stateDir, 'daemon.lock'))).mode & 0o777, 0o600)
    assert.equal((await stat(join(stateDir, 'host.lock'))).mode & 0o777, 0o600)

    const socketEntries = (await readdir(root)).filter(name => name.endsWith('.sock'))
    assert.deepEqual(socketEntries, [], 'the daemon must not bind a second socket in the test root')
    await assertNoTcpPorts(host.daemonPid)

    const status = runCli(env, workspace, ['host', 'status'])
    const ping = runCli(env, workspace, ['host', 'ping'])
    for (const output of [startRun, status, ping]) {
      assert.ok(!output.stdout.includes(secret as string))
      assert.ok(!output.stderr.includes(secret as string))
      assert.ok(!output.stdout.includes('dsh-auth-'))
      assert.ok(!output.stderr.includes('dsh-auth-'))
    }
    const stop = runCli(env, workspace, ['host', 'stop'])
    assert.equal(stop.status, 0, stop.stderr)
    assert.ok(!stop.stdout.includes(secret as string))
    assert.ok(!stop.stderr.includes(secret as string))
    await assertGone(host.hostPid)
    await assertGone(host.daemonPid)
    host = undefined
  } finally {
    process.umask(previousUmask)
    if (host !== undefined) {
      try { process.kill(host.hostPid, 'SIGKILL') } catch { /* already gone */ }
      try { process.kill(host.daemonPid, 'SIGKILL') } catch { /* already gone */ }
    }
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

async function writePatch(root: string): Promise<string> {
  const path = join(root, 'user.patch.json')
  await writeFile(path, JSON.stringify([{ id: 'ui-settings-general', config: { welcomeNoticeVersion: '2026-09-28.1' } }]))
  return path
}

function cliEnv(root: string, dshHome: string, stateDir: string): NodeJS.ProcessEnv {
  return {
    ...process.env,
    HOME: root,
    DSH_HOME: dshHome,
    DSH_AGENTS_HOME: join(dshHome, 'agents'),
    DSH_LIAISON_STATE_DIR: stateDir,
    DSH_TELEMETRY_DISABLED: '1',
  }
}

function startHost(env: NodeJS.ProcessEnv, cwd: string, port = 0): StartedHost {
  const result = runCli(env, cwd, ['host', 'start', '--port', String(port)])
  assert.equal(result.status, 0, `host start failed:\n${result.stdout}\n${result.stderr}`)
  return startedHost(result.json)
}

async function sleep(ms: number): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, ms))
}

async function freePort(): Promise<number> {
  const server = createServer()
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => { resolve() })
  })
  const address = server.address()
  assert.ok(address !== null && typeof address === 'object')
  const port = address.port
  await new Promise<void>((resolve, reject) => {
    server.close((error) => { if (error === undefined) resolve(); else reject(error) })
  })
  return port
}

function startedHost(json: unknown): StartedHost {
  const record = objectOf(json)
  if (record.ok !== true || typeof record.daemonPid !== 'number' || typeof record.hostPid !== 'number'
    || typeof record.origin !== 'string' || typeof record.port !== 'number') {
    throw new Error(`malformed host start result: ${JSON.stringify(json)}`)
  }
  return { daemonPid: record.daemonPid, hostPid: record.hostPid, origin: record.origin, port: record.port }
}

function objectOf(value: unknown): Record<string, unknown> {
  assert.equal(typeof value, 'object')
  assert.ok(value !== null && !Array.isArray(value))
  return value as Record<string, unknown>
}

function runCli(env: NodeJS.ProcessEnv, cwd: string, args: readonly string[]): CliResult {
  const result = spawnSync(process.execPath, [binPath, ...args], {
    cwd,
    env,
    encoding: 'utf8',
    timeout: 150_000,
  })
  const stdout = result.stdout ?? ''
  let json: unknown
  try {
    json = JSON.parse(stdout.trim())
  } catch {
    json = undefined
  }
  return { status: result.status, stdout, stderr: result.stderr ?? '', json }
}

async function spawnCli(env: NodeJS.ProcessEnv, cwd: string, args: readonly string[]): Promise<CliResult> {
  return await new Promise((resolve, reject) => {
    const child: ChildProcess = spawn(process.execPath, [binPath, ...args], { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] })
    let stdout = ''
    let stderr = ''
    const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error('CLI timed out')) }, 150_000)
    child.stdout?.setEncoding('utf8')
    child.stderr?.setEncoding('utf8')
    child.stdout?.on('data', (chunk: string) => { stdout += chunk })
    child.stderr?.on('data', (chunk: string) => { stderr += chunk })
    child.once('error', (error) => { clearTimeout(timer); reject(error) })
    child.once('close', (status) => {
      clearTimeout(timer)
      let json: unknown
      try { json = JSON.parse(stdout.trim()) } catch { json = undefined }
      resolve({ status, stdout, stderr, json })
    })
  })
}

interface RawIpcReply {
  readonly ok: boolean
  readonly result?: unknown
  readonly error?: { readonly code: string; readonly message: string }
}

interface RawIpcSession {
  call(
    method: 'host.start' | 'host.stop',
    params: Record<string, unknown>,
    deadline?: number,
  ): { readonly id: string; readonly promise: Promise<RawIpcReply> }
  close(): void
}

interface PendingCall {
  readonly resolve: (reply: RawIpcReply) => void
  readonly reject: (error: Error) => void
  readonly timer: NodeJS.Timeout
}

function openIpcSession(socketPath: string): RawIpcSession {
  const socket: Socket = createConnection(socketPath)
  socket.setEncoding('utf8')
  const pending = new Map<string, PendingCall>()
  let buffer = ''
  const failAll = (error: Error): void => {
    for (const [id, call] of pending) {
      clearTimeout(call.timer)
      pending.delete(id)
      call.reject(error)
    }
  }
  socket.on('error', failAll)
  socket.on('close', () => { failAll(new Error('IPC session closed before a reply')) })
  socket.on('data', (chunk: string) => {
    buffer += chunk
    while (true) {
      const newline = buffer.indexOf('\n')
      if (newline === -1) return
      const line = buffer.slice(0, newline)
      buffer = buffer.slice(newline + 1)
      if (line.trim() === '') continue
      let parsed: unknown
      try {
        parsed = JSON.parse(line)
      } catch (error) {
        failAll(error instanceof Error ? error : new Error(String(error)))
        return
      }
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) continue
      const record = parsed as Record<string, unknown>
      const id = record.id
      if (typeof id !== 'string') continue
      const call = pending.get(id)
      if (call === undefined) continue
      pending.delete(id)
      clearTimeout(call.timer)
      if (record.ok === true) {
        call.resolve({ ok: true, result: record.result })
        continue
      }
      const error = typeof record.error === 'object' && record.error !== null && !Array.isArray(record.error)
        ? record.error as Record<string, unknown>
        : {}
      call.resolve({
        ok: false,
        error: {
          code: typeof error.code === 'string' ? error.code : 'IPC_PROTOCOL',
          message: typeof error.message === 'string' ? error.message : 'IPC failure',
        },
      })
    }
  })
  return {
    call(method, params, deadline = Date.now() + 30_000) {
      const id = randomUUID()
      const promise = new Promise<RawIpcReply>((resolve, reject) => {
        const timer = setTimeout(() => {
          pending.delete(id)
          reject(new Error(`IPC ${method} timed out`))
        }, Math.max(1, deadline - Date.now()))
        pending.set(id, { resolve, reject, timer })
        socket.write(`${JSON.stringify({ id, method, params, deadline })}\n`)
      })
      return { id, promise }
    },
    close() {
      failAll(new Error('IPC session closed'))
      socket.destroy()
    },
  }
}

async function waitForHostRecord(
  stateDir: string,
  predicate: (record: Record<string, unknown>) => boolean,
): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    try {
      const parsed: unknown = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8'))
      if (typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)) {
        const record = parsed as Record<string, unknown>
        if (predicate(record)) return record
      }
    } catch {
      // The record is atomically replaced; absence or a partial read retries.
    }
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  throw new Error('the Host record did not reach the expected state')
}

async function assertGone(pid: number): Promise<void> {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    const state = await procState(pid)
    // A zombie has exited and cannot run code; Linux keeps only its exit
    // status until the parent reaps it.
    if (state === undefined || state === 'Z') return
    await new Promise(resolve => setTimeout(resolve, 25))
  }
  const state = await procState(pid)
  const cmdline = await readFile(`/proc/${String(pid)}/cmdline`, 'utf8').catch(() => '')
  const fds = await readdir(`/proc/${String(pid)}/fd`).catch(() => [])
  throw new Error(`process ${String(pid)} is still ${state ?? 'unknown'}; cmdline=${JSON.stringify(cmdline)}; fds=${fds.join(',')}`)
}

async function procState(pid: number): Promise<string | undefined> {
  try {
    const text = await readFile(`/proc/${String(pid)}/stat`, 'utf8')
    const close = text.lastIndexOf(')')
    if (close === -1) return '?'
    return text.slice(close + 1).trim().split(/\s+/u)[0]
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    // /proc reads may observe ESRCH when the process exits between lookup and
    // read; that is the same Linux contract as ENOENT for this check.
    if (code === 'ENOENT' || code === 'ESRCH') return undefined
    throw error
  }
}

async function assertNoTcpPorts(pid: number): Promise<void> {
  const fds = await readdir(`/proc/${String(pid)}/fd`).catch(() => [])
  const inodes = new Set<string>()
  for (const fd of fds) {
    const link = await readlink(`/proc/${String(pid)}/fd/${fd}`).catch(() => '')
    const match = /^socket:\[(\d+)\]$/u.exec(link)
    if (match?.[1] !== undefined) inodes.add(match[1])
  }
  if (inodes.size === 0) return
  for (const table of ['/proc/net/tcp', '/proc/net/tcp6']) {
    const text = await readFile(table, 'utf8').catch(() => '')
    for (const line of text.split('\n').slice(1)) {
      const fields = line.trim().split(/\s+/u)
      const inode = fields[9]
      const state = fields[3]
      if (inode !== undefined && state === '0A' && inodes.has(inode)) {
        throw new Error(`process ${String(pid)} owns TCP listening socket inode ${inode}`)
      }
    }
  }
}

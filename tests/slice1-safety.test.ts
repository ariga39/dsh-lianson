import { strict as assert } from 'node:assert'
import { spawn, type ChildProcess } from 'node:child_process'
import { EventEmitter } from 'node:events'
import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { PING_CALL_TIMEOUT_MS, START_CALL_TIMEOUT_MS, STOP_CALL_TIMEOUT_MS } from '../src/cli.ts'
import { LiaisonError } from '../src/errors.ts'
import { HOST_PING_DEADLINE_MS, HostManager, normalizeLoopbackOrigin } from '../src/host-manager.ts'
import { statePaths } from '../src/paths.ts'
import { scanHostProcesses } from '../src/proc-identity.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))

await test('default ports round-trip and foreign or credential origins are refused', () => {
  assert.equal(normalizeLoopbackOrigin('http://127.0.0.1/?token=x').port, 80)
  assert.equal(normalizeLoopbackOrigin('https://localhost/?token=x').port, 443)
  assert.equal(normalizeLoopbackOrigin('http://[::1]:8080/').port, 8080)
  assert.throws(() => normalizeLoopbackOrigin('http://127.0.0.1:0/'), /Host origin/)
  assert.throws(() => normalizeLoopbackOrigin('http://example.com/'), /loopback/)
  assert.throws(() => normalizeLoopbackOrigin('http://user:pass@127.0.0.1/'), /credentials/)
  assert.throws(() => normalizeLoopbackOrigin('http://127.0.0.1/not-root'), /bare root/)
})

await test('stop timeout covers a queued start plus lock and signal bounds', () => {
  assert.ok(
    STOP_CALL_TIMEOUT_MS >= START_CALL_TIMEOUT_MS + 30_000,
    'the stop reply deadline must cover a queued start plus lock and kill bounds',
  )
  assert.ok(
    PING_CALL_TIMEOUT_MS >= HOST_PING_DEADLINE_MS,
    'the CLI ping deadline must cover the complete Host ping foreground budget',
  )
})

await test('a failed probe of a verified live Host returns HOST_UNREACHABLE and keeps ownership', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-probe-'))
  try {
    const stateDir = join(root, 'state')
    const record = readyRecord({ hostPid: process.pid, hostStartTime: '1', origin: 'http://127.0.0.1:9', port: 9 })
    await writeState(stateDir, record)
    const before = await readFile(join(stateDir, 'host.json'), 'utf8')
    const procRoot = join(root, 'proc')
    const procDir = join(procRoot, String(process.pid))
    await mkdir(join(procRoot, 'self'), { recursive: true })
    await writeFile(join(procRoot, 'self', 'stat'), '1 (node) S 0')
    await writeFile(join(procRoot, 'self', 'environ'), '')
    await writeFile(join(procRoot, 'self', 'cmdline'), '')
    await mkdir(procDir, { recursive: true })
    await writeProc(procDir, 'S', '1', record)
    const signals: string[] = []
    const manager = new HostManager({
      stateDir,
      projectRoot,
      cwd: root,
      procRoot,
      startTimeoutMs: 5_000,
      stopGraceMs: 10,
      killGraceMs: 10,
      signalProcess: (_pid, signal) => { signals.push(signal) },
    })
    await assert.rejects(manager.start(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'HOST_UNREACHABLE')
      return true
    })
    assert.equal(signals.length, 0, 'a failed probe must never signal the verified Host')
    assert.equal(await readFile(join(stateDir, 'host.json'), 'utf8'), before, 'ownership must be retained unchanged')
    assert.doesNotThrow(() => { process.kill(process.pid, 0) })
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('start refuses unsupported procfs before creating state or spawning', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-noproc-'))
  try {
    const stateDir = join(root, 'state')
    const manager = new HostManager({
      stateDir,
      projectRoot,
      cwd: root,
      procRoot: join(root, 'absent-proc'),
      startTimeoutMs: 1_000,
    })
    await assert.rejects(manager.start(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'HOST_IDENTITY_UNSUPPORTED')
      return true
    })
    assert.equal(existsSync(stateDir), false, 'a pre-spawn refusal must not create state')
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('unreadable process evidence is unverified, not dead', async (t) => {
  if (process.getuid?.() === 0) {
    t.skip('root can read mode-000 proc files')
    return
  }
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-eacces-'))
  try {
    const stateDir = join(root, 'state')
    const record = readyRecord({ hostPid: process.pid, hostStartTime: '1' })
    await writeState(stateDir, record)
    const procRoot = join(root, 'proc')
    const procDir = join(procRoot, String(process.pid))
    await mkdir(join(procRoot, 'self'), { recursive: true })
    await writeFile(join(procRoot, 'self', 'stat'), '1 (node) S 0')
    await mkdir(procDir, { recursive: true })
    await writeProc(procDir, 'S', '1', record)
    await chmod(join(procDir, 'environ'), 0)
    const signals: string[] = []
    const manager = new HostManager({
      stateDir,
      projectRoot,
      cwd: root,
      procRoot,
      stopGraceMs: 10,
      killGraceMs: 10,
      signalProcess: (_pid, signal) => { signals.push(signal) },
    })
    await assert.rejects(manager.stop(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'HOST_IDENTITY_UNVERIFIED')
      return true
    })
    assert.equal(signals.length, 0)
    assert.equal(existsSync(join(stateDir, 'host.json')), true)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('keep-host reports dead or unverified ownership truthfully and never signals', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-keep-truth-'))
  try {
    const record = readyRecord({ hostPid: process.pid, hostStartTime: '1' })

    const aliveState = join(root, 'alive-state')
    await writeState(aliveState, record)
    const aliveProc = join(root, 'alive-proc')
    const aliveDir = join(aliveProc, String(process.pid))
    await mkdir(join(aliveProc, 'self'), { recursive: true })
    await writeFile(join(aliveProc, 'self', 'stat'), '1 (node) S 0')
    await mkdir(aliveDir, { recursive: true })
    await writeProc(aliveDir, 'S', '1', record)
    const aliveSignals: string[] = []
    const alive = new HostManager({
      stateDir: aliveState,
      projectRoot,
      cwd: root,
      procRoot: aliveProc,
      stopGraceMs: 10,
      killGraceMs: 10,
      signalProcess: (_pid, signal) => { aliveSignals.push(signal) },
    })
    const kept = await alive.stop({ keepHost: true })
    assert.equal(kept.stopReason, 'kept')
    assert.equal(kept.stoppedHost, false)
    assert.equal(aliveSignals.length, 0)
    assert.equal(existsSync(join(aliveState, 'host.json')), true)

    const deadState = join(root, 'dead-state')
    await writeState(deadState, record)
    const deadProc = join(root, 'dead-proc')
    await mkdir(join(deadProc, 'self'), { recursive: true })
    await writeFile(join(deadProc, 'self', 'stat'), '1 (node) S 0')
    const deadSignals: string[] = []
    const dead = new HostManager({
      stateDir: deadState,
      projectRoot,
      cwd: root,
      procRoot: deadProc,
      stopGraceMs: 10,
      killGraceMs: 10,
      signalProcess: (_pid, signal) => { deadSignals.push(signal) },
    })
    const alreadyExited = await dead.stop({ keepHost: true })
    assert.equal(alreadyExited.stopReason, 'already-exited')
    assert.equal(alreadyExited.stoppedHost, false)
    assert.equal(deadSignals.length, 0)
    assert.equal(existsSync(join(deadState, 'host.json')), false)

    const unknownState = join(root, 'unknown-state')
    await writeState(unknownState, record)
    const unknownSignals: string[] = []
    const unknown = new HostManager({
      stateDir: unknownState,
      projectRoot,
      cwd: root,
      procRoot: join(root, 'no-such-proc'),
      stopGraceMs: 10,
      killGraceMs: 10,
      signalProcess: (_pid, signal) => { unknownSignals.push(signal) },
    })
    await assert.rejects(unknown.stop({ keepHost: true }), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'HOST_IDENTITY_UNVERIFIED')
      return true
    })
    assert.equal(unknownSignals.length, 0)
    assert.equal(existsSync(join(unknownState, 'host.json')), true)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('ready record read failures finish as HOST_START_FAILED without an unhandled rejection', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-ready-read-'))
  try {
    for (const mode of ['malformed', 'read-error'] as const) {
      const stateDir = join(root, mode)
      const paths = statePaths(stateDir)
      const child = new EventEmitter() as EventEmitter & { pid: number; unref: () => void }
      child.pid = process.pid
      child.unref = () => undefined
      const spawnProcess = (() => {
        if (mode === 'malformed') {
          writeFileSync(paths.readyFile, '{')
        } else {
          // A directory read fails deterministically even for root, unlike
          // chmod(000) which root can still read.
          mkdirSync(paths.readyFile)
        }
        return child as unknown as ChildProcess
      }) as unknown as typeof spawn
      const manager = new HostManager({
        stateDir,
        projectRoot,
        cwd: root,
        startTimeoutMs: 5_000,
        stopGraceMs: 10,
        killGraceMs: 10,
        spawnProcess,
      })
      await assert.rejects(manager.start(), (error: unknown) => {
        assert.ok(error instanceof LiaisonError)
        assert.equal(error.code, 'HOST_START_FAILED')
        return true
      })
      assert.equal(existsSync(join(stateDir, 'host.json')), false)
      assert.equal(existsSync(join(stateDir, 'host-observer.patch.json')), false)
      assert.equal(existsSync(paths.readyFile), false)
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('malformed and foreign ownership records are refused and retained', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-record-'))
  try {
    for (const [name, text] of [
      ['malformed', '{"version":1'],
      ['foreign', JSON.stringify(readyRecord({ origin: 'http://example.com' }))],
    ] as const) {
      const stateDir = join(root, name)
      await mkdir(stateDir, { recursive: true, mode: 0o700 })
      const recordPath = join(stateDir, 'host.json')
      await writeFile(recordPath, text)
      const manager = new HostManager({
        stateDir,
        projectRoot,
        cwd: root,
        stopGraceMs: 10,
        killGraceMs: 10,
        signalProcess: () => { throw new Error('must not signal an invalid record') },
      })
      await assert.rejects(manager.stop(), (error: unknown) => {
        assert.ok(error instanceof LiaisonError)
        assert.equal(error.code, 'HOST_RECORD_INVALID')
        return true
      })
      assert.equal(existsSync(recordPath), true, `${name} record must be retained`)
      assert.equal(await readFile(recordPath, 'utf8'), text)
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('ready and launching records reject a non-positive hostPid before any effect', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-pid-zero-'))
  try {
    const records: Record<string, Record<string, unknown>> = {
      ready: readyRecord({ hostPid: 0 }),
      launching: {
        version: 1,
        state: 'launching',
        launchId: 'launch-zero',
        observerPatchPath: '/tmp/observer.patch.json',
        readyFile: '/tmp/ready.json',
        expectedCmdline: [process.execPath, '/tmp/fake-host.js'],
        startedAt: 1,
        hostPid: 0,
        hostStartTime: '1',
      },
    }
    for (const [mode, record] of Object.entries(records)) {
      const stateDir = join(root, mode)
      await writeState(stateDir, record)
      const recordPath = join(stateDir, 'host.json')
      const before = await readFile(recordPath, 'utf8')
      const signals: string[] = []
      const manager = new HostManager({
        stateDir,
        projectRoot,
        cwd: root,
        stopGraceMs: 10,
        killGraceMs: 10,
        signalProcess: (_pid, signal) => { signals.push(signal) },
      })
      for (const operation of [
        async (): Promise<void> => { await manager.start() },
        async (): Promise<void> => { await manager.stop() },
      ]) {
        await assert.rejects(operation(), (error: unknown) => {
          assert.ok(error instanceof LiaisonError)
          assert.equal(error.code, 'HOST_RECORD_INVALID')
          return true
        })
        assert.equal(await readFile(recordPath, 'utf8'), before, `${mode} record must be retained unchanged`)
        assert.equal(signals.length, 0, `${mode} invalid pid must never be signalled or cleaned up`)
      }
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('ready and launching records reject a malformed start time before any effect', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-start-time-'))
  try {
    const records: Record<string, Record<string, unknown>> = {
      ready: readyRecord({ hostStartTime: 'bogus' }),
      launching: {
        version: 1,
        state: 'launching',
        launchId: 'launch-bogus',
        observerPatchPath: '/tmp/observer.patch.json',
        readyFile: '/tmp/ready.json',
        expectedCmdline: [process.execPath, '/tmp/fake-host.js'],
        startedAt: 1,
        hostPid: process.pid,
        hostStartTime: 'bogus',
      },
    }
    for (const [mode, record] of Object.entries(records)) {
      const stateDir = join(root, mode)
      await writeState(stateDir, record)
      const recordPath = join(stateDir, 'host.json')
      const before = await readFile(recordPath, 'utf8')
      const signals: string[] = []
      const manager = new HostManager({
        stateDir,
        projectRoot,
        cwd: root,
        stopGraceMs: 10,
        killGraceMs: 10,
        signalProcess: (_pid, signal) => { signals.push(signal) },
      })
      for (const operation of [
        async (): Promise<void> => { await manager.start() },
        async (): Promise<void> => { await manager.stop() },
      ]) {
        await assert.rejects(operation(), (error: unknown) => {
          assert.ok(error instanceof LiaisonError)
          assert.equal(error.code, 'HOST_RECORD_INVALID')
          return true
        })
        assert.equal(await readFile(recordPath, 'utf8'), before, `${mode} record must be retained unchanged`)
        assert.equal(signals.length, 0, `${mode} malformed start time must never cause cleanup or signalling`)
      }
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('missing procfs refuses while a confirmed-absent process is reported already-exited', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-proc-'))
  try {
    const record = readyRecord({ hostPid: process.pid, hostStartTime: '1' })
    const missingState = join(root, 'missing-proc')
    await writeState(missingState, record)
    const signals: string[] = []
    const missing = new HostManager({
      stateDir: missingState,
      projectRoot,
      cwd: root,
      procRoot: join(root, 'does-not-exist'),
      stopGraceMs: 10,
      killGraceMs: 10,
      signalProcess: (_pid, signal) => { signals.push(signal) },
    })
    await assert.rejects(missing.stop(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'HOST_IDENTITY_UNVERIFIED')
      return true
    })
    assert.equal(signals.length, 0)
    assert.equal(existsSync(join(missingState, 'host.json')), true)

    const deadState = join(root, 'dead-proc')
    await writeState(deadState, record)
    const deadProc = join(root, 'empty-proc')
    await mkdir(join(deadProc, 'self'), { recursive: true })
    await writeFile(join(deadProc, 'self', 'stat'), '1 (node) S 0')
    const dead = new HostManager({
      stateDir: deadState,
      projectRoot,
      cwd: root,
      procRoot: deadProc,
      stopGraceMs: 10,
      killGraceMs: 10,
      signalProcess: (_pid, signal) => { signals.push(signal) },
    })
    const outcome = await dead.stop()
    assert.equal(outcome.stoppedHost, false)
    assert.equal(outcome.stopReason, 'already-exited')
    assert.equal(signals.length, 0)
    assert.equal(existsSync(join(deadState, 'host.json')), false)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('identity is re-verified before SIGKILL escalation', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-escalate-'))
  try {
    const stateDir = join(root, 'state')
    const record = readyRecord({ hostPid: process.pid, hostStartTime: '1' })
    await writeState(stateDir, record)
    const procRoot = join(root, 'proc')
    const procDir = join(procRoot, String(process.pid))
    await mkdir(join(procRoot, 'self'), { recursive: true })
    await writeFile(join(procRoot, 'self', 'stat'), '1 (node) S 0')
    await mkdir(procDir, { recursive: true })
    await writeProc(procDir, 'S', '1', record)
    const signals: string[] = []
    const manager = new HostManager({
      stateDir,
      projectRoot,
      cwd: root,
      procRoot,
      stopGraceMs: 30,
      killGraceMs: 30,
      signalProcess: (_pid, signal) => {
        signals.push(signal)
        if (signal === 'SIGTERM') {
          // The original process exited and its PID belongs to a different
          // process by the time escalation is considered.
          void writeProc(procDir, 'S', '2', record)
        }
      },
    })
    await manager.stop()
    assert.deepEqual(signals, ['SIGTERM'], 'a reused PID must never receive SIGKILL')
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('a process that survives SIGKILL refuses and keeps ownership evidence', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-survive-'))
  try {
    const stateDir = join(root, 'state')
    const record = readyRecord({ hostPid: process.pid, hostStartTime: '1' })
    await writeState(stateDir, record)
    const procRoot = join(root, 'proc')
    const procDir = join(procRoot, String(process.pid))
    await mkdir(join(procRoot, 'self'), { recursive: true })
    await writeFile(join(procRoot, 'self', 'stat'), '1 (node) S 0')
    await mkdir(procDir, { recursive: true })
    await writeProc(procDir, 'S', '1', record)
    const signals: string[] = []
    const manager = new HostManager({
      stateDir,
      projectRoot,
      cwd: root,
      procRoot,
      stopGraceMs: 20,
      killGraceMs: 20,
      signalProcess: (_pid, signal) => { signals.push(signal) },
    })
    await assert.rejects(manager.stop(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'HOST_STOP_FAILED')
      return true
    })
    assert.deepEqual(signals, ['SIGTERM', 'SIGKILL'])
    assert.equal(existsSync(join(stateDir, 'host.json')), true)
    assert.equal(existsSync(join(stateDir, 'host-observer.patch.json')), true)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('spawn failure is reported and leaves no launch ownership behind', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-spawn-'))
  try {
    const stateDir = join(root, 'state')
    const manager = new HostManager({
      stateDir,
      projectRoot,
      cwd: join(root, 'missing-cwd'),
      startTimeoutMs: 5_000,
      stopGraceMs: 10,
      killGraceMs: 10,
    })
    await assert.rejects(manager.start(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'HOST_START_FAILED')
      return true
    })
    assert.equal(existsSync(join(stateDir, 'host.json')), false)
    assert.equal(existsSync(join(stateDir, 'host-observer.patch.json')), false)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('a real child exiting before readiness fails promptly and cleans ownership', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-early-exit-'))
  let child: ChildProcess | undefined
  try {
    const stateDir = join(root, 'state')
    child = spawn(process.execPath, ['-e', 'process.exit(3)'], { stdio: 'ignore' })
    const spawnProcess = (() => child) as unknown as typeof spawn
    const manager = new HostManager({
      stateDir,
      projectRoot,
      cwd: root,
      startTimeoutMs: 10_000,
      stopGraceMs: 10,
      killGraceMs: 10,
      spawnProcess,
    })
    const startedAt = Date.now()
    await assert.rejects(manager.start(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'HOST_START_FAILED')
      assert.match(error.message, /exited before readiness/u)
      return true
    })
    assert.ok(Date.now() - startedAt < 2_000, 'an early child exit must not wait for the start timeout')
    assert.equal(existsSync(join(stateDir, 'host.json')), false)
    assert.equal(existsSync(join(stateDir, 'host-observer.patch.json')), false)
  } finally {
    try { child?.kill('SIGKILL') } catch { /* already gone */ }
    await rm(root, { recursive: true, force: true })
  }
})

await test('pre-PID scan ignores inherited markers on unrelated commands and refuses ambiguity', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-scan-'))
  const children: Array<ReturnType<typeof spawn>> = []
  try {
    const script = join(root, 'idle-host.mjs')
    await writeFile(script, 'setInterval(() => {}, 1000)\n')
    const launchId = 'scan-launch'
    const observerPatch = join(root, 'observer.patch.json')
    const expected = [process.execPath, script, '--profile', 'web', '--patch', observerPatch]
    const env = { ...process.env, DSH_LIAISON_HOST_ID: launchId }

    const unrelated = spawn(process.execPath, [script, '--other'], { env, stdio: 'ignore' })
    children.push(unrelated)
    const matching = spawn(process.execPath, expected.slice(1), { env, stdio: 'ignore' })
    children.push(matching)

    const unique = await waitForScan(launchId, expected, result =>
      result.kind === 'unique' && result.pid === matching.pid)
    assert.equal(unique.kind, 'unique')

    const second = spawn(process.execPath, expected.slice(1), { env, stdio: 'ignore' })
    children.push(second)
    const ambiguous = await waitForScan(launchId, expected, result => result.kind === 'ambiguous')
    assert.equal(ambiguous.kind, 'ambiguous')
    if (ambiguous.kind === 'ambiguous') assert.equal(ambiguous.pids.length, 2)
  } finally {
    for (const child of children) {
      try { child.kill('SIGKILL') } catch { /* already gone */ }
    }
    await rm(root, { recursive: true, force: true })
  }
})

async function waitForScan(
  launchId: string,
  expectedCmdline: readonly string[],
  predicate: (result: Awaited<ReturnType<typeof scanHostProcesses>>) => boolean,
): Promise<Awaited<ReturnType<typeof scanHostProcesses>>> {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    const result = await scanHostProcesses({ launchId, expectedCmdline })
    if (predicate(result)) return result
    await delay(25)
  }
  throw new Error('process scan did not reach the expected state')
}

function readyRecord(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    version: 1,
    state: 'ready',
    launchId: 'launch-test',
    observerPatchPath: '/tmp/observer.patch.json',
    hostPid: process.pid,
    hostStartTime: '1',
    expectedCmdline: [process.execPath, '/tmp/fake-host.js'],
    origin: 'http://127.0.0.1:9',
    port: 9,
    launchToken: 'launch-token-test',
    dshHome: '/tmp/dsh-home',
    cwd: '/tmp/cwd',
    startedAt: 1,
    ...overrides,
  }
}

async function writeState(stateDir: string, record: Record<string, unknown>): Promise<void> {
  await mkdir(stateDir, { recursive: true, mode: 0o700 })
  await writeFile(join(stateDir, 'host.json'), JSON.stringify(record))
  await writeFile(join(stateDir, 'host-observer.patch.json'), '[]')
}

async function writeProc(
  procDir: string,
  state: string,
  startTime: string,
  record: Record<string, unknown>,
): Promise<void> {
  const fields = Array.from({ length: 20 }, () => '0')
  fields[0] = state
  fields[19] = startTime
  await writeFile(join(procDir, 'stat'), `${String(process.pid)} (node) ${fields.join(' ')}`)
  await writeFile(join(procDir, 'environ'), `DSH_LIAISON_HOST_ID=${String(record.launchId)}\0HOME=/tmp\0`)
  const cmdline = record.expectedCmdline
  assert.ok(Array.isArray(cmdline))
  await writeFile(join(procDir, 'cmdline'), cmdline.map(String).join('\0'))
}

import { strict as assert } from 'node:assert'
import { spawnSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { LiaisonError } from '../src/errors.ts'
import { HostManager } from '../src/host-manager.ts'

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

await test('observer-first: the normal CLI launches a real Host and records a private ready credential', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-observer-'))
  const stateDir = join(root, 'state')
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const userPatch = join(root, 'user.patch.json')
  let daemonPid: number | undefined
  let hostPid: number | undefined
  try {
    await mkdir(workspace, { recursive: true })
    await writeFile(userPatch, JSON.stringify([
      { id: 'ui-settings-general', config: { welcomeNoticeVersion: '2026-09-28.1' } },
    ], null, 2))
    const started = runCli(stateDir, dshHome, workspace, [
      'host', 'start', '--port', '0', '--patch', userPatch,
    ])
    assert.equal(started.status, 0, `host start failed:\nstdout:\n${started.stdout}\nstderr:\n${started.stderr}`)
    const host = startedHost(started.json)
    daemonPid = host.daemonPid
    hostPid = host.hostPid

    assert.ok(Number.isSafeInteger(host.hostPid) && host.hostPid > 0)
    assert.ok(Number.isSafeInteger(host.daemonPid) && host.daemonPid > 0)
    assert.ok(host.port >= 1 && host.port <= 65_535)
    assert.equal(new URL(host.origin).port === '' ? 80 : Number(new URL(host.origin).port), host.port)
    assert.doesNotThrow(() => { process.kill(host.hostPid, 0) }, 'the real Host must be alive')

    const hostRecordPath = join(stateDir, 'host.json')
    const observerPatchPath = join(stateDir, 'host-observer.patch.json')
    const readyFile = join(stateDir, 'host-ready.json')
    const hostRecord = JSON.parse(await readFile(hostRecordPath, 'utf8')) as Record<string, unknown>
    assert.equal(hostRecord.state, 'ready')
    assert.equal(hostRecord.hostPid, host.hostPid)
    assert.equal(hostRecord.origin, host.origin)
    assert.equal(hostRecord.port, host.port)
    assert.equal(typeof hostRecord.launchToken, 'string')
    assert.ok((hostRecord.launchToken as string).length > 0)
    assert.ok(!existsSync(readyFile), 'the one-shot ready file is removed after commit')

    assert.equal((await stat(stateDir)).mode & 0o777, 0o700)
    assert.equal((await stat(hostRecordPath)).mode & 0o777, 0o600)
    assert.equal((await stat(observerPatchPath)).mode & 0o777, 0o600)
    assert.equal((await stat(join(stateDir, 'daemon.sock'))).mode & 0o777, 0o600)

    assert.ok(!started.stdout.includes(hostRecord.launchToken as string), 'CLI stdout must not print the launch token')
    assert.ok(!started.stderr.includes(hostRecord.launchToken as string), 'CLI stderr must not print the launch token')

    const token = encodeURIComponent(hostRecord.launchToken as string)
    const connect = await fetch(`${host.origin}/connect?token=${token}&interactions=answer`, { redirect: 'manual' })
    assert.equal(connect.status, 302, 'the recorded launch token must exchange for an operator credential')
    const cookie = connect.headers.get('set-cookie')
    assert.ok(cookie !== null && cookie.length > 0)
    assert.ok(!started.stdout.includes(cookie ?? ''), 'CLI stdout must not print operator cookies')

    const anonymous = await fetch(`${host.origin}/`, { redirect: 'manual' })
    assert.equal(anonymous.status, 401)
  } finally {
    if (hostPid !== undefined) stopCli(stateDir, dshHome, workspace, 'host', 'stop')
    if (daemonPid !== undefined) {
      try { process.kill(daemonPid, 'SIGKILL') } catch { /* already gone */ }
    }
    if (hostPid !== undefined) {
      try { process.kill(hostPid, 'SIGKILL') } catch { /* already gone */ }
    }
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('observer fail-fast: a custom overlay that suppresses token publication reports HOST_AUTH_UNAVAILABLE', { timeout: 120_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-observ-fail-'))
  const stateDir = join(root, 'state')
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const disableAuth = join(root, 'disable-auth.patch.json')
  const manager = new HostManager({
    stateDir,
    projectRoot,
    cwd: workspace,
    env: {
      ...process.env,
      HOME: workspace,
      DSH_HOME: dshHome,
      DSH_AGENTS_HOME: join(dshHome, 'agents'),
      DSH_TELEMETRY_DISABLED: '1',
    },
    port: 0,
    extraPatchPaths: [disableAuth],
    startTimeoutMs: 60_000,
    stopGraceMs: 5_000,
    killGraceMs: 2_000,
  })
  try {
    await mkdir(workspace, { recursive: true })
    await writeFile(disableAuth, JSON.stringify([{ id: 'authentication-token', disabled: true }]))
    const startedAt = Date.now()
    await assert.rejects(manager.start(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'HOST_AUTH_UNAVAILABLE')
      return true
    })
    assert.ok(Date.now() - startedAt < 30_000, 'an explicit observer failure must not wait for the start timeout')
    assert.equal(existsSync(join(stateDir, 'host.json')), false)
    assert.equal(existsSync(join(stateDir, 'host-observer.patch.json')), false)
  } finally {
    await manager.stop().catch(() => undefined)
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

function startedHost(json: unknown): StartedHost {
  assert.equal(typeof json, 'object')
  assert.ok(json !== null && !Array.isArray(json))
  const record = json as Record<string, unknown>
  assert.equal(record.ok, true)
  const daemonPid = record.daemonPid
  const hostPid = record.hostPid
  const origin = record.origin
  const port = record.port
  if (typeof daemonPid !== 'number' || typeof hostPid !== 'number'
    || typeof origin !== 'string' || typeof port !== 'number') {
    throw new Error(`malformed host start result: ${JSON.stringify(record)}`)
  }
  return { daemonPid, hostPid, origin, port }
}

function runCli(stateDir: string, dshHome: string, cwd: string, args: readonly string[]): CliResult {
  const result = spawnSync(process.execPath, [binPath, ...args], {
    cwd,
    env: {
      ...process.env,
      HOME: cwd,
      DSH_HOME: dshHome,
      DSH_AGENTS_HOME: join(dshHome, 'agents'),
      DSH_LIAISON_STATE_DIR: stateDir,
      DSH_TELEMETRY_DISABLED: '1',
    },
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

function stopCli(stateDir: string, dshHome: string, cwd: string, ...args: readonly string[]): void {
  spawnSync(process.execPath, [binPath, ...args], {
    cwd,
    env: {
      ...process.env,
      HOME: cwd,
      DSH_HOME: dshHome,
      DSH_AGENTS_HOME: join(dshHome, 'agents'),
      DSH_LIAISON_STATE_DIR: stateDir,
      DSH_TELEMETRY_DISABLED: '1',
    },
    encoding: 'utf8',
    timeout: 60_000,
  })
}

import { strict as assert } from 'node:assert'
import { spawn, spawnSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readdir, readFile, readlink, rm, stat, writeFile } from 'node:fs/promises'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import type { ChildProcess } from 'node:child_process'

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
  } finally {
    runCli(env, workspace, ['host', 'stop'])
    for (const pid of cleanupPids) {
      try { process.kill(pid, 'SIGKILL') } catch { /* already gone */ }
    }
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

async function assertGone(pid: number): Promise<void> {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    try {
      process.kill(pid, 0)
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ESRCH') return
      throw error
    }
    await new Promise(resolve => setTimeout(resolve, 25))
  }
  throw new Error(`process ${String(pid)} is still alive`)
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

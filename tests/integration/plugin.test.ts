import { strict as assert } from 'node:assert'
import { spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readFile, readlink, readdir, rm, stat, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { z } from 'zod'
import { HealthResponseSchema, PingResponseSchema, type HealthResponse } from '../../src/contract.ts'
import { startDshWeb, type RunningDshWeb } from '../slice0/host.ts'

const projectRoot = fileURLToPath(new URL('../..', import.meta.url))
const pluginPath = join(projectRoot, 'src', 'plugin.ts')
const muxObserverFixture = fileURLToPath(new URL('../fixtures/mux-observer.ts', import.meta.url))
const cliPath = join(projectRoot, 'bin', 'dsh-liaison.ts')

const MuxEvidenceSchema = z.object({
  before: z.object({ status: z.number().int(), authorized: z.boolean() }),
  oldCookieStatus: z.number().int(),
  fresh: z.object({ status: z.number().int(), authorized: z.boolean() }),
  cookieRotated: z.boolean(),
  mux409Count: z.number().int().nonnegative(),
}).strict()

type MuxEvidence = z.infer<typeof MuxEvidenceSchema>

interface CliResult {
  readonly code: number | null
  readonly stdout: string
  readonly stderr: string
}

async function runCli(args: readonly string[], env: Readonly<Record<string, string>>): Promise<CliResult> {
  return await new Promise<CliResult>((resolve, reject) => {
    const child = spawn(process.execPath, [cliPath, ...args], {
      cwd: projectRoot,
      env: { ...process.env, NODE_NO_WARNINGS: '1', ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    const stdout: string[] = []
    const stderr: string[] = []
    child.stdout.setEncoding('utf8')
    child.stderr.setEncoding('utf8')
    child.stdout.on('data', (chunk: string) => { stdout.push(chunk) })
    child.stderr.on('data', (chunk: string) => { stderr.push(chunk) })
    child.once('error', reject)
    child.once('close', (code) => {
      resolve({ code, stdout: stdout.join(''), stderr: stderr.join('') })
    })
  })
}

async function waitForHealth(
  env: Readonly<Record<string, string>>,
  predicate: (health: HealthResponse) => boolean,
  timeoutMs = 60_000,
): Promise<HealthResponse> {
  const deadline = Date.now() + timeoutMs
  let last = ''
  while (Date.now() < deadline) {
    const result = await runCli(['status'], env)
    if (result.code === 0) {
      const health = HealthResponseSchema.parse(JSON.parse(result.stdout))
      if (predicate(health)) return health
      last = result.stdout
    } else {
      last = `${result.stdout}${result.stderr}`
    }
    await delay(100)
  }
  throw new Error(`timed out waiting for liaison health: ${last}`)
}

async function waitForEvidence(path: string, timeoutMs: number): Promise<MuxEvidence> {
  const deadline = Date.now() + timeoutMs
  let last = 'not written'
  while (Date.now() < deadline) {
    try {
      const raw: unknown = JSON.parse(await readFile(path, 'utf8'))
      return MuxEvidenceSchema.parse(raw)
    } catch (error) {
      last = error instanceof Error ? error.message : String(error)
      await delay(50)
    }
  }
  throw new Error(`timed out waiting for mux evidence at ${path}: ${last}`)
}

async function waitForProcessGone(pid: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      process.kill(pid, 0)
    } catch (error) {
      if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'ESRCH') return
      throw error
    }
    await delay(20)
  }
  throw new Error(`process ${String(pid)} did not exit within ${String(timeoutMs)}ms`)
}

async function tcpListeningPorts(pid: number): Promise<number[]> {
  const inodes = new Set<string>()
  for (const name of await readdir(`/proc/${pid}/fd`)) {
    try {
      const target = await readlink(`/proc/${pid}/fd/${name}`)
      const match = /^socket:\[(\d+)\]$/u.exec(target)
      if (match?.[1] !== undefined) inodes.add(match[1])
    } catch {
      // Descriptors can close while the listing is read.
    }
  }
  const ports: number[] = []
  for (const table of ['/proc/net/tcp', '/proc/net/tcp6']) {
    const text = await readFile(table, 'utf8').catch(() => '')
    for (const line of text.split('\n').slice(1)) {
      const columns = line.trim().split(/\s+/u)
      const local = columns[1]
      const state = columns[3]
      const inode = columns[9]
      if (local === undefined || state !== '0A' || inode === undefined || !inodes.has(inode)) continue
      const port = local.split(':')[1]
      if (port !== undefined) ports.push(Number.parseInt(port, 16))
    }
  }
  return ports.sort((a, b) => a - b)
}

interface HostFixture {
  readonly root: string
  readonly dshHome: string
  readonly socketPath: string
  readonly env: Record<string, string>
  readonly host: RunningDshWeb
}

async function startHost(options: {
  readonly label: string
  readonly dshHome?: string
  readonly explicitSocket?: string
  readonly xdgRuntime?: string
  readonly extraPlugins?: readonly {
    readonly name: string
    readonly config?: Record<string, unknown>
  }[]
  readonly timeoutMs?: number
}): Promise<HostFixture> {
  const root = await mkdtemp(join(tmpdir(), `liaison-${options.label}-`))
  const workspace = join(root, 'workspace')
  const dshHome = options.dshHome ?? join(root, 'dsh-home')
  const agentsHome = join(root, 'agents-home')
  const bundledSkillDir = join(root, 'bundled-skills')
  const xdgRuntime = options.xdgRuntime ?? join(root, 'runtime')
  const patchPath = join(root, 'liaison.patch.json')
  await mkdir(workspace, { recursive: true })
  await mkdir(agentsHome, { recursive: true })
  await mkdir(bundledSkillDir, { recursive: true })
  await mkdir(xdgRuntime, { recursive: true, mode: 0o700 })
  await writeFile(patchPath, JSON.stringify([
    { id: 'llm-deepseek', disabled: true },
    { id: 'llm-deepseek-account', disabled: true },
    { id: 'llm-pi-ai', disabled: true },
    { id: 'session-title-llm', disabled: true },
    {
      insert: [
        { id: 'dsh-liaison', name: pluginPath },
        ...(options.extraPlugins ?? []).map((plugin, index) => ({
          id: `fixture-${String(index)}`,
          name: plugin.name,
          ...(plugin.config === undefined ? {} : { config: plugin.config }),
        })),
      ],
    },
  ], null, 2))
  const env: Record<string, string> = { XDG_RUNTIME_DIR: xdgRuntime }
  if (options.explicitSocket !== undefined) env.DSH_LIAISON_SOCKET = options.explicitSocket
  const socketPath = options.explicitSocket ?? join(xdgRuntime, 'dsh-liaison', 'dsh-liaison.sock')
  const host = await startDshWeb({
    projectRoot,
    workspace,
    dshHome,
    agentsHome,
    bundledSkillDir,
    patchPath,
    env,
    timeoutMs: options.timeoutMs ?? 90_000,
  })
  return { root, dshHome, socketPath, env, host }
}

await test('a real dsh web Host owns a private UDS endpoint with authenticated RPC', { timeout: 180_000 }, async () => {
  let fixture: HostFixture | undefined
  try {
    fixture = await startHost({ label: 'host' })
    const health = await waitForHealth(fixture.env, item => item.origin !== null && item.loopback.connected)
    assert.equal(health.dshHome, fixture.dshHome)
    assert.equal(health.origin, new URL(fixture.host.launchUrl).origin)
    assert.equal(health.socket, fixture.socketPath)
    assert.equal(health.pid, fixture.host.pid)
    assert.equal(health.service, 'dsh-liaison')

    const directory = await stat(join(fixture.env.XDG_RUNTIME_DIR ?? '', 'dsh-liaison'))
    assert.equal(directory.mode & 0o777, 0o700)
    const socket = await stat(fixture.socketPath)
    assert.equal(socket.mode & 0o777, 0o600)

    const status = await runCli(['status'], fixture.env)
    assert.equal(status.code, 0, status.stderr)
    assert.equal(status.stderr, '')
    assert.doesNotMatch(status.stdout, /token|cookie/iu)
    assert.ok(!status.stdout.includes(new URL(fixture.host.launchUrl).searchParams.get('token') ?? '\0'))

    const ping = await runCli(['ping'], fixture.env)
    assert.equal(ping.code, 0, ping.stderr)
    assert.equal(ping.stderr, '')
    const pong = PingResponseSchema.parse(JSON.parse(ping.stdout))
    assert.equal(pong.pong, true)
    assert.ok(pong.generation > 0)
    assert.ok(pong.models >= 0)
    assert.doesNotMatch(ping.stdout, /token|cookie/iu)

    const listeners = await tcpListeningPorts(fixture.host.pid ?? 0)
    assert.deepEqual(listeners, [Number(new URL(fixture.host.launchUrl).port)])

    const lockPath = `${fixture.socketPath}.lock`
    const runningHost = fixture.host
    await runningHost.stop()
    assert.equal(existsSync(fixture.socketPath), false)
    assert.equal(existsSync(lockPath), true, 'the lifetime lock file must never be unlinked')
    const hostPid = runningHost.pid
    if (hostPid !== undefined) await waitForProcessGone(hostPid, 15_000)
    fixture = undefined
  } finally {
    await fixture?.host.stop().catch(() => undefined)
    if (fixture !== undefined) await rm(fixture.root, { recursive: true, force: true })
  }
})

await test('a concurrent plugin cannot interrupt the live endpoint or survive a stale socket', { timeout: 240_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'liaison-collision-'))
  const sharedSocketDir = join(root, 'shared-socket')
  const socketPath = join(sharedSocketDir, 'liaison.sock')
  const env = { DSH_LIAISON_SOCKET: socketPath }
  let first: HostFixture | undefined
  let second: HostFixture | undefined
  let replacement: HostFixture | undefined
  try {
    await mkdir(sharedSocketDir, { recursive: true, mode: 0o700 })
    first = await startHost({ label: 'first', explicitSocket: socketPath })
    await waitForHealth(env, item => item.loopback.connected)
    const originalInode = (await stat(socketPath)).ino

    const secondOutcome = await startHost({
      label: 'second',
      explicitSocket: socketPath,
      timeoutMs: 60_000,
    }).then(
      (fixture): HostFixture | Error => fixture,
      (error: unknown): HostFixture | Error => error instanceof Error ? error : new Error(String(error)),
    )
    if (secondOutcome instanceof Error) {
      assert.match(secondOutcome.message, /dsh-liaison|flock|lock/iu)
    } else {
      // `dsh web` may keep running after a best-effort plugin row fails; the
      // plugin itself must still refuse to replace the live socket.
      second = secondOutcome
      assert.match(second.host.stderr(), /dsh-liaison: another instance owns/iu)
    }

    const surviving = await waitForHealth(env, item => item.loopback.connected)
    assert.equal(surviving.pid, first.host.pid)
    assert.equal((await stat(socketPath)).ino, originalInode)
    const ping = await runCli(['ping'], env)
    assert.equal(ping.code, 0, ping.stderr)

    await second?.host.stop().catch(() => undefined)
    second = undefined

    const pid = first.host.pid
    assert.ok(pid !== undefined)
    process.kill(pid, 'SIGKILL')
    await first.host.stop()
    await waitForProcessGone(pid, 15_000)
    assert.equal(existsSync(socketPath), true, 'SIGKILL must leave the stale socket behind')
    assert.equal(existsSync(`${socketPath}.lock`), true)

    replacement = await startHost({ label: 'replacement', explicitSocket: socketPath })
    const recovered = await waitForHealth(env, item => item.loopback.connected)
    assert.equal(recovered.dshHome, replacement.dshHome)
    assert.notEqual(recovered.pid, pid)
    const recoveredPing = await runCli(['ping'], env)
    assert.equal(recoveredPing.code, 0, recoveredPing.stderr)
  } finally {
    await first?.host.stop().catch(() => undefined)
    await second?.host.stop().catch(() => undefined)
    await replacement?.host.stop().catch(() => undefined)
    if (first !== undefined) await rm(first.root, { recursive: true, force: true })
    if (second !== undefined) await rm(second.root, { recursive: true, force: true })
    if (replacement !== undefined) await rm(replacement.root, { recursive: true, force: true })
    await rm(root, { recursive: true, force: true })
  }
})

await test('force-closing the mux re-exchanges the credential and keeps ping working', { timeout: 180_000 }, async () => {
  let fixture: HostFixture | undefined
  let evidenceRoot: string | undefined
  try {
    evidenceRoot = await mkdtemp(join(tmpdir(), 'liaison-observe-'))
    const evidencePath = join(evidenceRoot, 'mux-evidence.json')
    fixture = await startHost({
      label: 'reconnect',
      extraPlugins: [{ name: muxObserverFixture, config: { evidencePath } }],
    })
    const online = await waitForHealth(fixture.env, item => item.loopback.connected)
    const beforePing = await runCli(['ping'], fixture.env)
    assert.equal(beforePing.code, 0, beforePing.stderr)

    const pid = fixture.host.pid
    assert.ok(pid !== undefined)
    process.kill(pid, 'SIGUSR2')

    const evidence = await waitForEvidence(evidencePath, 30_000)
    assert.deepEqual(evidence.before, { status: 101, authorized: true })
    assert.equal(evidence.oldCookieStatus, 401)
    assert.deepEqual(evidence.fresh, { status: 101, authorized: true })
    assert.equal(evidence.cookieRotated, true)
    assert.equal(evidence.mux409Count, 0)

    const reconnected = await waitForHealth(fixture.env, item =>
      item.loopback.connected && item.loopback.reauthentications >= 1, 30_000)
    assert.ok((reconnected.loopback.generation ?? 0) >= (online.loopback.generation ?? 0))
    assert.ok(reconnected.loopback.reauthentications >= 1)

    const afterPing = await runCli(['ping'], fixture.env)
    assert.equal(afterPing.code, 0, afterPing.stderr)
    const pong = PingResponseSchema.parse(JSON.parse(afterPing.stdout))
    assert.equal(pong.pong, true)
    assert.ok(pong.reauthentications >= 1)
    assert.doesNotMatch(afterPing.stdout, /token|cookie/iu)
  } finally {
    await fixture?.host.stop().catch(() => undefined)
    if (fixture !== undefined) await rm(fixture.root, { recursive: true, force: true })
    if (evidenceRoot !== undefined) await rm(evidenceRoot, { recursive: true, force: true })
  }
})

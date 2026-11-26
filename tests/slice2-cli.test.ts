import { strict as assert } from 'node:assert'
import { spawn, type ChildProcess } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { createConnection, type Socket } from 'node:net'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import { apply as gatewayApply, inject as gatewayInject, type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client'
import type {} from '@deepseek-ai/dsh-api-remotes/types'
import sessionRemote from '@deepseek-ai/dsh-api-session-controller/remote'
import type { SessionWireEvent } from '@deepseek-ai/dsh-api-session-controller/types'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import { installConnection, type ConnectionWebSocket } from '@deepseek-ai/dsh-client-connection/client'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import WebSocket from 'ws'
import { acquireLock } from '../src/flock.ts'
import { ledgerPaths, resolveLedgerPath, type DelegationRecord } from '../src/delegation-registry.ts'
import { statePaths } from '../src/paths.ts'
import {
  TWO_MODEL_A,
  TWO_MODEL_B,
  TWO_MODEL_BLOCKED_FILE,
  TWO_MODEL_PROVIDER,
  TWO_MODEL_WRITE_FILE,
} from './fixtures/two-model-provider.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))
const binPath = join(projectRoot, 'bin', 'dsh-liaison.ts')
const fixturePath = fileURLToPath(new URL('./fixtures/two-model-provider.ts', import.meta.url))
const TEST_PARENT = join(homedir(), '.cache', 'dsh-liaison-tests')
await mkdir(TEST_PARENT, { recursive: true, mode: 0o700 })

interface CliResult {
  readonly status: number | null
  readonly stdout: string
  readonly stderr: string
  readonly json: unknown
}

interface RawReply {
  readonly ok: boolean
  readonly result?: unknown
  readonly error?: { readonly code: string; readonly message: string }
}

async function runCli(env: NodeJS.ProcessEnv, cwd: string, args: readonly string[]): Promise<CliResult> {
  return await spawnCli(env, cwd, args)
}

async function spawnCli(env: NodeJS.ProcessEnv, cwd: string, args: readonly string[]): Promise<CliResult> {
  return await new Promise((resolve, reject) => {
    const child: ChildProcess = spawn(process.execPath, [binPath, ...args], {
      cwd,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let stdout = ''
    let stderr = ''
    const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error('CLI timed out')) }, 180_000)
    child.stdout?.setEncoding('utf8')
    child.stderr?.setEncoding('utf8')
    child.stdout?.on('data', (chunk: string) => { stdout += chunk })
    child.stderr?.on('data', (chunk: string) => { stderr += chunk })
    child.once('error', (error) => { clearTimeout(timer); reject(error) })
    child.once('close', (status) => {
      clearTimeout(timer)
      let json: unknown
      try { json = JSON.parse(stdout.trim()) } catch {
        try { json = JSON.parse(stderr.trim()) } catch { json = undefined }
      }
      resolve({ status, stdout, stderr, json })
    })
  })
}

function objectOf(value: unknown): Record<string, unknown> {
  assert.ok(typeof value === 'object' && value !== null && !Array.isArray(value), `expected object, got ${String(value)}`)
  return value as Record<string, unknown>
}

function numberField(record: Record<string, unknown>, field: string): number {
  const value = record[field]
  assert.ok(typeof value === 'number', `${field} must be a number`)
  return value
}

function stringField(record: Record<string, unknown>, field: string): string {
  const value = record[field]
  assert.ok(typeof value === 'string', `${field} must be a string`)
  return value
}

function okResult(run: CliResult): Record<string, unknown> {
  assert.equal(run.status, 0, run.stderr)
  const envelope = objectOf(run.json)
  assert.equal(envelope.ok, true)
  return envelope
}

function errorResult(run: CliResult): { readonly code: string; readonly message: string } {
  assert.equal(run.status, 1, run.stderr)
  const envelope = objectOf(run.json)
  assert.equal(envelope.ok, false)
  const error = objectOf(envelope.error)
  return {
    code: stringField(error, 'code'),
    message: stringField(error, 'message'),
  }
}

function assertNoSecrets(run: CliResult): void {
  assert.ok(!run.stdout.includes('launchToken'), 'CLI stdout must not expose launchToken')
  assert.ok(!run.stderr.includes('launchToken'), 'CLI stderr must not expose launchToken')
  assert.ok(!run.stdout.includes('dsh-auth-'), 'CLI stdout must not expose Host credentials')
  assert.ok(!run.stderr.includes('dsh-auth-'), 'CLI stderr must not expose Host credentials')
}

function cliEnvironment(root: string): NodeJS.ProcessEnv {
  const inherited = Object.fromEntries(Object.entries(process.env).filter(([name]) =>
    !name.startsWith('DSH_') && name !== 'XDG_STATE_HOME' && name !== 'XDG_RUNTIME_DIR'))
  return {
    ...inherited,
    HOME: root,
    DSH_HOME: join(root, 'dsh-home'),
    DSH_AGENTS_HOME: join(root, 'agents-home'),
    DSH_TELEMETRY_DISABLED: '1',
    DSH_LIAISON_STATE_DIR: join(root, 'state'),
    XDG_STATE_HOME: join(root, 'xdg-state'),
    XDG_RUNTIME_DIR: join(root, 'run'),
  }
}

async function setupRepository(root: string): Promise<{ readonly worktreeA: string; readonly worktreeB: string }> {
  const repository = join(root, 'repository')
  const worktreeA = join(root, 'worktree-a')
  const worktreeB = join(root, 'worktree-b')
  await mkdir(repository, { recursive: true })
  await git(repository, 'init', '--quiet')
  await git(repository, 'config', 'user.email', 'fixture@example.invalid')
  await git(repository, 'config', 'user.name', 'Fixture')
  await writeFile(join(repository, 'README.md'), 'fixture\n')
  await git(repository, 'add', 'README.md')
  await git(repository, 'commit', '--quiet', '-m', 'fixture')
  await git(repository, 'worktree', 'add', '--quiet', '-b', 'branch-a', worktreeA)
  await git(repository, 'worktree', 'add', '--quiet', '-b', 'branch-b', worktreeB)
  return { worktreeA, worktreeB }
}

async function git(cwd: string, ...args: readonly string[]): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn('git', args, { cwd, stdio: 'ignore' })
    child.once('error', reject)
    child.once('close', (code) => {
      if (code === 0) resolve()
      else reject(new Error(`git ${args.join(' ')} exited ${String(code)}`))
    })
  })
}

async function writePatch(root: string, worktreeA: string, worktreeB: string): Promise<string> {
  const patchPath = join(root, 'slice2.patch.json')
  await writeFile(patchPath, JSON.stringify([
    { id: 'llm-deepseek', disabled: true },
    { id: 'llm-deepseek-account', disabled: true },
    { id: 'llm-pi-ai', disabled: true },
    { id: 'session-title-llm', disabled: true },
    { id: 'agent-default-model', config: { provider: TWO_MODEL_PROVIDER, model: TWO_MODEL_A } },
    {
      insert: [{
        id: 'slice2-two-model-provider',
        name: fixturePath,
        config: { worktreeA, worktreeB },
      }],
    },
  ], null, 2))
  return patchPath
}

function rawCall(socketPath: string, method: string, params: Record<string, unknown>, deadline: number): Promise<RawReply> {
  return new Promise<RawReply>((resolve, reject) => {
    const id = randomUUID()
    const socket: Socket = createConnection(socketPath)
    let buffer = ''
    let settled = false
    const finish = (outcome: { readonly value: RawReply } | { readonly error: Error }): void => {
      if (settled) return
      settled = true
      socket.destroy()
      if ('value' in outcome) resolve(outcome.value)
      else reject(outcome.error)
    }
    socket.setEncoding('utf8')
    socket.once('connect', () => {
      socket.write(`${JSON.stringify({ id, method, params, deadline })}\n`)
    })
    socket.on('data', (chunk: string) => {
      buffer += chunk
      const newline = buffer.indexOf('\n')
      if (newline === -1) return
      try {
        const parsed = objectOf(JSON.parse(buffer.slice(0, newline)) as unknown)
        if (parsed.id !== id) return
        if (parsed.ok === true) {
          finish({ value: { ok: true, result: parsed.result } })
          return
        }
        const error = objectOf(parsed.error)
        finish({
          value: {
            ok: false,
            error: {
              code: stringField(error, 'code'),
              message: stringField(error, 'message'),
            },
          },
        })
      } catch (error) {
        finish({ error: error instanceof Error ? error : new Error(String(error)) })
      }
    })
    socket.once('error', (error) => {
      finish({ error: new Error(`raw IPC ${method} failed: ${error.message}`) })
    })
    socket.once('close', () => {
      if (!settled) finish({ error: new Error(`raw IPC ${method} closed without a reply`) })
    })
    const timer = setTimeout(() => {
      finish({ error: new Error(`raw IPC ${method} timed out`) })
    }, 10_000)
    timer.unref()
  })
}


function sendRaw(
  socketPath: string,
  method: string,
  params: Record<string, unknown>,
  deadline: number,
): { close(): void } {
  const socket: Socket = createConnection(socketPath)
  socket.on('error', () => undefined)
  socket.once('connect', () => {
    socket.write(`${JSON.stringify({ id: randomUUID(), method, params, deadline })}\n`)
  })
  return {
    close() {
      socket.destroy()
    },
  }
}

async function waitGone(pid: number): Promise<void> {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    try {
      await readFile(`/proc/${String(pid)}/stat`, 'utf8')
    } catch {
      return
    }
    await delay(25)
  }
  throw new Error(`process ${String(pid)} did not exit`)
}

async function killDaemon(pid: number): Promise<void> {
  process.kill(pid, 'SIGKILL')
  await waitGone(pid)
}

async function readLedger(env: NodeJS.ProcessEnv): Promise<Record<string, DelegationRecord> | undefined> {
  const path = resolveLedgerPath(env)
  try {
    const parsed = objectOf(JSON.parse(await readFile(path, 'utf8')) as unknown)
    const delegations = objectOf(parsed.delegations)
    return delegations as Record<string, DelegationRecord>
  } catch {
    return undefined
  }
}

async function waitForLedger(
  env: NodeJS.ProcessEnv,
  predicate: (delegations: Record<string, DelegationRecord>) => boolean,
  timeoutMs = 30_000,
): Promise<Record<string, DelegationRecord>> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const delegations = await readLedger(env)
    if (delegations !== undefined && predicate(delegations)) return delegations
    await delay(50)
  }
  throw new Error('delegation ledger did not reach the expected state')
}

interface PassiveObserver {
  readonly remote: ClientRemote
  close(): Promise<void>
}

async function passiveObserverFor(env: NodeJS.ProcessEnv): Promise<PassiveObserver> {
  const record = objectOf(JSON.parse(await readFile(join(String(env.DSH_LIAISON_STATE_DIR), 'host.json'), 'utf8')) as unknown)
  const origin = stringField(record, 'origin')
  const token = stringField(record, 'launchToken')
  const connect = new URL('/connect', origin)
  connect.searchParams.set('token', token)
  connect.searchParams.set('interactions', 'none')
  const response = await fetch(connect, { redirect: 'manual' })
  assert.equal(response.status, 302)
  const cookie = response.headers.get('set-cookie')?.split(';', 1)[0]
  assert.ok(cookie !== undefined && cookie !== '')
  const baseUrl = new URL('/', origin).href
  const ctx = new Context()
  try {
    installConnection(ctx, {
      interactions: 'none',
      transport: {
        streamBaseUrl: baseUrl,
        fetch: async (input, init) => {
          const headers = new Headers(init.headers)
          headers.set('cookie', cookie)
          return await fetch(new URL(input, baseUrl), { ...init, headers })
        },
        createWebSocket: (url) => {
          const socket: ConnectionWebSocket = new WebSocket(url, { headers: { cookie } })
          return socket
        },
      },
    })
    await ctx.plugin(TypertRegistry)
    await ctx.plugin({ apply: gatewayApply, inject: gatewayInject })
    await ctx.remote.$mount(sessionRemote)
    return {
      remote: ctx.remote,
      async close(): Promise<void> {
        await ctx.fiber.dispose().catch(() => undefined)
      },
    }
  } catch (error) {
    await ctx.fiber.dispose().catch(() => undefined)
    throw error
  }
}

function objectValue(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

function requestIdOf(event: SessionWireEvent): string | undefined {
  if (event.type !== 'user/message') return undefined
  const source = objectValue(objectValue(event.data)?.['source'])
  const rpcId = source?.['rpcId']
  return typeof rpcId === 'string' ? rpcId : undefined
}

function toolResultErrorCode(event: SessionWireEvent): string | undefined {
  if (event.type !== 'tool/result') return undefined
  const error = objectValue(objectValue(event.data)?.['error'])
  const code = error?.['code']
  return typeof code === 'string' ? code : undefined
}

function toolCallArguments(event: SessionWireEvent): string | undefined {
  if (event.type !== 'tool/call') return undefined
  const args = objectValue(event.data)?.['arguments']
  return typeof args === 'string' ? args : undefined
}

function headerConfigOf(event: SessionWireEvent): Record<string, unknown> | undefined {
  if (event.type !== 'request/header') return undefined
  const header = objectValue(objectValue(event.data)?.['header'])
  return objectValue(header?.['config'])
}

async function completedEvents(
  observer: ClientRemote,
  sessionId: string,
  requestId: string,
  timeoutMs = 90_000,
): Promise<readonly SessionWireEvent[]> {
  const deadline = Date.now() + timeoutMs
  const brandedSessionId = brandString<SessionId>(sessionId)
  while (true) {
    const state = observer.session.state({ sessionId: brandedSessionId })
    const stateValue = await state
    if (!stateValue.ok) throw new Error(`${stateValue.error.code}: ${stateValue.error.message}`)
    const page = await observer.session.events({
      address: { kind: 'session', sessionId: brandedSessionId },
      throughSeq: stateValue.value.cursor,
      maxMessages: 1_000,
    })
    if (!page.ok) throw new Error(`${page.error.code}: ${page.error.message}`)
    const events = page.value.records.map(entry => entry.event)
    const target = events.find(event => requestIdOf(event) === requestId)
    const ended = target !== undefined && events.some(event => event.type === 'turn/end' && event.seq > target.seq)
    if (target !== undefined && ended && !stateValue.value.running) return events
    if (Date.now() >= deadline) throw new Error(`timed out waiting for request ${requestId}`)
    await delay(100)
  }
}

function derivedRequestId(ipcRequestId: string): string {
  return `request-${createHash('sha256').update(ipcRequestId).digest('hex').slice(0, 32)}`
}

async function startHost(env: NodeJS.ProcessEnv, cwd: string, patchPath: string): Promise<{ readonly daemonPid: number; readonly hostPid: number }> {
  const run = await runCli(env, cwd, ['host', 'start', '--patch', patchPath])
  const result = okResult(run)
  assertNoSecrets(run)
  assert.equal(result.state, 'ready')
  return { daemonPid: numberField(result, 'daemonPid'), hostPid: numberField(result, 'hostPid') }
}

async function delegationCreate(
  env: NodeJS.ProcessEnv,
  cwd: string,
  args: readonly string[],
): Promise<{ readonly run: CliResult; readonly result: Record<string, unknown> }> {
  const run = await runCli(env, cwd, ['delegation', 'create', ...args])
  const result = okResult(run)
  assertNoSecrets(run)
  return { run, result }
}

async function delegationPrompt(
  env: NodeJS.ProcessEnv,
  cwd: string,
  args: readonly string[],
): Promise<{ readonly run: CliResult; readonly result: Record<string, unknown> }> {
  const run = await runCli(env, cwd, ['delegation', 'prompt', ...args])
  const result = okResult(run)
  assertNoSecrets(run)
  return { run, result }
}

await test('normal delegation CLI: two models, worktree isolation, replay, verify, read-only fallback, and drain', { timeout: 300_000 }, async () => {
  const root = await mkdtemp(join(TEST_PARENT, 'slice2-cli-'))
  const env = cliEnvironment(root)
  const { worktreeA, worktreeB } = await setupRepository(root)
  const patchPath = await writePatch(root, worktreeA, worktreeB)
  const nonce = randomUUID().slice(0, 8)
  let daemonPid: number | undefined
  let observer: PassiveObserver | undefined
  try {
    const started = await startHost(env, worktreeA, patchPath)
    daemonPid = started.daemonPid

    const delegationA = `delegation-a-${nonce}`
    const delegationB = `delegation-b-${nonce}`
    const createdA = await delegationCreate(env, worktreeA, [
      '--title', `Delegation Alpha ${nonce}`,
      '--provider', TWO_MODEL_PROVIDER,
      '--model', TWO_MODEL_A,
      '--cwd', worktreeA,
      '--delegation-id', delegationA,
    ])
    const createdB = await delegationCreate(env, worktreeA, [
      '--title', `Delegation Beta ${nonce}`,
      '--provider', TWO_MODEL_PROVIDER,
      '--model', TWO_MODEL_B,
      '--cwd', worktreeB,
      '--delegation-id', delegationB,
    ])
    assert.equal(createdA.result.replayed, false)
    assert.equal(createdB.result.replayed, false)
    const recordA = objectOf(createdA.result.delegation)
    const recordB = objectOf(createdB.result.delegation)
    const sessionA = stringField(recordA, 'sessionId')
    const sessionB = stringField(recordB, 'sessionId')
    assert.notEqual(sessionA, sessionB)

    const getById = await runCli(env, worktreeA, ['delegation', 'get', '--delegation-id', delegationA])
    assert.equal(stringField(objectOf(okResult(getById).delegation), 'sessionId'), sessionA)
    assertNoSecrets(getById)
    const getBySession = await runCli(env, worktreeA, ['delegation', 'get', '--session-id', sessionB])
    assert.equal(stringField(objectOf(okResult(getBySession).delegation), 'delegationId'), delegationB)
    assertNoSecrets(getBySession)
    const listed = await runCli(env, worktreeA, ['delegation', 'list'])
    const listResult = okResult(listed)
    assertNoSecrets(listed)
    assert.equal((listResult.delegations as readonly unknown[]).length, 2)

    const requestA = `request-a-${nonce}`
    const requestB = `request-b-${nonce}`
    const promptA = await delegationPrompt(env, worktreeA, [
      '--delegation-id', delegationA,
      '--content', 'Write the isolation marker with the ordinary write tool.',
      '--request-id', requestA,
    ])
    const promptB = await delegationPrompt(env, worktreeA, [
      '--delegation-id', delegationB,
      '--content', 'Write the isolation marker with the ordinary write tool.',
      '--request-id', requestB,
    ])
    assert.equal(promptA.result.accepted, true)
    assert.equal(promptA.result.completed, false)
    assert.equal(typeof promptA.result.beforeCursor, 'number')
    assert.equal(promptB.result.accepted, true)

    observer = await passiveObserverFor(env)
    const eventsA = await completedEvents(observer.remote, sessionA, requestA)
    const eventsB = await completedEvents(observer.remote, sessionB, requestB)

    const replayA = await delegationPrompt(env, worktreeA, [
      '--delegation-id', delegationA,
      '--content', 'Write the isolation marker with the ordinary write tool.',
      '--request-id', requestA,
    ])
    assert.equal(replayA.result.replayed, true)
    assert.equal(replayA.result.beforeCursor, promptA.result.beforeCursor)
    const replayEvents = await completedEvents(observer.remote, sessionA, requestA)
    assert.equal(replayEvents.filter(event => requestIdOf(event) === requestA).length, 1)

    const verifyA = await runCli(env, worktreeA, ['delegation', 'verify-model', '--delegation-id', delegationA, '--request-id', requestA])
    const verifyB = await runCli(env, worktreeA, ['delegation', 'verify-model', '--delegation-id', delegationB, '--request-id', requestB])
    const verifiedA = okResult(verifyA)
    const verifiedB = okResult(verifyB)
    assertNoSecrets(verifyA)
    assertNoSecrets(verifyB)
    assert.deepEqual(verifiedA.observedModel, { provider: TWO_MODEL_PROVIDER, model: TWO_MODEL_A })
    assert.deepEqual(verifiedB.observedModel, { provider: TWO_MODEL_PROVIDER, model: TWO_MODEL_B })
    for (const event of eventsA.filter(item => item.type === 'request/header')) {
      const config = headerConfigOf(event)
      assert.equal(config?.['provider'], TWO_MODEL_PROVIDER)
      assert.equal(config?.['model'], TWO_MODEL_A)
    }
    for (const event of eventsB.filter(item => item.type === 'request/header')) {
      const config = headerConfigOf(event)
      assert.equal(config?.['provider'], TWO_MODEL_PROVIDER)
      assert.equal(config?.['model'], TWO_MODEL_B)
    }

    assert.equal(await readFile(join(worktreeA, TWO_MODEL_WRITE_FILE), 'utf8'), `${TWO_MODEL_A}:${sessionA}`)
    assert.equal(await readFile(join(worktreeB, TWO_MODEL_WRITE_FILE), 'utf8'), `${TWO_MODEL_B}:${sessionB}`)
    assert.equal(existsSync(join(worktreeA, TWO_MODEL_BLOCKED_FILE)), false)
    assert.equal(existsSync(join(worktreeB, TWO_MODEL_BLOCKED_FILE)), false)
    assert.equal(eventsA.filter(event => toolResultErrorCode(event) === 'FS_SANDBOX_DENIED').length, 1)
    assert.equal(eventsB.filter(event => toolResultErrorCode(event) === 'FS_SANDBOX_DENIED').length, 1)
    assert.ok(eventsA.some(event => toolCallArguments(event)?.includes(join(worktreeB, TWO_MODEL_BLOCKED_FILE)) === true))
    assert.ok(eventsB.some(event => toolCallArguments(event)?.includes(join(worktreeA, TWO_MODEL_BLOCKED_FILE)) === true))

    const expiredStop = await rawCall(
      statePaths(String(env.DSH_LIAISON_STATE_DIR)).socket,
      'host.stop',
      { keepHost: true },
      Date.now() - 1,
    )
    assert.equal(expiredStop.ok, false)
    assert.equal(expiredStop.error?.code, 'IPC_TIMEOUT')
    const afterExpiredStop = await runCli(env, worktreeA, ['delegation', 'get', '--delegation-id', delegationA])
    assert.equal(okResult(afterExpiredStop).ok, true)
    assertNoSecrets(afterExpiredStop)

    const keep = await runCli(env, worktreeA, ['host', 'stop', '--keep-host'])
    assert.equal(okResult(keep).stopReason, 'kept')
    assertNoSecrets(keep)
    await waitGone(started.daemonPid)
    daemonPid = undefined

    const offlineById = await runCli(env, worktreeA, ['delegation', 'get', '--delegation-id', delegationA])
    const offlineByIdResult = okResult(offlineById)
    assert.equal(offlineByIdResult.daemonPid, null)
    assert.equal(stringField(objectOf(offlineByIdResult.delegation), 'sessionId'), sessionA)
    assertNoSecrets(offlineById)
    const offlineBySession = await runCli(env, worktreeA, ['delegation', 'get', '--session-id', sessionA])
    assert.equal(okResult(offlineBySession).daemonPid, null)
    assertNoSecrets(offlineBySession)
    const offlineList = await runCli(env, worktreeA, ['delegation', 'list'])
    const offlineListResult = okResult(offlineList)
    assert.equal(offlineListResult.daemonPid, null)
    assert.equal((offlineListResult.delegations as readonly unknown[]).length, 2)
    assertNoSecrets(offlineList)

    const restarted = await startHost(env, worktreeA, patchPath)
    daemonPid = restarted.daemonPid
    assert.equal(restarted.hostPid, started.hostPid, 'the kept Host must be reattached')
    const delegationC = `delegation-c-${nonce}`
    await delegationCreate(env, worktreeA, [
      '--title', `Delegation Gamma ${nonce}`,
      '--provider', TWO_MODEL_PROVIDER,
      '--model', TWO_MODEL_A,
      '--cwd', worktreeA,
      '--delegation-id', delegationC,
    ])

    const stopped = await runCli(env, worktreeA, ['host', 'stop'])
    assert.equal(okResult(stopped).stoppedHost, true)
    assertNoSecrets(stopped)
    await waitGone(restarted.daemonPid)
    daemonPid = undefined
    await waitGone(restarted.hostPid)

    const noHost = await runCli(env, worktreeA, [
      'delegation', 'create',
      '--title', `No Host ${nonce}`,
      '--provider', TWO_MODEL_PROVIDER,
      '--model', TWO_MODEL_A,
      '--cwd', worktreeA,
    ])
    assert.equal(errorResult(noHost).code, 'HOST_NOT_RUNNING')
    assertNoSecrets(noHost)
    await runCli(env, worktreeA, ['host', 'stop'])

    const ledgerLock = await acquireLock(ledgerPaths(resolveLedgerPath(env)).ledgerLock, { waitMs: 0 })
    assert.notEqual(ledgerLock, undefined, 'a committed stop must release delegations.lock')
    await ledgerLock?.release()
    const retained = await readLedger(env)
    assert.ok(retained?.[delegationA] !== undefined)
  } finally {
    if (daemonPid !== undefined) {
      try { process.kill(daemonPid, 'SIGKILL') } catch { /* already gone */ }
    }
    await observer?.close().catch(() => undefined)
    await rm(root, { recursive: true, force: true })
  }
})

await test('dropped delegation responses replay across daemon restart with the original session and cursor', { timeout: 300_000 }, async () => {
  const root = await mkdtemp(join(TEST_PARENT, 'slice2-cli-lost-'))
  const env = cliEnvironment(root)
  const { worktreeA } = await setupRepository(root)
  const patchPath = await writePatch(root, worktreeA, worktreeA)
  const nonce = randomUUID().slice(0, 8)
  const delegationId = `delegation-lost-${nonce}`
  const createKey = `create-key-${nonce}`
  const promptKey = `prompt-key-${nonce}`
  const content = 'Persist exactly one replay-safe prompt.'
  let daemonPid: number | undefined
  let observer: PassiveObserver | undefined
  try {
    const started = await startHost(env, worktreeA, patchPath)
    daemonPid = started.daemonPid

    const createParams = {
      delegationId,
      title: `Delegation Lost ${nonce}`,
      provider: TWO_MODEL_PROVIDER,
      model: TWO_MODEL_A,
      cwd: worktreeA,
      ipcRequestId: createKey,
    }
    const lostCreate = sendRaw(
      statePaths(String(env.DSH_LIAISON_STATE_DIR)).socket,
      'delegation.create',
      createParams,
      Date.now() + 30_000,
    )
    const ready = await waitForLedger(env, delegations => delegations[delegationId]?.state === 'ready')
    const sessionId = ready[delegationId]?.sessionId
    assert.ok(sessionId)
    lostCreate.close()
    await killDaemon(started.daemonPid)
    daemonPid = undefined

    const replayCreate = await delegationCreate(env, worktreeA, [
      '--title', createParams.title,
      '--provider', createParams.provider,
      '--model', createParams.model,
      '--cwd', createParams.cwd,
      '--delegation-id', delegationId,
      '--id', createKey,
    ])
    assert.equal(replayCreate.result.replayed, true)
    assert.equal(stringField(objectOf(replayCreate.result.delegation), 'sessionId'), sessionId)
    const status = await runCli(env, worktreeA, ['host', 'status'])
    const statusResult = okResult(status)
    daemonPid = numberField(statusResult, 'daemonPid')

    const expiredCreate = await rawCall(
      statePaths(String(env.DSH_LIAISON_STATE_DIR)).socket,
      'delegation.create',
      {
        delegationId: `delegation-expired-${nonce}`,
        title: `Expired ${nonce}`,
        provider: TWO_MODEL_PROVIDER,
        model: TWO_MODEL_A,
        cwd: worktreeA,
        ipcRequestId: `expired-create-${nonce}`,
      },
      Date.now() - 1,
    )
    assert.equal(expiredCreate.ok, false)
    assert.equal(expiredCreate.error?.code, 'IPC_TIMEOUT')
    assert.equal((await readLedger(env))?.[`delegation-expired-${nonce}`], undefined)

    const lostPrompt = sendRaw(
      statePaths(String(env.DSH_LIAISON_STATE_DIR)).socket,
      'delegation.prompt',
      { delegationId, content, ipcRequestId: promptKey },
      Date.now() + 30_000,
    )
    const requestId = derivedRequestId(promptKey)
    await waitForLedger(env, delegations => {
      const receipt = delegations[delegationId]?.prompts?.[requestId]
      return receipt?.accepted === true
    })
    const beforeCursor = (await readLedger(env))?.[delegationId]?.prompts?.[requestId]?.beforeCursor
    assert.equal(typeof beforeCursor, 'number')
    lostPrompt.close()
    await killDaemon(daemonPid)
    daemonPid = undefined

    const replayPrompt = await delegationPrompt(env, worktreeA, [
      '--delegation-id', delegationId,
      '--content', content,
      '--id', promptKey,
    ])
    assert.equal(replayPrompt.result.replayed, true)
    assert.equal(replayPrompt.result.beforeCursor, beforeCursor)
    assert.equal(replayPrompt.result.requestId, requestId)

    const conflict = await runCli(env, worktreeA, [
      'delegation', 'prompt',
      '--delegation-id', delegationId,
      '--content', `${content} changed`,
      '--id', promptKey,
    ])
    assert.equal(errorResult(conflict).code, 'DELEGATION_PROMPT_CONFLICT')
    assertNoSecrets(conflict)

    const expiredPrompt = await rawCall(
      statePaths(String(env.DSH_LIAISON_STATE_DIR)).socket,
      'delegation.prompt',
      { delegationId, content: 'must not dispatch', ipcRequestId: `expired-prompt-${nonce}` },
      Date.now() - 1,
    )
    assert.equal(expiredPrompt.ok, false)
    assert.equal(expiredPrompt.error?.code, 'IPC_TIMEOUT')
    const expiredRequest = derivedRequestId(`expired-prompt-${nonce}`)
    assert.equal((await readLedger(env))?.[delegationId]?.prompts?.[expiredRequest], undefined)

    observer = await passiveObserverFor(env)
    const events = await completedEvents(observer.remote, sessionId, requestId)
    await observer.close()
    observer = undefined
    assert.equal(events.filter(event => requestIdOf(event) === requestId).length, 1)
    assert.equal(events.some(event => requestIdOf(event) === expiredRequest), false)
    assert.equal(await readFile(join(worktreeA, TWO_MODEL_WRITE_FILE), 'utf8'), `${TWO_MODEL_A}:${sessionId}`)
  } finally {
    if (daemonPid !== undefined) {
      try { process.kill(daemonPid, 'SIGKILL') } catch { /* already gone */ }
    }
    const stateDir = String(env.DSH_LIAISON_STATE_DIR)
    if (existsSync(stateDir)) {
      await runCli(env, worktreeA, ['host', 'stop']).catch(() => undefined)
    }
    await observer?.close().catch(() => undefined)
    await rm(root, { recursive: true, force: true })
  }
})

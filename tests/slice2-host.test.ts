import { strict as assert } from 'node:assert'
import { execFile } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { promisify } from 'node:util'
import { fileURLToPath } from 'node:url'
import type { SessionWireEvent } from '@deepseek-ai/dsh-api-session-controller/types'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import type { RemoteResult } from '@deepseek-ai/dsh-typert-protocol'
import { DelegationRegistry } from '../src/delegation-registry.ts'
import { startDshWeb, type RunningDshWeb } from '../src/host.ts'
import { connectOperator, type Operator } from '../src/operator.ts'
import {
  SessionCommands,
  type HostOperatorAccess,
  type SessionCommandsRemote,
  type VerifiedHostContext,
} from '../src/session-commands.ts'
import {
  TWO_MODEL_A,
  TWO_MODEL_B,
  TWO_MODEL_BLOCKED_FILE,
  TWO_MODEL_DONE,
  TWO_MODEL_PROVIDER,
  TWO_MODEL_WRITE_FILE,
} from './fixtures/two-model-provider.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))
const fixturePath = fileURLToPath(new URL('./fixtures/two-model-provider.ts', import.meta.url))
const exec = promisify(execFile)
const TEST_PARENT = join(homedir(), '.cache', 'dsh-liaison-tests')
await mkdir(TEST_PARENT, { recursive: true, mode: 0o700 })

function unwrap<T>(result: RemoteResult<T>): T {
  if (result.ok) return result.value
  throw new Error(`${result.error.code}: ${result.error.message}`)
}

function objectOf(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

function requestIdOf(event: SessionWireEvent): string | undefined {
  if (event.type !== 'user/message') return undefined
  const source = objectOf(objectOf(event.data)?.['source'])
  const rpcId = source?.['rpcId']
  return typeof rpcId === 'string' ? rpcId : undefined
}

function headerConfigOf(event: SessionWireEvent): Record<string, unknown> | undefined {
  if (event.type !== 'request/header') return undefined
  const header = objectOf(objectOf(event.data)?.['header'])
  return objectOf(header?.['config'])
}

function toolResultErrorCode(event: SessionWireEvent): string | undefined {
  if (event.type !== 'tool/result') return undefined
  const error = objectOf(objectOf(event.data)?.['error'])
  const code = error?.['code']
  return typeof code === 'string' ? code : undefined
}

function toolCallArguments(event: SessionWireEvent): string | undefined {
  if (event.type !== 'tool/call') return undefined
  const args = objectOf(event.data)?.['arguments']
  return typeof args === 'string' ? args : undefined
}

async function completedEvents(
  operator: Operator,
  sessionId: string,
  requestId: string,
  timeoutMs = 90_000,
): Promise<readonly SessionWireEvent[]> {
  const brandedSessionId = brandString<SessionId>(sessionId)
  const deadline = Date.now() + timeoutMs
  while (true) {
    const state = unwrap(await operator.remote.session.state({ sessionId: brandedSessionId }))
    const page = unwrap(await operator.remote.session.events({
      address: { kind: 'session', sessionId: brandedSessionId },
      throughSeq: state.cursor,
      maxMessages: 1_000,
    }))
    const events = page.records.map(record => record.event)
    const target = events.find(event => requestIdOf(event) === requestId)
    const ended = target !== undefined && events.some(event => event.type === 'turn/end' && event.seq > target.seq)
    if (target !== undefined && ended && !state.running) return events
    if (Date.now() >= deadline) throw new Error(`timed out waiting for request ${requestId} to complete`)
    await delay(100)
  }
}

async function setupRepository(root: string): Promise<{ readonly worktreeA: string; readonly worktreeB: string }> {
  const repository = join(root, 'repository')
  const worktreeA = join(root, 'worktree-a')
  const worktreeB = join(root, 'worktree-b')
  await mkdir(repository, { recursive: true })
  await exec('git', ['init', '--quiet'], { cwd: repository })
  await exec('git', ['config', 'user.email', 'fixture@example.invalid'], { cwd: repository })
  await exec('git', ['config', 'user.name', 'Fixture'], { cwd: repository })
  await writeFile(join(repository, 'README.md'), 'fixture\n')
  await exec('git', ['add', 'README.md'], { cwd: repository })
  await exec('git', ['commit', '--quiet', '-m', 'fixture'], { cwd: repository })
  await exec('git', ['worktree', 'add', '--quiet', '-b', 'branch-a', worktreeA], { cwd: repository })
  await exec('git', ['worktree', 'add', '--quiet', '-b', 'branch-b', worktreeB], { cwd: repository })
  return { worktreeA, worktreeB }
}

await test('real keyless Host: two models, stable prompt receipts, model proof, and worktree write isolation', { timeout: 300_000 }, async () => {
  const root = await mkdtemp(join(TEST_PARENT, 'slice2-host-'))
  const { worktreeA, worktreeB } = await setupRepository(root)
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const ledgerDir = join(root, 'ledger')
  const patchPath = join(root, 'slice2.patch.json')
  const nonce = `${Date.now()}-${String(process.pid)}`
  let host: RunningDshWeb | undefined
  let operator: Operator | undefined
  let registry: DelegationRegistry | undefined

  try {
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

    host = await startDshWeb({
      projectRoot,
      workspace: worktreeA,
      dshHome,
      agentsHome: join(root, 'agents-home'),
      bundledSkillDir: join(root, 'bundled-skills'),
      patchPath,
    })
    const activeOperator = await connectOperator(host.launchUrl)
    operator = activeOperator

    const origin = new URL(host.launchUrl)
    const context: VerifiedHostContext = {
      stateDir,
      dshHome,
      cwd: worktreeA,
      hostPid: host.pid ?? 0,
      origin: origin.origin,
      port: origin.port === '' ? 0 : Number(origin.port),
    }
    const access: HostOperatorAccess = {
      async hostContext() {
        return context
      },
      async withOperator<T>(
        operation: (remote: SessionCommandsRemote, signal: AbortSignal) => Promise<RemoteResult<T>>,
        timeoutMs = 15_000,
      ): Promise<RemoteResult<T>> {
        const controller = new AbortController()
        const timer = setTimeout(() => { controller.abort() }, timeoutMs)
        try {
          return await operation(activeOperator.remote, controller.signal)
        } finally {
          clearTimeout(timer)
        }
      },
    }

    registry = new DelegationRegistry({ ledgerFile: join(ledgerDir, 'delegations.json'), lockWaitMs: 1_000 })
    const commands = new SessionCommands({ registry, access, ledgerDir })

    const delegationA = await commands.create({
      delegationId: `delegation-a-${nonce}`,
      title: `Delegation Alpha ${nonce}`,
      provider: TWO_MODEL_PROVIDER,
      model: TWO_MODEL_A,
      cwd: worktreeA,
    })
    const delegationB = await commands.create({
      delegationId: `delegation-b-${nonce}`,
      title: `Delegation Beta ${nonce}`,
      provider: TWO_MODEL_PROVIDER,
      model: TWO_MODEL_B,
      cwd: worktreeB,
    })
    assert.equal(delegationA.replayed, false)
    assert.equal(delegationB.replayed, false)
    assert.notEqual(delegationA.delegation.sessionId, delegationB.delegation.sessionId)

    const requestA = `request-a-${nonce}`
    const requestB = `request-b-${nonce}`
    const promptA = await commands.prompt({
      delegationId: delegationA.delegation.delegationId,
      requestId: requestA,
      content: 'Create the isolation marker with the write tool.',
    })
    const promptB = await commands.prompt({
      delegationId: delegationB.delegation.delegationId,
      requestId: requestB,
      content: 'Create the isolation marker with the write tool.',
    })
    assert.equal(promptA.replayed, false)
    assert.equal(promptB.replayed, false)

    const eventsA = await completedEvents(activeOperator, delegationA.delegation.sessionId, requestA)
    const eventsB = await completedEvents(activeOperator, delegationB.delegation.sessionId, requestB)

    const replayA = await commands.prompt({
      delegationId: delegationA.delegation.delegationId,
      requestId: requestA,
      content: 'Create the isolation marker with the write tool.',
    })
    assert.equal(replayA.replayed, true)
    assert.equal(replayA.beforeCursor, promptA.beforeCursor)
    const eventsAfterReplay = await completedEvents(activeOperator, delegationA.delegation.sessionId, requestA)
    assert.equal(
      eventsAfterReplay.filter(event => requestIdOf(event) === requestA).length,
      1,
      'a replayed prompt must persist exactly one user message',
    )

    const verifiedA = await commands.verifyModel({
      delegationId: delegationA.delegation.delegationId,
      requestId: requestA,
    })
    const verifiedB = await commands.verifyModel({
      delegationId: delegationB.delegation.delegationId,
      requestId: requestB,
    })
    assert.deepEqual(verifiedA.expectedModel, { provider: TWO_MODEL_PROVIDER, model: TWO_MODEL_A })
    assert.deepEqual(verifiedA.observedModel, { provider: TWO_MODEL_PROVIDER, model: TWO_MODEL_A })
    assert.deepEqual(verifiedB.expectedModel, { provider: TWO_MODEL_PROVIDER, model: TWO_MODEL_B })
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

    const markerA = await readFile(join(worktreeA, TWO_MODEL_WRITE_FILE), 'utf8')
    const markerB = await readFile(join(worktreeB, TWO_MODEL_WRITE_FILE), 'utf8')
    assert.equal(markerA, `${TWO_MODEL_A}:${delegationA.delegation.sessionId}`)
    assert.equal(markerB, `${TWO_MODEL_B}:${delegationB.delegation.sessionId}`)
    assert.notEqual(markerA, markerB)
    assert.match(JSON.stringify(eventsA), new RegExp(TWO_MODEL_DONE, 'u'))
    assert.match(JSON.stringify(eventsB), new RegExp(TWO_MODEL_DONE, 'u'))

    const crossCallA = eventsA.find(event =>
      toolCallArguments(event)?.includes(join(worktreeB, TWO_MODEL_BLOCKED_FILE)) === true)
    const crossCallB = eventsB.find(event =>
      toolCallArguments(event)?.includes(join(worktreeA, TWO_MODEL_BLOCKED_FILE)) === true)
    assert.ok(crossCallA, 'delegation A must ask the ordinary write tool to target B')
    assert.ok(crossCallB, 'delegation B must ask the ordinary write tool to target A')

    const deniedA = eventsA.filter(event => toolResultErrorCode(event) === 'FS_SANDBOX_DENIED')
    const deniedB = eventsB.filter(event => toolResultErrorCode(event) === 'FS_SANDBOX_DENIED')
    assert.equal(deniedA.length, 1, 'A sibling write must be denied by the real fs sandbox')
    assert.equal(deniedB.length, 1, 'B sibling write must be denied by the real fs sandbox')
    assert.match(JSON.stringify(deniedA[0]?.data), /\[sandbox: file access denied under workspace-write mode\]/u)
    assert.match(JSON.stringify(deniedB[0]?.data), /\[sandbox: file access denied under workspace-write mode\]/u)
    assert.equal(existsSync(join(worktreeA, TWO_MODEL_BLOCKED_FILE)), false)
    assert.equal(existsSync(join(worktreeB, TWO_MODEL_BLOCKED_FILE)), false)
    assert.equal(await readFile(join(worktreeA, 'README.md'), 'utf8'), 'fixture\n')
    assert.equal(await readFile(join(worktreeB, 'README.md'), 'utf8'), 'fixture\n')

    const summaries = unwrap(await activeOperator.remote.session.list({}))
    const sessionA = summaries.items.find(item => item.sessionId === delegationA.delegation.sessionId)
    const sessionB = summaries.items.find(item => item.sessionId === delegationB.delegation.sessionId)
    assert.equal(sessionA?.cwd, worktreeA)
    assert.equal(sessionB?.cwd, worktreeB)
  } finally {
    await registry?.close().catch(() => undefined)
    await operator?.close().catch(() => undefined)
    await host?.stop().catch(() => undefined)
    await rm(root, { recursive: true, force: true })
  }
})

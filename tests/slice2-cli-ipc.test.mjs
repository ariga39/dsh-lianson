import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { spawnSync } from 'node:child_process'
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { loadUpstream } from '../src/upstream.mjs'
import { HostManager } from '../src/host-manager.mjs'
import { OperatorConnection } from '../src/operator-connection.mjs'
import { createOperatorAdapter } from '../src/operator-adapter.mjs'
import { call } from '../src/ipc-client.mjs'
import { startScriptedAgentServer } from './support/scripted-agent-server.mjs'
import { createKeylessEnvironment } from './support/keyless-environment.mjs'
import { dropIpcResponse } from './support/ipc-loss.mjs'
import { runCli, sleep } from './support/cli-harness.mjs'

function eventsOf(page) {
  return page.records.map(record => record.event)
}

async function waitForRequest(adapter, sessionId, requestId) {
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    const state = await adapter.sessionState({ sessionId })
    const events = eventsOf(await adapter.sessionEvents({
      address: { kind: 'session', sessionId },
      maxMessages: 500,
    }))
    const user = events.find(event => event.type === 'user/message' && event.data.source?.rpcId === requestId)
    const finished = user !== undefined && events.some(event => event.type === 'turn/end' && event.seq > user.seq)
    if (finished && !state.running && (state.queue === null || state.queue.length === 0)) return events
    await sleep(100)
  }
  throw new Error(`turn for ${requestId} in ${sessionId} did not finish`)
}

function git(args, cwd) {
  const result = spawnSync('git', args, { cwd, encoding: 'utf8' })
  if (result.status !== 0) {
    throw new Error(`git ${args.join(' ')} failed (${String(result.status)}):\n${result.stderr}`)
  }
  return result.stdout.trim()
}

test('slice2 integration: normal CLI and IPC through a real daemon with same-id replay', {
  timeout: 900_000,
}, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice2-cli-'))
  const mock = await startScriptedAgentServer()
  const fixture = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  const env = { ...fixture.env, DSH_LIAISON_STATE_DIR: stateDir }
  let hostStarted = false
  let observer
  let worktreeRoot

  t.after(async () => {
    await observer?.close().catch(() => {})
    if (hostStarted) {
      await runCli(['host', 'stop'], { cwd: fixture.cwd, env, timeoutMs: 60_000 }).catch(() => {})
    }
    await mock.close().catch(() => {})
    if (worktreeRoot !== undefined) await rm(worktreeRoot, { recursive: true, force: true })
    await rm(root, { recursive: true, force: true })
  })

  const started = await runCli([
    'host', 'start',
    '--workspace', fixture.cwd,
    '--dsh-home', fixture.dshHome,
    '--patch', fixture.overlayPath,
  ], { cwd: fixture.cwd, env })
  assert.equal(started.code, 0, started.stderr)
  assert.equal(started.envelope?.ok, true, started.stdout)
  hostStarted = true

  const manager = new HostManager({ stateDir, env })
  observer = new OperatorConnection({ hostManager: manager, upstream: await loadUpstream() })
  await observer.ensureConnected()
  const adapter = createOperatorAdapter(observer)
  const catalog = await adapter.modelCatalog()
  const models = catalog.groups.flatMap(group =>
    group.models.map(model => ({ provider: group.id, model: model.id })))
  assert.ok(models.length >= 2, 'mock Host must advertise two models')
  const [modelOne, modelTwo] = models

  async function cli(args) {
    const result = await runCli(args, { cwd: fixture.cwd, env })
    assert.equal(result.code, 0, `${args.join(' ')}\n${result.stdout}\n${result.stderr}`)
    assert.equal(result.envelope?.ok, true, result.stdout)
    return result.envelope
  }

  function createArgs({ label, cwd = fixture.cwd, model = modelOne, id = `${label}-create-${randomUUID()}` }) {
    return [
      'create',
      '--delegation-id', label,
      '--id', id,
      '--cwd', cwd,
      '--title', `${label} ${randomUUID()}`,
      '--provider', model.provider,
      '--model', model.model,
    ]
  }

  await t.test('normal CLI create, get, list, prompt, and model verification', async () => {
    const created = await cli(createArgs({ label: 'cli-a', id: 'cli-a-create' }))
    assert.equal(created.command, 'delegation.create')
    assert.equal(created.data.delegationId, 'cli-a')
    assert.equal(created.data.state, 'ready')
    assert.equal(created.data.cwd, await realpath(fixture.cwd))
    assert.deepEqual(created.data.expectedModel, { provider: modelOne.provider, model: modelOne.model })
    assert.match(created.data.sessionId, /^session-/u)
    assert.ok(typeof created.data.workspaceId === 'string' && created.data.workspaceId.length > 0)

    const byDelegation = await cli(['get', '--delegation-id', 'cli-a'])
    assert.equal(byDelegation.command, 'delegation.get')
    assert.equal(byDelegation.data.sessionId, created.data.sessionId)

    const bySession = await cli(['get', '--session-id', created.data.sessionId])
    assert.equal(bySession.data.delegationId, 'cli-a',
      'session-id lookup must expose the bound delegation for #4/#5')

    const listed = await cli(['list'])
    assert.equal(listed.command, 'delegation.list')
    assert.ok(listed.data.delegations.some(record =>
      record.delegationId === 'cli-a' && record.sessionId === created.data.sessionId))

    const requestId = 'cli-a-request'
    const prompted = await cli([
      'prompt',
      '--delegation-id', 'cli-a',
      '--id', 'cli-a-prompt',
      '--request-id', requestId,
      '--content', 'CLI_PROMPT_A',
    ])
    assert.equal(prompted.command, 'delegation.prompt')
    assert.equal(prompted.data.accepted, true)
    assert.equal(prompted.data.completed, false)
    assert.equal(prompted.data.sessionId, created.data.sessionId)
    await waitForRequest(adapter, created.data.sessionId, requestId)

    const verified = await cli([
      'verify-model',
      '--delegation-id', 'cli-a',
      '--request-id', requestId,
    ])
    assert.equal(verified.command, 'delegation.verifyModel')
    assert.equal(verified.data.provider, modelOne.provider)
    assert.equal(verified.data.model, modelOne.model)
    assert.ok(Number.isInteger(verified.data.seq))

    const stored = await cli(['get', '--delegation-id', 'cli-a'])
    assert.deepEqual(stored.data.verifiedModel, { provider: modelOne.provider, model: modelOne.model })

    await cli([
      'prompt',
      '--delegation-id', 'cli-a',
      '--id', 'cli-a-prompt-dup',
      '--request-id', requestId,
      '--content', 'CLI_PROMPT_A',
    ])
    const events = await waitForRequest(adapter, created.data.sessionId, requestId)
    assert.equal(events.filter(event =>
      event.type === 'user/message' && event.data.source?.rpcId === requestId).length, 1)
  })

  await t.test('dropped create response retries the same IPC id without a duplicate session', async () => {
    const lossCwd = join(root, 'loss-workspace')
    await mkdir(lossCwd, { recursive: true })
    const canonicalLoss = await realpath(lossCwd)
    const params = {
      delegationId: 'loss-create',
      cwd: canonicalLoss,
      title: `loss-create ${randomUUID()}`,
      model: modelOne,
      host: { cwd: canonicalLoss },
    }
    const request = { command: 'delegation.create', params, id: 'loss-create-ipc' }
    const proxyPath = join(root, 'loss-create-proxy.sock')
    const dropped = await dropIpcResponse({ socketPath: join(stateDir, 'daemon.sock'), proxyPath, request })
    assert.equal(dropped.ok, true, 'the real daemon must finish before the response is dropped')
    assert.equal(dropped.id, 'loss-create-ipc')

    const retried = await call(join(stateDir, 'daemon.sock'), { ...request, timeoutMs: 60_000 })
    assert.equal(retried.ok, true, JSON.stringify(retried))
    assert.equal(retried.id, 'loss-create-ipc')
    assert.equal(retried.data.sessionId, dropped.data.sessionId)
    assert.equal(retried.data.state, 'ready')

    const sessions = (await adapter.listSessions()).items.filter(item => item.cwd === canonicalLoss)
    assert.equal(sessions.length, 1, 'same-id replay must not create a second session')
    assert.equal(sessions[0].sessionId, retried.data.sessionId)
  })

  await t.test('dropped prompt response replays the same acceptance and persists one user message', async () => {
    const created = await cli(createArgs({ label: 'loss-prompt', id: 'loss-prompt-create' }))
    const requestId = 'loss-prompt-request'
    const request = {
      command: 'delegation.prompt',
      params: { delegationId: 'loss-prompt', content: 'LOSS_PROMPT', requestId },
      id: 'loss-prompt-ipc',
    }
    const dropped = await dropIpcResponse({
      socketPath: join(stateDir, 'daemon.sock'),
      proxyPath: join(root, 'loss-prompt-proxy.sock'),
      request,
    })
    assert.equal(dropped.ok, true)
    assert.equal(dropped.data.accepted, true)
    assert.equal(dropped.data.completed, false)

    const retried = await call(join(stateDir, 'daemon.sock'), { ...request, timeoutMs: 60_000 })
    assert.equal(retried.ok, true, JSON.stringify(retried))
    assert.deepEqual(retried.data, dropped.data, 'same-id replay returns the exact stored acceptance')
    const again = await call(join(stateDir, 'daemon.sock'), { ...request, timeoutMs: 60_000 })
    assert.deepEqual(again.data, dropped.data)

    const events = await waitForRequest(adapter, created.data.sessionId, requestId)
    assert.equal(events.filter(event =>
      event.type === 'user/message' && event.data.source?.rpcId === requestId).length, 1)
  })

  await t.test('CLI delegations keep linked worktrees isolated and prove each selected model', async () => {
    const cache = join(homedir(), '.cache')
    await mkdir(cache, { recursive: true })
    worktreeRoot = await mkdtemp(join(cache, 'dsh-slice2-cli-wt-'))
    const repo = join(worktreeRoot, 'repo')
    const wtA = join(worktreeRoot, 'wt-a')
    const wtB = join(worktreeRoot, 'wt-b')
    await mkdir(repo, { recursive: true })
    git(['init', '-q'], repo)
    git(['config', 'user.email', 'slice2-cli@example.invalid'], repo)
    git(['config', 'user.name', 'Slice 2 CLI'], repo)
    await writeFile(join(repo, 'baseline.txt'), 'baseline\n')
    git(['add', 'baseline.txt'], repo)
    git(['commit', '-q', '-m', 'baseline'], repo)
    git(['worktree', 'add', '-q', '-b', 'cli-wt-a', wtA, 'HEAD'], repo)
    git(['worktree', 'add', '-q', '-b', 'cli-wt-b', wtB, 'HEAD'], repo)
    const canonicalA = await realpath(wtA)
    const canonicalB = await realpath(wtB)

    const createdA = await cli(createArgs({ label: 'wt-a', cwd: canonicalA, model: modelOne }))
    const createdB = await cli(createArgs({ label: 'wt-b', cwd: canonicalB, model: modelTwo }))
    assert.equal(createdA.data.cwd, canonicalA)
    assert.equal(createdB.data.cwd, canonicalB)

    const requestA = `req-${randomUUID()}`
    const requestB = `req-${randomUUID()}`
    await cli(['prompt', '--delegation-id', 'wt-a', '--request-id', requestA, '--content', 'WRITE_LOCAL a-cli.txt'])
    await cli(['prompt', '--delegation-id', 'wt-b', '--request-id', requestB, '--content', 'WRITE_LOCAL b-cli.txt'])
    await waitForRequest(adapter, createdA.data.sessionId, requestA)
    await waitForRequest(adapter, createdB.data.sessionId, requestB)
    assert.equal(await readFile(join(canonicalA, 'a-cli.txt'), 'utf8'), `written by ${modelOne.model}\n`)
    assert.equal(await readFile(join(canonicalB, 'b-cli.txt'), 'utf8'), `written by ${modelTwo.model}\n`)

    const crossTarget = join(canonicalB, 'cross-from-cli-a.txt')
    const crossRequest = `req-${randomUUID()}`
    await cli(['prompt', '--delegation-id', 'wt-a', '--request-id', crossRequest, '--content', `WRITE_CROSS ${crossTarget}`])
    const crossEvents = await waitForRequest(adapter, createdA.data.sessionId, crossRequest)
    await assert.rejects(readFile(crossTarget), error => error.code === 'ENOENT')
    assert.ok(crossEvents.some(event =>
      event.type === 'tool/result' && event.data.error?.code === 'FS_SANDBOX_DENIED'))

    const verifiedA = await cli(['verify-model', '--delegation-id', 'wt-a', '--request-id', requestA])
    const verifiedB = await cli(['verify-model', '--delegation-id', 'wt-b', '--request-id', requestB])
    assert.deepEqual({ provider: verifiedA.data.provider, model: verifiedA.data.model }, modelOne)
    assert.deepEqual({ provider: verifiedB.data.provider, model: verifiedB.data.model }, modelTwo)
    assert.notEqual(`${verifiedA.data.provider}/${verifiedA.data.model}`,
      `${verifiedB.data.provider}/${verifiedB.data.model}`)
  })
})

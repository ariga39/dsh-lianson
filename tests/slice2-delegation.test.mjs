import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { spawnSync } from 'node:child_process'
import { mkdir, mkdtemp, readFile, readdir, realpath, rm, stat, writeFile } from 'node:fs/promises'
import { homedir, tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { after, test } from 'node:test'
import { loadUpstream } from '../src/upstream.mjs'
import { DelegationRegistry } from '../src/delegation-registry.mjs'
import { SessionCommands } from '../src/session-commands.mjs'
import { createOperatorAdapter, createRemoteAdapter } from '../src/operator-adapter.mjs'
import { startScriptedAgentServer } from './support/scripted-agent-server.mjs'
import { createScratchRoot } from './support/scratch.mjs'
import { startDshWeb } from './support/dsh-web-harness.mjs'
import { connectOperator } from './support/operator.mjs'

const FINAL = 'SLICE2_FINAL_DONE'

function deferred() {
  return Promise.withResolvers()
}

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), ms).unref()),
  ])
}

function errorCode(code) {
  return error => {
    assert.equal(error?.code, code, `expected ${code}, got ${String(error?.code)}: ${error?.message}`)
    return true
  }
}

function git(args, cwd) {
  const result = spawnSync('git', args, { cwd, encoding: 'utf8' })
  if (result.status !== 0) {
    throw new Error(`git ${args.join(' ')} failed (${String(result.status)}):\n${result.stderr}`)
  }
  return result.stdout.trim()
}

async function ledgerEntries(filePath) {
  const parsed = JSON.parse(await readFile(filePath, 'utf8'))
  return parsed.delegations
}

function eventsOf(page) {
  return page.records.map(record => record.event)
}

async function waitForRequest(adapter, sessionId, requestId) {
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    const state = await adapter.sessionState({ sessionId })
    const page = await adapter.sessionEvents({
      address: { kind: 'session', sessionId },
      maxMessages: 500,
    })
    const events = eventsOf(page)
    const user = events.find(event => event.type === 'user/message' && event.data.source?.rpcId === requestId)
    const finished = user !== undefined && events.some(event => event.type === 'turn/end' && event.seq > user.seq)
    if (finished && !state.running && (state.queue === null || state.queue.length === 0)) return events
    await new Promise(resolve => setTimeout(resolve, 100))
  }
  throw new Error(`turn for ${requestId} in ${sessionId} did not finish`)
}

async function waitForSession(adapter, sessionId) {
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    const state = await adapter.sessionState({ sessionId })
    const page = await adapter.sessionEvents({ address: { kind: 'session', sessionId }, maxMessages: 500 })
    if (!state.running && (state.queue === null || state.queue.length === 0)
      && eventsOf(page).some(event => event.type === 'turn/end')) return eventsOf(page)
    await new Promise(resolve => setTimeout(resolve, 100))
  }
  throw new Error(`session ${sessionId} did not become idle`)
}

async function sessionsInWorkspace(adapter, cwd) {
  const value = await adapter.listSessions()
  return value.items.filter(item => item.cwd === cwd)
}

test('slice2: delegation ledger, conflict-safe creation, per-session models, idempotent prompts, worktree isolation', {
  timeout: 900_000,
}, async t => {
  const root = await createScratchRoot('slice2-delegation-')
  const workspaceDir = join(root, 'workspace')
  const canonicalWorkspace = join(await realpath(root), 'workspace')
  const dshHome = join(root, 'home')
  const overlayPath = join(root, 'overlay.yml')
  let mock
  let web
  let operator
  let upstream

  after(async () => {
    await operator?.close().catch(() => {})
    await web?.close().catch(() => {})
    await mock?.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  mock = await startScriptedAgentServer()
  upstream = await loadUpstream()
  await mkdir(workspaceDir, { recursive: true })
  await writeFile(overlayPath, [
    '- id: llm-deepseek',
    '  config:',
    `    baseURL: '${mock.baseURL}'`,
    '- id: llm-deepseek-account',
    '  disabled: true',
    '- id: session-title-llm',
    '  disabled: true',
    '- id: agent-instructions',
    '  disabled: true',
    '- id: session-log-deepseek',
    '  disabled: true',
    '- id: session-telemetry-otel',
    '  disabled: true',
    '',
  ].join('\n'))

  web = await startDshWeb({ root, cwd: workspaceDir, dshHome, overlayPath })
  operator = await connectOperator({ upstream, launchUrl: web.launchUrl })
  await operator.ready()
  const adapter = createRemoteAdapter(operator.ctx.remote)
  const catalog = await adapter.modelCatalog()
  const models = catalog.groups.flatMap(group =>
    group.models.map(model => ({ provider: group.id, model: model.id })))
  assert.ok(models.length >= 2, 'the scripted Host must advertise at least two models')
  const [modelOne, modelTwo] = models

  function newCommands(label, subject = adapter, sessionOptions = {}) {
    const filePath = join(root, `state-${label}`, 'delegations.json')
    const registry = new DelegationRegistry({ filePath })
    return {
      filePath,
      registry,
      commands: new SessionCommands({ registry, adapter: subject, ...sessionOptions }),
    }
  }

  function input(label, overrides = {}) {
    return {
      delegationId: `${label}-${randomUUID()}`,
      cwd: workspaceDir,
      title: `${label} ${randomUUID()}`,
      model: modelOne,
      ...overrides,
    }
  }

  await t.test('write-ahead create survives a dropped response, rejects mismatch, and never duplicates', async () => {
    const { filePath, commands } = newCommands('drop')
    const params = input('drop')

    const faultedAdapter = createRemoteAdapter(operator.ctx.remote)
    const realCreate = faultedAdapter.createSession
    let dropped = false
    faultedAdapter.createSession = async request => {
      const ahead = JSON.parse(await readFile(filePath, 'utf8')).delegations
      assert.equal(ahead.length, 1, 'the ledger must be written before session.create')
      assert.equal(ahead[0].state, 'creating')
      assert.equal(ahead[0].sessionId, request.sessionId)
      const value = await realCreate(request)
      if (dropped) return value
      dropped = true
      const error = new Error('socket hang up after Host committed session.create')
      error.code = 'ECONNRESET'
      error.transient = true
      throw error
    }
    const faulted = new SessionCommands({
      registry: new DelegationRegistry({ filePath }),
      adapter: faultedAdapter,
    })

    await assert.rejects(faulted.createDelegation(params), errorCode('delegation/create-failed'))
    const [pending] = await ledgerEntries(filePath)
    assert.equal(pending.state, 'creating')
    assert.match(pending.sessionId, /^session-/u)
    assert.equal(pending.cwd, canonicalWorkspace)
    assert.ok(typeof pending.workspaceId === 'string' && pending.workspaceId.length > 0)
    const minted = pending.sessionId
    const ledgerStat = await stat(filePath)
    assert.equal(ledgerStat.mode & 0o777, 0o600, 'ledger must be owner read/write only')
    assert.deepEqual(await readdir(dirname(filePath)), ['delegations.json'], 'atomic writes must leave no temp files')

    await assert.rejects(
      commands.createDelegation({ ...params, title: `${params.title} changed` }),
      errorCode('delegation/mismatch'),
    )

    const created = await commands.createDelegation(params)
    assert.equal(created.sessionId, minted, 'retry must reclaim the exact session minted before the RPC')
    assert.equal(created.state, 'ready')
    assert.equal(created.verifiedModel, undefined)

    const sessions = await sessionsInWorkspace(adapter, canonicalWorkspace)
    assert.equal(sessions.filter(item => item.sessionId === minted).length, 1)
    assert.equal(sessions.length, 1, 'dropped-response retry must not create a second session')

    await assert.rejects(commands.createDelegation(params), errorCode('delegation/already-exists'))
    await assert.rejects(
      commands.createDelegation(input('drop-title-conflict', { title: params.title })),
      errorCode('delegation/title-conflict'),
    )
  })

  await t.test('same-delegation concurrent creates serialize and ready is terminal', async () => {
    const { registry, commands } = newCommands('concurrent-create')
    const params = input('concurrent-create')
    const before = await sessionsInWorkspace(adapter, canonicalWorkspace)
    const results = await Promise.allSettled([
      commands.createDelegation(params),
      commands.createDelegation(params),
    ])
    const fulfilled = results.filter(result => result.status === 'fulfilled')
    const rejected = results.filter(result => result.status === 'rejected')
    assert.equal(fulfilled.length, 1, 'exactly one create may bind the delegation')
    assert.equal(rejected.length, 1)
    assert.equal(rejected[0].reason?.code, 'delegation/already-exists')
    assert.equal(fulfilled[0].value.state, 'ready')
    const sessionId = fulfilled[0].value.sessionId

    const after = await sessionsInWorkspace(adapter, canonicalWorkspace)
    assert.equal(after.length, before.length + 1, 'the loser must not mint a second session')

    await registry.markConflict(params.delegationId, {
      upstreamCode: 'late-host-result',
      message: 'late result must not invalidate ready',
      substitutedSessionId: `session-${randomUUID()}`,
    })
    await registry.markAttachFailed(params.delegationId, {
      sessionId: `session-${randomUUID()}`,
      message: 'late attach failure must not invalidate ready',
    })
    const record = await registry.get(params.delegationId)
    assert.equal(record.state, 'ready')
    assert.equal(record.sessionId, sessionId)
    assert.equal(record.substitutedSessionId, undefined)
    assert.equal(record.returnedSessionId, undefined)
  })

  await t.test('shared temp roots and overlapping workspaces fail before creation', async () => {
    const { registry, commands } = newCommands('workspace-roots')
    const shared = await mkdtemp(join(tmpdir(), 'dsh-shared-temp-'))
    t.after(async () => { await rm(shared, { recursive: true, force: true }) })
    const tempParams = input('shared-temp', { cwd: shared })
    await assert.rejects(commands.createDelegation(tempParams), errorCode('delegation/workspace-unisolatable'))
    assert.equal(await registry.get(tempParams.delegationId), undefined,
      'the shared-temp rejection must precede workspace/session creation')

    const overlapRoot = join(root, 'overlap-root')
    const outer = join(overlapRoot, 'outer')
    const nested = join(outer, 'nested')
    await mkdir(nested, { recursive: true })
    await commands.createDelegation(input('overlap-outer', { cwd: outer }))
    await assert.rejects(
      commands.createDelegation(input('overlap-nested', { cwd: nested })),
      errorCode('delegation/workspace-overlap'),
    )
    await assert.rejects(
      commands.createDelegation(input('overlap-parent', { cwd: overlapRoot })),
      errorCode('delegation/workspace-overlap'),
    )

    const same = await commands.createDelegation(input('overlap-same', { cwd: outer }))
    assert.equal(same.state, 'ready', 'multiple delegations may intentionally share one workspace root')
  })

  await t.test('silent Host session substitution is rejected without binding', async () => {
    const { registry, commands } = newCommands('substitution')
    const workspace = await adapter.ensureWorkspace(workspaceDir)
    const requestedSessionId = `session-${randomUUID()}`
    const seedRequest = `request-${randomUUID()}`
    await adapter.createSession({ workspaceId: workspace.workspaceId, sessionId: requestedSessionId })
    assert.equal((await adapter.prompt({
      requestId: seedRequest,
      sessionId: requestedSessionId,
      mode: 'queue',
      content: [{ type: 'text', text: 'SUBSTITUTION_SEED_TURN' }],
    })).accepted, true)
    await waitForRequest(adapter, requestedSessionId, seedRequest)

    const params = input('substitution')
    await registry.beginCreate({
      delegationId: params.delegationId,
      cwd: canonicalWorkspace,
      workspaceId: workspace.workspaceId,
      sessionId: requestedSessionId,
      title: params.title,
      agentPreset: undefined,
      expectedModel: modelOne,
    })

    await assert.rejects(commands.createDelegation(params), error => {
      assert.equal(error?.code, 'delegation/session-substituted')
      assert.equal(error.details.requestedSessionId, requestedSessionId)
      assert.notEqual(error.details.returnedSessionId, requestedSessionId)
      return true
    })

    const record = await registry.get(params.delegationId)
    assert.equal(record.state, 'conflict')
    assert.equal(record.sessionId, requestedSessionId, 'the ledger must keep the original identity')
    assert.notEqual(record.substitutedSessionId, requestedSessionId)
    assert.equal(record.returnedSessionId, record.substitutedSessionId)
    assert.ok(!('boundSessionId' in record))
  })

  await t.test('workspace attach failure retains the created session without minting an orphan', async () => {
    const { filePath, commands } = newCommands('attach')
    const params = input('attach')

    const faultedAdapter = createRemoteAdapter(operator.ctx.remote)
    const realCreate = faultedAdapter.createSession
    faultedAdapter.createSession = async request => {
      const value = await realCreate(request)
      const error = new Error('simulated session/workspace-attach-failed')
      error.isDSHRemoteError = true
      error.code = 'session/workspace-attach-failed'
      error.details = { sessionId: value.sessionId, workspaceId: request.workspaceId }
      throw error
    }
    const faulted = new SessionCommands({
      registry: new DelegationRegistry({ filePath }),
      adapter: faultedAdapter,
    })

    await assert.rejects(faulted.createDelegation(params), error => {
      assert.equal(error?.code, 'delegation/attach-failed')
      assert.match(error.details.sessionId, /^session-/u)
      return true
    })
    const [record] = await ledgerEntries(filePath)
    assert.equal(record.state, 'attach-failed')
    assert.match(record.sessionId, /^session-/u)
    assert.ok(typeof record.workspaceId === 'string' && record.workspaceId.length > 0)

    const before = await sessionsInWorkspace(adapter, canonicalWorkspace)
    await assert.rejects(commands.createDelegation(params), errorCode('delegation/attach-failed'))
    const afterRetry = await sessionsInWorkspace(adapter, canonicalWorkspace)
    assert.equal(afterRetry.length, before.length, 'retry must not mint another session')
    assert.ok(afterRetry.some(item => item.sessionId === record.sessionId), 'the preserved session still exists')
  })

  await t.test('attach failure with a remote substituted session records conflict without rebinding', async () => {
    const { filePath, commands } = newCommands('attach-substituted')
    const params = input('attach-substituted')
    const substitutedSessionId = `session-${randomUUID()}`

    const faultedAdapter = createRemoteAdapter(operator.ctx.remote)
    const realCreate = faultedAdapter.createSession
    faultedAdapter.createSession = async request => {
      await realCreate(request)
      const error = new Error('simulated attach failure with a substituted session')
      error.isDSHRemoteError = true
      error.code = 'session/workspace-attach-failed'
      error.details = { sessionId: substitutedSessionId, workspaceId: request.workspaceId }
      throw error
    }
    const faulted = new SessionCommands({
      registry: new DelegationRegistry({ filePath }),
      adapter: faultedAdapter,
    })

    await assert.rejects(faulted.createDelegation(params), error => {
      assert.equal(error?.code, 'delegation/session-substituted')
      assert.notEqual(error.details.requestedSessionId, substitutedSessionId)
      assert.equal(error.details.returnedSessionId, substitutedSessionId)
      return true
    })
    const [record] = await ledgerEntries(filePath)
    assert.equal(record.state, 'conflict')
    assert.notEqual(record.sessionId, substitutedSessionId, 'the binding must not be overwritten')
    assert.equal(record.substitutedSessionId, substitutedSessionId)
    assert.equal(record.returnedSessionId, substitutedSessionId)

    await assert.rejects(commands.createDelegation(params), errorCode('delegation/conflict'))
  })

  await t.test('advertised model checks and request-window header proof isolate each session', async () => {
    const { registry, commands } = newCommands('models')
    const a = input('model-a', { model: modelOne })
    const b = input('model-b', { model: modelTwo })
    const createdA = await commands.createDelegation(a)
    const createdB = await commands.createDelegation(b)
    assert.deepEqual(createdA.expectedModel, { provider: modelOne.provider, model: modelOne.model })
    assert.deepEqual(createdB.expectedModel, { provider: modelTwo.provider, model: modelTwo.model })

    // B's selectModel also mutates the Host global default in the background;
    // A must nevertheless keep sending its own selected route.
    const requestA = `request-${randomUUID()}`
    const requestB = `request-${randomUUID()}`
    const promptA = await commands.submitPrompt({
      delegationId: a.delegationId,
      content: 'MODEL_PROOF_A',
      requestId: requestA,
    })
    assert.equal(promptA.delegationId, a.delegationId)
    assert.equal(promptA.sessionId, createdA.sessionId)
    assert.equal(promptA.requestId, requestA)
    assert.equal(promptA.accepted, true)
    assert.equal(promptA.completed, false)
    assert.equal(typeof promptA.beforeCursor, 'number')
    await waitForRequest(adapter, createdA.sessionId, requestA)
    const promptB = await commands.submitPrompt({
      delegationId: b.delegationId,
      content: 'MODEL_PROOF_B',
      requestId: requestB,
    })
    assert.equal(promptB.completed, false)
    await waitForRequest(adapter, createdB.sessionId, requestB)

    const verifiedA = await commands.verifyRequestModel({ delegationId: a.delegationId, requestId: requestA })
    const verifiedB = await commands.verifyRequestModel({ delegationId: b.delegationId, requestId: requestB })
    assert.deepEqual({ provider: verifiedA.provider, model: verifiedA.model }, modelOne)
    assert.deepEqual({ provider: verifiedB.provider, model: verifiedB.model }, modelTwo)
    assert.notEqual(`${verifiedA.provider}/${verifiedA.model}`, `${verifiedB.provider}/${verifiedB.model}`)

    const storedA = await registry.get(a.delegationId)
    assert.deepEqual(storedA.verifiedModel, { provider: modelOne.provider, model: modelOne.model })
    const eventsA = eventsOf(await adapter.sessionEvents({
      address: { kind: 'session', sessionId: createdA.sessionId },
      maxMessages: 500,
    }))
    const headersA = eventsA.filter(event => event.type === 'request/header')
    assert.ok(headersA.some(event =>
      event.data.header.config.provider === modelOne.provider
      && event.data.header.config.model === modelOne.model))

    const before = await sessionsInWorkspace(adapter, canonicalWorkspace)
    const unavailable = input('model-unavailable', {
      model: { provider: modelOne.provider, model: `not-advertised-${randomUUID()}` },
    })
    await assert.rejects(commands.createDelegation(unavailable), errorCode('delegation/model-unavailable'))
    const afterAttempt = await sessionsInWorkspace(adapter, canonicalWorkspace)
    assert.equal(afterAttempt.length, before.length, 'model pre-check must fail before session creation')
    assert.equal(await registry.get(unavailable.delegationId), undefined)
  })

  await t.test('request-window verification inherits an unchanged header and ignores later turns', async () => {
    const { commands } = newCommands('header-window', adapter, { historyPageMessages: 5 })
    const params = input('header-window', { model: modelOne })
    const created = await commands.createDelegation(params)

    const first = `request-${randomUUID()}`
    await commands.submitPrompt({ delegationId: params.delegationId, content: 'HEADER_WINDOW_ONE', requestId: first })
    await waitForRequest(adapter, created.sessionId, first)

    const second = `request-${randomUUID()}`
    await commands.submitPrompt({ delegationId: params.delegationId, content: 'HEADER_WINDOW_TWO', requestId: second })
    await waitForRequest(adapter, created.sessionId, second)

    const inherited = await commands.verifyRequestModel({
      delegationId: params.delegationId,
      requestId: second,
    })
    assert.deepEqual({ provider: inherited.provider, model: inherited.model }, modelOne,
      'a turn with an unchanged request header inherits the prior effective route')
    assert.ok(Number.isInteger(inherited.turnEndSeq), 'verification reports the pinned turn end')

    const unscoped = await commands.verifyRequestModel({ delegationId: params.delegationId })
    assert.deepEqual({ provider: unscoped.provider, model: unscoped.model }, modelOne,
      'an unscoped lookup paginates past pages with no request/header')

    // The later turn changes the persisted header. It must never satisfy an
    // earlier request's model proof.
    await adapter.selectModel({ sessionId: created.sessionId, ...modelTwo })
    const third = `request-${randomUUID()}`
    await commands.submitPrompt({ delegationId: params.delegationId, content: 'HEADER_WINDOW_THREE', requestId: third })
    const events = await waitForRequest(adapter, created.sessionId, third)
    const latestHeader = events.findLast(event => event.type === 'request/header')
    assert.equal(latestHeader?.data.header.config.model, modelTwo.model,
      'the later turn must persist the changed header')

    const firstAgain = await commands.verifyRequestModel({
      delegationId: params.delegationId,
      requestId: first,
    })
    const secondAgain = await commands.verifyRequestModel({
      delegationId: params.delegationId,
      requestId: second,
    })
    assert.deepEqual({ provider: firstAgain.provider, model: firstAgain.model }, modelOne)
    assert.deepEqual({ provider: secondAgain.provider, model: secondAgain.model }, modelOne)
  })

  await t.test('repeated prompt requestId is accepted once and persists one user message', async () => {
    // Exercise the Issue #2 `OperatorConnection.withClient(fn)` seam as well.
    const operatorAdapter = createOperatorAdapter({
      withClient: fn => fn(operator.ctx.remote),
    })
    const { commands } = newCommands('prompt', operatorAdapter)
    const params = input('prompt')
    const created = await commands.createDelegation(params)
    const requestId = `request-${randomUUID()}`

    const [first, second] = await Promise.all([
      commands.submitPrompt({ delegationId: params.delegationId, content: 'IDEMPOTENT_PROMPT', requestId }),
      commands.submitPrompt({ delegationId: params.delegationId, content: 'IDEMPOTENT_PROMPT', requestId }),
    ])
    assert.deepEqual({ accepted: first.accepted, completed: first.completed }, { accepted: true, completed: false })
    assert.deepEqual({ accepted: second.accepted, completed: second.completed }, { accepted: true, completed: false })
    assert.equal(first.requestId, requestId)
    assert.equal(second.requestId, requestId)

    const events = await waitForRequest(adapter, created.sessionId, requestId)
    const userMessages = events.filter(event =>
      event.type === 'user/message' && event.data.source?.rpcId === requestId)
    assert.equal(userMessages.length, 1, 'Host requestId dedup must persist exactly one user message')
    assert.ok(events.some(event => event.type === 'assistant/message'
      && JSON.stringify(event.data).includes(FINAL)))
  })

  await t.test('two linked worktrees stay write-isolated without widening sandbox roots', async () => {
    // Workspace-write deliberately treats /tmp as writable. Put the linked
    // worktrees outside the platform temp area so the fence is meaningful.
    const cache = join(homedir(), '.cache')
    await mkdir(cache, { recursive: true })
    const worktreeRoot = await mkdtemp(join(cache, 'dsh-slice2-wt-'))
    t.after(async () => { await rm(worktreeRoot, { recursive: true, force: true }) })
    const repo = join(worktreeRoot, 'worktree-repo')
    const wtA = join(worktreeRoot, 'wt-a')
    const wtB = join(worktreeRoot, 'wt-b')
    await mkdir(repo, { recursive: true })
    git(['init', '-q'], repo)
    git(['config', 'user.email', 'slice2@example.invalid'], repo)
    git(['config', 'user.name', 'Slice 2'], repo)
    await writeFile(join(repo, 'baseline.txt'), 'baseline\n')
    git(['add', 'baseline.txt'], repo)
    git(['commit', '-q', '-m', 'baseline'], repo)
    git(['worktree', 'add', '-q', '-b', 'wt-a', wtA, 'HEAD'], repo)
    git(['worktree', 'add', '-q', '-b', 'wt-b', wtB, 'HEAD'], repo)
    const canonicalA = await realpath(wtA)
    const canonicalB = await realpath(wtB)

    const { commands } = newCommands('worktrees')
    const a = await commands.createDelegation(input('worktree-a', { cwd: canonicalA }))
    const b = await commands.createDelegation(input('worktree-b', { cwd: canonicalB }))

    const writeLocalA = `request-${randomUUID()}`
    await commands.submitPrompt({
      delegationId: a.delegationId,
      requestId: writeLocalA,
      content: 'WRITE_LOCAL a-local.txt',
    })
    await waitForRequest(adapter, a.sessionId, writeLocalA)
    assert.equal(await readFile(join(canonicalA, 'a-local.txt'), 'utf8'), `written by ${modelOne.model}\n`)

    const writeLocalB = `request-${randomUUID()}`
    await commands.submitPrompt({
      delegationId: b.delegationId,
      requestId: writeLocalB,
      content: 'WRITE_LOCAL b-local.txt',
    })
    await waitForRequest(adapter, b.sessionId, writeLocalB)
    assert.equal(await readFile(join(canonicalB, 'b-local.txt'), 'utf8'), `written by ${modelOne.model}\n`)

    const crossTarget = join(canonicalB, 'cross-from-a.txt')
    const writeCross = `request-${randomUUID()}`
    await commands.submitPrompt({
      delegationId: a.delegationId,
      requestId: writeCross,
      content: `WRITE_CROSS ${crossTarget}`,
    })
    const events = await waitForRequest(adapter, a.sessionId, writeCross)
    await assert.rejects(readFile(crossTarget), error => error.code === 'ENOENT')
    const denials = events.filter(event => event.type === 'tool/result' && event.data.error?.code === 'FS_SANDBOX_DENIED')
    assert.ok(denials.length >= 1, `the Host sandbox must deny A writing inside B worktree: ${JSON.stringify(events
      .filter(event => event.type === 'tool/result')
      .map(event => event.data))}`)
    assert.equal((await readFile(join(canonicalA, 'a-local.txt'), 'utf8')), `written by ${modelOne.model}\n`)
    assert.equal((await readFile(join(canonicalB, 'b-local.txt'), 'utf8')), `written by ${modelOne.model}\n`)
  })
})

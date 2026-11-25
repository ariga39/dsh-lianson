import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { loadUpstream } from '../src/upstream.mjs'
import { HostManager } from '../src/host-manager.mjs'
import { OperatorConnection } from '../src/operator-connection.mjs'
import { createOperatorAdapter } from '../src/operator-adapter.mjs'
import { startScriptedAgentServer } from './support/scripted-agent-server.mjs'
import { createKeylessEnvironment } from './support/keyless-environment.mjs'
import { runCli, sleep, waitFor } from './support/cli-harness.mjs'

test('slice3 CLI/IPC: status, wait and events over the real daemon keep delegations independent', {
  timeout: 900_000,
}, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice3-cli-'))
  const mock = await startScriptedAgentServer()
  const fixture = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  const env = { ...fixture.env, DSH_LIAISON_STATE_DIR: stateDir }
  let hostStarted = false
  let observer

  t.after(async () => {
    await observer?.close().catch(() => {})
    if (hostStarted) {
      await runCli(['host', 'stop'], { cwd: fixture.cwd, env, timeoutMs: 60_000 }).catch(() => {})
    }
    await mock.close().catch(() => {})
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
  const catalog = await createOperatorAdapter(observer).modelCatalog()
  const [model] = catalog.groups.flatMap(group =>
    group.models.map(entry => ({ provider: group.id, model: entry.id })))

  async function cli(args, options = {}) {
    const result = await runCli(args, { cwd: fixture.cwd, env, ...options })
    assert.equal(result.code, 0, `${args.join(' ')}\n${result.stdout}\n${result.stderr}`)
    assert.equal(result.envelope?.ok, true, result.stdout)
    return result.envelope
  }

  async function create(label) {
    return cli([
      'create',
      '--delegation-id', label,
      '--id', `${label}-create-${randomUUID()}`,
      '--cwd', fixture.cwd,
      '--title', `${label} ${randomUUID()}`,
      '--provider', model.provider,
      '--model', model.model,
    ])
  }

  const a = await create('watch-a')
  const b = await create('watch-b')
  assert.equal(a.data.state, 'ready')
  assert.equal(b.data.state, 'ready')
  assert.notEqual(a.data.sessionId, b.data.sessionId)

  const beforeA = await cli(['status', '--delegation-id', 'watch-a'])
  const beforeB = await cli(['status', '--delegation-id', 'watch-b'])
  assert.equal(beforeA.command, 'delegation.status')
  assert.equal(beforeA.data.status, 'idle')
  assert.equal(beforeB.data.status, 'idle')
  assert.ok(Number.isInteger(beforeA.data.state.cursor))
  assert.ok(Number.isInteger(beforeB.data.state.cursor))

  const promptA = await cli(['prompt', '--delegation-id', 'watch-a', '--content', 'HOLD 8000'])
  assert.equal(promptA.data.accepted, true)
  assert.equal(promptA.data.completed, false)
  await waitFor(async () => {
    const status = await cli(['status', '--delegation-id', 'watch-a'])
    return status.data.status === 'running'
  }, { timeoutMs: 15_000, label: 'A to report running' })

  let aSettled = false
  const aWaitPromise = runCli([
    'wait',
    '--delegation-id', 'watch-a',
    '--after-seq', String(beforeA.data.state.cursor),
    '--timeout-ms', '30000',
    '--slice-timeout-ms', '1000',
  ], { cwd: fixture.cwd, env }).then(
    result => ({ result }),
    error => ({ error }),
  ).finally(() => { aSettled = true })

  await sleep(300)
  assert.equal(aSettled, false, 'the A wait CLI is still running')

  const statusStart = performance.now()
  const statusB = await cli(['status', '--delegation-id', 'watch-b'])
  const statusMs = performance.now() - statusStart
  assert.equal(statusB.data.status, 'idle')
  assert.equal(statusB.data.sessionId, b.data.sessionId)
  assert.equal(aSettled, false, 'status B must answer while wait A is in flight')
  assert.ok(statusMs < 5_000, `status B stayed responsive (${Math.round(statusMs)} ms)`)

  const promptB = await cli(['prompt', '--delegation-id', 'watch-b', '--content', 'WRITE_LOCAL slice3-cli-b.txt'])
  assert.equal(promptB.data.accepted, true)
  const waitB = await cli([
    'wait',
    '--delegation-id', 'watch-b',
    '--after-seq', String(beforeB.data.state.cursor),
    '--timeout-ms', '20000',
    '--slice-timeout-ms', '1000',
  ])
  assert.equal(waitB.data.reason, 'idle')
  assert.equal(waitB.data.indeterminate, false)
  assert.ok(waitB.data.assistantMessages.some(message => /SLICE2_FINAL_DONE/u.test(message.text)))
  assert.equal(aSettled, false, 'B completed while the unrelated A wait was still running')
  assert.equal(await readFile(join(fixture.cwd, 'slice3-cli-b.txt'), 'utf8'), `written by ${model.model}\n`)

  const aWait = await aWaitPromise
  assert.equal(aWait.error, undefined, aWait.error?.message)
  assert.equal(aWait.result.code, 0, aWait.result.stderr)
  assert.equal(aWait.result.envelope?.ok, true, aWait.result.stdout)
  const waitedA = aWait.result.envelope.data
  assert.equal(waitedA.reason, 'idle')
  assert.equal(waitedA.indeterminate, false)
  assert.ok(waitedA.slices >= 2, `wait A crossed multiple Host windows (${waitedA.slices})`)
  assert.ok(waitedA.assistantMessages.some(message => /SLICE3_HOLD_FINAL/u.test(message.text)))
  assert.equal(waitedA.subagentCatalog.length, 0)
  assert.deepEqual(waitedA.activeSubagents, [])

  const eventsB = await cli([
    'events', '--delegation-id', 'watch-b',
    '--after-seq', String(beforeB.data.state.cursor),
    '--page-size', '2',
  ])
  assert.equal(eventsB.command, 'delegation.events')
  assert.ok(eventsB.data.pagesRead >= 2, 'a small page budget paginates through IPC')
  assert.deepEqual(
    eventsB.data.events.map(event => event.seq),
    Array.from({ length: eventsB.data.cursor - beforeB.data.state.cursor },
      (_, index) => beforeB.data.state.cursor + 1 + index),
    'CLI events stay contiguous with zero gaps or duplicates',
  )
  assert.ok(eventsB.data.messages.some(message =>
    message.role === 'user' && message.text.includes('slice3-cli-b.txt')))
  const writeCall = eventsB.data.toolActivity.calls.find(call => call.name === 'write')
  assert.ok(writeCall?.result !== undefined)
  assert.match(JSON.stringify(writeCall.result.output), /Created file/u)
  assert.equal(eventsB.data.requestHeaders.at(-1).config.provider, model.provider)
  assert.equal(eventsB.data.requestHeaders.at(-1).config.model, model.model)
  assert.ok(eventsB.data.turnEnds.length >= 1)

  const eventsA = await cli([
    'events', '--delegation-id', 'watch-a',
    '--after-seq', String(beforeA.data.state.cursor),
    '--page-size', '2',
  ])
  assert.ok(eventsA.data.messages.some(message =>
    message.role === 'assistant' && message.text.includes('SLICE3_HOLD_FINAL')))
  const pinned = eventsA.data.cursor
  const pinnedAgain = await cli([
    'events', '--delegation-id', 'watch-a',
    '--after-seq', String(beforeA.data.state.cursor),
    '--through-seq', String(pinned),
    '--page-size', '2',
  ])
  assert.equal(pinnedAgain.data.cursor, pinned, 'through-seq pins the same tail cursor')
  assert.deepEqual(pinnedAgain.data.events.map(event => event.seq),
    eventsA.data.events.map(event => event.seq))

  const fullB = await cli([
    'events', '--delegation-id', 'watch-b',
    '--after-seq', '-1',
    '--page-size', '5',
  ])
  assert.deepEqual(
    fullB.data.events.map(event => event.seq),
    Array.from({ length: fullB.data.cursor + 1 }, (_, index) => index),
    'an explicit negative --after-seq reads from seq 0',
  )

  const missing = await runCli(['status', '--delegation-id', 'missing-delegation'], {
    cwd: fixture.cwd, env,
  })
  assert.equal(missing.code, 1)
  assert.equal(missing.envelope?.ok, false)
  assert.equal(missing.envelope.error.code, 'delegation/not-found')

  const usage = await runCli(['wait'], { cwd: fixture.cwd, env })
  assert.equal(usage.code, 2)
  assert.equal(usage.envelope?.ok, false)
  assert.equal(usage.envelope.error.code, 'USAGE')

  t.diagnostic(`status B latency while wait A pending: ${statusMs.toFixed(1)} ms; A slices: ${waitedA.slices}`)
})

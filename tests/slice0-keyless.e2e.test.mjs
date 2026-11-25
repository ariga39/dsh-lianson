import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, test } from 'node:test'
import { loadUpstream } from '../src/upstream.mjs'
import { startScriptedMessagesServer } from './support/scripted-messages-server.mjs'
import { startDshWeb } from './support/dsh-web-harness.mjs'
import { connectOperator } from './support/operator.mjs'
import { assertNoWriterHeld, assertNoWriterHeldInPage, openSessionInBrowser } from './support/browser.mjs'

const PROMPT = 'SLICE0_KEYLESS_PROMPT: read the fixture and ask to proceed.'
const PREFIX = 'SLICE0_STREAM_PREFIX '
const SENTINEL = 'SLICE0_FINAL_SENTINEL'
const FIXTURE_CONTENT = 'SLICE0_FIXTURE_CONTENT'

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), ms).unref()),
  ])
}

function unwrap(result) {
  if (result?.ok === true) return result.value
  const error = result?.error ?? result
  throw new Error(`Remote call failed: ${error?.code ?? ''} ${error?.message ?? String(error)} ${JSON.stringify(error?.details ?? {})}`)
}

test('keyless slice0: external operator, duplicate prompts, two browser visits, real tool evidence', { timeout: 240_000 }, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice0-'))
  const cwd = join(root, 'workspace')
  const dshHome = join(root, 'home')
  const overlayPath = join(root, 'overlay.yml')
  const fixturePath = join(cwd, 'fixture.txt')
  const question = Promise.withResolvers()
  const answerGate = Promise.withResolvers()
  const pageErrors = []
  const consoleMessages = []
  let mock
  let upstream
  let web
  let operator
  let browser

  after(async () => {
    answerGate.resolve()
    mock?.gates.releaseStep1.resolve()
    mock?.gates.releaseStep2.resolve()
    mock?.gates.releaseStep3.resolve()
    await browser?.close().catch(() => {})
    await operator?.close().catch(() => {})
    await web?.close().catch(() => {})
    await mock?.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  mock = await startScriptedMessagesServer()
  upstream = await loadUpstream()

  await mkdir(cwd, { recursive: true })
  await writeFile(fixturePath, `${FIXTURE_CONTENT}\n`)
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

  web = await startDshWeb({ root, cwd, dshHome, overlayPath })
  t.diagnostic(`cold start ${web.startupMs} ms`)

  operator = await connectOperator({ upstream, launchUrl: web.launchUrl })
  const remote = operator.ctx.remote

  const sessionId = `session-${randomUUID()}`
  const requestId = `request-${randomUUID()}`
  const title = `slice0-${randomUUID()}`

  await operator.ready()
  operator.ctx.typert.contexts.registerClient('agent', {
    identity: ctx => ctx === operator.ctx ? sessionId : undefined,
    resolve: id => id === sessionId ? operator.ctx : undefined,
  })
  remote.$on('user-questions/request', async (request) => {
    question.resolve(request)
    await answerGate.promise
    return { answers: [{ id: 'q1', selected: ['Yes (Recommended)'] }] }
  })

  const workspace = unwrap(await remote.workspace.create({ path: cwd })).workspace
  const created = unwrap(await remote.session.create({ workspaceId: workspace.workspaceId, sessionId }))
  assert.equal(created.sessionId, sessionId, 'create must keep the minted session identity')
  assert.equal(unwrap(await remote.session.rename({ sessionId, title })).title, title)

  const catalog = unwrap(await remote.session.modelCatalog())
  const group = catalog.groups.find(candidate => candidate.id === 'deepseek-official')
  assert.ok(group !== undefined && group.models.length > 0, 'deepseek-official must advertise a model')
  const selected = { provider: group.id, model: group.models[0].id }
  assert.deepEqual(unwrap(await remote.session.selectModel({ sessionId, ...selected })).selected, selected)

  const before = unwrap(await remote.session.state({ sessionId }))
  assert.equal(before.active, true)
  assert.equal(before.running, false)

  const prompt = async () => unwrap(await remote.session.prompt({
    requestId,
    sessionId,
    mode: 'queue',
    content: [{ type: 'text', text: PROMPT }],
  }))

  assert.deepEqual(await prompt(), { accepted: true })
  await withTimeout(mock.gates.step1Entered.promise, 30_000, 'step 1 request')
  assert.deepEqual(await prompt(), { accepted: true }, 'in-flight duplicate requestId must be accepted')
  mock.gates.releaseStep1.resolve()

  await withTimeout(mock.gates.step2Entered.promise, 60_000, 'step 2 request')
  assert.deepEqual(await prompt(), { accepted: true }, 'post-persistence duplicate requestId must be accepted')
  mock.gates.releaseStep2.resolve()

  const waiting = unwrap(await remote.session.wait({ sessionId, afterSeq: before.cursor, timeoutMs: 60_000 }))
  assert.equal(waiting.reason, 'input-required')
  assert.ok(waiting.state.pendingInput.includes('question'))

  await withTimeout(question.promise, 30_000, 'question event')
  browser = await upstream.chromium.launch()
  let page = await openSessionInBrowser({
    browser, launchUrl: web.launchUrl, sessionId, title, workspaceTitle: workspace.title, pageErrors, consoleMessages,
  })
  await page.getByText(PROMPT, { exact: true }).first().waitFor({ state: 'visible', timeout: 30_000 })
  assert.equal(await page.getByText(PROMPT, { exact: true }).count(), 1, 'original prompt appears once')
  const process = page.locator('[data-turn-process][aria-expanded="false"]')
  const processDeadline = Date.now() + 10_000
  while (await process.count() === 0 && Date.now() < processDeadline) {
    await new Promise(resolve => setTimeout(resolve, 200))
  }
  for (let index = 0; index < await process.count(); index += 1) await process.nth(index).click()
  const readRow = page.locator('[data-tool="read"]').first()
  await readRow.waitFor({ state: 'visible', timeout: 15_000 })
  await readRow.click({ position: { x: 8, y: 8 } })
  const fixture = page.getByText(FIXTURE_CONTENT, { exact: false }).first()
  await fixture.waitFor({ state: 'visible', timeout: 30_000 })
  await page.getByText('Proceed with slice0?', { exact: true }).first().waitFor({ state: 'visible', timeout: 30_000 })
  await assertNoWriterHeldInPage(page, consoleMessages, 'browser visit 1')
  await page.close()
  await browser.close()
  browser = undefined

  answerGate.resolve()
  await withTimeout(mock.gates.step3Streaming.promise, 60_000, 'step 3 streaming')

  browser = await upstream.chromium.launch()
  page = await openSessionInBrowser({
    browser, launchUrl: web.launchUrl, sessionId, title, workspaceTitle: workspace.title, pageErrors, consoleMessages,
  })
  const streaming = page.locator('[data-streaming="true"]').filter({ hasText: PREFIX })
  await streaming.first().waitFor({ state: 'visible', timeout: 30_000 })
  await assertNoWriterHeldInPage(page, consoleMessages, 'browser visit 2')
  await page.close()
  await browser.close()
  browser = undefined

  mock.gates.releaseStep3.resolve()
  const idle = unwrap(await remote.session.wait({ sessionId, afterSeq: before.cursor, timeoutMs: 60_000 }))
  assert.equal(idle.reason, 'idle')
  assert.deepEqual(pageErrors, [], 'both browser visits must produce no page errors')
  assertNoWriterHeld({ consoleMessages, uiTexts: [], label: 'both browser visits' })
  assert.doesNotMatch(web.output(), /session\/writer-held|writer[- ]held/iu, 'Host logs must not report writer contention')

  assert.equal(mock.requests.length, 3, 'exactly three scripted model requests')
  assert.match(mock.requests[0].url, /^\/v1\/messages$/u)
  assert.equal(mock.requests[0].headers['x-api-key'], 'dsh-mock-key')
  assert.match(JSON.stringify(mock.requests[1].body), /SLICE0_FIXTURE_CONTENT/u)
  assert.match(JSON.stringify(mock.requests[2].body), /Yes \(Recommended\)/u)

  const pageResult = unwrap(await remote.session.events({ address: { kind: 'session', sessionId } }))
  const events = pageResult.records.map(record => record.event)
  assert.deepEqual(events.map(event => event.seq), Array.from({ length: pageResult.cursor + 1 }, (_, index) => index))

  const userMessages = events.filter(event => event.type === 'user/message' && event.data.source.rpcId === requestId)
  assert.equal(userMessages.length, 1, 'dedup must persist exactly one user message')

  const turnStart = events.filter(event => event.type === 'turn/start').at(-1)
  assert.ok(turnStart !== undefined, 'the prompt turn must start')
  const turn = turnStart.data.turn
  const turnEnd = events.find(event =>
    event.type === 'turn/end' && event.data.turn === turn && event.data.reason.kind === 'completed')
  assert.ok(turnEnd !== undefined, 'the prompt turn must end completed')
  assert.ok(turnEnd.seq > turnStart.seq)
  const inTurn = events.filter(event => event.seq >= turnStart.seq && event.seq <= turnEnd.seq)

  const userMessage = inTurn.find(event => event.type === 'user/message' && event.data.source.rpcId === requestId)
  assert.ok(userMessage !== undefined)
  assert.equal(userMessage.data.source.kind, 'user')

  const readCall = inTurn.find(event => event.type === 'tool/call' && event.data.name === 'read')
  assert.ok(readCall !== undefined, 'real read tool call persisted in its turn')
  const readResult = inTurn.find(event => event.type === 'tool/result'
    && event.data.turn === readCall.data.turn
    && event.data.step === readCall.data.step
    && event.data.message.toolCallId === readCall.data.callId)
  assert.ok(readResult !== undefined, 'read result must pair with the read call')

  const header = inTurn.find(event => event.type === 'request/header' && event.seq > userMessage.seq && event.seq < readCall.seq)
  assert.ok(header !== undefined, 'the first step records its header after the user message and before dispatch')
  assert.equal(header.data.header.config.provider, selected.provider)
  assert.equal(header.data.header.config.model, selected.model)

  const askCall = inTurn.find(event => event.type === 'tool/call'
    && event.data.name === 'ask_user_question' && event.seq > readResult.seq)
  assert.ok(askCall !== undefined, 'question tool call follows the read result')
  const askResult = inTurn.find(event => event.type === 'tool/result'
    && event.data.turn === askCall.data.turn
    && event.data.step === askCall.data.step
    && event.data.message.toolCallId === askCall.data.callId)
  assert.ok(askResult !== undefined, 'question result must pair with the question call')

  const finalAssistant = inTurn.find(event => event.type === 'assistant/message'
    && event.data.turn === turn
    && JSON.stringify(event.data).includes(SENTINEL))
  assert.ok(finalAssistant !== undefined, 'final assistant sentinel persists in its turn')
  assert.ok(finalAssistant.seq > askResult.seq)

  // Documented closure order: turn/start -> user/message -> request/header -> read call/result
  // -> question call/result -> final assistant/message -> matching turn/end(completed).
  const order = [
    turnStart.seq,
    userMessage.seq,
    header.seq,
    readCall.seq,
    readResult.seq,
    askCall.seq,
    askResult.seq,
    finalAssistant.seq,
    turnEnd.seq,
  ]
  assert.deepEqual([...order].sort((left, right) => left - right), order, 'turn events must follow documented relative order')
})

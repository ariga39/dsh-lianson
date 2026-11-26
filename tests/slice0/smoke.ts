import { strict as assert } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { access, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { basename, join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { brandString } from '@deepseek-ai/dsh-brand'
import type {
  SessionEventEntry,
  SessionRequestId,
  SessionState,
  SessionWireEvent,
} from '@deepseek-ai/dsh-api-session-controller/types'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import type { RemoteResult } from '@deepseek-ai/dsh-typert-protocol'
import { chromium, type Browser, type Page } from 'playwright'
import {
  MOCK_MODEL,
  MOCK_PROVIDER,
  QUESTION_ID,
  QUESTION_OPTION,
  READ_CALL_ID,
} from './constants.ts'
import { failureWithHostDiagnostics, launchTokenOf } from './failure.ts'
import { startDshWeb, type RunningDshWeb } from './host.ts'
import { connectOperator, type Operator } from './operator.ts'

/** Evidence returned by the complete Slice 0 run. */
export interface Slice0Report {
  readonly coldStartMs: number
  readonly duplicateInFlightAccepted: boolean
  readonly duplicateAfterStepAccepted: boolean
  readonly userMessages: number
  readonly contiguousEvents: boolean
  readonly turnEndedCompleted: boolean
  readonly pageErrors: number
  readonly writerHeldErrors: number
  readonly modelProvider: string
  readonly modelId: string
  readonly finalCursor: number
  readonly eventCount: number
  readonly consoleErrors: readonly string[]
}

const UI_WAIT_MS = 30_000
const CONTROL_WAIT_MS = 30_000

/**
 * Run the complete issue-1 Slice 0 vertical slice.
 * @returns typed evidence for the test's assertions.
 */
export async function runSlice0(): Promise<Slice0Report> {
  const projectRoot = fileURLToPath(new URL('../..', import.meta.url))
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-slice0-'))
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const agentsHome = join(root, 'agents-home')
  const bundledSkillDir = join(root, 'bundled-skills')
  const controlDir = join(root, 'control')
  const patchPath = join(root, 'slice0.patch.json')
  const sentinelPath = join(workspace, 'sentinel.txt')
  const nonce = randomUUID()
  const sentinel = `SLICE0_READ_OK ${nonce}`
  const title = `Slice 0 external operator ${nonce}`
  const promptText = `Run the Slice 0 external operator scenario ${nonce}`
  const pageErrors: string[] = []
  const consoleErrors: string[] = []
  let writerHeldErrors = 0
  let host: RunningDshWeb | undefined
  let operator: Operator | undefined
  let sessionIdForDiagnostics: SessionId | undefined

  try {
    await mkdir(workspace, { recursive: true })
    await mkdir(controlDir, { recursive: true })
    await writeFile(sentinelPath, `${sentinel}\n`)
    await writeFile(patchPath, JSON.stringify([
      { id: 'llm-deepseek', disabled: true },
      { id: 'llm-deepseek-account', disabled: true },
      { id: 'llm-pi-ai', disabled: true },
      { id: 'session-title-llm', disabled: true },
      { id: 'ui-settings-general', config: { welcomeNoticeVersion: '2026-09-28.1' } },
      { id: 'agent-default-model', config: { provider: MOCK_PROVIDER, model: MOCK_MODEL } },
      {
        insert: [{
          id: 'slice0-mock-provider',
          name: fileURLToPath(new URL('./mock-provider.ts', import.meta.url)),
          config: { controlDir, readPath: sentinelPath, nonce },
        }],
      },
    ], null, 2))

    host = await startDshWeb({
      projectRoot,
      workspace,
      dshHome,
      agentsHome,
      bundledSkillDir,
      patchPath,
    })
    operator = await connectOperator(host.launchUrl)

    const createdWorkspace = unwrap(await operator.remote.workspace.create({ path: workspace }))
    const workspaceId = createdWorkspace.workspace.workspaceId
    const sessionId = brandString<SessionId>(`session-${nonce}`)
    sessionIdForDiagnostics = sessionId
    operator.bindSession(sessionId)
    const created = unwrap(await operator.remote.session.create({
      workspaceId,
      sessionId,
      agentPreset: 'standard',
    }))
    assert.equal(created.sessionId, sessionId, 'Host must return the requested minted sessionId')
    const renamed = unwrap(await operator.remote.session.rename({ sessionId, title }))
    assert.equal(renamed.title, title)

    const catalog = unwrap(await operator.remote.session.modelCatalog())
    const route = catalog.groups.find(group => group.id === MOCK_PROVIDER)?.models.find(model => model.id === MOCK_MODEL)
    assert.ok(route, 'scripted model must appear in the Host model catalog')
    const selected = unwrap(await operator.remote.session.selectModel({
      sessionId,
      provider: MOCK_PROVIDER,
      model: MOCK_MODEL,
    }))
    assert.equal(selected.selected.provider, MOCK_PROVIDER)
    assert.equal(selected.selected.model, MOCK_MODEL)

    const beforeCursor = unwrap(await operator.remote.session.state({ sessionId })).cursor
    const requestId = brandString<SessionRequestId>(`request-${nonce}`)
    const first = unwrap(await operator.remote.session.prompt({
      requestId,
      sessionId,
      mode: 'queue',
      content: [{ type: 'text', text: promptText }],
    }))
    assert.equal(first.accepted, true)

    await waitForFile(join(controlDir, 'step1-started'), CONTROL_WAIT_MS)
    const duplicateInFlight = unwrap(await operator.remote.session.prompt({
      requestId,
      sessionId,
      mode: 'queue',
      content: [{ type: 'text', text: promptText }],
    }))
    assert.equal(duplicateInFlight.accepted, true)
    await writeFile(join(controlDir, 'step1-release'), 'go\n')

    const readResultEntry = await waitForEvent(operator, sessionId, (event) =>
      event.event.type === 'tool/result' && toolCallIdOf(event.event) === READ_CALL_ID, CONTROL_WAIT_MS)
    assert.match(JSON.stringify(readResultEntry.event.data), new RegExp(escapeRegExp(sentinel), 'u'))

    const duplicateAfterStep = unwrap(await operator.remote.session.prompt({
      requestId,
      sessionId,
      mode: 'queue',
      content: [{ type: 'text', text: promptText }],
    }))
    assert.equal(duplicateAfterStep.accepted, true)

    const pending = await waitForInputRequired(operator, sessionId, 30_000)
    assert.ok(pending.pendingInput.includes('question'), 'pending input must identify a question')
    const questionCallEntry = await waitForEvent(operator, sessionId, (event) =>
      event.event.type === 'tool/call' && stringField(event.event.data, 'name') === 'ask_user_question', CONTROL_WAIT_MS)
    const questionCallId = stringField(questionCallEntry.event.data, 'callId')
    assert.ok(questionCallId, 'pending question must carry its tool call id')

    const firstVisit = await visitSession(host.launchUrl, {
      title,
      workspace: basename(workspace),
      promptText,
      sentinel,
      sessionId,
      expectQuestion: true,
      expectStreaming: false,
    })
    pageErrors.push(...firstVisit.pageErrors)
    consoleErrors.push(...firstVisit.consoleErrors)
    writerHeldErrors += firstVisit.writerHeldErrors

    operator.answerQuestion({ answers: [{ id: QUESTION_ID, selected: [QUESTION_OPTION] }] })

    await waitForFile(join(controlDir, 'step3-started'), CONTROL_WAIT_MS)
    const secondVisit = await visitSession(host.launchUrl, {
      title,
      workspace: basename(workspace),
      promptText,
      sentinel,
      sessionId,
      expectQuestion: false,
      expectStreaming: true,
    })
    pageErrors.push(...secondVisit.pageErrors)
    consoleErrors.push(...secondVisit.consoleErrors)
    writerHeldErrors += secondVisit.writerHeldErrors
    await writeFile(join(controlDir, 'step3-release'), 'go\n')

    await waitForIdle(operator, sessionId, beforeCursor, 60_000)
    const page = unwrap(await operator.remote.session.events({ address: { kind: 'session', sessionId } }))
    const events = page.records.map(record => record.event)
    const seqs = events.map(event => event.seq)
    const expectedSeqs = Array.from({ length: page.cursor + 1 }, (_, index) => index)
    assert.deepEqual(seqs, expectedSeqs, 'event seq values must cover [0..cursor] contiguously')

    const promptMessages = events.filter((event) => {
      if (event.type !== 'user/message') return false
      const source = recordOf(recordOf(event.data)?.source)
      return source?.kind === 'user' && source.rpcId === requestId
    })
    assert.equal(promptMessages.length, 1, 'duplicate requestId must persist exactly one user message')

    const headers = events.filter(event => event.type === 'request/header')
    assert.ok(headers.length > 0, 'the turn must persist request headers')
    for (const header of headers) {
      const config = recordOf(recordOf(recordOf(header.data)?.header)?.config)
      assert.equal(config?.provider, MOCK_PROVIDER)
      assert.equal(config?.model, MOCK_MODEL)
    }

    const turnStarts = events.filter(event => event.type === 'turn/start')
    const turnEnds = events.filter(event => event.type === 'turn/end')
    assert.equal(turnStarts.length, 1)
    assert.equal(turnEnds.length, 1)
    const turnEnd = turnEnds[0]
    assert.ok(turnEnd, 'the turn must close')
    const endReason = recordOf(recordOf(turnEnd.data)?.reason)
    assert.equal(endReason?.kind, 'completed')

    const promptMessage = promptMessages[0]
    const readCall = events.find(event => event.type === 'tool/call'
      && stringField(event.data, 'callId') === READ_CALL_ID
      && stringField(event.data, 'name') === 'read')
    const readResult = events.find(event => event.type === 'tool/result' && toolCallIdOf(event) === READ_CALL_ID)
    const questionCall = events.find(event => event.type === 'tool/call'
      && stringField(event.data, 'name') === 'ask_user_question')
    const questionResult = events.find(event => event.type === 'tool/result' && toolCallIdOf(event) === questionCallId)
    const turnEndIndex = events.indexOf(turnEnd)
    const finalAssistant = events.slice(0, turnEndIndex)
      .filter(event => event.type === 'assistant/message')
      .at(-1)

    assert.ok(promptMessage, 'the turn must persist the prompt message')
    assert.ok(readCall, 'the turn must persist the read tool call')
    assert.ok(readResult, 'the turn must persist the read tool result')
    assert.ok(questionCall, 'the turn must persist the question tool call')
    assert.ok(questionResult, 'the turn must persist the question answer result')
    assert.ok(finalAssistant, 'the turn must persist a final assistant message')
    assert.match(JSON.stringify(finalAssistant.data), /SLICE0_FINAL_OK/u)
    assert.match(JSON.stringify(questionResult.data), new RegExp(escapeRegExp(QUESTION_OPTION), 'u'))

    const ordered = [turnStarts[0], promptMessage, readCall, readResult, questionCall, questionResult, finalAssistant, turnEnd]
    for (let index = 1; index < ordered.length; index += 1) {
      const previous = ordered[index - 1]
      const current = ordered[index]
      assert.ok(previous && current, 'the expected event ordering must be complete')
      assert.ok(previous.seq < current.seq, `expected ${previous.type} before ${current.type}`)
    }

    const currentBody = firstVisit.body + secondVisit.body
    assert.ok(!currentBody.includes('session/writer-held'), 'browser body must not report session/writer-held')

    const runningHost = host
    const stopStarted = Date.now()
    await runningHost.stop()
    assert.ok(Date.now() - stopStarted < 15_000, 'Host cleanup must remain bounded')
    const hostPid = runningHost.pid
    if (hostPid !== undefined) {
      assert.throws(() => { process.kill(hostPid, 0) }, /ESRCH/u, 'Host process must exit')
    }
    assert.match(runningHost.stdout(), /(?:^|\n)dsh web: https?:\/\/\S+/u, 'Host must print a complete launch line')
    host = undefined
    await operator.close()
    operator = undefined
    const hostStderr = runningHost.stderr()
    writerHeldErrors += occurrences(hostStderr, 'session/writer-held')
    assert.ok(!hostStderr.includes('session/writer-held'), 'Host stderr must not report session/writer-held')

    return {
      coldStartMs: runningHost.coldStartMs,
      duplicateInFlightAccepted: duplicateInFlight.accepted,
      duplicateAfterStepAccepted: duplicateAfterStep.accepted,
      userMessages: promptMessages.length,
      contiguousEvents: seqs.length === page.cursor + 1 && seqs.every((seq, index) => seq === index),
      turnEndedCompleted: endReason.kind === 'completed',
      pageErrors: pageErrors.length,
      writerHeldErrors,
      modelProvider: selected.selected.provider,
      modelId: selected.selected.model,
      finalCursor: page.cursor,
      eventCount: events.length,
      consoleErrors,
    }
  } catch (error) {
    const stateDetails = operator !== undefined && sessionIdForDiagnostics !== undefined
      ? await describeState(operator, sessionIdForDiagnostics).catch(() => '')
      : ''
    const secret = launchTokenOf(host?.launchUrl)
    if (operator !== undefined) {
      await operator.close().catch(() => undefined)
      operator = undefined
    }
    throw await failureWithHostDiagnostics({
      error,
      ...host === undefined ? {} : { host },
      ...secret === undefined ? {} : { secret },
      ...stateDetails === '' ? {} : { extra: stateDetails },
    })
  } finally {
    await operator?.close().catch(() => undefined)
    await host?.stop().catch(() => undefined)
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
}

interface VisitExpectation {
  readonly title: string
  readonly workspace: string
  readonly promptText: string
  readonly sentinel: string
  readonly sessionId: SessionId
  readonly expectQuestion: boolean
  readonly expectStreaming: boolean
}

interface VisitEvidence {
  readonly pageErrors: readonly string[]
  readonly consoleErrors: readonly string[]
  readonly writerHeldErrors: number
  readonly body: string
}

async function visitSession(launchUrl: string, expectation: VisitExpectation): Promise<VisitEvidence> {
  const browser: Browser = await chromium.launch({ headless: true })
  const page: Page = await browser.newPage({
    viewport: { width: 1680, height: 1000 },
    locale: 'en-US',
  })
  const pageErrors: string[] = []
  const consoleErrors: string[] = []
  const consoleTexts: string[] = []
  await page.addInitScript(() => {
    const watch = (): void => {
      try {
        if (!document.body?.innerText.includes('session/writer-held')) return
        const seen = Number(localStorage.getItem('dsh.test.writerHeld') ?? '0')
        localStorage.setItem('dsh.test.writerHeld', String(seen + 1))
      } catch {
        // Storage can be unavailable before the origin is established.
      }
    }
    new MutationObserver(watch).observe(document, { subtree: true, childList: true, characterData: true })
    document.addEventListener('DOMContentLoaded', watch)
  })
  page.on('pageerror', (error) => { pageErrors.push(error.message) })
  page.on('console', (message) => {
    consoleTexts.push(message.text())
    if (message.type() === 'error') consoleErrors.push(message.text())
  })
  try {
    await page.goto(launchUrl, { waitUntil: 'domcontentloaded' })
    await page.locator('[class*="frame"]').first().waitFor({ timeout: UI_WAIT_MS })
    await openSession(page, expectation)
    await page.getByText(expectation.promptText, { exact: false }).first().waitFor({ timeout: UI_WAIT_MS })
    const readRow = page.locator('[data-tool="read"]').first()
    await readRow.waitFor({ timeout: UI_WAIT_MS })
    const sentinelText = page.getByText(expectation.sentinel, { exact: false }).first()
    if (!(await sentinelText.isVisible().catch(() => false))) await readRow.click()
    await sentinelText.waitFor({ timeout: UI_WAIT_MS })
    if (expectation.expectQuestion) {
      await page.locator('[data-question-key]').first().waitFor({ timeout: UI_WAIT_MS })
    }
    if (expectation.expectStreaming) {
      await page.locator('[data-streaming="true"]').first().waitFor({ timeout: UI_WAIT_MS })
    }
    const context = page.context()
    const cookies = await context.cookies()
    assert.ok(cookies.length > 0, 'the browser must hold its authenticated cookie')
    const stored = await page.evaluate(() => localStorage.getItem('dsh.sessions.current'))
    assert.ok(stored !== null, 'the page must persist its current session selection')
    const parsed: unknown = JSON.parse(stored)
    assert.equal(recordOf(parsed)?.sessionId, expectation.sessionId,
      'the page must select the externally minted session id')
    const body = await page.locator('body').innerText()
    const transientNotices = await page.evaluate(() => Number(localStorage.getItem('dsh.test.writerHeld') ?? '0'))
    const observedText = [body, ...pageErrors, ...consoleTexts].join('\n')
    return {
      pageErrors,
      consoleErrors,
      writerHeldErrors: occurrences(observedText, 'session/writer-held')
        + (Number.isFinite(transientNotices) ? transientNotices : 0),
      body,
    }
  } finally {
    await page.close()
    await browser.close()
  }
}

async function openSession(page: Page, expectation: VisitExpectation): Promise<void> {
  const sessionRow = page.getByRole('treeitem', { name: new RegExp(escapeRegExp(expectation.title), 'u') }).first()
  const workspaceRow = page.getByRole('treeitem', { name: new RegExp(escapeRegExp(expectation.workspace), 'u') }).first()
  const deadline = Date.now() + UI_WAIT_MS
  while (Date.now() < deadline) {
    if (await sessionRow.isVisible().catch(() => false)) break
    await workspaceRow.waitFor({ state: 'visible', timeout: UI_WAIT_MS })
    if (await workspaceRow.getAttribute('aria-expanded') !== 'true') await workspaceRow.click()
    await delay(250)
  }
  if (!(await sessionRow.isVisible().catch(() => false))) {
    const tree = page.locator('[role="treeitem"]')
    const rows: Array<{ readonly name: string; readonly expanded: string | null }> = []
    const count = await tree.count()
    for (let index = 0; index < count; index += 1) {
      const row = tree.nth(index)
      rows.push({
        name: (await row.innerText()).trim().slice(0, 80),
        expanded: await row.getAttribute('aria-expanded'),
      })
    }
    throw new Error(`the scripted Session is not visible in the workspace tree: ${JSON.stringify(rows)}`)
  }
  await sessionRow.click()
}

async function describeState(operator: Operator, sessionId: SessionId): Promise<string> {
  const state = unwrap(await operator.remote.session.state({ sessionId }))
  const page = unwrap(await operator.remote.session.events({ address: { kind: 'session', sessionId } }))
  const recent = page.records.slice(-40).map(record => `${String(record.event.seq)}:${record.event.type}`).join(', ')
  const pending = state.pendingInput.length === 0 ? 'none' : state.pendingInput.join(',')
  return `\nSession status: active=${String(state.active)} running=${String(state.running)}`
    + ` cursor=${String(state.cursor)} queue=${String(state.queue?.length ?? 0)} pending=${pending}`
    + `\nRecent event types: ${recent}`
}

async function waitForInputRequired(operator: Operator, sessionId: SessionId, timeoutMs: number): Promise<SessionState> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const wait = unwrap(await operator.remote.session.wait({ sessionId, timeoutMs: 5_000 }))
    if (wait.reason === 'input-required') return wait.state
    if (wait.reason === 'idle') await delay(100)
  }
  throw new Error('timed out waiting for the scripted pending question')
}

async function waitForIdle(operator: Operator, sessionId: SessionId, afterSeq: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const wait = unwrap(await operator.remote.session.wait({ sessionId, afterSeq, timeoutMs: 5_000 }))
    if (wait.reason === 'idle') return
  }
  throw new Error('timed out waiting for the scripted turn to become idle')
}

async function waitForEvent(
  operator: Operator,
  sessionId: SessionId,
  predicate: (record: SessionEventEntry) => boolean,
  timeoutMs: number,
): Promise<SessionEventEntry> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const page = unwrap(await operator.remote.session.events({ address: { kind: 'session', sessionId } }))
    const match = page.records.find(predicate)
    if (match !== undefined) return match
    await delay(50)
  }
  throw new Error('timed out waiting for a scripted Session event')
}

async function waitForFile(path: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      await access(path)
      return
    } catch {
      await delay(25)
    }
  }
  throw new Error(`timed out waiting for ${basename(path)}`)
}

function unwrap<T>(result: RemoteResult<T>): T {
  if (!result.ok) throw new Error(`${result.error.code}: ${result.error.message}`)
  return result.value
}

function toolCallIdOf(event: SessionWireEvent): string | undefined {
  return stringField(recordOf(event.data)?.message, 'toolCallId')
}

function stringField(value: unknown, key: string): string | undefined {
  const field = recordOf(value)?.[key]
  return typeof field === 'string' ? field : undefined
}

function recordOf(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

function occurrences(text: string, needle: string): number {
  return text.split(needle).length - 1
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')
}

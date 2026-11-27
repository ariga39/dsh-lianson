/**
 * Real Host integration: one `dsh web --no-open --patch` process drives the
 * ordinary liaison CLI and a real Playwright page. Scenarios reuse the Host;
 * only the abrupt-exit recovery starts a second one.
 */

import assert from 'node:assert/strict'
import { readFile, writeFile, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { test } from 'node:test'
import { openSessionInBrowser } from './support/browser.mjs'
import { startDshWeb } from './support/dsh-web.mjs'
import { expectSocketInUse, listeningTcpPorts, socketModes, waitFor } from './support/host-checks.mjs'
import { runLiaison, waitForPlugin } from './support/liaison.mjs'
import { createScratch, createWorktrees } from './support/scratch.mjs'
import { startScriptedModel } from './support/scripted-model.mjs'
import { loadUpstream, REPO_ROOT } from './support/upstream.mjs'

const NOW = Date.now().toString(36)
const TITLES = {
  main: `integration main ${NOW}`,
  approval: `integration interaction ${NOW}`,
  noBrowser: `integration no-browser ${NOW}`,
  controls: `integration controls ${NOW}`,
  queue: `integration queue ${NOW}`,
  stop: `integration stop ${NOW}`,
  rework: `integration rework ${NOW}`,
}

function deferred() {
  const value = Promise.withResolvers()
  return { promise: value.promise, resolve: value.resolve, reject: value.reject }
}

function latestMarker(text, markers) {
  let selected
  let index = -1
  for (const marker of markers) {
    const found = text.lastIndexOf(marker)
    if (found > index) { selected = marker; index = found }
  }
  return selected
}

test('real dsh web, liaison CLI, and dsh Web integration', { timeout: 900_000 }, async (t) => {
  const scratch = await createScratch()
  const { worktrees } = await createWorktrees(scratch.root)
  const model = await startScriptedModel()
  const dshHome = join(scratch.root, 'dsh-home')
  const runtimeDir = join(scratch.root, 'runtime')
  const overlay = join(scratch.root, 'overlay.yml')
  const env = {
    DSH_HOME: dshHome,
    DSH_LIAISON_RUNTIME_DIR: runtimeDir,
    ...scratch.env,
  }
  await writeFile(overlay, [
    '- id: llm-deepseek',
    '  config:',
    `    baseURL: '${model.baseURL}'`,
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
    '- insert:',
    '    - id: dsh-liaison',
    `      name: '${join(REPO_ROOT, 'src/plugin.ts')}'`,
    '',
  ].join('\n'))

  let host
  let upstream
  let browser
  const pageErrors = []
  const pendingReleases = []
  const startHost = async () => {
    host = await startDshWeb({
      root: scratch.root,
      cwd: worktrees[0],
      dshHome,
      runtimeDir,
      patches: [overlay],
      env: scratch.env,
    })
    await waitForPlugin({ env })
    await waitFor(async () => {
      const status = (await runLiaison({ args: ['status'], env })).value
      return status.loopback.state === 'connected'
    }, 30_000, 'loopback connected')
    return host
  }

  t.after(async () => {
    for (const release of pendingReleases) release.resolve()
    await browser?.close().catch(() => {})
    await host?.close().catch(() => {})
    await model.close().catch(() => {})
    await scratch.remove().catch(() => {})
  })

  await startHost()
  upstream = await loadUpstream()

  await t.test('plugin boot uses one private socket, no extra TCP port, and one owner', async () => {
    const status = (await runLiaison({ args: ['status'], env })).value
    assert.equal(status.plugin.name, 'dsh-liaison')
    assert.equal(status.loopback.state, 'connected')
    const modes = await socketModes(status.socketPath)
    assert.deepEqual(modes, { socket: '600', directory: '700' })
    const ports = await listeningTcpPorts(host.pid)
    const advertised = Number(new URL(host.launchUrl).port)
    assert.deepEqual(ports, [advertised], 'the Host listener is the only TCP port')
    const otherHome = join(scratch.root, 'second-home')
    await expectSocketInUse({ DSH_HOME: otherHome, DSH_LIAISON_RUNTIME_DIR: runtimeDir })
  })

  await t.test('create, prompt, wait, events, and verify-model on the actual Session', async () => {
    model.setHandler(async ({ text }) => text.includes('MAIN_TURN')
      ? { kind: 'text', text: 'MAIN_SENTINEL' }
      : { kind: 'text', text: 'UNEXPECTED' })
    const created = (await runLiaison({
      args: ['create', '--delegation-id', 'main', '--cwd', worktrees[0], '--title', TITLES.main],
      env,
    })).value
    assert.equal(created.replayed, false)
    assert.match(created.delegation.sessionId, /^session-/)

    const prompted = (await runLiaison({
      args: ['prompt', '--delegation-id', 'main', '--request-id', 'main-r1', '--text', 'MAIN_TURN'],
      env,
    })).value
    assert.equal(prompted.accepted, true)

    const waited = (await runLiaison({
      args: ['wait', '--delegation-id', 'main', '--timeout-ms', '30000'],
      env,
    })).value
    assert.equal(waited.reason, 'idle')
    assert.equal(waited.status, 'completed')
    assert.ok(waited.output.records.some(record => JSON.stringify(record).includes('MAIN_SENTINEL')))

    const events = (await runLiaison({ args: ['events', '--delegation-id', 'main'], env })).value
    const seqs = events.events.map(event => event.seq)
    assert.deepEqual(seqs, Array.from({ length: seqs.length }, (_, index) => index))
    assert.ok(events.events.some(event => event.type === 'assistant/message'
      && JSON.stringify(event.data).includes('MAIN_SENTINEL')))

    const verified = (await runLiaison({
      args: ['verify-model', '--delegation-id', 'main', '--request-id', 'main-r1'],
      env,
    })).value
    assert.equal(verified.verification, 'proven')
    assert.deepEqual(verified.observed, verified.expected)
  })

  await t.test('workspace-write stays inside two sibling git worktrees', async () => {
    const writes = new Map()
    model.setHandler(async ({ text }) => {
      const marker = latestMarker(text, ['WRITE_A', 'WRITE_B', 'ESCAPE_A'])
      if (marker === undefined) return { kind: 'text', text: 'UNEXPECTED' }
      const count = (writes.get(marker) ?? 0) + 1
      writes.set(marker, count)
      if (count > 1) return { kind: 'text', text: `${marker}_DONE` }
      if (marker === 'ESCAPE_A') {
        return {
          kind: 'tool',
          name: 'bash',
          input: {
            command: `printf leaked > ${JSON.stringify(join(worktrees[1], 'escape.txt'))}`,
            description: 'Try to write into the sibling worktree',
          },
        }
      }
      return {
        kind: 'tool',
        name: 'write',
        input: { file_path: 'owner.txt', content: `${marker}_CONTENT` },
      }
    })
    await runLiaison({ args: ['create', '--delegation-id', 'write-a', '--cwd', worktrees[0], '--title', `write a ${NOW}`], env })
    await runLiaison({ args: ['create', '--delegation-id', 'write-b', '--cwd', worktrees[1], '--title', `write b ${NOW}`], env })
    await runLiaison({ args: ['prompt', '--delegation-id', 'write-a', '--request-id', 'wa1', '--text', 'WRITE_A'], env })
    await runLiaison({ args: ['prompt', '--delegation-id', 'write-b', '--request-id', 'wb1', '--text', 'WRITE_B'], env })
    const [a, b] = await Promise.all([
      runLiaison({ args: ['wait', '--delegation-id', 'write-a', '--timeout-ms', '30000'], env }),
      runLiaison({ args: ['wait', '--delegation-id', 'write-b', '--timeout-ms', '30000'], env }),
    ])
    assert.equal(a.value.status, 'completed')
    assert.equal(b.value.status, 'completed')
    assert.equal(await readFile(join(worktrees[0], 'owner.txt'), 'utf8'), 'WRITE_A_CONTENT')
    assert.equal(await readFile(join(worktrees[1], 'owner.txt'), 'utf8'), 'WRITE_B_CONTENT')
    await assert.rejects(stat(join(worktrees[1], 'escape.txt')), (error) => error?.code === 'ENOENT')

    await runLiaison({ args: ['prompt', '--delegation-id', 'write-a', '--request-id', 'wa2', '--text', 'ESCAPE_A'], env })
    const escaped = (await runLiaison({ args: ['wait', '--delegation-id', 'write-a', '--timeout-ms', '30000'], env })).value
    assert.equal(escaped.status, 'completed')
    await assert.rejects(stat(join(worktrees[1], 'escape.txt')), (error) => error?.code === 'ENOENT')
  })

  await t.test('dsh Web shows prior work and live work, and closes without stopping execution', async () => {
    const liveRelease = deferred()
    pendingReleases.push(liveRelease)
    model.setHandler(async ({ text }) => {
      const marker = latestMarker(text, ['PRIOR_TURN', 'LIVE_TURN', 'AFTER_CLOSE'])
      if (marker === 'PRIOR_TURN') return { kind: 'text', text: 'PRIOR_SENTINEL' }
      if (marker === 'LIVE_TURN') {
        return {
          kind: 'text',
          parts: [{ text: 'LIVE_PREFIX ' }, { text: 'LIVE_SUFFIX', gate: liveRelease.promise }],
        }
      }
      if (marker === 'AFTER_CLOSE') return { kind: 'text', text: 'AFTER_CLOSE_SENTINEL' }
      return { kind: 'text', text: 'UNEXPECTED' }
    })
    const created = (await runLiaison({ args: ['get', '--delegation-id', 'main'], env })).value.delegation
    await runLiaison({ args: ['prompt', '--delegation-id', 'main', '--request-id', 'prior', '--text', 'PRIOR_TURN'], env })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'main', '--timeout-ms', '30000'], env })).value.status, 'completed')

    await runLiaison({ args: ['prompt', '--delegation-id', 'main', '--request-id', 'live', '--text', 'LIVE_TURN'], env })
    browser = await upstream.chromium.launch()
    const page = await openSessionInBrowser({
      browser, launchUrl: host.launchUrl, sessionId: created.sessionId, title: TITLES.main, cwd: worktrees[0], pageErrors,
    })
    await page.getByText('PRIOR_SENTINEL', { exact: true }).first().waitFor({ state: 'visible', timeout: 30_000 })
    await page.getByText('LIVE_PREFIX', { exact: false }).first().waitFor({ state: 'visible', timeout: 30_000 })
    liveRelease.resolve()
    await page.getByText('LIVE_SUFFIX', { exact: false }).first().waitFor({ state: 'visible', timeout: 30_000 })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'main', '--timeout-ms', '30000'], env })).value.status, 'completed')

    await page.close()
    await browser.close()
    browser = undefined
    await runLiaison({ args: ['prompt', '--delegation-id', 'main', '--request-id', 'after-close', '--text', 'AFTER_CLOSE'], env })
    const after = (await runLiaison({ args: ['wait', '--delegation-id', 'main', '--timeout-ms', '30000'], env })).value
    assert.equal(after.status, 'completed')
    assert.ok(JSON.stringify(after.output).includes('AFTER_CLOSE_SENTINEL'))
    assert.deepEqual(pageErrors, [])
  })

  await t.test('approval and question settle through the CLI while dsh Web is open', async () => {
    const counts = new Map()
    model.setHandler(async ({ text }) => {
      const marker = latestMarker(text, ['APPROVAL_CLI', 'QUESTION_CLI', 'QUESTION_BROWSER'])
      if (marker === undefined) return { kind: 'text', text: 'UNEXPECTED' }
      {
        const count = (counts.get(marker) ?? 0) + 1
        counts.set(marker, count)
        if (count > 1) return { kind: 'text', text: `${marker}_DONE` }
        if (marker === 'APPROVAL_CLI') {
          return {
            kind: 'tool',
            name: 'bash',
            input: {
              command: `printf approved > ${JSON.stringify(join(scratch.root, 'approved-outside.txt'))}`,
              description: 'Write the approval marker outside the workspace',
              sandbox_permissions: 'danger-full-access',
              justification: 'The integration test must prove the approved escalation ran.',
            },
          }
        }
        return {
          kind: 'tool',
          name: 'ask_user_question',
          input: {
            questions: [{
              id: 'q1',
              header: 'Integration',
              question: 'Proceed with the integration check?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          },
        }
      }
      return { kind: 'text', text: 'UNEXPECTED' }
    })

    await runLiaison({ args: ['create', '--delegation-id', 'interaction', '--cwd', worktrees[0], '--title', TITLES.approval], env })
    await runLiaison({ args: ['prompt', '--delegation-id', 'interaction', '--request-id', 'approval', '--text', 'APPROVAL_CLI'], env })
    const waitingApproval = (await runLiaison({ args: ['wait', '--delegation-id', 'interaction', '--timeout-ms', '30000'], env })).value
    assert.equal(waitingApproval.reason, 'input-required')

    const mainSession = (await runLiaison({ args: ['get', '--delegation-id', 'interaction'], env })).value.delegation
    browser = await upstream.chromium.launch()
    let page = await openSessionInBrowser({
      browser, launchUrl: host.launchUrl, sessionId: mainSession.sessionId, title: TITLES.approval, cwd: worktrees[0], pageErrors,
    })
    const approvalPanel = page.locator('[data-approval-key]')
    await approvalPanel.waitFor({ state: 'visible', timeout: 30_000 })

    let list
    await waitFor(async () => {
      list = (await runLiaison({ args: ['interaction', 'list', '--delegation-id', 'interaction'], env })).value
      return list.interactions.length === 1
    }, 30_000, 'pending approval')
    assert.equal(list.interactions[0].kind, 'approval')
    await runLiaison({ args: ['interaction', 'approve', '--interaction-id', list.interactions[0].id], env })
    await approvalPanel.waitFor({ state: 'hidden', timeout: 30_000 })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'interaction', '--timeout-ms', '30000'], env })).value.status, 'completed')
    assert.equal(await readFile(join(scratch.root, 'approved-outside.txt'), 'utf8'), 'approved')

    await runLiaison({ args: ['prompt', '--delegation-id', 'interaction', '--request-id', 'question-cli', '--text', 'QUESTION_CLI'], env })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'interaction', '--timeout-ms', '30000'], env })).value.reason, 'input-required')
    await waitFor(async () => {
      list = (await runLiaison({ args: ['interaction', 'list', '--delegation-id', 'interaction'], env })).value
      return list.interactions.some(item => item.kind === 'question')
    }, 30_000, 'pending question')
    const cliQuestion = list.interactions.find(item => item.kind === 'question')
    const questionPanel = page.locator('[data-question-key]')
    await questionPanel.waitFor({ state: 'visible', timeout: 30_000 })
    await runLiaison({
      args: ['interaction', 'answer', '--interaction-id', cliQuestion.id, '--answers', JSON.stringify([{ id: 'q1', selected: ['Yes'] }])],
      env,
    })
    await questionPanel.waitFor({ state: 'hidden', timeout: 30_000 })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'interaction', '--timeout-ms', '30000'], env })).value.status, 'completed')

    await runLiaison({ args: ['prompt', '--delegation-id', 'interaction', '--request-id', 'question-browser', '--text', 'QUESTION_BROWSER'], env })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'interaction', '--timeout-ms', '30000'], env })).value.reason, 'input-required')
    await questionPanel.waitFor({ state: 'visible', timeout: 30_000 })
    await questionPanel.getByRole('radio', { name: 'Yes', exact: true }).click()
    await questionPanel.getByRole('button', { name: 'Submit', exact: true }).click()
    await waitFor(async () => {
      list = (await runLiaison({ args: ['interaction', 'list', '--delegation-id', 'interaction'], env })).value
      return list.interactions.length === 0
    }, 30_000, 'browser answer cancels the CLI prompt')
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'interaction', '--timeout-ms', '30000'], env })).value.status, 'completed')
    assert.match(model.requests.at(-1).text, /Yes/u)

    await page.close()
    await browser.close()
    browser = undefined
    assert.deepEqual(pageErrors, [])
  })

  await t.test('the same approval and question flow works with no browser ever opened', async () => {
    const counts = new Map()
    model.setHandler(async ({ text }) => {
      const marker = latestMarker(text, ['NO_BROWSER_APPROVAL', 'NO_BROWSER_QUESTION'])
      if (marker === undefined) return { kind: 'text', text: 'UNEXPECTED' }
      {
        const count = (counts.get(marker) ?? 0) + 1
        counts.set(marker, count)
        if (count > 1) return { kind: 'text', text: `${marker}_DONE` }
        if (marker === 'NO_BROWSER_APPROVAL') {
          return {
            kind: 'tool',
            name: 'bash',
            input: {
              command: `printf no-browser > ${JSON.stringify(join(scratch.root, 'no-browser-outside.txt'))}`,
              description: 'Write the no-browser approval marker',
              sandbox_permissions: 'danger-full-access',
              justification: 'The no-browser integration check needs this approved write.',
            },
          }
        }
        return {
          kind: 'tool',
          name: 'ask_user_question',
          input: {
            questions: [{
              id: 'q1', header: 'Integration', question: 'Continue without a browser?',
              options: [{ label: 'Yes' }, { label: 'No' }],
            }],
          },
        }
      }
      return { kind: 'text', text: 'UNEXPECTED' }
    })
    await runLiaison({ args: ['create', '--delegation-id', 'no-browser', '--cwd', worktrees[1], '--title', TITLES.noBrowser], env })
    await runLiaison({ args: ['prompt', '--delegation-id', 'no-browser', '--request-id', 'nba', '--text', 'NO_BROWSER_APPROVAL'], env })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'no-browser', '--timeout-ms', '30000'], env })).value.reason, 'input-required')
    let list
    await waitFor(async () => {
      list = (await runLiaison({ args: ['interaction', 'list', '--delegation-id', 'no-browser'], env })).value
      return list.interactions.length === 1
    }, 30_000, 'no-browser approval')
    await runLiaison({ args: ['interaction', 'approve', '--interaction-id', list.interactions[0].id], env })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'no-browser', '--timeout-ms', '30000'], env })).value.status, 'completed')

    await runLiaison({ args: ['prompt', '--delegation-id', 'no-browser', '--request-id', 'nbq', '--text', 'NO_BROWSER_QUESTION'], env })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'no-browser', '--timeout-ms', '30000'], env })).value.reason, 'input-required')
    await waitFor(async () => {
      list = (await runLiaison({ args: ['interaction', 'list', '--delegation-id', 'no-browser'], env })).value
      return list.interactions.some(item => item.kind === 'question')
    }, 30_000, 'no-browser question')
    const question = list.interactions.find(item => item.kind === 'question')
    await runLiaison({
      args: ['interaction', 'answer', '--interaction-id', question.id, '--answers', JSON.stringify([{ id: 'q1', selected: ['Yes'] }])],
      env,
    })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'no-browser', '--timeout-ms', '30000'], env })).value.status, 'completed')
  })

  await t.test('steer, queue, and bounded stop drive the original Session', async () => {
    const steerSecond = deferred()
    const steerRelease = deferred()
    let steerCount = 0
    const queueEntered = deferred()
    const queueRelease = deferred()
    let queueCount = 0
    const stopEntered = deferred()
    let stopCount = 0
    model.setHandler(async ({ text, signal }) => {
      if (text.includes('STEER_TARGET')) {
        steerCount += 1
        if (steerCount === 1) return { kind: 'tool', name: 'read', input: { file_path: 'README.md' } }
        steerSecond.resolve()
        await steerRelease.promise
        return { kind: 'text', text: text.includes('STEER_GUIDANCE') ? 'STEER_SEEN' : 'STEER_MISSING' }
      }
      if (text.includes('QUEUE_TARGET')) {
        queueCount += 1
        if (queueCount === 1) {
          queueEntered.resolve()
          await queueRelease.promise
          return { kind: 'text', text: 'QUEUE_FIRST_DONE' }
        }
        return { kind: 'text', text: text.includes('QUEUED_EDITED') ? 'QUEUE_EDITED_SEEN' : 'QUEUE_MISSING' }
      }
      if (text.includes('STOP_TARGET')) {
        stopCount += 1
        if (stopCount === 1) {
          stopEntered.resolve()
          return { kind: 'hold' }
        }
        return { kind: 'text', text: 'AFTER_STOP_SENTINEL' }
      }
      return { kind: 'text', text: 'UNEXPECTED' }
    })

    await runLiaison({ args: ['create', '--delegation-id', 'controls', '--cwd', worktrees[0], '--title', TITLES.controls], env })
    await runLiaison({ args: ['prompt', '--delegation-id', 'controls', '--request-id', 'steer-1', '--text', 'STEER_TARGET'], env })
    await steerSecond.promise
    const steered = (await runLiaison({ args: ['steer', '--delegation-id', 'controls', '--request-id', 'steer-guidance', '--text', 'STEER_GUIDANCE'], env })).value
    assert.equal(steered.accepted, true)
    steerRelease.resolve()
    const steeredWait = (await runLiaison({ args: ['wait', '--delegation-id', 'controls', '--timeout-ms', '30000'], env })).value
    assert.equal(steeredWait.status, 'completed')
    assert.ok(JSON.stringify(steeredWait.output).includes('STEER_SEEN'))

    await runLiaison({ args: ['create', '--delegation-id', 'queue', '--cwd', worktrees[1], '--title', TITLES.queue], env })
    await runLiaison({ args: ['prompt', '--delegation-id', 'queue', '--request-id', 'queue-1', '--text', 'QUEUE_TARGET'], env })
    await queueEntered.promise
    await runLiaison({ args: ['prompt', '--delegation-id', 'queue', '--request-id', 'queue-2', '--text', 'QUEUED_ORIGINAL'], env })
    const queued = (await runLiaison({ args: ['wait', '--delegation-id', 'queue', '--timeout-ms', '2000'], env })).value
    assert.equal(queued.state.running, true)
    assert.equal(queued.state.queue.length, 1)
    await runLiaison({
      args: ['queue', '--delegation-id', 'queue', '--item-id', queued.state.queue[0].id, '--action', 'edit', '--text', 'QUEUED_EDITED'],
      env,
    })
    queueRelease.resolve()
    const queueWait = (await runLiaison({ args: ['wait', '--delegation-id', 'queue', '--timeout-ms', '30000'], env })).value
    assert.equal(queueWait.status, 'completed')
    const queueEvents = (await runLiaison({ args: ['events', '--delegation-id', 'queue'], env })).value
    assert.ok(JSON.stringify(queueEvents).includes('QUEUED_EDITED'))
    assert.ok(JSON.stringify(queueEvents).includes('QUEUE_EDITED_SEEN'))
    assert.match(model.requests.at(-1).text, /QUEUED_EDITED/u)

    await runLiaison({ args: ['create', '--delegation-id', 'stop', '--cwd', worktrees[0], '--title', TITLES.stop], env })
    await runLiaison({ args: ['prompt', '--delegation-id', 'stop', '--request-id', 'stop-1', '--text', 'STOP_TARGET'], env })
    await stopEntered.promise
    const started = Date.now()
    const stopped = (await runLiaison({ args: ['stop', '--delegation-id', 'stop', '--timeout-ms', '15000'], env, timeoutMs: 25_000 })).value
    assert.ok(Date.now() - started < 20_000)
    assert.equal(stopped.budgetExpired, false)
    assert.equal(stopped.stopped, true)
    const stoppedWait = (await runLiaison({ args: ['wait', '--delegation-id', 'stop', '--timeout-ms', '15000'], env })).value
    assert.equal(stoppedWait.status, 'completed')
  })

  await t.test('rework keeps the original Session after an abrupt Host exit and stale-socket recovery', async () => {
    model.setHandler(async ({ text }) => {
      const marker = latestMarker(text, ['REWORK_ONE', 'REWORK_AFTER_RESTART'])
      if (marker === 'REWORK_ONE') return { kind: 'text', text: 'REWORK_ONE_DONE' }
      if (marker === 'REWORK_AFTER_RESTART') return { kind: 'text', text: 'REWORK_AFTER_RESTART_DONE' }
      return { kind: 'text', text: 'UNEXPECTED' }
    })
    const before = (await runLiaison({
      args: ['create', '--delegation-id', 'rework', '--cwd', worktrees[0], '--title', TITLES.rework],
      env,
    })).value.delegation
    await runLiaison({ args: ['prompt', '--delegation-id', 'rework', '--request-id', 'rework-1', '--text', 'REWORK_ONE'], env })
    assert.equal((await runLiaison({ args: ['wait', '--delegation-id', 'rework', '--timeout-ms', '30000'], env })).value.status, 'completed')

    const oldSocket = (await runLiaison({ args: ['status'], env })).value.socketPath
    await host.kill('SIGKILL')
    assert.equal((await stat(oldSocket)).isSocket(), true, 'an abrupt exit leaves the stale socket')
    await waitFor(async () => {
      try {
        const result = await runLiaison({ args: ['get', '--delegation-id', 'rework'], env })
        return result.value.delegation.sessionId === before.sessionId
      } catch {
        return false
      }
    }, 5_000, 'offline ledger read after abrupt exit')

    await startHost()
    const afterRestart = (await runLiaison({ args: ['get', '--delegation-id', 'rework'], env })).value.delegation
    assert.equal(afterRestart.sessionId, before.sessionId)
    const reworked = (await runLiaison({
      args: ['rework', '--delegation-id', 'rework', '--request-id', 'rework-2', '--text', 'REWORK_AFTER_RESTART'],
      env,
    })).value
    assert.equal(reworked.sessionId, before.sessionId)
    assert.equal(reworked.unarchived, true)
    const waited = (await runLiaison({ args: ['wait', '--delegation-id', 'rework', '--timeout-ms', '30000'], env })).value
    assert.equal(waited.status, 'completed')
    const events = (await runLiaison({ args: ['events', '--delegation-id', 'rework'], env })).value
    assert.ok(JSON.stringify(events).includes('REWORK_AFTER_RESTART_DONE'))
  })
})

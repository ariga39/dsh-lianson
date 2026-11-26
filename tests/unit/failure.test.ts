import { strict as assert } from 'node:assert'
import { spawnSync } from 'node:child_process'
import { test } from 'node:test'
import { inspect } from 'node:util'
import { fileURLToPath } from 'node:url'
import { failureWithHostDiagnostics, launchTokenOf } from '../slice0/failure.ts'

const SECRET = 'canary-launch-token-0123456789abcdef'
const canaryFixture = fileURLToPath(new URL('../fixtures/report-canary.ts', import.meta.url))

await test('a fresh failure keeps the redacted primary message and omits nested causes', async () => {
  const deep = new Error(`deep cause ${SECRET}`)
  const nested = new Error(`nested cause ?token=${SECRET}`, { cause: deep })
  const outer = new Error(`page.goto: net::ERR_CONNECTION_REFUSED at http://127.0.0.1/?token=${SECRET}`)
  outer.name = 'TimeoutError'
  outer.stack = `TimeoutError: original stack ?token=${SECRET}`
  outer.cause = nested
  Object.defineProperty(outer, 'url', {
    value: `http://127.0.0.1/?token=${SECRET}`,
    enumerable: true,
  })

  assert.equal(launchTokenOf(`http://127.0.0.1:9/?token=${SECRET}`), SECRET)
  const failure = await failureWithHostDiagnostics({ error: outer, secret: SECRET })
  const rendered = [failure.message, failure.stack ?? '', String(failure), inspect(failure, { depth: 10, colors: false })].join('\n')

  assert.match(failure.message, /TimeoutError: page\.goto/u)
  assert.match(failure.message, /<redacted>/u)
  assert.equal(failure.cause, undefined)
  assert.doesNotMatch(rendered, /nested cause|deep cause|original stack/u)
  assert.ok(!rendered.includes(SECRET))
})

await test('reaps the Host before reading shutdown-only stderr', async () => {
  const chunks: string[] = []
  let stopped = false
  const host = {
    async stop(): Promise<void> {
      stopped = true
      chunks.push(`shutdown-only ?token=${SECRET}\n`, `shutdown-only bare ${SECRET}\n`)
    },
    stderr(): string {
      return stopped ? chunks.join('') : ''
    },
  }

  const failure = await failureWithHostDiagnostics({
    error: new Error('navigation failed before the Host closed'),
    host,
    secret: SECRET,
  })
  assert.equal(stopped, true)
  assert.match(failure.message, /Host stderr:\nshutdown-only/u)
  assert.ok(!inspect(failure, { depth: 10, colors: false }).includes(SECRET))
})

await test('a cleanup failure preserves the original failure', async () => {
  let stopped = false
  const host = {
    async stop(): Promise<void> {
      stopped = true
      throw new Error(`cleanup failed ?token=${SECRET}`)
    },
    stderr(): string {
      return stopped ? `final stderr ?token=${SECRET}\n` : ''
    },
  }

  const failure = await failureWithHostDiagnostics({
    error: new Error(`original failure ?token=${SECRET}`),
    host,
    secret: SECRET,
  })
  assert.match(failure.message, /original failure/u)
  assert.match(failure.message, /Host stderr:\nfinal stderr/u)
  assert.doesNotMatch(failure.message, /cleanup failed/u)
  assert.ok(!inspect(failure, { depth: 10, colors: false }).includes(SECRET))
})

await test('node:test reporter shows the diagnostic tail and no launch token', () => {
  const env: NodeJS.ProcessEnv = { ...process.env, DSH_CANARY_TOKEN: SECRET }
  delete env.NODE_TEST_CONTEXT
  const result = spawnSync(process.execPath, ['--test', canaryFixture], {
    env,
    encoding: 'utf8',
  })
  const output = `${result.stdout}\n${result.stderr}`
  assert.notEqual(result.status, 0)
  assert.match(output, /reporter canary throws a sanitized navigation failure/u)
  assert.match(output, /reporter-final-stderr/u)
  assert.ok(!output.includes(SECRET), 'the node:test reporter must not print the launch token')
})

import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { startDshWeb } from './support/dsh-web-harness.mjs'
import { scrubTokenDiagnostics, tokenOfUrl } from './support/scrub.mjs'

test('token scrubber covers assertion, navigation and connection diagnostics', () => {
  const canary = 'CANARY_TOKEN_DO_NOT_PRINT_7f3c9a'
  const url = `http://127.0.0.1:4567/?token=${canary}`
  const samples = [
    `AssertionError: expected ${url} to be reachable`,
    `page.goto: net::ERR_CONNECTION_REFUSED navigating to "${url}"`,
    `fetch failed: GET http://127.0.0.1:4567/connect?token=${canary}&interactions=answer`,
    `raw diagnostic ${canary}`,
  ]
  for (const sample of samples) {
    const scrubbed = scrubTokenDiagnostics(sample, canary)
    assert.equal(scrubbed.includes(canary), false, `canary leaked from: ${sample}`)
    assert.match(scrubbed, /<redacted>/u)
  }
  assert.equal(tokenOfUrl(url), canary)
})

test('startup failure diagnostics never expose a synthetic launch canary', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh diagnostics-'))
  const canary = 'CANARY_STARTUP_FAILURE_9d21'
  try {
    const fakeBin = join(root, 'fake-cli.mjs')
    await writeFile(fakeBin, [
      `console.error('connect failed http://127.0.0.1:9/connect?token=${canary}&interactions=answer')`,
      `console.error('navigation failed http://127.0.0.1:9/?token=${canary}')`,
      'process.exit(1)',
      '',
    ].join('\n'))
    await assert.rejects(
      startDshWeb({
        root,
        cwd: root,
        dshHome: join(root, 'home'),
        overlayPath: join(root, 'overlay.yml'),
        bin: fakeBin,
      }),
      error => {
        assert.equal(error.message.includes(canary), false)
        assert.match(error.message, /<redacted>/u)
        return true
      },
    )
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

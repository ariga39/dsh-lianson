import { strict as assert } from 'node:assert'
import { createHash } from 'node:crypto'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import {
  DELEGATION_REGISTRY_ERROR_CODES,
  DelegationRegistry,
  DelegationRegistryError,
  ledgerPaths,
  pathsOverlap,
  resolveLedgerPath,
  type CreateDelegationInput,
  type DelegationRegistryErrorCode,
} from '../src/delegation-registry.ts'
import { acquireLock } from '../src/flock.ts'
import { writePrivateFile } from '../src/fs-safe.ts'

const TEST_PARENT = join(homedir(), '.cache', 'dsh-liaison-tests')
await mkdir(TEST_PARENT, { recursive: true, mode: 0o700 })

async function scratchRoot(label: string): Promise<string> {
  return await mkdtemp(join(TEST_PARENT, `slice2-registry-${label}-`))
}

function createInput(root: string, overrides: Partial<CreateDelegationInput> = {}): CreateDelegationInput {
  return {
    delegationId: 'del-1',
    sessionId: 'session-requested',
    title: 'Alpha',
    cwd: join(root, 'work'),
    expectedModel: { provider: 'provider-a', model: 'model-a' },
    createFingerprint: 'fingerprint-1',
    ...overrides,
  }
}

function contentHash(content: string): string {
  return createHash('sha256').update(content).digest('hex')
}

async function expectCode(promise: Promise<unknown>, code: DelegationRegistryErrorCode): Promise<DelegationRegistryError> {
  try {
    await promise
  } catch (error) {
    assert.ok(error instanceof DelegationRegistryError, `expected DelegationRegistryError, got ${String(error)}`)
    assert.equal(error.code, code)
    return error
  }
  assert.fail(`expected ${code}`)
}

async function readLedger(ledgerFile: string): Promise<Record<string, { state: string; sessionId: string; title: string }>> {
  const text = await readFile(ledgerFile, 'utf8')
  const parsed = JSON.parse(text) as {
    readonly delegations: Record<string, { readonly state: string; readonly sessionId: string; readonly title: string }>
  }
  return parsed.delegations
}

await test('ledger paths prefer durable state roots and pure overlap checks reject only real containment', () => {
  for (const code of [
    'DELEGATION_NOT_FOUND',
    'DELEGATION_ALREADY_EXISTS',
    'DELEGATION_REPLAY_CONFLICT',
    'DELEGATION_PROMPT_CONFLICT',
    'DELEGATION_LEDGER_READONLY',
    'DELEGATION_LEDGER_CLOSING',
    'DELEGATION_LEDGER_INVALID',
    'WORKSPACE_OVERLAP',
  ] as const) {
    assert.ok(DELEGATION_REGISTRY_ERROR_CODES.includes(code), `missing ${code}`)
  }

  const env = {
    DSH_LIAISON_LEDGER_PATH: '/srv/custom/delegations.json',
    XDG_STATE_HOME: '/srv/state',
    HOME: '/home/person',
    XDG_RUNTIME_DIR: '/run/user/1000',
  }
  assert.equal(resolveLedgerPath(env), resolve('/srv/custom/delegations.json'))
  assert.equal(
    resolveLedgerPath({ XDG_STATE_HOME: '/srv/state', HOME: '/home/person', XDG_RUNTIME_DIR: '/run/user/1000' }),
    join('/srv/state', 'dsh-liaison', 'delegations.json'),
  )
  const homeOnly = resolveLedgerPath({ HOME: '/home/person', XDG_RUNTIME_DIR: '/run/user/1000' })
  assert.equal(homeOnly, join('/home/person', '.local', 'state', 'dsh-liaison', 'delegations.json'))
  assert.ok(!homeOnly.includes('/run/user/1000'))

  assert.deepEqual(ledgerPaths('/srv/state/dsh-liaison/delegations.json'), {
    ledgerDir: '/srv/state/dsh-liaison',
    ledgerFile: '/srv/state/dsh-liaison/delegations.json',
    ledgerLock: '/srv/state/dsh-liaison/delegations.lock',
  })

  assert.equal(pathsOverlap('/srv/work', '/srv/work'), true)
  assert.equal(pathsOverlap('/srv/work', '/srv/work/nested'), true)
  assert.equal(pathsOverlap('/srv/work/nested', '/srv/work'), true)
  assert.equal(pathsOverlap('/srv/work', '/srv/work-other'), false)
  assert.equal(pathsOverlap('/srv/work', '/srv/other'), false)
})

await test('ledger is private and publishes to memory only after its atomic write succeeds', async () => {
  const root = await scratchRoot('publish')
  try {
    const ledgerFile = join(root, 'state', 'delegations.json')
    let writes = 0
    const registry = new DelegationRegistry({
      ledgerFile,
      writeFile: async (path, content, mode) => {
        writes += 1
        if (writes > 1) throw new Error('injected write failure')
        await writePrivateFile(path, content, mode)
      },
    })

    const created = await registry.beginCreate(createInput(root))
    assert.equal(created.replayed, false)
    assert.equal(created.record.state, 'creating')
    assert.equal((await stat(dirname(ledgerFile))).mode & 0o777, 0o700)
    assert.equal((await stat(ledgerFile)).mode & 0o777, 0o600)
    assert.equal((await readLedger(ledgerFile))['del-1']?.state, 'creating')

    await assert.rejects(
      registry.transition('del-1', { state: 'ready', workspaceId: 'ws-1' }),
      /injected write failure/u,
    )
    assert.equal((await registry.get('del-1')).state, 'creating')
    assert.equal((await readLedger(ledgerFile))['del-1']?.state, 'creating')
    assert.equal(writes, 2)
    await registry.close()

    const failingFile = join(root, 'failing', 'delegations.json')
    const failing = new DelegationRegistry({
      ledgerFile: failingFile,
      writeFile: async () => { throw new Error('write refused') },
    })
    await assert.rejects(
      failing.beginCreate(createInput(root, { delegationId: 'del-fail', title: 'Failure' })),
      /write refused/u,
    )
    assert.equal(existsSync(failingFile), false)
    assert.equal((await failing.list()).length, 0)
    await failing.close()
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('writer acquisition is single-flight, contention is read-only, and handoff reloads committed records', async () => {
  const root = await scratchRoot('writer')
  try {
    const ledgerFile = join(root, 'state', 'delegations.json')
    const registry = new DelegationRegistry({ ledgerFile, lockWaitMs: 25 })
    await Promise.all(Array.from({ length: 16 }, async () => registry.ensureWriter()))
    await registry.beginCreate(createInput(root))

    const contender = new DelegationRegistry({ ledgerFile, lockWaitMs: 25 })
    await expectCode(contender.ensureWriter(), 'DELEGATION_LEDGER_READONLY')

    const reader = new DelegationRegistry({ ledgerFile })
    assert.equal((await reader.get('del-1')).sessionId, 'session-requested')
    assert.equal((await reader.list()).length, 1)

    await registry.transition('del-1', { state: 'ready', workspaceId: 'ws-1' })
    await registry.close()

    const raw = await acquireLock(ledgerPaths(ledgerFile).ledgerLock)
    assert.notEqual(raw, undefined, 'released writer lock must be immediately acquirable')
    await raw?.release()

    const handoff = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
    await handoff.ensureWriter()
    assert.equal((await handoff.get('del-1')).state, 'ready')
    await handoff.close()

    const readOnly = new DelegationRegistry({ ledgerFile })
    assert.equal((await readOnly.list()).length, 1)
    const stillFree = await acquireLock(ledgerPaths(ledgerFile).ledgerLock)
    assert.notEqual(stillFree, undefined, 'read-only access must not acquire the writer lock')
    await stillFree?.release()
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('close fences new writes, drains an admitted write, releases the lock, and is idempotent', async () => {
  const root = await scratchRoot('close')
  try {
    const ledgerFile = join(root, 'state', 'delegations.json')
    let releaseWrite!: () => void
    let writeStarted!: () => void
    const gate = new Promise<void>((release) => { releaseWrite = release })
    const started = new Promise<void>((resolveStarted) => { writeStarted = resolveStarted })
    let writes = 0
    const registry = new DelegationRegistry({
      ledgerFile,
      writeFile: async (path, content, mode) => {
        writes += 1
        if (writes === 1) {
          writeStarted()
          await gate
        }
        await writePrivateFile(path, content, mode)
      },
    })

    const admitted = registry.beginCreate(createInput(root))
    await started
    const closing = registry.close()
    let closed = false
    void closing.then(() => { closed = true })
    await delay(20)
    assert.equal(closed, false, 'close must wait for the admitted write')

    await expectCode(
      registry.beginCreate(createInput(root, { delegationId: 'del-2', title: 'Beta', cwd: join(root, 'work-2') })),
      'DELEGATION_LEDGER_CLOSING',
    )
    await expectCode(registry.ensureWriter(), 'DELEGATION_LEDGER_CLOSING')

    releaseWrite()
    await admitted
    await closing
    assert.equal((await readLedger(ledgerFile))['del-1']?.state, 'creating')

    const reopened = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
    await reopened.ensureWriter()
    assert.equal((await reopened.get('del-1')).title, 'Alpha')
    await reopened.close()
    await reopened.close()
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('create replays reuse the persisted session while terminal states stay immutable', async () => {
  const root = await scratchRoot('transitions')
  try {
    const ledgerFile = join(root, 'state', 'delegations.json')
    const registry = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
    try {
      const first = await registry.beginCreate(createInput(root))
      assert.equal(first.replayed, false)

      const replay = await registry.beginCreate(createInput(root, { sessionId: 'session-substitute' }))
      assert.equal(replay.replayed, true)
      assert.equal(replay.record.sessionId, 'session-requested')
      await expectCode(
        registry.beginCreate(createInput(root, { createFingerprint: 'fingerprint-2' })),
        'DELEGATION_REPLAY_CONFLICT',
      )

      await registry.transition('del-1', { state: 'ready', workspaceId: 'ws-1' })
      const beforeReplay = await readFile(ledgerFile, 'utf8')
      const readyReplay = await registry.beginCreate(createInput(root, { sessionId: 'session-other' }))
      assert.equal(readyReplay.replayed, true)
      assert.equal(readyReplay.record.state, 'ready')
      assert.equal(await readFile(ledgerFile, 'utf8'), beforeReplay, 'a completed replay must not rewrite the ledger')

      await expectCode(
        registry.transition('del-1', { state: 'conflict' }),
        'DELEGATION_ALREADY_EXISTS',
      )
      assert.equal((await registry.get('del-1')).state, 'ready')

      await registry.beginCreate(createInput(root, {
        delegationId: 'del-conflict',
        title: 'Conflict',
        createFingerprint: 'fingerprint-c',
      }))
      const conflicted = await registry.transition('del-conflict', { state: 'conflict' })
      assert.equal(conflicted.state, 'conflict')
      assert.equal((await readLedger(ledgerFile))['del-conflict']?.state, 'conflict')
      await expectCode(
        registry.transition('del-conflict', { state: 'ready' }),
        'DELEGATION_ALREADY_EXISTS',
      )
      await expectCode(
        registry.beginCreate(createInput(root, {
          delegationId: 'del-conflict',
          title: 'Conflict',
          createFingerprint: 'fingerprint-c',
        })),
        'DELEGATION_ALREADY_EXISTS',
      )

      await registry.beginCreate(createInput(root, {
        delegationId: 'del-attach',
        title: 'Attach',
        createFingerprint: 'fingerprint-a',
      }))
      await registry.transition('del-attach', { state: 'attach-failed' })
      const repeated = await registry.transition('del-attach', { state: 'attach-failed' })
      assert.equal(repeated.state, 'attach-failed')
      await expectCode(
        registry.transition('del-attach', { state: 'ready' }),
        'DELEGATION_ALREADY_EXISTS',
      )
    } finally {
      await registry.close()
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('title collisions and distinct nested workspaces fail atomically while exact sharing is allowed', async () => {
  const root = await scratchRoot('overlap')
  try {
    const ledgerFile = join(root, 'state', 'delegations.json')
    const registry = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
    try {
      await registry.beginCreate(createInput(root, { cwd: join(root, 'work'), title: 'Alpha' }))

      await expectCode(
        registry.beginCreate(createInput(root, {
          delegationId: 'del-title',
          title: 'Alpha',
          cwd: join(root, 'work'),
          createFingerprint: 'fingerprint-title',
        })),
        'DELEGATION_TITLE_CONFLICT',
      )
      await expectCode(
        registry.beginCreate(createInput(root, {
          delegationId: 'del-nested',
          title: 'Nested',
          cwd: join(root, 'work', 'nested'),
          createFingerprint: 'fingerprint-nested',
        })),
        'WORKSPACE_OVERLAP',
      )

      await registry.beginCreate(createInput(root, {
        delegationId: 'del-prefix',
        title: 'Alpha',
        cwd: join(root, 'work-other'),
        createFingerprint: 'fingerprint-prefix',
      }))
      await registry.beginCreate(createInput(root, {
        delegationId: 'del-shared',
        title: 'Beta',
        cwd: join(root, 'work'),
        createFingerprint: 'fingerprint-shared',
      }))

      const ids = (await registry.list()).map(record => record.delegationId)
      assert.deepEqual(ids, ['del-1', 'del-prefix', 'del-shared'])
      assert.equal(Object.keys(await readLedger(ledgerFile)).length, 3)
    } finally {
      await registry.close()
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('prompt receipts survive restarts with their original cursor and reject conflicting content', async () => {
  const root = await scratchRoot('prompts')
  try {
    const ledgerFile = join(root, 'state', 'delegations.json')
    const registry = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
    const hash = contentHash('original prompt')
    try {
      await registry.beginCreate(createInput(root))
      await registry.transition('del-1', { state: 'ready', workspaceId: 'ws-1' })

      const pending = await registry.beginPrompt('del-1', {
        requestId: 'req-1',
        contentHash: hash,
        beforeCursor: 41,
      })
      assert.equal(pending.replayed, false)
      assert.equal(pending.receipt.accepted, false)
      assert.equal(pending.receipt.beforeCursor, 41)

      const pendingReplay = await registry.beginPrompt('del-1', {
        requestId: 'req-1',
        contentHash: hash,
        beforeCursor: 999,
      })
      assert.equal(pendingReplay.replayed, false)
      assert.equal(pendingReplay.receipt.beforeCursor, 41)

      await registry.acceptPrompt('del-1', 'req-1')
      const acceptedReplay = await registry.beginPrompt('del-1', {
        requestId: 'req-1',
        contentHash: hash,
        beforeCursor: 999,
      })
      assert.equal(acceptedReplay.replayed, true)
      assert.equal(acceptedReplay.receipt.accepted, true)
      assert.equal(acceptedReplay.receipt.beforeCursor, 41)

      const beforeConflict = await readFile(ledgerFile, 'utf8')
      await expectCode(
        registry.beginPrompt('del-1', {
          requestId: 'req-1',
          contentHash: contentHash('different prompt'),
          beforeCursor: 41,
        }),
        'DELEGATION_PROMPT_CONFLICT',
      )
      assert.equal(await readFile(ledgerFile, 'utf8'), beforeConflict)

      for (let index = 0; index < 64; index += 1) {
        const requestId = `req-many-${String(index)}`
        await registry.beginPrompt('del-1', {
          requestId,
          contentHash: contentHash(`prompt ${String(index)}`),
          beforeCursor: 100 + index,
        })
      }
      const retained = (await registry.get('del-1')).prompts ?? {}
      assert.equal(Object.keys(retained).length, 65)
      assert.equal(retained['req-1']?.accepted, true)
      assert.equal(retained['req-1']?.beforeCursor, 41)
      assert.equal(retained['req-many-63']?.accepted, false)
    } finally {
      await registry.close()
    }

    const reopened = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
    try {
      await reopened.ensureWriter()
      const receipt = await reopened.promptReceipt('del-1', 'req-1')
      assert.equal(receipt?.accepted, true)
      assert.equal(receipt?.contentHash, hash)
      assert.equal(receipt?.beforeCursor, 41)
      assert.equal((await reopened.promptReceipt('del-1', 'absent')), undefined)
    } finally {
      await reopened.close()
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('verified model evidence advances monotonically and persists across a reload', async () => {
  const root = await scratchRoot('models')
  try {
    const ledgerFile = join(root, 'state', 'delegations.json')
    const registry = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
    try {
      await registry.beginCreate(createInput(root))
      await registry.transition('del-1', { state: 'ready', workspaceId: 'ws-1' })

      const first = await registry.recordVerifiedModel('del-1', {
        model: { provider: 'provider-a', model: 'model-a' },
        turnSeq: 10,
      })
      assert.equal(first.verifiedTurnSeq, 10)
      assert.deepEqual(first.verifiedModel, { provider: 'provider-a', model: 'model-a' })

      const stale = await registry.recordVerifiedModel('del-1', {
        model: { provider: 'provider-stale', model: 'model-stale' },
        turnSeq: 4,
      })
      assert.equal(stale.verifiedTurnSeq, 10)
      assert.deepEqual(stale.verifiedModel, { provider: 'provider-a', model: 'model-a' })

      const sameTurn = await registry.recordVerifiedModel('del-1', {
        model: { provider: 'provider-b', model: 'model-b', reasoningEffort: 'high' },
        turnSeq: 10,
      })
      assert.equal(sameTurn.verifiedTurnSeq, 10)
      assert.deepEqual(sameTurn.verifiedModel, {
        provider: 'provider-b',
        model: 'model-b',
        reasoningEffort: 'high',
      })

      const newer = await registry.recordVerifiedModel('del-1', {
        model: { provider: 'provider-c', model: 'model-c' },
        turnSeq: 12,
      })
      assert.equal(newer.verifiedTurnSeq, 12)
      assert.deepEqual(newer.verifiedModel, { provider: 'provider-c', model: 'model-c' })
    } finally {
      await registry.close()
    }

    const reopened = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
    try {
      const persisted = await reopened.get('del-1')
      assert.equal(persisted.verifiedTurnSeq, 12)
      assert.deepEqual(persisted.verifiedModel, { provider: 'provider-c', model: 'model-c' })
    } finally {
      await reopened.close()
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('malformed or unsupported ledgers fail loudly and are never rewritten', async () => {
  const root = await scratchRoot('invalid')
  try {
    const ledgerFile = join(root, 'state', 'delegations.json')
    await mkdir(dirname(ledgerFile), { recursive: true, mode: 0o700 })
    for (const text of [
      '{"version":1',
      '{"version":2,"delegations":{}}',
      '{"version":1,"delegations":[]}',
    ]) {
      await writeFile(ledgerFile, text)
      const registry = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
      await expectCode(registry.get('del-1'), 'DELEGATION_LEDGER_INVALID')
      await expectCode(registry.ensureWriter(), 'DELEGATION_LEDGER_INVALID')
      assert.equal(await readFile(ledgerFile, 'utf8'), text)
      await registry.close()
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

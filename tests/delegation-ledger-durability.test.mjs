import assert from 'node:assert/strict'
import { mkdir, mkdtemp, rename, rm, unlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { DelegationRegistry } from '../src/delegation-registry.mjs'

const MODEL = { provider: 'deepseek-official', model: 'deepseek-flash' }

function input(delegationId, cwd) {
  return {
    delegationId,
    cwd,
    workspaceId: `workspace-${delegationId}`,
    sessionId: `session-${delegationId}`,
    title: delegationId,
    expectedModel: MODEL,
  }
}

test('delegation ledger publishes memory only after the durable write', { timeout: 30_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-ledger-durability-'))
  t.after(async () => { await rm(root, { recursive: true, force: true }) })

  const stateDir = join(root, 'state')
  await mkdir(stateDir, { recursive: true })
  const filePath = join(stateDir, 'delegations.json')
  const registry = new DelegationRegistry({ filePath })
  await registry.beginCreate(input('durable', join(root, 'workspace')))

  // Make the next atomic publication fail by replacing its directory with a
  // regular file. No permission assumptions are needed.
  const moved = join(root, 'state-moved')
  await rename(stateDir, moved)
  await writeFile(stateDir, 'blocked')
  await assert.rejects(registry.markReady('durable'))
  const inMemory = await registry.get('durable')
  assert.equal(inMemory.state, 'creating', 'failed persistence must not publish ready')

  await unlink(stateDir)
  await rename(moved, stateDir)
  const restarted = new DelegationRegistry({ filePath })
  assert.equal((await restarted.get('durable')).state, 'creating',
    'a restarted reader sees the previous durable state')

  const ready = await registry.markReady('durable')
  assert.equal(ready.state, 'ready')
  const restartedAfter = new DelegationRegistry({ filePath })
  assert.equal((await restartedAfter.get('durable')).state, 'ready')
})

test('close fences new writes, drains queued writes, and releases ownership', { timeout: 30_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-ledger-close-'))
  t.after(async () => { await rm(root, { recursive: true, force: true }) })
  const filePath = join(root, 'state', 'delegations.json')
  const writer = new DelegationRegistry({ filePath })
  assert.equal(await writer.acquireWriter(), true)

  const draining = writer.beginCreate(input('draining', join(root, 'workspace')))
  const closing = writer.close()
  const late = writer.beginCreate(input('late', join(root, 'workspace')))
  await draining
  await assert.rejects(late, error => {
    assert.equal(error?.code, 'delegation/ledger-closing')
    return true
  })
  await closing
  const restarted = new DelegationRegistry({ filePath })
  assert.equal((await restarted.get('draining')).state, 'creating')
  assert.equal(await restarted.acquireWriter(), true, 'the lifetime lock is free after close drains')
  await restarted.close()
})

test('one ledger file has one lifetime writer and read-only readers stay fresh', { timeout: 30_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-ledger-writer-'))
  t.after(async () => { await rm(root, { recursive: true, force: true }) })
  const filePath = join(root, 'state', 'delegations.json')
  const cwd = join(root, 'workspace')

  const writer = new DelegationRegistry({ filePath })
  assert.equal(await writer.acquireWriter(), true)
  await writer.beginCreate(input('first', cwd))
  await writer.markReady('first')

  const reader = new DelegationRegistry({ filePath })
  assert.equal(await reader.acquireWriter(), false, 'a second runtime must not become a second writer')
  assert.equal(reader.readOnly, true)
  assert.equal((await reader.get('first')).state, 'ready',
    'the read-only view loads fresh durable state')
  await assert.rejects(
    reader.beginCreate(input('second', cwd)),
    error => {
      assert.equal(error?.code, 'delegation/ledger-readonly')
      return true
    },
  )
  await assert.rejects(
    reader.ensureWriter({ timeoutMs: 50 }),
    error => {
      assert.equal(error?.code, 'delegation/ledger-readonly')
      return true
    },
  )

  const closing = new Promise(resolve => {
    setTimeout(() => { writer.close().then(resolve) }, 100)
  })
  assert.equal(await reader.ensureWriter({ timeoutMs: 2_000 }), true,
    'a live reader takes ownership when the previous writer exits')
  await closing
  await reader.beginCreate(input('second', cwd))
  await reader.markReady('second')
  const restarted = new DelegationRegistry({ filePath })
  assert.equal((await restarted.get('second')).state, 'ready')
  await reader.close()
})

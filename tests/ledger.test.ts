import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { LiaisonError } from '../src/errors.ts'
import { Ledger } from '../src/ledger.ts'
import type { DelegationRecord } from '../src/ledger.ts'

const record: DelegationRecord = {
  delegationId: 'delegation-a',
  sessionId: 'session-a',
  cwd: '/tmp/project',
  title: 'Delegation A',
  model: 'fixture/model-a',
  modelVerification: 'indeterminate',
  status: 'creating',
  createdAt: 10,
  updatedAt: 10,
}

describe('SQLite ledger', () => {
  let root: string
  let path: string
  let ledger: Ledger

  before(async () => {
    root = await mkdtemp(join(tmpdir(), 'dsh-liaison-ledger-'))
    path = join(root, 'ledger.sqlite3')
    ledger = Ledger.open(path)
  })

  after(async () => {
    ledger.close()
    await rm(root, { recursive: true, force: true })
  })

  it('persists a creating row, promotes it, and lists it', () => {
    ledger.insertDelegation(record)
    assert.deepEqual(ledger.findDelegation('delegation-a'), record)
    ledger.insertReceipt({
      delegationId: 'delegation-a',
      requestId: 'request-a',
      beforeCursor: 4,
      promptHash: 'hash-a',
      createdAt: 11,
    })
    assert.equal(ledger.findReceipt('delegation-a', 'request-a')?.beforeCursor, 4)
    ledger.markReady('delegation-a', 12)
    ledger.setModelVerification('delegation-a', 'proven', 13)
    assert.equal(ledger.findDelegation('delegation-a')?.status, 'ready')
    assert.equal(ledger.findDelegation('delegation-a')?.modelVerification, 'proven')
    assert.deepEqual(ledger.listDelegations().map(item => item.delegationId), ['delegation-a'])
  })

  it('enforces unique delegation, session, and receipt keys', () => {
    assert.throws(
      () => { ledger.insertDelegation(record) },
      (error: unknown) => error instanceof LiaisonError && error.code === 'conflict',
    )
    assert.throws(
      () => { ledger.insertDelegation({ ...record, delegationId: 'delegation-b' }) },
      (error: unknown) => error instanceof LiaisonError && error.code === 'conflict',
    )
    assert.throws(
      () => {
        ledger.insertReceipt({
          delegationId: 'delegation-a', requestId: 'request-a', beforeCursor: 9, promptHash: 'other', createdAt: 14,
        })
      },
      (error: unknown) => error instanceof LiaisonError && error.code === 'conflict',
    )
  })

  it('reports a non-conflict insert fault as a ledger failure', async () => {
    const faultRoot = await mkdtemp(join(tmpdir(), 'dsh-liaison-ledger-fault-'))
    const faultPath = join(faultRoot, 'ledger.sqlite3')
    const faultLedger = Ledger.open(faultPath)
    const driver = new DatabaseSync(faultPath)
    try {
      driver.exec('DROP TABLE delegations')
      assert.throws(
        () => { faultLedger.insertDelegation({ ...record, delegationId: 'delegation-fault', sessionId: 'session-fault' }) },
        (error: unknown) => error instanceof LiaisonError && error.code === 'ledger',
      )
    } finally {
      driver.close()
      faultLedger.close()
      await rm(faultRoot, { recursive: true, force: true })
    }
  })

  it('enforces non-empty identifiers at the database boundary', () => {
    assert.throws(
      () => { ledger.insertDelegation({ ...record, delegationId: '', sessionId: 'session-empty' }) },
      LiaisonError,
    )
  })

  it('answers offline reads from a read-only connection', async () => {
    ledger.close()
    const offline = Ledger.open(path, { readOnly: true })
    try {
      assert.equal(offline.findDelegation('delegation-a')?.sessionId, 'session-a')
      assert.deepEqual(offline.listDelegations().map(item => item.delegationId), ['delegation-a'])
    } finally {
      offline.close()
    }
    ledger = Ledger.open(path)
  })
})

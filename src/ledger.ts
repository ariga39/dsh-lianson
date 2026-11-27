/**
 * SQLite delegation ledger. Every write is one short synchronous statement or
 * transaction; no remote call or filesystem I/O runs while a transaction is
 * open.
 */

import { existsSync, mkdirSync } from 'node:fs'
import { dirname } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { LiaisonError } from './errors.ts'

/** Lifecycle of one delegation row. */
export type DelegationStatus = 'creating' | 'ready'

/** Truthful model verification state. */
export type ModelVerification = 'proven' | 'mismatch' | 'indeterminate'

/** One persisted delegation. */
export interface DelegationRecord {
  readonly delegationId: string
  readonly sessionId: string
  readonly cwd: string
  readonly title: string
  /** `provider/model` selected for this delegation. */
  readonly model: string
  readonly modelVerification: ModelVerification
  readonly status: DelegationStatus
  readonly createdAt: number
  readonly updatedAt: number
}

/** One durable prompt receipt, written before the remote prompt call. */
export interface PromptReceipt {
  readonly delegationId: string
  readonly requestId: string
  readonly beforeCursor: number
  readonly promptHash: string
  readonly createdAt: number
}

interface DelegationRow {
  delegation_id: string
  session_id: string
  cwd: string
  title: string
  model: string
  model_verification: ModelVerification
  status: DelegationStatus
  created_at: number
  updated_at: number
}

interface ReceiptRow {
  delegation_id: string
  request_id: string
  before_cursor: number
  prompt_hash: string
  created_at: number
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS delegations (
  delegation_id TEXT PRIMARY KEY
    CHECK (length(trim(delegation_id)) > 0),
  session_id TEXT NOT NULL UNIQUE
    CHECK (length(trim(session_id)) > 0),
  cwd TEXT NOT NULL
    CHECK (length(trim(cwd)) > 0),
  title TEXT NOT NULL
    CHECK (length(trim(title)) > 0),
  model TEXT NOT NULL
    CHECK (length(trim(model)) > 0),
  model_verification TEXT NOT NULL
    CHECK (model_verification IN ('proven', 'mismatch', 'indeterminate')),
  status TEXT NOT NULL
    CHECK (status IN ('creating', 'ready')),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS prompt_receipts (
  delegation_id TEXT NOT NULL
    CHECK (length(trim(delegation_id)) > 0)
    REFERENCES delegations(delegation_id),
  request_id TEXT NOT NULL
    CHECK (length(trim(request_id)) > 0),
  before_cursor INTEGER NOT NULL CHECK (before_cursor >= 0),
  prompt_hash TEXT NOT NULL
    CHECK (length(trim(prompt_hash)) > 0),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (delegation_id, request_id)
);
`

/** SQLite-backed delegation and prompt-receipt store. */
export class Ledger {
  private closed = false

  private readonly db: DatabaseSync

  private constructor(db: DatabaseSync) {
    this.db = db
  }

  /**
   * Open the ledger, creating its parent directory and schema when writable.
   * @param path - SQLite database path.
   * @param options - `readOnly` skips directory/schema creation and runs one query connection.
   * @returns the open ledger.
   */
  static open(path: string, options: { readonly readOnly?: boolean } = {}): Ledger {
    const readOnly = options.readOnly === true
    if (readOnly) {
      if (!existsSync(path)) throw new LiaisonError('ledger', `ledger does not exist at ${path}`, { path })
    } else {
      mkdirSync(dirname(path), { recursive: true, mode: 0o700 })
    }
    let db: DatabaseSync
    try {
      db = new DatabaseSync(path, readOnly ? { readOnly: true } : {})
    } catch (error) {
      throw new LiaisonError('ledger', `cannot open ledger ${path}: ${error instanceof Error ? error.message : String(error)}`, { path })
    }
    try {
      db.exec('PRAGMA busy_timeout = 2000')
      if (!readOnly) {
        db.exec('PRAGMA journal_mode = WAL')
        db.exec('PRAGMA synchronous = NORMAL')
        db.exec('PRAGMA foreign_keys = ON')
        db.exec(SCHEMA)
      }
    } catch (error) {
      db.close()
      throw new LiaisonError('ledger', `cannot initialize ledger ${path}: ${error instanceof Error ? error.message : String(error)}`, { path })
    }
    return new Ledger(db)
  }

  /** @returns the delegation with this id, or undefined. */
  findDelegation(delegationId: string): DelegationRecord | undefined {
    this.assertOpen()
    const row = this.db.prepare('SELECT * FROM delegations WHERE delegation_id = ?').get(delegationId) as DelegationRow | undefined
    return row === undefined ? undefined : delegationFromRow(row)
  }

  /** @returns every delegation ordered by creation time. */
  listDelegations(): DelegationRecord[] {
    this.assertOpen()
    const rows = this.db.prepare('SELECT * FROM delegations ORDER BY created_at ASC, delegation_id ASC').all() as unknown as DelegationRow[]
    return rows.map(delegationFromRow)
  }

  /** @returns the delegation bound to one Host Session id, or undefined. */
  findDelegationBySession(sessionId: string): DelegationRecord | undefined {
    this.assertOpen()
    const row = this.db.prepare('SELECT * FROM delegations WHERE session_id = ?').get(sessionId) as DelegationRow | undefined
    return row === undefined ? undefined : delegationFromRow(row)
  }

  /**
   * Insert one `creating` delegation. A duplicate delegation id or session id
   * surfaces as a conflict rather than replacing durable state.
   * @param record - complete row minted before any remote create call.
   */
  insertDelegation(record: DelegationRecord): void {
    this.assertOpen()
    try {
      this.db.prepare(`
        INSERT INTO delegations (
          delegation_id, session_id, cwd, title, model, model_verification, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        record.delegationId,
        record.sessionId,
        record.cwd,
        record.title,
        record.model,
        record.modelVerification,
        record.status,
        record.createdAt,
        record.updatedAt,
      )
    } catch (error) {
      throw conflictFromSqlite(error, `delegation ${record.delegationId} cannot be inserted`)
    }
  }

  /**
   * Mark a delegation ready after every remote step succeeded.
   * @param delegationId - delegation being completed.
   * @param updatedAt - commit timestamp.
   */
  markReady(delegationId: string, updatedAt: number): void {
    this.assertOpen()
    this.db.prepare('UPDATE delegations SET status = ?, updated_at = ? WHERE delegation_id = ?')
      .run('ready', updatedAt, delegationId)
  }

  /**
   * Store the verification verdict for one delegation.
   * @param delegationId - delegation inspected.
   * @param verification - truthful verdict.
   * @param updatedAt - commit timestamp.
   */
  setModelVerification(delegationId: string, verification: ModelVerification, updatedAt: number): void {
    this.assertOpen()
    this.db.prepare('UPDATE delegations SET model_verification = ?, updated_at = ? WHERE delegation_id = ?')
      .run(verification, updatedAt, delegationId)
  }

  /** @returns the stored receipt for one `(delegation, request)` pair, or undefined. */
  findReceipt(delegationId: string, requestId: string): PromptReceipt | undefined {
    this.assertOpen()
    const row = this.db.prepare('SELECT * FROM prompt_receipts WHERE delegation_id = ? AND request_id = ?')
      .get(delegationId, requestId) as ReceiptRow | undefined
    return row === undefined ? undefined : receiptFromRow(row)
  }

  /**
   * Insert one receipt before the remote prompt call. A duplicate pair is
   * returned as a conflict so the caller re-reads and applies its hash rule.
   * @param receipt - durable cursor and content hash.
   */
  insertReceipt(receipt: PromptReceipt): void {
    this.assertOpen()
    try {
      this.db.prepare(`
        INSERT INTO prompt_receipts (delegation_id, request_id, before_cursor, prompt_hash, created_at)
        VALUES (?, ?, ?, ?, ?)
      `).run(receipt.delegationId, receipt.requestId, receipt.beforeCursor, receipt.promptHash, receipt.createdAt)
    } catch (error) {
      throw conflictFromSqlite(error, `prompt receipt ${receipt.delegationId}/${receipt.requestId} cannot be inserted`)
    }
  }

  /**
   * Refresh a delegation's activity timestamp after a prompt.
   * @param delegationId - delegation whose turn was admitted.
   * @param updatedAt - commit timestamp.
   */
  touchDelegation(delegationId: string, updatedAt: number): void {
    this.assertOpen()
    this.db.prepare('UPDATE delegations SET updated_at = ? WHERE delegation_id = ?').run(updatedAt, delegationId)
  }

  /** Close the SQLite connection. Idempotent. */
  close(): void {
    if (this.closed) return
    this.closed = true
    this.db.close()
  }

  private assertOpen(): void {
    if (this.closed) throw new LiaisonError('ledger', 'ledger is closed')
  }
}

function delegationFromRow(row: DelegationRow): DelegationRecord {
  return {
    delegationId: row.delegation_id,
    sessionId: row.session_id,
    cwd: row.cwd,
    title: row.title,
    model: row.model,
    modelVerification: row.model_verification,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }
}

function receiptFromRow(row: ReceiptRow): PromptReceipt {
  return {
    delegationId: row.delegation_id,
    requestId: row.request_id,
    beforeCursor: row.before_cursor,
    promptHash: row.prompt_hash,
    createdAt: row.created_at,
  }
}

/** SQLite primary result code for any constraint violation (`sqlite3.h`). */
const SQLITE_CONSTRAINT = 19

function conflictFromSqlite(error: unknown, message: string): LiaisonError {
  const errcode = (error as { errcode?: unknown } | null)?.errcode
  if (typeof errcode === 'number' && (errcode & 0xff) === SQLITE_CONSTRAINT) {
    return new LiaisonError('conflict', message)
  }
  return new LiaisonError('ledger', `${message}: ${error instanceof Error ? error.message : String(error)}`)
}

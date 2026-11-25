/** Durable write-ahead delegation ledger. */

import { randomUUID } from 'node:crypto'
import { chmod, mkdir, open, readFile, rename, unlink } from 'node:fs/promises'
import { dirname } from 'node:path'
import { LiaisonError } from './errors.mjs'
import { createLock } from './host-manager.mjs'

const ACTIVE_STATES = new Set(['creating', 'ready'])
const MAX_CREATE_REPLAYS = 4_096

function clone(value) {
  return structuredClone(value)
}

function cloneMap(map) {
  return new Map([...map.entries()].map(([key, value]) => [key, clone(value)]))
}

function normalizeModel(model) {
  return {
    provider: model.provider,
    model: model.model,
    ...(model.reasoningEffort === undefined ? {} : { reasoningEffort: model.reasoningEffort }),
  }
}

function sameModel(left, right) {
  return left.provider === right.provider
    && left.model === right.model
    && (left.reasoningEffort ?? null) === (right.reasoningEffort ?? null)
}

function emptyLedger() {
  return { version: 1, delegations: [], createReplays: [] }
}

/**
 * A durable ledger with one lifetime writer per ledger file.
 *
 * Mutations are made on a draft copy, atomically persisted, and only then
 * published in memory. A write failure therefore leaves both the retry view and
 * the restarted view at the previous durable state.
 */
export class DelegationRegistry {
  #filePath
  #records = new Map()
  #createReplays = new Map()
  #loadPromise
  #tail = Promise.resolve()
  #readOnly = false
  #writerLock = null

  /** @param {{ filePath: string }} options - absolute ledger path. */
  constructor({ filePath }) {
    if (typeof filePath !== 'string' || filePath === '') throw new TypeError('DelegationRegistry requires filePath')
    this.#filePath = filePath
  }

  /** @returns the ledger path. */
  get filePath() {
    return this.#filePath
  }

  /** @returns whether this process failed to acquire the lifetime writer lock. */
  get readOnly() {
    return this.#readOnly
  }

  /**
   * Acquire the one lifetime writer lock shared by every runtime directory.
   * When another process owns it, reads continue from fresh disk state and
   * mutations fail closed.
   * @returns true when this registry became the writer.
   */
  async acquireWriter() {
    if (this.#writerLock !== null) return true
    await mkdir(dirname(this.#filePath), { recursive: true, mode: 0o700 })
    let release
    try {
      const lock = await createLock(`${this.#filePath}.lock`)
      release = await lock.acquire({ timeoutMs: 0 })
    } catch (error) {
      if (error?.code !== 'HOST_LOCK_TIMEOUT' && error?.code !== 'LOCK_UNAVAILABLE') throw error
      this.#readOnly = true
      this.#loadPromise = undefined
      await this.#load()
      return false
    }
    this.#writerLock = release
    this.#readOnly = false
    this.#loadPromise = undefined
    await this.#load()
    return true
  }

  /** Release the lifetime writer lock. Process exit releases it as well. */
  async close() {
    const release = this.#writerLock
    this.#writerLock = null
    await release?.()
  }

  /** @param {string} delegationId - delegation identity. @returns a copy or undefined. */
  async get(delegationId) {
    await this.#load()
    const record = this.#records.get(delegationId)
    return record === undefined ? undefined : clone(record)
  }

  /** @returns every record, newest creation first. */
  async list() {
    await this.#load()
    return [...this.#records.values()]
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
      .map(clone)
  }

  /**
   * Find the record owning one Host session id.
   * @param {string} sessionId - Host session identity.
   * @returns a copy or undefined.
   */
  async findBySessionId(sessionId) {
    await this.#load()
    for (const record of this.#records.values()) {
      if (record.sessionId === sessionId) return clone(record)
    }
    return undefined
  }

  /**
   * Find the ready delegation bound to one Host session id.
   * @param {string} sessionId - Host session identity.
   * @returns a copy or undefined.
   */
  async findBoundBySessionId(sessionId) {
    await this.#load()
    for (const record of this.#records.values()) {
      if (record.sessionId === sessionId && record.state === 'ready') return clone(record)
    }
    return undefined
  }

  /**
   * Persist a fresh `creating` record, or validate and resume an existing one.
   * @param {object} input - delegation parameters and the pre-minted session id.
   * @returns the persisted record.
   */
  async beginCreate(input) {
    return this.#enqueue(async () => {
      await this.#load()
      this.#assertWritable()
      const draft = this.#draft()
      const now = new Date().toISOString()
      const expectedModel = normalizeModel(input.expectedModel)
      const existing = draft.records.get(input.delegationId)
      if (existing !== undefined) {
        if (existing.state === 'attach-failed') {
          throw new LiaisonError(
            'delegation/attach-failed',
            `delegation "${input.delegationId}" retained session "${existing.sessionId}" after a workspace attach failure`,
            {
              delegationId: existing.delegationId,
              sessionId: existing.sessionId,
              workspaceId: existing.workspaceId,
              lastError: existing.lastError,
            },
          )
        }
        if (existing.state === 'ready') {
          throw new LiaisonError(
            'delegation/already-exists',
            `delegation "${input.delegationId}" is already ready`,
            { delegationId: existing.delegationId, sessionId: existing.sessionId },
          )
        }
        if (existing.state === 'conflict') {
          throw new LiaisonError(
            'delegation/conflict',
            `delegation "${input.delegationId}" is in conflict`,
            {
              delegationId: existing.delegationId,
              sessionId: existing.sessionId,
              substitutedSessionId: existing.substitutedSessionId,
            },
          )
        }
        this.#assertRetryMatches(existing, { ...input, expectedModel })
        existing.updatedAt = now
        await this.#commit(draft)
        return clone(existing)
      }

      const titleConflict = [...draft.records.values()].find(candidate =>
        candidate.workspaceId === input.workspaceId
        && candidate.title === input.title
        && ACTIVE_STATES.has(candidate.state))
      if (titleConflict !== undefined) {
        throw new LiaisonError(
          'delegation/title-conflict',
          `workspace already has an active delegation titled "${input.title}"`,
          { delegationId: input.delegationId, conflictingDelegationId: titleConflict.delegationId },
        )
      }

      const record = {
        delegationId: input.delegationId,
        cwd: input.cwd,
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        title: input.title,
        ...(input.agentPreset === undefined ? {} : { agentPreset: input.agentPreset }),
        expectedModel,
        state: 'creating',
        createdAt: now,
        updatedAt: now,
      }
      draft.records.set(record.delegationId, record)
      await this.#commit(draft)
      return clone(record)
    })
  }

  /**
   * Record a Host identity conflict. A late result never changes a ready binding.
   * @param {string} delegationId - delegation identity.
   * @param {{ upstreamCode?: string, message?: string, substitutedSessionId?: string }} conflict - conflict details.
   * @returns the persisted record.
   */
  async markConflict(delegationId, conflict) {
    return this.#enqueue(async () => {
      await this.#load()
      this.#assertWritable()
      const draft = this.#draft()
      const record = this.#require(draft.records, delegationId)
      if (record.state === 'ready') return clone(record)
      record.state = 'conflict'
      record.updatedAt = new Date().toISOString()
      record.lastError = {
        ...(conflict.upstreamCode === undefined ? {} : { code: conflict.upstreamCode }),
        message: conflict.message ?? 'session creation conflicted',
      }
      if (conflict.substitutedSessionId !== undefined) {
        record.substitutedSessionId = conflict.substitutedSessionId
        record.returnedSessionId = conflict.substitutedSessionId
      }
      await this.#commit(draft)
      return clone(record)
    })
  }

  /**
   * Preserve the Host-created session after a workspace attach failure.
   * @param {string} delegationId - delegation identity.
   * @param {{ sessionId?: string, workspaceId?: string, message?: string }} failure - attach failure details.
   * @returns the persisted record.
   */
  async markAttachFailed(delegationId, failure) {
    return this.#enqueue(async () => {
      await this.#load()
      this.#assertWritable()
      const draft = this.#draft()
      const record = this.#require(draft.records, delegationId)
      if (record.state === 'ready') return clone(record)
      if (failure.sessionId !== undefined && failure.sessionId !== record.sessionId) {
        record.state = 'conflict'
        record.updatedAt = new Date().toISOString()
        record.substitutedSessionId = failure.sessionId
        record.returnedSessionId = failure.sessionId
        record.lastError = {
          code: 'session/workspace-attach-failed',
          message: failure.message ?? 'session was created but could not attach to its workspace',
        }
        await this.#commit(draft)
        throw new LiaisonError(
          'delegation/session-substituted',
          `Host attached session "${failure.sessionId}" after requesting "${record.sessionId}"; `
            + 'refusing to overwrite the delegation binding',
          {
            delegationId,
            requestedSessionId: record.sessionId,
            returnedSessionId: failure.sessionId,
            workspaceId: record.workspaceId,
            upstreamCode: 'session/workspace-attach-failed',
          },
        )
      }
      record.state = 'attach-failed'
      record.updatedAt = new Date().toISOString()
      record.lastError = {
        code: 'session/workspace-attach-failed',
        message: failure.message ?? 'session was created but could not attach to its workspace',
      }
      await this.#commit(draft)
      return clone(record)
    })
  }

  /**
   * Mark a fully bound delegation ready and atomically persist the create IPC
   * replay identity/result in the same ledger write.
   * @param {string} delegationId - delegation identity.
   * @param {{ ipcRequestId?: string, ipcFingerprint?: string }} [replay] - successful create request identity.
   * @returns the persisted record.
   */
  async markReady(delegationId, replay = {}) {
    return this.#enqueue(async () => {
      await this.#load()
      this.#assertWritable()
      const draft = this.#draft()
      const record = this.#require(draft.records, delegationId)
      if (record.state === 'ready') {
        if (replay.ipcRequestId !== undefined && replay.ipcFingerprint !== undefined
          && !draft.createReplays.has(replay.ipcRequestId)) {
          draft.createReplays.set(replay.ipcRequestId, {
            requestId: replay.ipcRequestId,
            fingerprint: replay.ipcFingerprint,
            response: clone(record),
          })
          this.#pruneCreateReplays(draft.createReplays)
          await this.#commit(draft)
        }
        return clone(record)
      }
      record.state = 'ready'
      record.updatedAt = new Date().toISOString()
      delete record.lastError
      const response = clone(record)
      if (replay.ipcRequestId !== undefined && replay.ipcFingerprint !== undefined) {
        draft.createReplays.set(replay.ipcRequestId, {
          requestId: replay.ipcRequestId,
          fingerprint: replay.ipcFingerprint,
          response,
        })
        this.#pruneCreateReplays(draft.createReplays)
      }
      await this.#commit(draft)
      return clone(record)
    })
  }

  /**
   * Read one durable create success by caller IPC request id.
   * @param {string} requestId - the original IPC request id.
   * @returns `{ requestId, fingerprint, response }` or undefined.
   */
  async getCreateReplay(requestId) {
    await this.#load()
    const entry = this.#createReplays.get(requestId)
    return entry === undefined ? undefined : clone(entry)
  }

  /**
   * Persist the model proven by a request header.
   * @param {string} delegationId - delegation identity.
   * @param {{ provider: string, model: string, reasoningEffort?: string }} model - verified route.
   * @returns the persisted record.
   */
  async setVerifiedModel(delegationId, model) {
    return this.#enqueue(async () => {
      await this.#load()
      this.#assertWritable()
      const draft = this.#draft()
      const record = this.#require(draft.records, delegationId)
      record.verifiedModel = normalizeModel(model)
      record.updatedAt = new Date().toISOString()
      await this.#commit(draft)
      return clone(record)
    })
  }

  #draft() {
    return {
      records: cloneMap(this.#records),
      createReplays: cloneMap(this.#createReplays),
    }
  }

  #assertWritable() {
    if (!this.#readOnly) return
    throw new LiaisonError(
      'delegation/ledger-readonly',
      `delegation ledger "${this.#filePath}" is owned by another writer`,
      { filePath: this.#filePath },
    )
  }

  async #commit(draft) {
    this.#assertWritable()
    await this.#persist(draft.records, draft.createReplays)
    this.#records = draft.records
    this.#createReplays = draft.createReplays
  }

  #pruneCreateReplays(replays) {
    while (replays.size > MAX_CREATE_REPLAYS) {
      replays.delete(replays.keys().next().value)
    }
  }

  #assertRetryMatches(record, input) {
    const mismatched = []
    if (record.cwd !== input.cwd) mismatched.push('cwd')
    if (record.workspaceId !== input.workspaceId) mismatched.push('workspaceId')
    if (record.title !== input.title) mismatched.push('title')
    if ((record.agentPreset ?? null) !== (input.agentPreset ?? null)) mismatched.push('agentPreset')
    if (!sameModel(record.expectedModel, input.expectedModel)) mismatched.push('expectedModel')
    if (mismatched.length > 0) {
      throw new LiaisonError(
        'delegation/mismatch',
        `delegation "${input.delegationId}" cannot resume with different parameters: ${mismatched.join(', ')}`,
        {
          delegationId: input.delegationId,
          mismatched,
          stored: {
            cwd: record.cwd,
            workspaceId: record.workspaceId,
            title: record.title,
            expectedModel: record.expectedModel,
            agentPreset: record.agentPreset,
          },
          requested: {
            cwd: input.cwd,
            workspaceId: input.workspaceId,
            title: input.title,
            expectedModel: input.expectedModel,
            agentPreset: input.agentPreset,
          },
        },
      )
    }
  }

  #require(records, delegationId) {
    const record = records.get(delegationId)
    if (record === undefined) {
      throw new LiaisonError(
        'delegation/not-found',
        `delegation "${delegationId}" is not in the ledger`,
        { delegationId },
      )
    }
    return record
  }

  #enqueue(operation) {
    const running = this.#tail.then(operation, operation)
    this.#tail = running.then(() => undefined, () => undefined)
    return running
  }

  async #load() {
    if (this.#readOnly) {
      await this.#readInto()
      return
    }
    this.#loadPromise ??= this.#readInto()
    return this.#loadPromise
  }

  async #readInto() {
    const records = new Map()
    const createReplays = new Map()
    let text
    try {
      text = await readFile(this.#filePath, 'utf8')
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error
      this.#records = records
      this.#createReplays = createReplays
      return
    }
    if (text.trim() !== '') {
      let parsed
      try {
        parsed = JSON.parse(text)
      } catch (error) {
        throw new LiaisonError('delegation/ledger-corrupt', `cannot parse delegation ledger ${this.#filePath}`, {}, { cause: error })
      }
      if (parsed?.version !== 1 || !Array.isArray(parsed.delegations)) {
        throw new LiaisonError('delegation/ledger-corrupt', `unsupported delegation ledger ${this.#filePath}`)
      }
      for (const record of parsed.delegations) {
        if (typeof record?.delegationId !== 'string' || typeof record?.sessionId !== 'string') {
          throw new LiaisonError('delegation/ledger-corrupt', `invalid delegation record in ${this.#filePath}`)
        }
        if (records.has(record.delegationId)) {
          throw new LiaisonError('delegation/ledger-corrupt', `duplicate delegation id "${record.delegationId}"`)
        }
        records.set(record.delegationId, record)
      }
      if (Array.isArray(parsed.createReplays)) {
        for (const entry of parsed.createReplays) {
          if (typeof entry?.requestId !== 'string' || typeof entry?.fingerprint !== 'string') continue
          if (entry.response === null || typeof entry.response !== 'object') continue
          createReplays.set(entry.requestId, entry)
        }
      }
    }
    this.#records = records
    this.#createReplays = createReplays
  }

  async #persist(records, createReplays) {
    const directory = dirname(this.#filePath)
    await mkdir(directory, { recursive: true, mode: 0o700 })
    const temporary = `${this.#filePath}.${process.pid}.${randomUUID()}.tmp`
    const body = `${JSON.stringify({
      ...emptyLedger(),
      delegations: [...records.values()],
      createReplays: [...createReplays.values()],
    }, null, 2)}\n`
    let handle
    try {
      handle = await open(temporary, 'wx', 0o600)
      await handle.writeFile(body, 'utf8')
      await handle.sync()
      await handle.chmod(0o600)
    } catch (error) {
      await handle?.close().catch(() => {})
      handle = undefined
      await unlink(temporary).catch(() => {})
      throw error
    } finally {
      await handle?.close()
    }
    try {
      await rename(temporary, this.#filePath)
    } catch (error) {
      await unlink(temporary).catch(() => {})
      throw error
    }
    await chmod(this.#filePath, 0o600)
  }
}

/** Durable write-ahead delegation ledger. */

import { randomUUID } from 'node:crypto'
import { chmod, mkdir, open, readFile, rename, unlink } from 'node:fs/promises'
import { dirname } from 'node:path'
import { LiaisonError } from './errors.mjs'

const ACTIVE_STATES = new Set(['creating', 'ready'])

function clone(value) {
  return structuredClone(value)
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
 * A single-process ledger with a serialized write queue.
 *
 * Every mutation is persisted to a `0600` temporary file, fsynced, and renamed
 * over the ledger before the caller is allowed to issue the corresponding Host
 * RPC. Reads never observe a partially written JSON document.
 */
export class DelegationRegistry {
  #filePath
  #records = new Map()
  #createReplays = new Map()
  #loadPromise
  #tail = Promise.resolve()

  /** @param {{ filePath: string }} options - absolute ledger path. */
  constructor({ filePath }) {
    if (typeof filePath !== 'string' || filePath === '') throw new TypeError('DelegationRegistry requires filePath')
    this.#filePath = filePath
  }

  /** @returns the ledger path. */
  get filePath() {
    return this.#filePath
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
   * Interaction routing and state observation use this narrower lookup so a
   * creating, attach-failed, or conflicted record is never treated as bound.
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
      const now = new Date().toISOString()
      const expectedModel = normalizeModel(input.expectedModel)
      const existing = this.#records.get(input.delegationId)
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
        await this.#persist()
        return clone(existing)
      }

      const titleConflict = [...this.#records.values()].find(candidate =>
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
      this.#records.set(record.delegationId, record)
      await this.#persist()
      return clone(record)
    })
  }

  /**
   * Record a Host identity conflict, preserving the requested session id.
   * A late result never changes a ready binding.
   * @param {string} delegationId - delegation identity.
   * @param {{ upstreamCode?: string, message?: string, substitutedSessionId?: string }} conflict - conflict details.
   * @returns the persisted record.
   */
  async markConflict(delegationId, conflict) {
    return this.#enqueue(async () => {
      await this.#load()
      const record = this.#require(delegationId)
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
      await this.#persist()
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
      const record = this.#require(delegationId)
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
        await this.#persist()
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
      await this.#persist()
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
      const record = this.#require(delegationId)
      if (record.state === 'ready') {
        if (replay.ipcRequestId !== undefined && replay.ipcFingerprint !== undefined
          && !this.#createReplays.has(replay.ipcRequestId)) {
          this.#createReplays.set(replay.ipcRequestId, {
            requestId: replay.ipcRequestId,
            fingerprint: replay.ipcFingerprint,
            response: clone(record),
          })
          this.#pruneCreateReplays()
          await this.#persist()
        }
        return clone(record)
      }
      record.state = 'ready'
      record.updatedAt = new Date().toISOString()
      delete record.lastError
      const response = clone(record)
      if (replay.ipcRequestId !== undefined && replay.ipcFingerprint !== undefined) {
        this.#createReplays.set(replay.ipcRequestId, {
          requestId: replay.ipcRequestId,
          fingerprint: replay.ipcFingerprint,
          response,
        })
        this.#pruneCreateReplays()
      }
      await this.#persist()
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

  #pruneCreateReplays() {
    while (this.#createReplays.size > 4_096) {
      this.#createReplays.delete(this.#createReplays.keys().next().value)
    }
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
      const record = this.#require(delegationId)
      record.verifiedModel = normalizeModel(model)
      record.updatedAt = new Date().toISOString()
      await this.#persist()
      return clone(record)
    })
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

  #require(delegationId) {
    const record = this.#records.get(delegationId)
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
    this.#loadPromise ??= this.#read()
    return this.#loadPromise
  }

  async #read() {
    let text
    try {
      text = await readFile(this.#filePath, 'utf8')
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error
      return
    }
    if (text.trim() === '') return
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
      if (this.#records.has(record.delegationId)) {
        throw new LiaisonError('delegation/ledger-corrupt', `duplicate delegation id "${record.delegationId}"`)
      }
      this.#records.set(record.delegationId, record)
    }
    if (Array.isArray(parsed.createReplays)) {
      for (const entry of parsed.createReplays) {
        if (typeof entry?.requestId !== 'string' || typeof entry?.fingerprint !== 'string') continue
        if (entry.response === null || typeof entry.response !== 'object') continue
        this.#createReplays.set(entry.requestId, entry)
      }
    }
  }

  async #persist() {
    const directory = dirname(this.#filePath)
    await mkdir(directory, { recursive: true, mode: 0o700 })
    const temporary = `${this.#filePath}.${process.pid}.${randomUUID()}.tmp`
    const body = `${JSON.stringify({
      ...emptyLedger(),
      delegations: [...this.#records.values()],
      createReplays: [...this.#createReplays.values()],
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

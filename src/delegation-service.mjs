/** Named delegation operations shared by the daemon, CLI, and later slices. */

import { LiaisonError } from './errors.mjs'
import {
  EventReader, extractMessages, extractRequestHeaders, extractToolActivity,
} from './event-reader.mjs'
import { SessionCommands } from './session-commands.mjs'
import { StateWaiter } from './state-waiter.mjs'

function badRequest(error) {
  return new LiaisonError(
    'delegation/bad-request',
    error instanceof Error ? error.message : String(error),
    {},
    { cause: error },
  )
}

function turnEnds(events) {
  return events
    .filter(event => event.type === 'turn/end')
    .map(event => ({
      seq: event.seq,
      time: event.time,
      turn: event.data?.turn,
      reason: event.data?.reason ?? null,
    }))
}

/** One delegation service over the durable ledger and Host operator adapter. */
export class DelegationService {
  #registry
  #commands
  #waiter
  #reader

  /**
   * @param {{ registry: import('./delegation-registry.mjs').DelegationRegistry, adapter: object }} options
   */
  constructor({ registry, adapter }) {
    this.#registry = registry
    this.#commands = new SessionCommands({ registry, adapter })
    this.#waiter = new StateWaiter({ registry, adapter })
    this.#reader = new EventReader({ adapter })
  }

  /** Create or resume one delegation session. */
  async create(params) {
    try {
      await this.#ensureWritable()
      return await this.#commands.createDelegation(params)
    } catch (error) {
      if (error?.name === 'TypeError') throw badRequest(error)
      throw error
    }
  }

  /**
   * Replay one durable successful create response by IPC request id.
   * @param {string} requestId - caller IPC request id.
   * @param {string} fingerprint - canonical request fingerprint.
   * @returns the original ready record, or undefined.
   */
  async createReplay(requestId, fingerprint) {
    const entry = await this.#registry.getCreateReplay(requestId)
    if (entry === undefined) return undefined
    if (entry.fingerprint !== fingerprint) {
      throw new LiaisonError(
        'IPC_ID_CONFLICT',
        `Request id "${requestId}" was already used with different command parameters`,
        { requestId },
      )
    }
    return entry.response
  }

  /**
   * Read one record by delegation id or, for bound-session lookup, session id.
   * @param {{ delegationId?: string, sessionId?: string }} query
   */
  async get({ delegationId, sessionId } = {}) {
    if (typeof delegationId !== 'string' && typeof sessionId !== 'string') {
      throw new LiaisonError('delegation/bad-request', 'delegation.get requires delegationId or sessionId')
    }
    const record = typeof delegationId === 'string'
      ? await this.#registry.get(delegationId)
      : await this.#registry.findBySessionId(sessionId)
    if (record === undefined || (typeof sessionId === 'string' && record.sessionId !== sessionId)) {
      throw new LiaisonError(
        'delegation/not-found',
        `no delegation matches ${typeof delegationId === 'string' ? `id "${delegationId}"` : `session "${sessionId}"`}`,
        { delegationId, sessionId },
      )
    }
    return record
  }

  /** List every ledger record. */
  async list() {
    return { delegations: await this.#registry.list() }
  }

  /** Submit one idempotent prompt. */
  async prompt(params) {
    try {
      await this.#ensureWritable()
      return await this.#commands.submitPrompt(params)
    } catch (error) {
      if (error?.name === 'TypeError') throw badRequest(error)
      throw error
    }
  }

  /** Verify one prompt's persisted request-header model. */
  async verifyModel(params) {
    await this.#ensureWritable()
    return this.#commands.verifyRequestModel(params)
  }

  async #ensureWritable() {
    await this.#registry.ensureWriter()
  }

  /**
   * Read one delegation's passive status and subagent catalog.
   * @param {{ delegationId: string }} params
   */
  async status(params = {}) {
    if (typeof params.delegationId !== 'string' || params.delegationId === '') {
      throw new LiaisonError('delegation/bad-request', 'delegation.status requires delegationId')
    }
    try {
      return await this.#waiter.getStatus({ delegationId: params.delegationId })
    } catch (error) {
      if (error?.name === 'TypeError') throw badRequest(error)
      throw error
    }
  }

  /**
   * Wait across Host wait windows for one delegation.
   * @param {object} params - delegationId, afterSeq, timeoutMs, sliceTimeoutMs, waitForSubagents and maxMessages.
   */
  async wait(params = {}) {
    if (typeof params.delegationId !== 'string' || params.delegationId === '') {
      throw new LiaisonError('delegation/bad-request', 'delegation.wait requires delegationId')
    }
    try {
      return await this.#waiter.wait({
        delegationId: params.delegationId,
        ...(params.afterSeq === undefined ? {} : { afterSeq: params.afterSeq }),
        ...(params.timeoutMs === undefined ? {} : { timeoutMs: params.timeoutMs }),
        ...(params.sliceTimeoutMs === undefined ? {} : { sliceTimeoutMs: params.sliceTimeoutMs }),
        ...(params.maxMessages === undefined ? {} : { maxMessages: params.maxMessages }),
        ...(params.waitForSubagents === undefined ? {} : { waitForSubagents: params.waitForSubagents }),
      })
    } catch (error) {
      if (error?.name === 'TypeError') throw badRequest(error)
      throw error
    }
  }

  /**
   * Read a contiguous event range plus structured views.
   * @param {{ delegationId: string, afterSeq?: number, throughSeq?: number, pageSize?: number }} params
   */
  async events(params = {}) {
    if (typeof params.delegationId !== 'string' || params.delegationId === '') {
      throw new LiaisonError('delegation/bad-request', 'delegation.events requires delegationId')
    }
    const record = await this.#requireReady(params.delegationId)
    try {
      const page = await this.#reader.readRange({
        sessionId: record.sessionId,
        ...(params.afterSeq === undefined ? {} : { afterSeq: params.afterSeq }),
        ...(params.throughSeq === undefined ? {} : { throughSeq: params.throughSeq }),
        ...(params.pageSize === undefined ? {} : { pageSize: params.pageSize }),
      })
      return {
        delegationId: params.delegationId,
        sessionId: record.sessionId,
        cursor: page.cursor,
        pinnedCursor: page.pinnedCursor,
        afterSeq: page.afterSeq,
        pagesRead: page.pagesRead,
        events: page.events,
        messages: extractMessages(page.events),
        toolActivity: extractToolActivity(page.events),
        requestHeaders: extractRequestHeaders(page.events),
        turnEnds: turnEnds(page.events),
      }
    } catch (error) {
      if (error?.name === 'TypeError') throw badRequest(error)
      throw error
    }
  }

  async #requireReady(delegationId) {
    const record = await this.#registry.get(delegationId)
    if (record === undefined) {
      throw new LiaisonError(
        'delegation/not-found',
        `delegation "${delegationId}" is not in the ledger`,
        { delegationId },
      )
    }
    if (record.state !== 'ready') {
      throw new LiaisonError(
        'delegation/not-ready',
        `delegation "${delegationId}" is ${record.state}, not ready`,
        { delegationId, state: record.state, sessionId: record.sessionId },
      )
    }
    return record
  }

  /**
   * Bound-session lookup for state observation and interaction routing (#4/#5).
   * @param {string} sessionId - Host session identity.
   * @returns the ready record or undefined.
   */
  async boundSession(sessionId) {
    return this.#registry.findBoundBySessionId(sessionId)
  }

  /**
   * Require a ready delegation for a Host session.
   * @param {string} sessionId - Host session identity.
   * @returns the ready record.
   */
  async requireBoundSession(sessionId) {
    const record = await this.boundSession(sessionId)
    if (record === undefined) {
      throw new LiaisonError(
        'delegation/not-found',
        `session "${sessionId}" is not a bound ready delegation`,
        { sessionId },
      )
    }
    return record
  }
}

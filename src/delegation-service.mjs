/** Named delegation operations shared by the daemon, CLI, and later slices. */

import { LiaisonError } from './errors.mjs'
import { SessionCommands } from './session-commands.mjs'

function badRequest(error) {
  return new LiaisonError(
    'delegation/bad-request',
    error instanceof Error ? error.message : String(error),
    {},
    { cause: error },
  )
}

/** One delegation service over the durable ledger and Host operator adapter. */
export class DelegationService {
  #registry
  #commands

  /**
   * @param {{ registry: import('./delegation-registry.mjs').DelegationRegistry, adapter: object }} options
   */
  constructor({ registry, adapter }) {
    this.#registry = registry
    this.#commands = new SessionCommands({ registry, adapter })
  }

  /** Create or resume one delegation session. */
  async create(params) {
    try {
      return await this.#commands.createDelegation(params)
    } catch (error) {
      if (error?.name === 'TypeError') throw badRequest(error)
      throw error
    }
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
      return await this.#commands.submitPrompt(params)
    } catch (error) {
      if (error?.name === 'TypeError') throw badRequest(error)
      throw error
    }
  }

  /** Verify one prompt's persisted request-header model. */
  async verifyModel(params) {
    return this.#commands.verifyRequestModel(params)
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

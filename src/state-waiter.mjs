/**
 * Passive status observation and deadline-spanning waits.
 *
 * A single `session.wait` call is bounded (default 30 s on the Host). This
 * waiter slices the caller's deadline into `session.wait` windows, advances the
 * exclusive output cursor after every slice, and backfills truncated output
 * from the contiguous event log so no assistant message is dropped.
 */

import { LiaisonError, remoteCodeOf } from './errors.mjs'
import { EventReader, extractMessages, readSubagentStatus } from './event-reader.mjs'

function defaultSleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}

/**
 * Derive the caller-visible status from one passive state sample.
 * @param {object} state - `SessionState`.
 * @returns {'input-required'|'running'|'queued'|'idle'} status.
 */
export function deriveStatus(state) {
  if ((state?.pendingInput?.length ?? 0) > 0) return 'input-required'
  if (state?.running === true) return 'running'
  if ((state?.queue?.length ?? 0) > 0) return 'queued'
  return 'idle'
}

function assertPositiveInteger(value, name) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`)
  }
}

/**
 * Whether background child work cannot be ruled out from one observation.
 * Unknown/external catalog rows are historical and have no local status; they
 * are reported but never make an otherwise healthy idle result indeterminate.
 * @param {object} subagents - `readSubagentStatus` result.
 * @returns {string|null} stable indeterminate reason, or null when status is trustworthy.
 */
function subagentIndeterminate(subagents) {
  if (subagents?.available !== true) return 'subagent-status-unavailable'
  if ((subagents.unavailableSubagents?.length ?? 0) > 0) return 'subagent-status-unavailable'
  return null
}

/** Passive status and multi-window wait over a bound delegation. */
export class StateWaiter {
  #registry
  #adapter
  #reader
  #clock
  #sleep
  #subagentPollMs

  /**
   * @param {{ registry: object, adapter: object, clock?: () => number, sleep?: (ms: number) => Promise<unknown>, subagentPollMs?: number }} options - ledger, Host seam and time sources.
   */
  constructor({ registry, adapter, clock = () => Date.now(), sleep = defaultSleep, subagentPollMs = 250 }) {
    if (registry === null || typeof registry !== 'object' || typeof registry.get !== 'function') {
      throw new TypeError('StateWaiter requires a registry with get')
    }
    if (adapter === null || typeof adapter !== 'object' || typeof adapter.sessionWait !== 'function') {
      throw new TypeError('StateWaiter requires an adapter with sessionWait')
    }
    this.#registry = registry
    this.#adapter = adapter
    this.#reader = new EventReader({ adapter })
    this.#clock = clock
    this.#sleep = sleep
    this.#subagentPollMs = subagentPollMs
  }

  /**
   * Read one delegation's passive status plus its subagent catalog.
   * @param {{ delegationId: string }} request - bound delegation.
   * @returns the status snapshot.
   * @throws {LiaisonError} `delegation/not-found` or `delegation/not-ready`.
   */
  async getStatus({ delegationId }) {
    const record = await this.#requireReady(delegationId)
    const state = await this.#adapter.sessionState({ sessionId: record.sessionId })
    const subagents = await this.#readSubagents(record.sessionId)
    return {
      delegationId,
      sessionId: record.sessionId,
      status: deriveStatus(state),
      state,
      subagentCatalog: subagents.catalog,
      activeSubagents: subagents.activeSubagents,
      unknownSubagents: subagents.unknownSubagents,
      externalSubagents: subagents.externalSubagents,
      subagents,
    }
  }

  /**
   * Wait across multiple Host wait windows until input is required or idle.
   * @param {{ delegationId: string, afterSeq?: number, timeoutMs?: number, sliceTimeoutMs?: number, waitForSubagents?: boolean, maxMessages?: number }} request - delegation, output cursor and bounds.
   * @returns the stopping reason, last state, chronological assistant messages and subagent observation.
   * @throws {LiaisonError} Host failures other than a per-slice `session/wait-timeout`.
   */
  async wait({
    delegationId,
    afterSeq = -1,
    timeoutMs = 120_000,
    sliceTimeoutMs = 25_000,
    waitForSubagents = false,
    maxMessages = 500,
  }) {
    if (!Number.isSafeInteger(afterSeq) || afterSeq < -1) {
      throw new TypeError('afterSeq must be an integer greater than or equal to -1')
    }
    assertPositiveInteger(timeoutMs, 'timeoutMs')
    assertPositiveInteger(sliceTimeoutMs, 'sliceTimeoutMs')
    assertPositiveInteger(maxMessages, 'maxMessages')
    const record = await this.#requireReady(delegationId)
    const sessionId = record.sessionId
    const startedAt = this.#clock()
    const deadline = startedAt + timeoutMs
    let cursor = afterSeq
    let lastState
    let slices = 0
    let backfilled = 0
    let outputTruncated = false
    const collected = new Map()

    const absorb = records => {
      for (const record of records ?? []) {
        const event = record?.event ?? record
        if (event?.type !== 'assistant/message' || !Number.isSafeInteger(event.seq)) continue
        if (!collected.has(event.seq)) collected.set(event.seq, event)
      }
    }

    for (;;) {
      const now = this.#clock()
      if (now >= deadline) break
      const remaining = deadline - now
      const stepMs = Math.max(1, Math.min(sliceTimeoutMs, remaining))
      let value
      try {
        value = await this.#adapter.sessionWait({
          sessionId,
          afterSeq: cursor,
          timeoutMs: stepMs,
          maxMessages,
        })
      } catch (error) {
        // The Host raises this only when no state arrived inside the slice; the
        // caller's overall deadline still owns the wait.
        if (remoteCodeOf(error) === 'session/wait-timeout') {
          if (this.#clock() >= deadline) break
          if (this.#clock() - now < 1) await this.#sleep(1)
          continue
        }
        throw error
      }
      slices += 1
      const truncated = value.output?.truncated === true
      if (truncated) outputTruncated = true
      absorb(value.output?.records)

      const stateCursor = Number.isSafeInteger(value.state?.cursor) ? value.state.cursor : cursor
      if (truncated && stateCursor > cursor) {
        const page = await this.#reader.readRange({
          sessionId,
          afterSeq: cursor,
          throughSeq: stateCursor,
          pageSize: maxMessages,
        })
        absorb(page.events)
        backfilled += 1
      }
      cursor = Math.max(cursor, stateCursor)
      lastState = value.state ?? lastState

      if (value.reason === 'input-required') {
        return this.#snapshot({
          delegationId,
          sessionId,
          reason: 'input-required',
          state: value.state,
          pendingInput: value.state?.pendingInput ?? [],
          startedAt,
          cursor,
          slices,
          backfilled,
          outputTruncated,
          subagents: null,
          collected,
        })
      }

      if (value.reason === 'idle') {
        if ((value.state?.queue?.length ?? 0) > 0) {
          // A nonempty root queue means work is pending even while no turn is
          // running. Never report idle; wait for the queued turn within the
          // caller's deadline.
          const left = deadline - this.#clock()
          if (left <= 0) {
            return this.#snapshot({
              delegationId,
              sessionId,
              reason: 'timeout',
              state: lastState,
              startedAt,
              cursor,
              slices,
              backfilled,
              outputTruncated,
              subagents: await this.#readSubagents(sessionId),
              collected,
            })
          }
          await this.#sleep(Math.min(this.#subagentPollMs, left))
          continue
        }

        const subagents = await this.#readSubagents(sessionId)
        const indeterminateReason = waitForSubagents ? subagentIndeterminate(subagents) : null
        if (indeterminateReason !== null) {
          const left = deadline - this.#clock()
          if (left <= 0) {
            return this.#snapshot({
              delegationId,
              sessionId,
              reason: 'timeout',
              state: lastState,
              startedAt,
              cursor,
              slices,
              backfilled,
              outputTruncated,
              subagents,
              collected,
              indeterminate: true,
              indeterminateReason,
            })
          }
          await this.#sleep(Math.min(this.#subagentPollMs, left))
          if (this.#clock() >= deadline) {
            return this.#snapshot({
              delegationId,
              sessionId,
              reason: 'timeout',
              state: lastState,
              startedAt,
              cursor,
              slices,
              backfilled,
              outputTruncated,
              subagents,
              collected,
              indeterminate: true,
              indeterminateReason,
            })
          }
          continue
        }

        if (waitForSubagents && subagents.activeSubagents.length > 0) {
          const left = deadline - this.#clock()
          if (left <= 0) {
            return this.#snapshot({
              delegationId,
              sessionId,
              reason: 'timeout',
              state: lastState,
              startedAt,
              cursor,
              slices,
              backfilled,
              outputTruncated,
              subagents,
              collected,
            })
          }
          await this.#sleep(Math.min(this.#subagentPollMs, left))
          continue
        }
        return this.#snapshot({
          delegationId,
          sessionId,
          reason: 'idle',
          state: lastState,
          startedAt,
          cursor,
          slices,
          backfilled,
          outputTruncated,
          subagents,
          collected,
        })
      }

      // reason === 'timeout': spend the rest of the caller's deadline in
      // another slice. Guard against a Host that returns immediately without
      // consuming its own timeout window.
      if (this.#clock() - now < 1 && this.#clock() < deadline) await this.#sleep(1)
    }

    if (lastState === undefined) {
      lastState = await this.#adapter.sessionState({ sessionId })
    }
    const subagents = await this.#readSubagents(sessionId)
    return this.#snapshot({
      delegationId,
      sessionId,
      reason: 'timeout',
      state: lastState,
      startedAt,
      cursor,
      slices,
      backfilled,
      outputTruncated,
      subagents,
      collected,
    })
  }

  #snapshot({
    delegationId, sessionId, reason, state, pendingInput, startedAt,
    cursor, slices, backfilled, outputTruncated, subagents, collected,
    indeterminate = false, indeterminateReason = null,
  }) {
    const assistantMessages = [...collected.values()]
      .sort((left, right) => left.seq - right.seq)
      .map(event => extractMessages([event])[0])
    return {
      delegationId,
      sessionId,
      reason,
      cursor,
      slices,
      elapsedMs: this.#clock() - startedAt,
      backfilled,
      outputTruncated,
      indeterminate,
      indeterminateReason,
      state: state ?? null,
      pendingInput: pendingInput ?? state?.pendingInput ?? [],
      assistantMessages,
      subagentCatalog: subagents?.catalog ?? null,
      activeSubagents: subagents?.activeSubagents ?? null,
      unknownSubagents: subagents?.unknownSubagents ?? null,
      externalSubagents: subagents?.externalSubagents ?? null,
      subagents,
    }
  }

  async #readSubagents(sessionId) {
    try {
      return await readSubagentStatus(this.#adapter, sessionId)
    } catch (error) {
      return {
        available: false,
        reason: 'probe-error',
        errorCode: remoteCodeOf(error) ?? null,
        asOfSeq: null,
        catalog: [],
        activeSubagents: [],
        unknownSubagents: [],
        externalSubagents: [],
        unavailableSubagents: [],
      }
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
}

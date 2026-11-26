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
 * Whether local background work cannot be ruled out from one observation.
 * External/unknown rows are handled separately as unprovable boundaries.
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
  constructor({
    registry,
    adapter,
    clock = () => Date.now(),
    sleep = defaultSleep,
    subagentPollMs = 250,
  }) {
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
   *
   * Root-only waits are unchanged. With `waitForSubagents`, the observable
   * local descendant tree is joined; durable external/unknown children and
   * unreadable local nodes are reported as indeterminate rather than idle.
   * @param {{ delegationId: string, afterSeq?: number, timeoutMs?: number, sliceTimeoutMs?: number, waitForSubagents?: boolean, maxMessages?: number }} request - delegation, output cursor and bounds.
   * @returns the stopping reason (`idle`, `input-required`, `timeout` or `indeterminate`), last state, chronological assistant messages and subagent observation.
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
    const noticeSeqs = new Map()
    const noticeFloor = new Map()
    const noticeScanned = new Map()
    let pendingNoticeReason = null

    const absorb = records => {
      for (const record of records ?? []) {
        const event = record?.event ?? record
        if (event?.type !== 'assistant/message' || !Number.isSafeInteger(event.seq)) continue
        if (!collected.has(event.seq)) collected.set(event.seq, event)
      }
    }

    const noteActive = (tree, rootCursor) => {
      const byId = new Map((tree.descendants ?? tree.catalog ?? [])
        .map(node => [node.id, node]))
      for (const node of tree.localSubagents ?? []) {
        if (node.active !== true) continue
        const parentNode = node.parentSessionId === sessionId
          ? null
          : byId.get(node.parentSessionId)
        const parentCursor = parentNode === null
          ? rootCursor
          : parentNode?.cursor
        const floor = Number.isSafeInteger(parentCursor)
          ? Math.max(noticeSeqs.get(node.id) ?? -1, parentCursor)
          : (noticeSeqs.get(node.id) ?? -1)
        noticeFloor.set(node.id, floor)
      }
    }

    const noticeSatisfied = childId =>
      (noticeSeqs.get(childId) ?? -1) > (noticeFloor.get(childId) ?? -1)

    const backfill = async throughSeq => {
      if (!Number.isSafeInteger(throughSeq) || throughSeq <= cursor) return
      const page = await this.#reader.readRange({
        sessionId,
        afterSeq: cursor,
        throughSeq,
        pageSize: maxMessages,
      })
      absorb(page.events)
      cursor = throughSeq
      backfilled += 1
    }

    const verifyNotices = async (tree, rootState) => {
      // Upstream finishDisposal deletes the child before notifySettlement
      // (packages/subagent/subagent/src/manager.ts:1119-1122), so "child
      // settled + parent idle" does not prove the notice turn was scheduled.
      // The public evidence is the durable parent user/message carrying
      // source.kind === 'subagent-settled' and senderSessionId === child id
      // (continuation-messages.ts:168-174).
      const byId = new Map((tree.descendants ?? tree.catalog ?? [])
        .map(node => [node.id, node]))
      const byParent = new Map()
      for (const node of tree.localSubagents ?? []) {
        if (node.settled !== true || noticeSatisfied(node.id)) continue
        const parentId = node.parentSessionId
        const parentNode = parentId === sessionId ? null : byId.get(parentId)
        if (parentNode === undefined) {
          return { pending: [], unavailableReason: 'subagent-settlement-notice-unavailable' }
        }
        // Upstream skips delivery when no live parent Agent exists; no parent
        // turn can start in that case.
        const parentLive = parentNode === null ? rootState?.active === true : parentNode.live === true
        if (!parentLive) continue
        const group = byParent.get(parentId) ?? { parentNode, ids: [] }
        group.ids.push(node.id)
        byParent.set(parentId, group)
      }

      const pending = []
      for (const [parentId, group] of byParent) {
        const parentNode = group.parentNode
        const address = parentNode === null
          ? undefined
          : {
              kind: 'subagent',
              parentSessionId: parentNode.parentSessionId,
              childSessionId: parentNode.id,
              mode: parentNode.mode,
            }
        let page
        try {
          page = await this.#reader.readRange({
            sessionId: parentId,
            ...(address === undefined ? {} : { address }),
            afterSeq: noticeScanned.get(parentId) ?? 0,
            pageSize: 500,
          })
        } catch {
          return { pending, unavailableReason: 'subagent-settlement-notice-unavailable' }
        }
        noticeScanned.set(parentId, page.cursor)
        // Index every notice in the page before checking any sibling; one
        // parent cursor must never discard another child's notice.
        for (const event of page.events) {
          if (event.type === 'user/message'
            && event.data?.source?.kind === 'subagent-settled'
            && typeof event.data.source.senderSessionId === 'string') {
            const previous = noticeSeqs.get(event.data.source.senderSessionId) ?? -1
            if (event.seq > previous) {
              noticeSeqs.set(event.data.source.senderSessionId, event.seq)
            }
          }
        }
        for (const childId of group.ids) {
          if (!noticeSatisfied(childId)) pending.push(childId)
        }
      }
      return { pending, unavailableReason: null }
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
        await backfill(stateCursor)
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
        if ((value.state?.pendingInput?.length ?? 0) > 0) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'input-required',
            state: value.state,
            pendingInput: value.state.pendingInput,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents: null,
            collected,
          })
        }
        if ((value.state?.queue?.length ?? 0) > 0) {
          // A nonempty root queue means work is pending even while no turn is
          // running. Never report idle; wait for the queued turn within the
          // caller's deadline.
          pendingNoticeReason = null
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

        const subagents = await this.#readSubagents(sessionId, { recursive: waitForSubagents })
        if (!waitForSubagents) {
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

        noteActive(subagents, lastState?.cursor)

        // A durable external/unknown row names a child with no local session to
        // observe. Its mere presence proves neither running nor finished, so
        // the tree is explicitly indeterminate instead of waiting forever.
        if ((subagents.externalSubagents?.length ?? 0) > 0) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: lastState,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-external-status-unavailable',
          })
        }
        if ((subagents.unknownSubagents?.length ?? 0) > 0) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: lastState,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-unknown-status-unavailable',
          })
        }
        if (subagents.complete === false) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: lastState,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-tree-truncated',
          })
        }

        const indeterminateReason = subagentIndeterminate(subagents)
        if (indeterminateReason !== null) {
          pendingNoticeReason = null
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

        if ((subagents.localSubagents?.length ?? 0) === 0) {
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

        const childInput = subagents.localSubagents.find(node =>
          (node.pendingInput?.length ?? 0) > 0)
        if (childInput !== undefined) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: lastState,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-input-required',
          })
        }

        if (subagents.activeSubagents.length > 0) {
          pendingNoticeReason = null
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

        const liveUnsettled = subagents.localSubagents.find(node =>
          node.live === true
          && node.settled !== true
          && node.active !== true
          && (node.pendingInput?.length ?? 0) === 0)
        if (liveUnsettled !== undefined) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: lastState,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-live-unsettled',
          })
        }

        // Every observable local descendant is settled. Success requires the
        // child's durable settlement notice in its direct parent's history, not
        // a quiet timer. Then recheck the parent/root state and queue.
        const verification = await verifyNotices(subagents, lastState)
        if (verification.unavailableReason !== null) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: lastState,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents,
            collected,
            indeterminate: true,
            indeterminateReason: verification.unavailableReason,
          })
        }
        if (verification.pending.length > 0) {
          pendingNoticeReason = 'subagent-settlement-notice-unobserved'
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
              indeterminateReason: 'subagent-settlement-notice-unobserved',
            })
          }
          await this.#sleep(Math.min(this.#subagentPollMs, left))
          continue
        }

        pendingNoticeReason = null
        let freshTree
        try {
          freshTree = await this.#readSubagents(sessionId, { recursive: true })
        } catch {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: lastState,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-settlement-recheck-unavailable',
          })
        }
        noteActive(freshTree, lastState?.cursor)
        const freshVerification = await verifyNotices(freshTree, lastState)
        if (freshVerification.unavailableReason !== null) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: lastState,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents: freshTree,
            collected,
            indeterminate: true,
            indeterminateReason: freshVerification.unavailableReason,
          })
        }
        if (freshVerification.pending.length > 0) {
          pendingNoticeReason = 'subagent-settlement-notice-unobserved'
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
              subagents: freshTree,
              collected,
              indeterminate: true,
              indeterminateReason: 'subagent-settlement-notice-unobserved',
            })
          }
          await this.#sleep(Math.min(this.#subagentPollMs, left))
          continue
        }
        let freshRoot
        try {
          // Resolve fresh-tree notices before this sample: a notice may queue a
          // parent turn, and a root sampled first would be stale.
          freshRoot = await this.#adapter.sessionState({ sessionId })
        } catch {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: lastState,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents: freshTree,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-settlement-recheck-unavailable',
          })
        }
        if ((freshRoot?.pendingInput?.length ?? 0) > 0) {
          try {
            await backfill(freshRoot.cursor)
          } catch {
            return this.#snapshot({
              delegationId,
              sessionId,
              reason: 'indeterminate',
              state: freshRoot,
              startedAt,
              cursor,
              slices,
              backfilled,
              outputTruncated,
              subagents: freshTree,
              collected,
              indeterminate: true,
              indeterminateReason: 'subagent-settlement-recheck-unavailable',
            })
          }
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'input-required',
            state: freshRoot,
            pendingInput: freshRoot.pendingInput,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents: freshTree,
            collected,
          })
        }
        if (freshTree.activeSubagents.length > 0
          || freshRoot?.running === true
          || (freshRoot?.queue?.length ?? 0) > 0) {
          continue
        }
        const freshChildInput = freshTree.localSubagents.find(node =>
          (node.pendingInput?.length ?? 0) > 0)
        if (freshChildInput !== undefined) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: freshRoot,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents: freshTree,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-input-required',
          })
        }
        const freshLiveUnsettled = freshTree.localSubagents.find(node =>
          node.live === true
          && node.settled !== true
          && node.active !== true
          && (node.pendingInput?.length ?? 0) === 0)
        if (freshLiveUnsettled !== undefined) {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: freshRoot,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents: freshTree,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-live-unsettled',
          })
        }
        if ((freshTree.externalSubagents?.length ?? 0) > 0
          || (freshTree.unknownSubagents?.length ?? 0) > 0
          || freshTree.available !== true
          || (freshTree.unavailableSubagents?.length ?? 0) > 0
          || freshTree.complete === false) {
          // Handle the boundary with its explicit indeterminate reason on the
          // next idle observation.
          continue
        }

        try {
          // A follow-up turn may have completed between the idle sample and the
          // post-delivery recheck; absorb its assistant output through the
          // fresh root cursor before returning idle.
          await backfill(freshRoot?.cursor)
        } catch {
          return this.#snapshot({
            delegationId,
            sessionId,
            reason: 'indeterminate',
            state: freshRoot,
            startedAt,
            cursor,
            slices,
            backfilled,
            outputTruncated,
            subagents: freshTree,
            collected,
            indeterminate: true,
            indeterminateReason: 'subagent-settlement-recheck-unavailable',
          })
        }
        return this.#snapshot({
          delegationId,
          sessionId,
          reason: 'idle',
          state: freshRoot,
          startedAt,
          cursor,
          slices,
          backfilled,
          outputTruncated,
          subagents: freshTree,
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
      indeterminate: pendingNoticeReason !== null,
      indeterminateReason: pendingNoticeReason,
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

  async #readSubagents(sessionId, options = {}) {
    try {
      return await readSubagentStatus(this.#adapter, sessionId, options)
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

/** One absolute, monotonic foreground deadline shared by every stage of a call. */
export interface Deadline {
  /** Monotonic end instant from `performance.now()`. */
  readonly end: number
  /** Non-negative milliseconds left before the deadline. */
  remaining(): number
  /** Whether the deadline has passed. */
  expired(): boolean
}

/**
 * Create one absolute foreground budget.
 * @param timeoutMs - total milliseconds allowed for the complete operation.
 * @returns a monotonic deadline.
 */
export function createDeadline(timeoutMs: number): Deadline {
  const end = performance.now() + Math.max(0, timeoutMs)
  return {
    end,
    remaining: () => Math.max(0, end - performance.now()),
    expired: () => performance.now() >= end,
  }
}

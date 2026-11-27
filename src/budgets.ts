/** Command budgets shared by the CLI transport and the plugin service. */

/** Default overall wait budget for one `wait` command. */
export const DEFAULT_WAIT_TIMEOUT_MS = 120_000

/** Default overall stop budget for one `stop` command. */
export const DEFAULT_STOP_TIMEOUT_MS = 30_000

/** Extra transport time allowed for the plugin to return its result document. */
export const RESULT_RETURN_GRACE_MS = 5_000

/**
 * Transport budget for a command whose own work may consume `commandBudgetMs`.
 * @param commandBudgetMs - acknowledged wait or stop budget.
 * @returns the higher socket timeout that still leaves time for the result.
 */
export function transportBudgetFor(commandBudgetMs: number): number {
  return commandBudgetMs + RESULT_RETURN_GRACE_MS
}

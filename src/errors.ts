/** Structured failures shared by the plugin, the CLI, and the ledger. */

/** Stable machine-readable failure codes. */
export type LiaisonErrorCode =
  | 'usage'
  | 'not-found'
  | 'conflict'
  | 'unsafe-cwd'
  | 'invalid-cwd'
  | 'model-unavailable'
  | 'dsh-unavailable'
  | 'plugin-offline'
  | 'socket-in-use'
  | 'unsafe-runtime-dir'
  | 'ledger'
  | 'protocol'
  | 'internal'

/** One failure with a stable code and optional structured details. */
export class LiaisonError extends Error {
  readonly code: LiaisonErrorCode
  readonly details: Readonly<Record<string, unknown>> | undefined

  /**
   * @param code - stable machine-readable failure code.
   * @param message - human-readable explanation safe to print.
   * @param details - optional redacted context for the JSON document.
   */
  constructor(code: LiaisonErrorCode, message: string, details?: Readonly<Record<string, unknown>>) {
    super(message)
    this.name = 'LiaisonError'
    this.code = code
    this.details = details
  }
}

/**
 * Normalize any caught value into a {@link LiaisonError} without leaking
 * stack traces or non-Error values into JSON output.
 * @param error - value caught at a command or request boundary.
 * @returns the structured failure.
 */
export function asLiaisonError(error: unknown): LiaisonError {
  if (error instanceof LiaisonError) return error
  if (error instanceof Error) return new LiaisonError('internal', error.message)
  return new LiaisonError('internal', String(error))
}

/** Keys whose values never belong in CLI JSON. */
const SECRET_KEY = /(?:token|cookie|authorization|secret|password)/iu

/** `token=...` or `cookie: ...` fragments inside free-form error text. */
const SECRET_TEXT = /\b(token|cookie|authorization|password|secret)=([^\s&"']+)/giu

/**
 * Recursively replace secret-bearing keys and inline `token=` fragments.
 * @param value - JSON-compatible or plain value about to be printed.
 * @returns a redacted deep copy.
 */
export function redact(value: unknown): unknown {
  if (typeof value === 'string') return value.replace(SECRET_TEXT, '$1=[redacted]')
  if (Array.isArray(value)) return value.map(item => redact(item))
  if (value !== null && typeof value === 'object') {
    const output: Record<string, unknown> = {}
    for (const [key, item] of Object.entries(value)) {
      output[key] = SECRET_KEY.test(key) ? '[redacted]' : redact(item)
    }
    return output
  }
  return value
}

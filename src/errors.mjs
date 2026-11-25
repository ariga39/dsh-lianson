/** Stable machine-readable failure used across the daemon, IPC, CLI, and delegations. */
export class LiaisonError extends Error {
  /**
   * @param {string} code stable upper-snake or namespaced failure code
   * @param {string} message human-readable, already safe to show after redaction
   * @param {Record<string, unknown>} [details] optional structured context
   * @param {ErrorOptions} [options] standard Error options such as `cause`
   */
  constructor(code, message, details, options) {
    super(message, options)
    this.name = 'LiaisonError'
    this.code = code
    if (details !== undefined) this.details = details
  }
}

/**
 * Convert any thrown value into a redacted serializable error envelope.
 * @param {unknown} error
 * @param {(value: string) => string} redact
 */
export function errorEnvelope(error, redact) {
  const code = typeof error?.code === 'string' ? error.code : 'INTERNAL_ERROR'
  const message = error instanceof Error ? error.message : String(error)
  const details = error?.details
  return {
    code,
    message: redact(message),
    ...details === undefined ? {} : { details: JSON.parse(redact(JSON.stringify(details))) },
  }
}

/**
 * Recognize a Host RemoteError shape without depending on an upstream class.
 * @param value - caught value.
 * @returns the value when it carries a stable Remote code.
 */
export function remoteCodeOf(value) {
  return value !== null && typeof value === 'object' && typeof value.code === 'string'
    ? value.code
    : undefined
}

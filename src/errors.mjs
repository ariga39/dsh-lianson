/** Stable machine-readable failure used across the daemon, IPC, and CLI. */
export class LiaisonError extends Error {
  /**
   * @param {string} code stable upper-snake failure code
   * @param {string} message human-readable, already safe to show after redaction
   * @param {Record<string, unknown>} [details] optional structured context
   */
  constructor(code, message, details) {
    super(message)
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

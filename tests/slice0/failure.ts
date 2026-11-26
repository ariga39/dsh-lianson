import { redact } from './launch-lines.ts'

/** A Host-like process whose final stderr is available after it closes. */
export interface ReapableHost {
  stderr(): string
  stop(): Promise<void>
}

/** Inputs for one sanitized failure presentation. */
export interface FailureDiagnosticsOptions {
  readonly error: unknown
  readonly host?: ReapableHost
  readonly secret?: string
  /** Additional sanitized context, such as a bounded Session status summary. */
  readonly extra?: string
}

const SECRET_PLACEHOLDER = '<redacted>'

/**
 * Read the launch token carried by an authenticated application URL.
 * @param launchUrl - printed `dsh web` URL, when a Host started.
 * @returns the token query value, if any.
 */
export function launchTokenOf(launchUrl: string | undefined): string | undefined {
  if (launchUrl === undefined) return undefined
  try {
    const token = new URL(launchUrl).searchParams.get('token')
    return token === null || token === '' ? undefined : token
  } catch {
    return undefined
  }
}

/**
 * Remove query-token patterns and one exact secret from diagnostic text.
 * @param text - arbitrary diagnostic text.
 * @param secret - exact launch token, when known.
 * @returns text safe to print.
 */
export function redactText(text: string, secret: string | undefined): string {
  const patterned = redact(text)
  return secret === undefined || secret === '' ? patterned : patterned.split(secret).join(SECRET_PLACEHOLDER)
}

/**
 * Reap the Host before reading its final stderr, then build one fresh sanitized Error.
 * Cleanup failures never replace the original failure, and the untrusted error graph
 * (cause, custom fields, original stack) is omitted from the report.
 * @param options - original failure, optional Host, exact secret, and extra context.
 * @returns a fresh Error whose message carries the redacted diagnostics.
 */
export async function failureWithHostDiagnostics(options: FailureDiagnosticsOptions): Promise<Error> {
  let hostDetails = ''
  if (options.host !== undefined) {
    try {
      await options.host.stop()
    } catch {
      // Preserve the original failure when shutdown itself fails.
    }
    try {
      hostDetails = options.host.stderr()
    } catch {
      // A failed diagnostic read must not replace the original failure.
    }
  }
  const original = options.error instanceof Error
    ? options.error
    : new Error(typeof options.error === 'string' ? options.error : 'Unknown failure')
  const primary = original.message === '' ? original.name : `${original.name}: ${original.message}`
  const message = redactText(
    `${primary}${options.extra ?? ''}${hostDetails === '' ? '' : `\nHost stderr:\n${hostDetails}`}`,
    options.secret,
  )
  return new Error(message)
}

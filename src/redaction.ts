const REDACTED = '<redacted>'

/**
 * Remove launch tokens, cookies, and known literal secrets from diagnostic text.
 * @param text - arbitrary log, error, or JSON text.
 * @param secrets - exact credential values known to the caller.
 * @returns text safe to print.
 */
export function redactSecrets(text: string, secrets: readonly string[] = []): string {
  let safe = text
    .replace(/((?:^|[?&\s])(?:token|launchToken|launch_token)=)[^\s&"'#]+/gimu, `$1${REDACTED}`)
    .replace(/((?:^|\n)\s*(?:set-)?cookie\s*:\s*)[^\r\n]+/gimu, `$1${REDACTED}`)
    .replace(/(dsh-auth-[A-Za-z0-9_-]+=)[^;\s"']+/gu, `$1${REDACTED}`)
  for (const secret of secrets) {
    if (secret === '') continue
    safe = safe.split(secret).join(REDACTED)
  }
  return safe
}

/**
 * Redact one JSON-serializable value by rendering and re-parsing it.
 * @param value - value whose string leaves may carry credentials.
 * @param secrets - exact credential values.
 * @returns a structurally equal value with every string redacted.
 */
export function redactValue<T>(value: T, secrets: readonly string[] = []): T {
  return redactUnknown(value, secrets) as T
}

function redactUnknown(value: unknown, secrets: readonly string[]): unknown {
  if (typeof value === 'string') return redactSecrets(value, secrets)
  if (Array.isArray(value)) return value.map(item => redactUnknown(item, secrets))
  if (typeof value === 'object' && value !== null) {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, redactUnknown(item, secrets)]),
    )
  }
  return value
}

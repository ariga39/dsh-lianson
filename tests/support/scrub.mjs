/** Token-safe diagnostics for harness and browser failures. */

export function tokenOfUrl(value) {
  try {
    return new URL(value).searchParams.get('token') ?? undefined
  } catch {
    return undefined
  }
}

export function scrubTokenDiagnostics(input, token) {
  let text = String(input)
  text = text.replace(/([?&]token=)[^&#\s)"'`]+/giu, '$1<redacted>')
  if (token !== undefined && token !== '') {
    for (const candidate of new Set([token, encodeURIComponent(token)])) {
      text = text.split(candidate).join('<redacted>')
    }
  }
  return text
}

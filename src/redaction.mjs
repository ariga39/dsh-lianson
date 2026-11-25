/** Credential redaction shared by process output, errors, IPC responses, and CLI output. */

const TOKEN_QUERY = /([?&](?:token|access_token|api[_-]?key|launch[_-]?token)=)[^\s&#"'<>]+/giu
const AUTH_HEADER = /^(\s*(?:cookie|set-cookie)\s*:\s*)[^\r\n]*/gimu
const BEARER = /\bBearer\s+[A-Za-z0-9._~+/=-]+/giu
const API_COOKIE = /\bdsh-auth-[A-Za-z0-9_-]+=[A-Za-z0-9._~-]+/gu

/**
 * Redact URL tokens, cookie headers, bearer credentials, and caller-supplied
 * literals. Extra secrets shorter than 6 characters are ignored so a short
 * accidental value cannot corrupt ordinary output.
 * @param {unknown} value
 * @param {Iterable<string>} [extraSecrets]
 * @returns {string}
 */
export function redactSecrets(value, extraSecrets = []) {
  let text = String(value ?? '')
  text = text.replace(TOKEN_QUERY, '$1<redacted>')
  text = text.replace(AUTH_HEADER, '$1<redacted>')
  text = text.replace(BEARER, 'Bearer <redacted>')
  text = text.replace(API_COOKIE, 'dsh-auth-<redacted>')
  const secrets = [...new Set(extraSecrets)]
    .filter(secret => typeof secret === 'string' && secret.length >= 6)
    .sort((left, right) => right.length - left.length)
  for (const secret of secrets) text = text.split(secret).join('<redacted>')
  return text
}

/**
 * Build a redactor bound to runtime credentials such as the Host launch token
 * and the active v2 cookie.
 * @param {Iterable<string>} extraSecrets
 */
export function createRedactor(extraSecrets = []) {
  const secrets = () => [...extraSecrets]
  return value => redactSecrets(value, secrets())
}

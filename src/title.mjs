/** Host-compatible session-title normalization for the delegation ledger. */

// Mirrors @deepseek-ai/dsh-session-title normalize.ts at the pinned upstream
// commit. The base profile accepts at most 80 UTF-8 bytes per title.
const MAX_TITLE_BYTES = 80
const OSC_SEQUENCE = /(?:\u001B\]|\u009D)(?:(?!\u0007|\u001B\\)[\s\S])*(?:\u0007|\u001B\\|$)/gu
const CSI_SEQUENCE = /(?:\u001B\[|\u009B)[0-?]*[ -/]*[@-~]/gu
const ESC_SEQUENCE = /\u001B[@-_]/gu
const CONTROL_CHARACTER = /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F]/gu
const DIRECTIONAL_CONTROL = /[\u200B\u200E\u200F\u202A-\u202E\u2060-\u2064\u2066-\u206F\uFEFF]/gu

function cleanTitleText(input) {
  return input
    .replace(OSC_SEQUENCE, '')
    .replace(CSI_SEQUENCE, '')
    .replace(ESC_SEQUENCE, '')
    .replace(CONTROL_CHARACTER, '')
    .replace(DIRECTIONAL_CONTROL, '')
    .replace(/\s+/gu, ' ')
    .trim()
}

function truncateTitleUtf8(input, maxBytes) {
  if (Buffer.byteLength(input, 'utf8') <= maxBytes) return input
  let used = 0
  let output = ''
  for (const character of input) {
    const bytes = Buffer.byteLength(character, 'utf8')
    if (used + bytes > maxBytes) break
    output += character
    used += bytes
  }
  return output
}

/**
 * Normalize one title exactly as the Host's title service does for the base
 * profile, so ledger uniqueness and database storage use the accepted title.
 * @param {string} title - untrusted caller title.
 * @returns the normalized title, possibly empty.
 */
export function normalizeSessionTitle(title) {
  return truncateTitleUtf8(cleanTitleText(title), MAX_TITLE_BYTES).trimEnd()
}

/** Split arbitrary process output chunks into complete launch lines. */
export interface LaunchLineCollector {
  /** Append one stream chunk and return the complete lines it finished. */
  push(chunk: string): string[]
  /** Return the unterminated tail, if any, when the process closed. */
  finish(): string[]
}

/** Create a line accumulator that never emits a partial line. */
export function createLaunchLineCollector(): LaunchLineCollector {
  let pending = ''
  return {
    push(chunk: string): string[] {
      pending += chunk
      const parts = pending.split('\n')
      pending = parts.pop() ?? ''
      return parts.map(stripCarriageReturn)
    },
    finish(): string[] {
      if (pending === '') return []
      const tail = stripCarriageReturn(pending)
      pending = ''
      return [tail]
    },
  }
}

/**
 * Pick the authenticated application URL from complete `dsh web:` output lines.
 * @param lines - complete stdout lines from the Host.
 * @returns the first launched application URL carrying a token query parameter.
 */
export function findLaunchUrl(lines: readonly string[]): string | undefined {
  const candidates: string[] = []
  for (const line of lines) {
    const match = /^dsh web:\s+(https?:\/\/\S+)\s*$/u.exec(line)
    const candidate = match?.[1]
    if (candidate !== undefined) candidates.push(candidate)
  }
  return candidates.find((candidate) => {
    try {
      return new URL(candidate).searchParams.has('token')
    } catch {
      return false
    }
  })
}

/**
 * Remove launch tokens from diagnostic text.
 * @param text - arbitrary process output.
 * @returns the text with every token query value replaced.
 */
export function redact(text: string): string {
  return text.replace(/([?&]token=)[^\s&)]+/gu, '$1<redacted>')
}

function stripCarriageReturn(line: string): string {
  return line.endsWith('\r') ? line.slice(0, -1) : line
}

import { existsSync, mkdtempSync, renameSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { captureCommand, runCommand } from './commands.ts'

/** Inputs for staging one exact upstream commit. */
export interface StageUpstreamOptions {
  readonly url: string
  readonly commit: string
  readonly destination: string
  /** Directory that holds the temporary staging checkout. */
  readonly stagingParent: string
}

/**
 * Shallow-fetch one exact commit into a staging checkout, then rename it into place.
 * User Git hooks are disabled for every command and a failed staging attempt is removed.
 * @param options - remote URL, commit, destination, and staging parent.
 */
export function stageUpstream(options: StageUpstreamOptions): void {
  if (existsSync(options.destination)) {
    throw new Error(`${options.destination} already exists; refusing to replace it`)
  }
  const staging = mkdtempSync(join(options.stagingParent, '.deepseek-harness.staging-'))
  try {
    runCommand('git', ['init', '--quiet'], staging)
    runCommand('git', ['remote', 'add', 'origin', options.url], staging)
    runCommand('git', [
      '-c', 'core.hooksPath=/dev/null',
      'fetch', '--depth', '1', '--no-tags', 'origin', options.commit,
    ], staging)
    runCommand('git', ['-c', 'core.hooksPath=/dev/null', 'checkout', '--detach', 'FETCH_HEAD'], staging)
    const head = captureCommand('git', ['rev-parse', 'HEAD'], staging).trim()
    if (head !== options.commit) {
      throw new Error(`staged upstream is at ${head}, expected ${options.commit}`)
    }
    if (existsSync(options.destination)) {
      throw new Error(`${options.destination} appeared during staging; refusing to replace it`)
    }
    renameSync(staging, options.destination)
  } finally {
    // A failed partial staging must not poison the next attempt.
    rmSync(staging, { recursive: true, force: true })
  }
}

/**
 * Verify that the checkout is the pinned clean commit.
 * @param upstreamRoot - DeepSeek Harness checkout.
 * @param projectRoot - directory from which the relative checkout is inspected.
 * @param commit - exact expected commit.
 */
export function assertUpstreamIdentity(upstreamRoot: string, projectRoot: string, commit: string): void {
  const head = captureCommand('git', ['-C', upstreamRoot, 'rev-parse', 'HEAD'], projectRoot).trim()
  if (head !== commit) {
    throw new Error(`upstream deepseek-harness is at ${head}, expected ${commit}`)
  }
  const status = captureCommand(
    'git', ['-C', upstreamRoot, 'status', '--porcelain', '--untracked-files=normal'], projectRoot,
  ).trim()
  if (status !== '') {
    throw new Error(`upstream deepseek-harness is not clean; refusing to modify it:\n${status}`)
  }
}

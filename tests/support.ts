/** Test roots outside the platform temp areas so trust accepts them. */

import { mkdir, mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'

/** Parent for ordinary project fixtures; upstream always treats `/tmp` as globally writable. */
const BASE = join(process.cwd(), '.cache', 'dsh-liaison-tests')

/**
 * Create an ordinary, isolated test directory.
 * @param prefix - descriptive directory prefix.
 * @returns the created absolute directory.
 */
export async function makeTestRoot(prefix: string): Promise<string> {
  await mkdir(BASE, { recursive: true, mode: 0o700 })
  return mkdtemp(join(BASE, prefix))
}

/**
 * Remove one test directory tree.
 * @param root - directory returned by {@link makeTestRoot}.
 */
export async function removeTestRoot(root: string): Promise<void> {
  await rm(root, { recursive: true, force: true })
}

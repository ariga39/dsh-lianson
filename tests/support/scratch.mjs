import { mkdir, mkdtemp } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'

/**
 * Hermetic test scratch outside the Host's globally writable temp roots.
 * Delegation cwds under os.tmpdir() are intentionally rejected by production
 * validation, so integration fixtures that create delegations must live here.
 */
export async function createScratchRoot(prefix) {
  const base = join(homedir(), '.cache', 'dsh-liaison-tests')
  await mkdir(base, { recursive: true, mode: 0o700 })
  return mkdtemp(join(base, prefix))
}

import { randomUUID } from 'node:crypto'
import { chmod, mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'

/**
 * Create a state directory with owner-only permissions even when it already exists.
 * @param directory - absolute directory path.
 */
export async function ensurePrivateDirectory(directory: string): Promise<void> {
  await mkdir(directory, { recursive: true, mode: 0o700 })
  await chmod(directory, 0o700)
}

/**
 * Atomically replace a private file: temp file, explicit chmod, then rename.
 * @param path - destination path.
 * @param content - complete file text.
 * @param mode - final permission bits, default `0600`.
 */
export async function writePrivateFile(path: string, content: string, mode = 0o600): Promise<void> {
  const temporary = `${path}.${String(process.pid)}.${randomUUID()}.tmp`
  try {
    await writeFile(temporary, content, { encoding: 'utf8', mode })
    await chmod(temporary, mode)
    await rename(temporary, path)
  } catch (error) {
    await rm(temporary, { force: true }).catch(() => undefined)
    throw error
  }
}

/**
 * Read a file, treating absence as undefined.
 * @param path - file path.
 * @param encoding - text encoding, default utf8.
 * @returns the contents, or undefined when the file does not exist.
 */
export async function readOptionalFile(path: string, encoding: BufferEncoding = 'utf8'): Promise<string | undefined> {
  try {
    return await readFile(path, encoding)
  } catch (error) {
    if (isMissing(error)) return undefined
    throw error
  }
}

/**
 * Remove a file or empty directory, ignoring absence.
 * @param path - path to remove.
 */
export async function removeIfExists(path: string): Promise<void> {
  await rm(path, { force: true, recursive: false })
}

/**
 * Whether an error reports a missing filesystem entry.
 * @param error - thrown value.
 * @returns true for ENOENT/ESRCH.
 */
export function isMissing(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | null)?.code
  return code === 'ENOENT' || code === 'ESRCH'
}

/**
 * Whether an error reports a permission failure.
 * @param error - thrown value.
 * @returns true for EACCES/EPERM.
 */
export function isPermissionDenied(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | null)?.code
  return code === 'EACCES' || code === 'EPERM'
}

/**
 * Ensure the parent directory exists before writing a sibling temporary file.
 * @param path - file whose parent must exist.
 */
export async function ensureParent(path: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
}

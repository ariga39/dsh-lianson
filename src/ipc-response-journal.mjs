/** Durable replay journal for successful delegated create responses. */

import { randomUUID } from 'node:crypto'
import { chmod, mkdir, open, readFile, rename, unlink } from 'node:fs/promises'
import { dirname } from 'node:path'

const VERSION = 1
const MAX_ENTRIES = 4_096

function clone(value) {
  return structuredClone(value)
}

/** A small atomic JSON journal keyed by caller IPC request id. */
export class IpcResponseJournal {
  #filePath
  #entries = new Map()
  #loadPromise
  #tail = Promise.resolve()

  /** @param {{ filePath: string }} options - absolute journal path. */
  constructor({ filePath }) {
    if (typeof filePath !== 'string' || filePath === '') {
      throw new TypeError('IpcResponseJournal requires filePath')
    }
    this.#filePath = filePath
  }

  /** Load persisted entries once. */
  async load() {
    return this.#load()
  }

  /**
   * Read one persisted response entry.
   * @param {string} id - caller IPC request id.
   */
  async get(id) {
    await this.#load()
    const entry = this.#entries.get(id)
    return entry === undefined ? undefined : clone(entry)
  }

  /**
   * Persist one successful response entry before the caller receives it.
   * @param {string} id - caller IPC request id.
   * @param {{ fingerprint: string, outcome: object }} entry - replayable result.
   */
  async set(id, entry) {
    return this.#enqueue(async () => {
      await this.#load()
      this.#entries.set(id, { ...clone(entry), storedAt: new Date().toISOString() })
      if (this.#entries.size > MAX_ENTRIES) {
        this.#entries.delete(this.#entries.keys().next().value)
      }
      await this.#persist()
    })
  }

  #enqueue(operation) {
    const running = this.#tail.then(operation, operation)
    this.#tail = running.then(() => undefined, () => undefined)
    return running
  }

  async #load() {
    this.#loadPromise ??= this.#read()
    return this.#loadPromise
  }

  async #read() {
    let text
    try {
      text = await readFile(this.#filePath, 'utf8')
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error
      return
    }
    if (text.trim() === '') return
    let parsed
    try {
      parsed = JSON.parse(text)
    } catch {
      // A replay cache is advisory. A damaged file must not prevent the
      // daemon from accepting new work.
      return
    }
    if (parsed?.version !== VERSION || !Array.isArray(parsed.responses)) return
    for (const entry of parsed.responses) {
      if (typeof entry?.id !== 'string' || typeof entry?.fingerprint !== 'string') continue
      if (entry.outcome === null || typeof entry.outcome !== 'object') continue
      this.#entries.set(entry.id, entry)
    }
  }

  async #persist() {
    const directory = dirname(this.#filePath)
    await mkdir(directory, { recursive: true, mode: 0o700 })
    const temporary = `${this.#filePath}.${process.pid}.${randomUUID()}.tmp`
    const body = `${JSON.stringify({
      version: VERSION,
      responses: [...this.#entries.entries()].map(([id, entry]) => ({ id, ...entry })),
    }, null, 2)}\n`
    let handle
    try {
      handle = await open(temporary, 'wx', 0o600)
      await handle.writeFile(body, 'utf8')
      await handle.sync()
      await handle.chmod(0o600)
    } catch (error) {
      await handle?.close().catch(() => {})
      handle = undefined
      await unlink(temporary).catch(() => {})
      throw error
    } finally {
      await handle?.close()
    }
    try {
      await rename(temporary, this.#filePath)
    } catch (error) {
      await unlink(temporary).catch(() => {})
      throw error
    }
    await chmod(this.#filePath, 0o600)
  }
}

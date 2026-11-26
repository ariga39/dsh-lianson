import { strict as assert } from 'node:assert'
import { spawn } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { HealthResponseSchema, PingResponseSchema } from '../../src/contract.ts'

const projectRoot = fileURLToPath(new URL('../..', import.meta.url))
const cliPath = join(projectRoot, 'bin', 'dsh-liaison.ts')

interface CliResult {
  readonly code: number | null
  readonly stdout: string
  readonly stderr: string
}

async function runCli(args: readonly string[], env: Readonly<Record<string, string>>): Promise<CliResult> {
  return await new Promise<CliResult>((resolve, reject) => {
    const child = spawn(process.execPath, [cliPath, ...args], {
      cwd: projectRoot,
      env: { ...process.env, NODE_NO_WARNINGS: '1', ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    const stdout: string[] = []
    const stderr: string[] = []
    child.stdout.setEncoding('utf8')
    child.stderr.setEncoding('utf8')
    child.stdout.on('data', (chunk: string) => { stdout.push(chunk) })
    child.stderr.on('data', (chunk: string) => { stderr.push(chunk) })
    child.once('error', reject)
    child.once('close', (code) => {
      resolve({ code, stdout: stdout.join(''), stderr: stderr.join('') })
    })
  })
}

async function readBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of request) chunks.push(Buffer.from(chunk as Buffer))
  return Buffer.concat(chunks).toString('utf8')
}

async function withFakeServer(
  handler: (request: IncomingMessage, response: ServerResponse) => Promise<void> | void,
  run: (socketPath: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'liaison-cli-'))
  const socketPath = join(root, 'liaison.sock')
  const server = createServer((request, response) => {
    void Promise.resolve(handler(request, response)).catch((error: unknown) => {
      response.writeHead(500)
      response.end(String(error))
    })
  })
  try {
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject)
      server.listen(socketPath, () => {
        server.off('error', reject)
        resolve()
      })
    })
    await run(socketPath)
  } finally {
    await new Promise<void>((resolve) => { server.close(() => { resolve() }) })
    await rm(root, { recursive: true, force: true })
  }
}

await test('status prints the validated health document without credentials', async () => {
  const health = HealthResponseSchema.parse({
    ok: true,
    service: 'dsh-liaison',
    pid: 123,
    dshHome: '/tmp/dsh-home',
    origin: 'http://127.0.0.1:4321',
    socket: '/tmp/liaison.sock',
    startedAt: '2026-11-26T00:00:00.000Z',
    loopback: { connected: true, generation: 1, reauthentications: 0 },
  })
  let seenPath: string | undefined
  let seenMethod: string | undefined
  await withFakeServer((request, response) => {
    seenPath = request.url
    seenMethod = request.method
    response.writeHead(200, { 'content-type': 'application/json' })
    response.end(JSON.stringify(health))
  }, async (socketPath) => {
    const result = await runCli(['status'], { DSH_LIAISON_SOCKET: socketPath })
    assert.equal(result.code, 0, result.stderr)
    assert.equal(result.stderr, '')
    assert.equal(seenPath, '/health')
    assert.equal(seenMethod, 'GET')
    assert.deepEqual(JSON.parse(result.stdout), health)
    assert.doesNotMatch(result.stdout, /token|cookie/iu)
  })
})

await test('ping posts a validated nonce and prints the authenticated result', async () => {
  let seenPath: string | undefined
  let seenMethod: string | undefined
  let seenBody: unknown
  await withFakeServer(async (request, response) => {
    seenPath = request.url
    seenMethod = request.method
    seenBody = JSON.parse(await readBody(request)) as unknown
    const body = PingResponseSchema.parse({
      ok: true,
      pong: true,
      nonce: 'unit-nonce',
      generation: 2,
      reauthentications: 1,
      models: 3,
    })
    response.writeHead(200, { 'content-type': 'application/json' })
    response.end(JSON.stringify(body))
  }, async (socketPath) => {
    const result = await runCli(['ping', '--nonce', 'unit-nonce'], { DSH_LIAISON_SOCKET: socketPath })
    assert.equal(result.code, 0, result.stderr)
    assert.equal(seenPath, '/ping')
    assert.equal(seenMethod, 'POST')
    assert.deepEqual(seenBody, { nonce: 'unit-nonce' })
    assert.deepEqual(JSON.parse(result.stdout), {
      ok: true,
      pong: true,
      nonce: 'unit-nonce',
      generation: 2,
      reauthentications: 1,
      models: 3,
    })
    assert.doesNotMatch(result.stdout, /token|cookie/iu)
  })
})

await test('an absent endpoint returns structured JSON without spawning anything', async () => {
  const root = await mkdtemp(join(tmpdir(), 'liaison-absent-'))
  const socketPath = join(root, 'missing.sock')
  try {
    for (const command of ['status', 'ping']) {
      const result = await runCli([command], { DSH_LIAISON_SOCKET: socketPath })
      assert.equal(result.code, 1)
      const parsed: unknown = JSON.parse(result.stdout)
      assert.ok(typeof parsed === 'object' && parsed !== null)
      assert.equal(Reflect.get(parsed, 'ok'), false)
      const error: unknown = Reflect.get(parsed, 'error')
      assert.ok(typeof error === 'object' && error !== null)
      assert.equal(Reflect.get(error, 'code'), 'endpoint-unreachable')
      assert.match(String(Reflect.get(error, 'message')), /ENOENT|connect/iu)
      await assert.rejects(readFile(socketPath), /ENOENT/u)
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

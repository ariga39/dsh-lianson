import { strict as assert } from 'node:assert'
import { join, resolve } from 'node:path'
import { test } from 'node:test'
import { resolveLiaisonLockPath, resolveLiaisonSocketPath } from '../../src/socket-path.ts'

await test('DSH_LIAISON_SOCKET overrides the default socket path', () => {
  assert.equal(
    resolveLiaisonSocketPath({ DSH_LIAISON_SOCKET: '/tmp/custom/liaison.sock' }),
    resolve('/tmp/custom/liaison.sock'),
  )
})

await test('the XDG runtime directory is the default socket root', () => {
  assert.equal(
    resolveLiaisonSocketPath({ XDG_RUNTIME_DIR: '/run/user/1000' }),
    join('/run/user/1000', 'dsh-liaison', 'dsh-liaison.sock'),
  )
})

await test('the state directory is the fallback when no runtime directory is configured', () => {
  assert.equal(
    resolveLiaisonSocketPath({ HOME: '/home/operator' }),
    join('/home/operator', '.local', 'state', 'dsh-liaison', 'dsh-liaison.sock'),
  )
})

await test('blank overrides are ignored', () => {
  assert.equal(
    resolveLiaisonSocketPath({ XDG_RUNTIME_DIR: '  ', DSH_LIAISON_SOCKET: '   ', HOME: '/home/operator' }),
    join('/home/operator', '.local', 'state', 'dsh-liaison', 'dsh-liaison.sock'),
  )
})

await test('the lock file is a sibling that is never unlinked', () => {
  assert.equal(resolveLiaisonLockPath('/run/user/1000/dsh-liaison/dsh-liaison.sock'), '/run/user/1000/dsh-liaison/dsh-liaison.sock.lock')
})

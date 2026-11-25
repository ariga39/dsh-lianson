import assert from 'node:assert/strict'
import { join } from 'node:path'
import { test } from 'node:test'
import { resolveDelegationStateDir, resolveStateDir } from '../src/host-manager.mjs'

test('runtime state stays on the original #2 path and the durable ledger is separate', () => {
  assert.equal(
    resolveStateDir({
      DSH_LIAISON_STATE_DIR: '/tmp/fixture-state',
      XDG_RUNTIME_DIR: '/run/user/12345',
      XDG_STATE_HOME: '/tmp/xdg-state',
      HOME: '/home/fixture',
    }),
    '/tmp/fixture-state',
    'explicit test/fixture override wins for transport state',
  )
  assert.equal(
    resolveDelegationStateDir({
      DSH_LIAISON_STATE_DIR: '/tmp/fixture-state',
      XDG_RUNTIME_DIR: '/run/user/12345',
      XDG_STATE_HOME: '/tmp/xdg-state',
      HOME: '/home/fixture',
    }),
    '/tmp/fixture-state',
    'explicit override isolates both runtime and durable state together',
  )
  assert.equal(
    resolveStateDir({
      XDG_RUNTIME_DIR: '/run/user/12345',
      XDG_STATE_HOME: '/tmp/xdg-state',
      HOME: '/home/fixture',
    }),
    join('/run/user/12345', 'dsh-liaison'),
    'Host/socket discovery keeps the original runtime path',
  )
  assert.equal(
    resolveDelegationStateDir({
      XDG_RUNTIME_DIR: '/run/user/12345',
      XDG_STATE_HOME: '/tmp/xdg-state',
      HOME: '/home/fixture',
    }),
    join('/tmp/xdg-state', 'dsh-liaison'),
    'the durable ledger ignores XDG_RUNTIME_DIR',
  )
  assert.equal(
    resolveDelegationStateDir({
      XDG_RUNTIME_DIR: '/run/user/12345',
      HOME: '/home/fixture',
    }),
    join('/home/fixture', '.local', 'state', 'dsh-liaison'),
  )
  assert.equal(
    resolveDelegationStateDir({
      XDG_STATE_HOME: 'relative-state',
      HOME: '/home/fixture',
    }),
    join('/home/fixture', '.local', 'state', 'dsh-liaison'),
    'a relative XDG_STATE_HOME is ignored',
  )
})

import assert from 'node:assert/strict'
import { join } from 'node:path'
import { test } from 'node:test'
import { resolveStateDir } from '../src/host-manager.mjs'

test('state directory stays durable and isolated from runtime directories', () => {
  assert.equal(
    resolveStateDir({
      DSH_LIAISON_STATE_DIR: '/tmp/fixture-state',
      XDG_STATE_HOME: '/tmp/xdg-state',
      HOME: '/home/fixture',
    }),
    '/tmp/fixture-state',
    'explicit test/fixture override wins',
  )
  assert.equal(
    resolveStateDir({
      XDG_STATE_HOME: '/tmp/xdg-state',
      HOME: '/home/fixture',
    }),
    join('/tmp/xdg-state', 'dsh-liaison'),
  )
  assert.equal(
    resolveStateDir({
      XDG_RUNTIME_DIR: '/run/user/12345',
      HOME: '/home/fixture',
    }),
    join('/home/fixture', '.local', 'state', 'dsh-liaison'),
    'XDG_RUNTIME_DIR must not hold the durable ledger',
  )
  assert.equal(
    resolveStateDir({
      XDG_STATE_HOME: 'relative-state',
      HOME: '/home/fixture',
    }),
    join('/home/fixture', '.local', 'state', 'dsh-liaison'),
    'a relative XDG_STATE_HOME is ignored',
  )
})

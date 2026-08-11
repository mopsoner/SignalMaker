import assert from 'node:assert/strict'
import test from 'node:test'

import { createPollingController } from './usePollingQuery.js'

class VisibilitySource extends EventTarget {
  visibilityState = 'visible'

  setVisibility(state) {
    this.visibilityState = state
    this.dispatchEvent(new Event('visibilitychange'))
  }
}

const flushPromises = () => new Promise((resolve) => setImmediate(resolve))

test('does not poll while the document is hidden and resumes once when visible', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const visibility = new VisibilitySource()
  visibility.visibilityState = 'hidden'
  let calls = 0
  const controller = createPollingController(async () => { calls += 1 }, 1000, visibility)

  controller.start()
  t.mock.timers.tick(5000)
  await flushPromises()
  assert.equal(calls, 0)

  visibility.setVisibility('visible')
  await flushPromises()
  assert.equal(calls, 1)
  t.mock.timers.tick(999)
  await flushPromises()
  assert.equal(calls, 1)
  t.mock.timers.tick(1)
  await flushPromises()
  assert.equal(calls, 2)
  controller.stop()
})

test('shares an in-flight request between polling and manual refreshes', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const visibility = new VisibilitySource()
  let resolveRequest
  let calls = 0
  const controller = createPollingController(() => {
    calls += 1
    return new Promise((resolve) => { resolveRequest = resolve })
  }, 1000, visibility)

  controller.start()
  await flushPromises()
  assert.equal(calls, 1)
  const first = controller.refresh()
  const second = controller.refresh()
  assert.strictEqual(first, second)
  assert.equal(calls, 1)
  t.mock.timers.tick(5000)
  assert.equal(calls, 1)

  resolveRequest()
  await first
  await flushPromises()
  t.mock.timers.tick(1000)
  await flushPromises()
  assert.equal(calls, 2)
  controller.stop()
})

test('cleans up its timer and visibility listener when stopped', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const visibility = new VisibilitySource()
  let calls = 0
  const controller = createPollingController(async () => { calls += 1 }, 1000, visibility)

  controller.start()
  await flushPromises()
  assert.equal(calls, 1)
  controller.stop()
  t.mock.timers.tick(5000)
  visibility.setVisibility('hidden')
  visibility.setVisibility('visible')
  await flushPromises()
  assert.equal(calls, 1)
})

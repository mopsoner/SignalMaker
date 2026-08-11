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

function coordinationPair() {
  let leader = null
  const listeners = new Set()
  const make = (id) => ({
    claim() { leader ??= id; return leader === id },
    isLeader() { return leader === id },
    publish(message) { listeners.forEach((listener) => listener(message)) },
    subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener) },
    close() { if (leader === id) leader = null },
  })
  return [make('first'), make('second')]
}

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

test('two consumers of the same endpoint share polling results and fail over', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const [firstCoordination, secondCoordination] = coordinationPair()
  let requests = 0
  const received = [[], []]
  const loader = async () => ({ request: ++requests })
  const first = createPollingController(loader, 1000, new VisibilitySource(), {
    coordination: firstCoordination,
    onResult: (value) => received[0].push(value),
  })
  const second = createPollingController(loader, 1000, new VisibilitySource(), {
    coordination: secondCoordination,
    onResult: (value) => received[1].push(value),
  })

  first.start()
  second.start()
  await flushPromises()
  assert.equal(requests, 1)
  assert.deepEqual(received, [[{ request: 1 }], [{ request: 1 }]])

  first.stop()
  t.mock.timers.tick(1000)
  await flushPromises()
  assert.equal(requests, 2)
  assert.deepEqual(received[1], [{ request: 1 }, { request: 2 }])
  second.stop()
})

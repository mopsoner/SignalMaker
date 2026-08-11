const PREFIX = 'signalmaker:polling:'

const randomId = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`

// A short, deterministic identifier keeps localStorage and BroadcastChannel
// names manageable without making callers maintain endpoint keys by hand.
export function pollingKey(value) {
  let hash = 2166136261
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return (hash >>> 0).toString(36)
}

export function createPollingCoordination(key, options = {}) {
  const storage = options.storage ?? (typeof localStorage === 'undefined' ? null : localStorage)
  const Channel = options.BroadcastChannel ?? globalThis.BroadcastChannel
  if (!key || !storage) return null

  const owner = options.owner ?? randomId()
  const ttl = options.ttl ?? 10000
  const lockKey = `${PREFIX}lock:${key}`
  const channel = Channel ? new Channel(`${PREFIX}results:${key}`) : null
  const listeners = new Set()
  let heartbeat = null

  const readLock = () => {
    try { return JSON.parse(storage.getItem(lockKey) || 'null') } catch { return null }
  }
  const writeLock = () => storage.setItem(lockKey, JSON.stringify({ owner, expiresAt: Date.now() + ttl }))
  const isLeader = () => {
    const lock = readLock()
    return lock?.owner === owner && lock.expiresAt > Date.now()
  }
  const claim = () => {
    const lock = readLock()
    if (!lock || lock.expiresAt <= Date.now() || lock.owner === owner) {
      try { writeLock() } catch { return false }
    }
    const won = isLeader()
    if (won && heartbeat === null) heartbeat = setInterval(writeLock, Math.max(1000, Math.floor(ttl / 3)))
    return won
  }
  const handleMessage = (event) => listeners.forEach((listener) => listener(event.data))
  if (channel) channel.addEventListener('message', handleMessage)

  return {
    claim,
    isLeader,
    publish(message) { channel?.postMessage(message) },
    subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener) },
    close() {
      if (heartbeat !== null) clearInterval(heartbeat)
      heartbeat = null
      if (isLeader()) {
        try { storage.removeItem(lockKey) } catch {}
      }
      channel?.removeEventListener('message', handleMessage)
      channel?.close()
      listeners.clear()
    },
  }
}

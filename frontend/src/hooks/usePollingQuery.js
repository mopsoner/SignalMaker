import { useCallback, useEffect, useRef, useState } from 'react'
import { createPollingCoordination, pollingKey } from './pollingCoordination.js'

const isDocumentVisible = (visibilitySource) => !visibilitySource || visibilitySource.visibilityState !== 'hidden'

// Kept separate from React state so the timer, visibility listener, and manual
// refresh all share the same in-flight request guard.
export function createPollingController(run, interval, visibilitySource = typeof document === 'undefined' ? null : document, options = {}) {
  let active = false
  let timer = null
  let inFlight = null
  const coordination = options.coordination ?? null
  const unsubscribe = coordination?.subscribe((message) => {
    if (!active || message?.type !== 'result') return
    options.onResult?.(message.value)
  })

  const clearTimer = () => {
    if (timer !== null) clearTimeout(timer)
    timer = null
  }

  const refresh = (manual = true) => {
    if (!inFlight) {
      inFlight = Promise.resolve()
        .then(() => {
          // Manual refreshes are intentional user actions. Periodic/initial
          // refreshes, however, are performed only by the elected tab.
          if (!manual && coordination && !coordination.claim()) return undefined
          return run()
        })
        .then((value) => {
          if (value !== undefined && (!coordination || coordination.isLeader())) {
            coordination?.publish({ type: 'result', value })
          }
          return value
        })
        .finally(() => { inFlight = null })
    }
    return inFlight
  }

  const schedule = () => {
    clearTimer()
    if (!active || !isDocumentVisible(visibilitySource)) return
    timer = setTimeout(async () => {
      timer = null
      await refresh(false)
      // The delay starts after the response, preventing slow requests from
      // overlapping with the next polling request.
      schedule()
    }, interval)
  }

  const refreshAndSchedule = async () => {
    if (!active || !isDocumentVisible(visibilitySource)) return
    await refresh(false)
    schedule()
  }

  const handleVisibilityChange = () => {
    clearTimer()
    if (isDocumentVisible(visibilitySource)) void refreshAndSchedule()
  }

  return {
    start() {
      if (active) return
      active = true
      visibilitySource?.addEventListener('visibilitychange', handleVisibilityChange)
      void refreshAndSchedule()
    },
    stop() {
      active = false
      clearTimer()
      visibilitySource?.removeEventListener('visibilitychange', handleVisibilityChange)
      unsubscribe?.()
      coordination?.close()
    },
    refresh,
  }
}

export function usePollingQuery(loader, interval = 15000, options = {}) {
  const { enabled = true } = options
  const [data, setData] = useState(undefined)
  const [loading, setLoading] = useState(enabled)
  const [error, setError] = useState(null)
  const loaderRef = useRef(loader)
  const mountedRef = useRef(true)
  const controllerRef = useRef(null)
  loaderRef.current = loader

  useEffect(() => () => { mountedRef.current = false }, [])

  const run = useCallback(async () => {
    if (!enabled) return undefined
    if (mountedRef.current) setLoading(true)
    try {
      const value = await loaderRef.current()
      if (mountedRef.current) {
        setData(value)
        setError(null)
      }
      return value
    } catch (err) {
      if (mountedRef.current) setError(err.message || String(err))
      return undefined
    } finally {
      if (mountedRef.current) setLoading(false)
    }
  }, [enabled])

  useEffect(() => {
    if (!enabled) {
      setLoading(false)
      controllerRef.current = null
      return undefined
    }

    const locationKey = typeof window === 'undefined' ? '' : `${window.location.pathname}${window.location.search}`
    const identity = options.queryKey ?? `${locationKey}:${interval}:${loaderRef.current.toString()}`
    const coordination = createPollingCoordination(pollingKey(identity), { ttl: Math.max(10000, interval * 2) })
    const controller = createPollingController(run, interval, undefined, {
      coordination,
      onResult(value) {
        if (mountedRef.current) {
          setData(value)
          setError(null)
          setLoading(false)
        }
      },
    })
    controllerRef.current = controller
    controller.start()
    return () => {
      controller.stop()
      if (controllerRef.current === controller) controllerRef.current = null
    }
  }, [enabled, interval, options.queryKey, run])

  const refresh = useCallback(() => {
    if (!enabled) return Promise.resolve(undefined)
    return controllerRef.current?.refresh() ?? run()
  }, [enabled, run])

  return { data, loading, error, refresh }
}

import { useCallback, useEffect, useRef, useState } from 'react'

const isDocumentVisible = (visibilitySource) => !visibilitySource || visibilitySource.visibilityState !== 'hidden'

// Kept separate from React state so the timer, visibility listener, and manual
// refresh all share the same in-flight request guard.
export function createPollingController(run, interval, visibilitySource = typeof document === 'undefined' ? null : document) {
  let active = false
  let timer = null
  let inFlight = null

  const clearTimer = () => {
    if (timer !== null) clearTimeout(timer)
    timer = null
  }

  const refresh = () => {
    if (!inFlight) {
      inFlight = Promise.resolve()
        .then(run)
        .finally(() => { inFlight = null })
    }
    return inFlight
  }

  const schedule = () => {
    clearTimer()
    if (!active || !isDocumentVisible(visibilitySource)) return
    timer = setTimeout(async () => {
      timer = null
      await refresh()
      // The delay starts after the response, preventing slow requests from
      // overlapping with the next polling request.
      schedule()
    }, interval)
  }

  const refreshAndSchedule = async () => {
    if (!active || !isDocumentVisible(visibilitySource)) return
    await refresh()
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

    const controller = createPollingController(run, interval)
    controllerRef.current = controller
    controller.start()
    return () => {
      controller.stop()
      if (controllerRef.current === controller) controllerRef.current = null
    }
  }, [enabled, interval, run])

  const refresh = useCallback(() => {
    if (!enabled) return Promise.resolve(undefined)
    return controllerRef.current?.refresh() ?? run()
  }, [enabled, run])

  return { data, loading, error, refresh }
}

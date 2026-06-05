import { useCallback, useEffect, useRef, useState } from 'react'

export function usePollingQuery(loader, interval = 15000, options = {}) {
  const { enabled = true } = options
  const [data, setData] = useState(undefined)
  const [loading, setLoading] = useState(enabled)
  const [error, setError] = useState(null)
  const loaderRef = useRef(loader)
  loaderRef.current = loader

  const run = useCallback(async () => {
    if (!enabled) return undefined
    setLoading(true)
    try {
      const value = await loaderRef.current()
      setData(value)
      setError(null)
      return value
    } catch (err) {
      setError(err.message || String(err))
      return undefined
    } finally {
      setLoading(false)
    }
  }, [enabled])

  useEffect(() => {
    if (!enabled) {
      setLoading(false)
      return undefined
    }
    let active = true
    async function runIfActive() {
      if (!active) return
      await run()
    }
    runIfActive()
    const timer = setInterval(runIfActive, interval)
    return () => {
      active = false
      clearInterval(timer)
    }
  }, [enabled, loader, interval, run])

  return { data, loading, error, refresh: run }
}

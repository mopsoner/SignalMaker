import { useCallback, useEffect, useRef, useState } from 'react'

export function usePollingQuery(loader, interval = 15000) {
  const [data, setData] = useState(undefined)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const loaderRef = useRef(loader)
  loaderRef.current = loader

  const run = useCallback(async () => {
    setLoading(true)
    try {
      const value = await loaderRef.current()
      setData(value)
      setError(null)
    } catch (err) {
      setError(err.message || String(err))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
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
  }, [loader, interval, run])

  return { data, loading, error, refresh: run }
}

import { useEffect, useState } from 'react'

export function usePollingQuery(loader, interval = 15000) {
  const [data, setData] = useState(undefined)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let active = true

    async function run() {
      try {
        const value = await loader()
        if (!active) return
        setData(value)
        setError(null)
      } catch (err) {
        if (!active) return
        setError(err.message || String(err))
      } finally {
        if (active) setLoading(false)
      }
    }

    run()
    const timer = setInterval(run, interval)
    return () => {
      active = false
      clearInterval(timer)
    }
  }, [loader, interval])

  return { data, loading, error }
}

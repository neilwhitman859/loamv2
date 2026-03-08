import { useEffect, useState } from 'react'
import { supabase } from '../lib/supabase'

interface Options {
  table: string
  id: string | undefined
  select: string
}

export function useEntityDetail<T = Record<string, unknown>>({ table, id, select }: Options) {
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!id) return
    setLoading(true)
    supabase
      .from(table)
      .select(select)
      .eq('id', id)
      .single()
      .then(({ data: row, error: err }) => {
        if (err) setError(err.message)
        else setData(row as T)
        setLoading(false)
      })
  }, [table, id, select])

  return { data, loading, error }
}

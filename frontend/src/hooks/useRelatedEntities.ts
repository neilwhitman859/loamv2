import { useEffect, useState, useCallback } from 'react'
import { supabase } from '../lib/supabase'

interface Options {
  table: string
  column: string
  value: string | undefined
  select: string
  pageSize?: number
  orderBy?: string
  ascending?: boolean
  enabled?: boolean
}

export function useRelatedEntities<T = Record<string, unknown>>(opts: Options) {
  const { table, column, value, select, pageSize = 20, orderBy = 'name', ascending = true, enabled = true } = opts
  const [data, setData] = useState<T[]>([])
  const [loading, setLoading] = useState(true)
  const [page, setPage] = useState(0)
  const [totalCount, setTotalCount] = useState(0)
  const [countFetched, setCountFetched] = useState(false)

  // Always fetch count eagerly (even when tab not active)
  const fetchCount = useCallback(async () => {
    if (!value || countFetched) return
    const { count } = await supabase
      .from(table)
      .select('*', { count: 'exact', head: true })
      .eq(column, value)
    setTotalCount(count ?? 0)
    setCountFetched(true)
  }, [table, column, value, countFetched])

  useEffect(() => { fetchCount() }, [fetchCount])

  const load = useCallback(async () => {
    if (!value || !enabled) { setLoading(false); return }
    setLoading(true)
    const from = page * pageSize
    const to = from + pageSize - 1
    const { data: rows, count, error } = await supabase
      .from(table)
      .select(select, { count: 'exact' })
      .eq(column, value)
      .order(orderBy, { ascending })
      .range(from, to)
    if (!error && rows) {
      setData(rows as T[])
      setTotalCount(count ?? 0)
    }
    setLoading(false)
  }, [table, column, value, select, page, pageSize, orderBy, ascending, enabled])

  useEffect(() => { load() }, [load])
  useEffect(() => { setPage(0) }, [value])

  return { data, loading, page, setPage, totalCount, pageSize }
}

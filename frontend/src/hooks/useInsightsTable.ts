import { useState, useEffect, useCallback, useRef } from 'react'
import { supabase } from '../lib/supabase'

interface Options {
  table: string
  nameColumn: string
  joinSelect?: string
  pageSize?: number
}

export function useInsightsTable({ table, nameColumn, joinSelect, pageSize = 50 }: Options) {
  const [data, setData] = useState<Record<string, unknown>[]>([])
  const [loading, setLoading] = useState(true)
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(0)
  const [totalCount, setTotalCount] = useState(0)
  const [sortColumn, setSortColumn] = useState(nameColumn)
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc')
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined)

  const fetchData = useCallback(async () => {
    setLoading(true)

    const selectStr = joinSelect || '*'
    let query = supabase
      .from(table)
      .select(selectStr, { count: 'exact' })
      .order(sortColumn, { ascending: sortDirection === 'asc' })
      .range(page * pageSize, (page + 1) * pageSize - 1)

    if (search.trim()) {
      query = query.ilike(nameColumn, `%${search.trim()}%`)
    }

    const { data: rows, count, error } = await query

    if (!error && rows) {
      setData(rows as unknown as Record<string, unknown>[])
      setTotalCount(count ?? 0)
    }
    setLoading(false)
  }, [table, nameColumn, joinSelect, search, page, pageSize, sortColumn, sortDirection])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  const handleSearch = (val: string) => {
    setSearch(val)
    setPage(0)
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {}, 300)
  }

  const handleSort = (col: string) => {
    if (col === sortColumn) {
      setSortDirection(d => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortColumn(col)
      setSortDirection('asc')
    }
    setPage(0)
  }

  return {
    data,
    loading,
    search,
    setSearch: handleSearch,
    page,
    setPage,
    totalCount,
    sortColumn,
    sortDirection,
    handleSort,
    pageSize,
  }
}

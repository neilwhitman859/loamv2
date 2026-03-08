import { useState, useEffect, useCallback, useRef } from 'react'
import { supabase } from '../lib/supabase'

interface Options {
  table: string
  nameColumn: string
  searchColumn?: string                    // Column to search (defaults to nameColumn)
  joinSelect?: string
  pageSize?: number
  defaultSortColumn?: string               // Initial sort column (defaults to nameColumn)
  defaultSortDirection?: 'asc' | 'desc'    // Initial sort direction (defaults to 'asc')
}

export function useInsightsTable({
  table,
  nameColumn,
  searchColumn,
  joinSelect,
  pageSize = 50,
  defaultSortColumn,
  defaultSortDirection = 'asc',
}: Options) {
  const [data, setData] = useState<Record<string, unknown>[]>([])
  const [loading, setLoading] = useState(true)
  const [search, setSearchVal] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [page, setPage] = useState(0)
  const [totalCount, setTotalCount] = useState(0)
  const [sortColumn, setSortColumn] = useState(defaultSortColumn || nameColumn)
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>(defaultSortDirection)
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined)

  const effectiveSearchColumn = searchColumn || nameColumn

  const fetchData = useCallback(async () => {
    setLoading(true)

    const selectStr = joinSelect || '*'
    let query = supabase
      .from(table)
      .select(selectStr, { count: 'exact' })
      .order(sortColumn, { ascending: sortDirection === 'asc' })
      .range(page * pageSize, (page + 1) * pageSize - 1)

    if (debouncedSearch.trim()) {
      query = query.ilike(effectiveSearchColumn, `%${debouncedSearch.trim()}%`)
    }

    const { data: rows, count, error } = await query

    if (!error && rows) {
      setData(rows as unknown as Record<string, unknown>[])
      setTotalCount(count ?? 0)
    }
    setLoading(false)
  }, [table, effectiveSearchColumn, joinSelect, debouncedSearch, page, pageSize, sortColumn, sortDirection])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  const handleSearch = (val: string) => {
    setSearchVal(val)
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      setDebouncedSearch(val)
      setPage(0)
    }, 300)
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

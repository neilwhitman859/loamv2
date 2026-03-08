import { useState, useEffect, useCallback, useRef } from 'react'
import { useParams, useNavigate, Link, useSearchParams } from 'react-router-dom'
import { supabase } from '../lib/supabase'

// ---------------------------------------------------------------------------
// Column definitions for key tables (can't query information_schema with anon)
// ---------------------------------------------------------------------------

interface ColumnDef {
  name: string
  type: string
  nullable: boolean
  fk?: string
}

const TABLE_COLUMNS: Record<string, ColumnDef[]> = {
  wines: [
    { name: 'id', type: 'uuid', nullable: false },
    { name: 'slug', type: 'text', nullable: false },
    { name: 'name', type: 'text', nullable: false },
    { name: 'producer_id', type: 'uuid', nullable: false, fk: 'producers' },
    { name: 'appellation_id', type: 'uuid', nullable: true, fk: 'appellations' },
    { name: 'region_id', type: 'uuid', nullable: false, fk: 'regions' },
    { name: 'country_id', type: 'uuid', nullable: false, fk: 'countries' },
    { name: 'varietal_category_id', type: 'uuid', nullable: true, fk: 'varietal_categories' },
    { name: 'wine_type', type: 'text', nullable: true },
    { name: 'sweetness_level', type: 'text', nullable: true },
    { name: 'alcohol_pct', type: 'decimal', nullable: true },
    { name: 'aging_vessel', type: 'text', nullable: true },
    { name: 'aging_months', type: 'int', nullable: true },
  ],
  producers: [
    { name: 'id', type: 'uuid', nullable: false },
    { name: 'slug', type: 'text', nullable: false },
    { name: 'name', type: 'text', nullable: false },
    { name: 'name_normalized', type: 'text', nullable: false },
    { name: 'country_id', type: 'uuid', nullable: false, fk: 'countries' },
    { name: 'region_id', type: 'uuid', nullable: true, fk: 'regions' },
    { name: 'website', type: 'text', nullable: true },
    { name: 'established_year', type: 'int', nullable: true },
  ],
  wine_vintage_scores: [
    { name: 'id', type: 'uuid', nullable: false },
    { name: 'wine_id', type: 'uuid', nullable: false, fk: 'wines' },
    { name: 'vintage_year', type: 'int', nullable: true },
    { name: 'publication_id', type: 'uuid', nullable: false, fk: 'publications' },
    { name: 'score_raw', type: 'text', nullable: true },
    { name: 'score_normalized', type: 'numeric', nullable: true },
    { name: 'reviewer_name', type: 'text', nullable: true },
    { name: 'tasting_note', type: 'text', nullable: true },
  ],
  wine_vintage_prices: [
    { name: 'id', type: 'uuid', nullable: false },
    { name: 'wine_id', type: 'uuid', nullable: false, fk: 'wines' },
    { name: 'vintage_year', type: 'int', nullable: true },
    { name: 'price_usd', type: 'numeric', nullable: true },
    { name: 'merchant_name', type: 'text', nullable: true },
    { name: 'merchant_url', type: 'text', nullable: true },
    { name: 'bottle_size_ml', type: 'int', nullable: true },
  ],
  countries: [
    { name: 'id', type: 'uuid', nullable: false },
    { name: 'slug', type: 'text', nullable: false },
    { name: 'name', type: 'text', nullable: false },
    { name: 'iso_code', type: 'text', nullable: true },
  ],
  grapes: [
    { name: 'id', type: 'uuid', nullable: false },
    { name: 'slug', type: 'text', nullable: false },
    { name: 'name', type: 'text', nullable: false },
    { name: 'color', type: 'text', nullable: true },
    { name: 'origin_country_id', type: 'uuid', nullable: true, fk: 'countries' },
  ],
  regions: [
    { name: 'id', type: 'uuid', nullable: false },
    { name: 'slug', type: 'text', nullable: false },
    { name: 'name', type: 'text', nullable: false },
    { name: 'country_id', type: 'uuid', nullable: false, fk: 'countries' },
    { name: 'parent_id', type: 'uuid', nullable: true, fk: 'regions' },
  ],
  appellations: [
    { name: 'id', type: 'uuid', nullable: false },
    { name: 'slug', type: 'text', nullable: false },
    { name: 'name', type: 'text', nullable: false },
    { name: 'designation_type', type: 'text', nullable: true },
    { name: 'country_id', type: 'uuid', nullable: false, fk: 'countries' },
    { name: 'region_id', type: 'uuid', nullable: false, fk: 'regions' },
    { name: 'latitude', type: 'decimal', nullable: true },
    { name: 'longitude', type: 'decimal', nullable: true },
  ],
}

// ---------------------------------------------------------------------------
// All 44 tables, grouped by domain
// ---------------------------------------------------------------------------

interface TableGroup {
  label: string
  tables: string[]
}

const TABLE_GROUPS: TableGroup[] = [
  {
    label: 'Geography',
    tables: ['countries', 'regions', 'appellations', 'appellation_vintages'],
  },
  {
    label: 'Producers',
    tables: ['producers', 'producer_regions', 'producer_aliases', 'producer_insights', 'producer_documents'],
  },
  {
    label: 'Wines',
    tables: [
      'wines',
      'wine_vintages',
      'wine_grapes',
      'wine_vintage_grapes',
      'wine_regions',
      'wine_farming_certifications',
      'wine_biodiversity_certifications',
      'wine_insights',
      'wine_vintage_insights',
      'wine_vintage_documents',
      'wine_vintage_scores',
      'wine_vintage_prices',
    ],
  },
  {
    label: 'Grapes',
    tables: ['grapes', 'varietal_categories', 'grape_insights'],
  },
  {
    label: 'Insights',
    tables: [
      'country_insights',
      'region_insights',
      'appellation_insights',
      'soil_type_insights',
      'water_body_insights',
    ],
  },
  {
    label: 'Environmental',
    tables: [
      'soil_types',
      'water_bodies',
      'farming_certifications',
      'biodiversity_certifications',
      'wine_soils',
      'appellation_soils',
      'region_soils',
      'appellation_water_bodies',
      'region_water_bodies',
    ],
  },
  {
    label: 'Reference',
    tables: ['source_types', 'publications', 'enrichment_log', 'trends', 'region_name_mappings'],
  },
]

const ALL_TABLES = TABLE_GROUPS.flatMap((g) => g.tables)

const PAGE_SIZE = 25

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function truncateUuid(val: string): string {
  if (typeof val === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-/.test(val)) {
    return val.slice(0, 8)
  }
  return val
}

function isUuidColumn(colName: string, columns: ColumnDef[] | null): boolean {
  if (columns) {
    const col = columns.find((c) => c.name === colName)
    return col?.type === 'uuid'
  }
  // Heuristic: column name ends with _id or is exactly 'id'
  return colName === 'id' || colName.endsWith('_id')
}

function getFkTarget(colName: string, columns: ColumnDef[] | null): string | undefined {
  if (columns) {
    return columns.find((c) => c.name === colName)?.fk
  }
  return undefined
}

function formatCellValue(val: unknown): string {
  if (val === null || val === undefined) return '--'
  if (typeof val === 'object') return JSON.stringify(val)
  return String(val)
}

function isTextLikeColumn(colName: string, columns: ColumnDef[] | null): boolean {
  if (columns) {
    const col = columns.find((c) => c.name === colName)
    if (!col) return false
    return ['text', 'varchar', 'citext'].includes(col.type)
  }
  // Heuristic for dynamic columns
  return (
    colName === 'name' ||
    colName === 'slug' ||
    colName.endsWith('_name') ||
    colName.includes('note') ||
    colName.includes('url') ||
    colName.includes('website')
  )
}

function getSearchableColumns(columns: ColumnDef[] | string[]): string[] {
  if (columns.length === 0) return []
  if (typeof columns[0] === 'string') {
    return (columns as string[]).filter((c) =>
      c === 'name' ||
      c === 'slug' ||
      c.endsWith('_name') ||
      c.includes('note') ||
      c.includes('url') ||
      c.includes('website')
    )
  }
  return (columns as ColumnDef[])
    .filter((c) => ['text', 'varchar', 'citext'].includes(c.type))
    .map((c) => c.name)
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function TableBrowser() {
  const { tableName: urlTable } = useParams<{ tableName?: string }>()
  const [searchParams] = useSearchParams()
  const highlightId = searchParams.get('highlight')
  const navigate = useNavigate()

  const [selectedTable, setSelectedTable] = useState<string>(urlTable || '')
  const [rowCount, setRowCount] = useState<number | null>(null)
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [dynamicColumns, setDynamicColumns] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const [page, setPage] = useState(0)
  const [sortColumn, setSortColumn] = useState<string | null>(null)
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc')
  const [search, setSearchInput] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [sidebarFilter, setSidebarFilter] = useState('')
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined)

  // Sync URL param to state
  useEffect(() => {
    if (urlTable && urlTable !== selectedTable) {
      setSelectedTable(urlTable)
      setPage(0)
      setSearchInput('')
      setDebouncedSearch('')
      setSortColumn(null)
      setSortDirection('asc')
    }
  }, [urlTable]) // eslint-disable-line react-hooks/exhaustive-deps

  const knownColumns = selectedTable ? TABLE_COLUMNS[selectedTable] ?? null : null
  const effectiveColumns: string[] = knownColumns
    ? knownColumns.map((c) => c.name)
    : dynamicColumns

  const searchableColumns = knownColumns
    ? getSearchableColumns(knownColumns)
    : getSearchableColumns(dynamicColumns)

  // Fetch row count
  useEffect(() => {
    if (!selectedTable) {
      setRowCount(null)
      return
    }
    let cancelled = false
    ;(async () => {
      const { count } = await supabase
        .from(selectedTable)
        .select('*', { count: 'exact', head: true })
      if (!cancelled) setRowCount(count)
    })()
    return () => { cancelled = true }
  }, [selectedTable])

  // Fetch rows
  const fetchRows = useCallback(async () => {
    if (!selectedTable) return
    setLoading(true)

    let query = supabase
      .from(selectedTable)
      .select('*', { count: 'exact' })
      .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)

    if (sortColumn) {
      query = query.order(sortColumn, { ascending: sortDirection === 'asc' })
    }

    // Apply search across all searchable columns with OR
    if (debouncedSearch.trim() && searchableColumns.length > 0) {
      const orFilter = searchableColumns
        .map((c) => `${c}.ilike.%${debouncedSearch.trim()}%`)
        .join(',')
      query = query.or(orFilter)
    }

    const { data, count, error } = await query

    if (!error && data) {
      setRows(data as Record<string, unknown>[])
      setRowCount(count)

      // Build dynamic columns from first row if no hardcoded schema
      if (!knownColumns && data.length > 0) {
        setDynamicColumns(Object.keys(data[0]))
      } else if (!knownColumns && data.length === 0) {
        setDynamicColumns([])
      }
    } else {
      setRows([])
      if (!knownColumns) setDynamicColumns([])
    }
    setLoading(false)
  }, [selectedTable, page, sortColumn, sortDirection, debouncedSearch, searchableColumns.join(','), knownColumns]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    fetchRows()
  }, [fetchRows])

  // Handlers
  const handleSelectTable = (table: string) => {
    setSelectedTable(table)
    setPage(0)
    setSearchInput('')
    setDebouncedSearch('')
    setSortColumn(null)
    setSortDirection('asc')
    setDynamicColumns([])
    navigate(`/dev/tables/${table}`, { replace: true })
  }

  const handleSearch = (val: string) => {
    setSearchInput(val)
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      setDebouncedSearch(val)
      setPage(0)
    }, 300)
  }

  const handleSort = (col: string) => {
    if (col === sortColumn) {
      setSortDirection((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortColumn(col)
      setSortDirection('asc')
    }
    setPage(0)
  }

  const totalPages = rowCount != null ? Math.ceil(rowCount / PAGE_SIZE) : 0

  // Filter sidebar tables
  const filteredGroups = TABLE_GROUPS.map((g) => ({
    ...g,
    tables: g.tables.filter((t) =>
      t.toLowerCase().includes(sidebarFilter.toLowerCase())
    ),
  })).filter((g) => g.tables.length > 0)

  return (
    <div className="flex gap-6 -mx-6 -mt-6 min-h-[calc(100vh-3rem)]">
      {/* Sidebar: table list */}
      <div className="w-64 flex-shrink-0 bg-white border-r border-earth-200 overflow-y-auto">
        <div className="p-4 border-b border-earth-200">
          <h2 className="text-sm font-semibold text-earth-800 mb-2">Tables</h2>
          <input
            type="text"
            value={sidebarFilter}
            onChange={(e) => setSidebarFilter(e.target.value)}
            placeholder="Filter tables..."
            className="w-full px-2 py-1.5 text-xs border border-earth-300 rounded focus:outline-none focus:ring-2 focus:ring-wine-500/30 focus:border-wine-500 bg-white"
          />
        </div>
        <div className="py-2">
          {filteredGroups.map((group) => (
            <div key={group.label}>
              <div className="px-4 pt-3 pb-1 text-[10px] font-semibold uppercase tracking-widest text-earth-400">
                {group.label}
              </div>
              {group.tables.map((t) => (
                <button
                  key={t}
                  onClick={() => handleSelectTable(t)}
                  className={`block w-full text-left px-4 py-1.5 text-xs transition-colors ${
                    selectedTable === t
                      ? 'bg-wine-50 text-wine-700 font-medium border-r-2 border-wine-500'
                      : 'text-earth-600 hover:bg-earth-50 hover:text-earth-900'
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>
          ))}
        </div>
        <div className="px-4 py-3 border-t border-earth-200 text-[10px] text-earth-400">
          {ALL_TABLES.length} tables
        </div>
      </div>

      {/* Main content */}
      <div className="flex-1 py-6 pr-6 min-w-0">
        {!selectedTable ? (
          <div className="text-center py-24">
            <div className="text-earth-400 text-lg mb-2">Select a table</div>
            <p className="text-earth-400 text-sm">
              Choose a table from the sidebar to browse its schema and data.
            </p>
          </div>
        ) : (
          <>
            {/* Header */}
            <div className="mb-6">
              <h1 className="text-2xl font-bold text-earth-900 mb-1">{selectedTable}</h1>
              <p className="text-sm text-earth-500">
                {rowCount != null ? rowCount.toLocaleString() : '...'} rows
                {knownColumns ? (
                  <span className="ml-2 text-earth-400">
                    ({knownColumns.length} columns, schema known)
                  </span>
                ) : dynamicColumns.length > 0 ? (
                  <span className="ml-2 text-earth-400">
                    ({dynamicColumns.length} columns, inferred)
                  </span>
                ) : null}
              </p>
            </div>

            {/* Column schema */}
            {knownColumns && (
              <div className="mb-6">
                <h2 className="text-sm font-semibold text-earth-700 mb-2">Column Schema</h2>
                <div className="bg-white rounded-lg border border-earth-200 overflow-hidden">
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="bg-earth-50 border-b border-earth-200">
                          <th className="px-3 py-2 text-left font-semibold text-earth-600">Column</th>
                          <th className="px-3 py-2 text-left font-semibold text-earth-600">Type</th>
                          <th className="px-3 py-2 text-left font-semibold text-earth-600">Nullable</th>
                          <th className="px-3 py-2 text-left font-semibold text-earth-600">FK Target</th>
                        </tr>
                      </thead>
                      <tbody>
                        {knownColumns.map((col) => (
                          <tr key={col.name} className="border-b border-earth-100 hover:bg-earth-50/50">
                            <td className="px-3 py-1.5 font-mono text-earth-900">{col.name}</td>
                            <td className="px-3 py-1.5">
                              <span className="inline-block px-1.5 py-0.5 bg-earth-100 text-earth-600 rounded text-[10px] font-mono">
                                {col.type}
                              </span>
                            </td>
                            <td className="px-3 py-1.5 text-earth-500">{col.nullable ? 'yes' : 'no'}</td>
                            <td className="px-3 py-1.5">
                              {col.fk ? (
                                <button
                                  onClick={() => handleSelectTable(col.fk!)}
                                  className="text-wine-600 hover:text-wine-800 hover:underline font-medium"
                                >
                                  {col.fk}
                                </button>
                              ) : (
                                <span className="text-earth-300">--</span>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            )}

            {/* Search */}
            {searchableColumns.length > 0 && (
              <div className="mb-4">
                <input
                  type="text"
                  value={search}
                  onChange={(e) => handleSearch(e.target.value)}
                  placeholder={`Search ${searchableColumns.slice(0, 3).join(', ')}${searchableColumns.length > 3 ? '...' : ''}`}
                  className="w-full max-w-sm px-3 py-2 border border-earth-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-wine-500/30 focus:border-wine-500 bg-white"
                />
              </div>
            )}

            {/* Data table */}
            <div className="bg-white rounded-lg border border-earth-200 overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-earth-50 border-b border-earth-200">
                      {effectiveColumns.map((col) => (
                        <th
                          key={col}
                          className="px-4 py-3 text-left font-semibold text-earth-700 cursor-pointer hover:text-wine-700 select-none whitespace-nowrap"
                          onClick={() => handleSort(col)}
                        >
                          <span className="inline-flex items-center gap-1">
                            {col}
                            {sortColumn === col && (
                              <span className="text-wine-500">
                                {sortDirection === 'asc' ? '\u2191' : '\u2193'}
                              </span>
                            )}
                          </span>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {loading ? (
                      <tr>
                        <td
                          colSpan={effectiveColumns.length || 1}
                          className="px-4 py-12 text-center text-earth-400"
                        >
                          Loading...
                        </td>
                      </tr>
                    ) : rows.length === 0 ? (
                      <tr>
                        <td
                          colSpan={effectiveColumns.length || 1}
                          className="px-4 py-12 text-center text-earth-400"
                        >
                          {debouncedSearch ? 'No results found' : 'Table is empty'}
                        </td>
                      </tr>
                    ) : (
                      rows.map((row, rowIdx) => {
                        const rowId = String(row.id ?? rowIdx)
                        const isHighlighted = highlightId && rowId.startsWith(highlightId)
                        return (
                          <tr
                            key={rowIdx}
                            className={`border-b border-earth-100 transition-colors ${
                              isHighlighted
                                ? 'bg-wine-50 ring-1 ring-inset ring-wine-200'
                                : 'hover:bg-earth-50'
                            }`}
                          >
                            {effectiveColumns.map((col) => {
                              const raw = row[col]
                              const display = formatCellValue(raw)
                              const isUuid = isUuidColumn(col, knownColumns)
                              const fkTarget = getFkTarget(col, knownColumns)
                              const isText = isTextLikeColumn(col, knownColumns)

                              // FK link
                              if (fkTarget && raw != null && raw !== '') {
                                return (
                                  <td key={col} className="px-4 py-3 whitespace-nowrap">
                                    <Link
                                      to={`/dev/tables/${fkTarget}?highlight=${String(raw).slice(0, 8)}`}
                                      className="text-wine-600 hover:text-wine-800 hover:underline font-mono text-xs"
                                      title={String(raw)}
                                    >
                                      {truncateUuid(String(raw))}
                                    </Link>
                                  </td>
                                )
                              }

                              // UUID (non-FK)
                              if (isUuid && raw != null) {
                                return (
                                  <td
                                    key={col}
                                    className="px-4 py-3 whitespace-nowrap font-mono text-xs text-earth-500"
                                    title={String(raw)}
                                  >
                                    {truncateUuid(String(raw))}
                                  </td>
                                )
                              }

                              // Long text: truncate
                              if (isText && display.length > 80) {
                                return (
                                  <td
                                    key={col}
                                    className="px-4 py-3 max-w-xs text-earth-700"
                                    title={display}
                                  >
                                    <span className="line-clamp-2 text-xs">{display}</span>
                                  </td>
                                )
                              }

                              // Default
                              return (
                                <td
                                  key={col}
                                  className="px-4 py-3 whitespace-nowrap text-earth-700"
                                >
                                  {display === '--' ? (
                                    <span className="text-earth-300">--</span>
                                  ) : (
                                    display
                                  )}
                                </td>
                              )
                            })}
                          </tr>
                        )
                      })
                    )}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between px-4 py-3 border-t border-earth-200 bg-earth-50/50">
                  <span className="text-xs text-earth-500">
                    Showing {page * PAGE_SIZE + 1}
                    {'\u2013'}
                    {Math.min((page + 1) * PAGE_SIZE, rowCount!)} of{' '}
                    {rowCount!.toLocaleString()} rows
                  </span>
                  <div className="flex gap-1">
                    <button
                      onClick={() => setPage(0)}
                      disabled={page === 0}
                      className="px-2 py-1 text-xs rounded border border-earth-300 bg-white hover:bg-earth-50 disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      First
                    </button>
                    <button
                      onClick={() => setPage(page - 1)}
                      disabled={page === 0}
                      className="px-3 py-1 text-xs rounded border border-earth-300 bg-white hover:bg-earth-50 disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      Prev
                    </button>
                    <span className="px-3 py-1 text-xs text-earth-600">
                      {page + 1} / {totalPages}
                    </span>
                    <button
                      onClick={() => setPage(page + 1)}
                      disabled={page >= totalPages - 1}
                      className="px-3 py-1 text-xs rounded border border-earth-300 bg-white hover:bg-earth-50 disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      Next
                    </button>
                    <button
                      onClick={() => setPage(totalPages - 1)}
                      disabled={page >= totalPages - 1}
                      className="px-2 py-1 text-xs rounded border border-earth-300 bg-white hover:bg-earth-50 disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      Last
                    </button>
                  </div>
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}

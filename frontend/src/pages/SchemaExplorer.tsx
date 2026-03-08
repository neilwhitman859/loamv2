import { useEffect, useState, useRef, useCallback, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { supabase } from '../lib/supabase'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ColumnInfo { name: string; type: string; nullable: boolean }
interface TableInfo { table_name: string; columns: ColumnInfo[] }
interface FKInfo { from_table: string; from_column: string; to_table: string; to_column: string }
interface RowCount { table_name: string; count: number }
type Domain = 'geography' | 'producers' | 'wines' | 'grapes' | 'insights' | 'environmental' | 'reference' | 'staging'

// ---------------------------------------------------------------------------
// Domain classification — maps table names to domains
// ---------------------------------------------------------------------------

const DOMAIN_RULES: [RegExp, Domain][] = [
  [/^countries$/, 'geography'],
  [/^regions$/, 'geography'],
  [/^appellations$/, 'geography'],
  [/^appellation_vintages$/, 'geography'],
  [/^producers$/, 'producers'],
  [/^producer_/, 'producers'],
  [/^wines$/, 'wines'],
  [/^wine_/, 'wines'],
  [/^grapes$/, 'grapes'],
  [/^varietal_categories$/, 'grapes'],
  [/^grape_insights$/, 'insights'],
  [/^country_insights$/, 'insights'],
  [/^region_insights$/, 'insights'],
  [/^appellation_insights$/, 'insights'],
  [/^producer_insights$/, 'insights'],
  [/^soil_type_insights$/, 'insights'],
  [/^water_body_insights$/, 'insights'],
  [/^soil_types$/, 'environmental'],
  [/^water_bodies$/, 'environmental'],
  [/^farming_certifications$/, 'environmental'],
  [/^biodiversity_certifications$/, 'environmental'],
  [/^appellation_soils$/, 'environmental'],
  [/^region_soils$/, 'environmental'],
  [/^appellation_water_bodies$/, 'environmental'],
  [/^region_water_bodies$/, 'environmental'],
  [/^appellation_documents$/, 'environmental'],
  [/^source_types$/, 'reference'],
  [/^publications$/, 'reference'],
  [/^enrichment_log$/, 'reference'],
  [/^trends$/, 'reference'],
  [/^region_name_mappings$/, 'staging'],
  [/^wine_candidates$/, 'staging'],
  [/^producer_dedup/, 'staging'],
]

function classifyDomain(tableName: string): Domain {
  for (const [re, domain] of DOMAIN_RULES) {
    if (re.test(tableName)) return domain
  }
  return 'reference'
}

// ---------------------------------------------------------------------------
// Domain colors
// ---------------------------------------------------------------------------

const DOMAIN_COLORS: Record<Domain, { bg: string; border: string; header: string; text: string; label: string }> = {
  geography:     { bg: '#eff6ff', border: '#93c5fd', header: '#2563eb', text: '#1e40af', label: 'Geography' },
  producers:     { bg: '#f0fdf4', border: '#86efac', header: '#16a34a', text: '#166534', label: 'Producers' },
  wines:         { bg: '#fef2f2', border: '#fca5a5', header: '#dc2626', text: '#991b1b', label: 'Wines' },
  grapes:        { bg: '#fffbeb', border: '#fcd34d', header: '#d97706', text: '#92400e', label: 'Grapes' },
  insights:      { bg: '#faf5ff', border: '#c084fc', header: '#9333ea', text: '#6b21a8', label: 'Insights' },
  environmental: { bg: '#f0fdfa', border: '#5eead4', header: '#0d9488', text: '#115e59', label: 'Environmental' },
  reference:     { bg: '#f9fafb', border: '#d1d5db', header: '#6b7280', text: '#374151', label: 'Reference' },
  staging:       { bg: '#fff7ed', border: '#fdba74', header: '#ea580c', text: '#9a3412', label: 'Staging' },
}

const DOMAIN_ORDER: Domain[] = ['geography', 'producers', 'wines', 'grapes', 'insights', 'environmental', 'reference', 'staging']

// ---------------------------------------------------------------------------
// Layout constants
// ---------------------------------------------------------------------------

const TABLE_W = 230
const TABLE_HEADER_H = 36
const COL_ROW_H = 16
const TABLE_GAP_X = 50
const TABLE_GAP_Y = 20

interface TablePos { x: number; y: number; w: number; h: number }

// Arrange tables into columns by domain groups
function computeLayout(
  tables: TableInfo[],
  expanded: Set<string>,
): { positions: Record<string, TablePos>; totalW: number; totalH: number } {
  // Group tables by domain
  const grouped = new Map<Domain, TableInfo[]>()
  for (const d of DOMAIN_ORDER) grouped.set(d, [])
  for (const t of tables) {
    const d = classifyDomain(t.table_name)
    grouped.get(d)?.push(t)
  }

  // Layout: 4 columns of domain groups
  const columnAssignment: Domain[][] = [
    ['geography', 'reference'],
    ['producers', 'grapes'],
    ['wines'],
    ['insights', 'environmental', 'staging'],
  ]

  const positions: Record<string, TablePos> = {}
  let colX = 40

  for (const colDomains of columnAssignment) {
    let maxColW = 0
    let curY = 40

    for (const domain of colDomains) {
      const domainTables = grouped.get(domain) ?? []
      if (domainTables.length === 0) continue

      curY += 28 // domain label space

      for (const table of domainTables) {
        const isExpanded = expanded.has(table.table_name)
        const h = isExpanded
          ? TABLE_HEADER_H + table.columns.length * COL_ROW_H + 8
          : TABLE_HEADER_H
        positions[table.table_name] = { x: colX, y: curY, w: TABLE_W, h }
        curY += h + TABLE_GAP_Y
        maxColW = Math.max(maxColW, TABLE_W)
      }
      curY += 16
    }
    colX += maxColW + TABLE_GAP_X
  }

  const totalW = colX + 40
  const totalH = Math.max(...Object.values(positions).map(p => p.y + p.h), 600) + 80
  return { positions, totalW, totalH }
}

// ---------------------------------------------------------------------------
// SVG Table Node
// ---------------------------------------------------------------------------

function TableNode({
  table,
  pos,
  rowCount,
  fks,
  highlighted,
  expanded,
  onToggle,
  onHover,
  onNav,
}: {
  table: TableInfo
  pos: TablePos
  rowCount: number | null
  fks: FKInfo[]
  highlighted: boolean
  expanded: boolean
  onToggle: () => void
  onHover: (hovering: boolean) => void
  onNav: () => void
}) {
  const domain = classifyDomain(table.table_name)
  const c = DOMAIN_COLORS[domain]
  const countLabel = rowCount !== null ? rowCount.toLocaleString() : '...'

  // Which columns are FKs?
  const fkCols = new Set(
    fks.filter(f => f.from_table === table.table_name).map(f => f.from_column)
  )

  return (
    <g
      onMouseEnter={() => onHover(true)}
      onMouseLeave={() => onHover(false)}
      style={{ cursor: 'pointer' }}
    >
      {/* Shadow */}
      <rect x={pos.x + 2} y={pos.y + 2} width={pos.w} height={pos.h} rx={6} fill="rgba(0,0,0,0.06)" />
      {/* Card */}
      <rect
        x={pos.x} y={pos.y} width={pos.w} height={pos.h} rx={6}
        fill={highlighted ? '#fefce8' : c.bg}
        stroke={highlighted ? '#eab308' : c.border}
        strokeWidth={highlighted ? 2 : 1}
      />
      {/* Header bar */}
      <rect x={pos.x} y={pos.y} width={pos.w} height={TABLE_HEADER_H} rx={6} fill={c.header} />
      <rect x={pos.x} y={pos.y + TABLE_HEADER_H - 6} width={pos.w} height={6} fill={c.header} />
      {/* Table name */}
      <text
        x={pos.x + 10} y={pos.y + 14}
        fill="white" fontSize={11} fontWeight={600} fontFamily="monospace"
        onClick={(e) => { e.stopPropagation(); onNav() }}
        style={{ cursor: 'pointer' }}
      >
        {table.table_name}
      </text>
      {/* Row count */}
      <text
        x={pos.x + pos.w - 10} y={pos.y + 14}
        fill="rgba(255,255,255,0.8)" fontSize={9} fontFamily="monospace" textAnchor="end"
      >
        {countLabel}
      </text>
      {/* Expand toggle */}
      <text
        x={pos.x + pos.w - 10} y={pos.y + 28}
        fill="rgba(255,255,255,0.6)" fontSize={9} textAnchor="end"
        onClick={(e) => { e.stopPropagation(); onToggle() }}
        style={{ cursor: 'pointer' }}
      >
        {expanded ? '\u25B2' : '\u25BC'} {table.columns.length} cols
      </text>

      {/* Columns when expanded */}
      {expanded && table.columns.map((col, i) => {
        const cy = pos.y + TABLE_HEADER_H + 2 + i * COL_ROW_H
        const isFK = fkCols.has(col.name)
        return (
          <g key={col.name}>
            {i % 2 === 0 && (
              <rect x={pos.x + 1} y={cy} width={pos.w - 2} height={COL_ROW_H} fill="rgba(0,0,0,0.02)" />
            )}
            <text
              x={pos.x + 10} y={cy + 11}
              fill={isFK ? '#e11d48' : col.name === 'id' ? c.header : c.text}
              fontSize={9.5} fontWeight={isFK || col.name === 'id' ? 600 : 400}
              fontFamily="monospace"
            >
              {isFK ? '\u2192 ' : col.name === 'id' ? '\u26BF ' : '  '}{col.name}
            </text>
            <text
              x={pos.x + pos.w - 10} y={cy + 11}
              fill="#9ca3af" fontSize={8} fontFamily="monospace" textAnchor="end"
            >
              {col.type}{col.nullable ? '?' : ''}
            </text>
          </g>
        )
      })}
    </g>
  )
}

// ---------------------------------------------------------------------------
// FK Line
// ---------------------------------------------------------------------------

function FKLine({
  fk,
  positions,
  highlighted,
}: {
  fk: FKInfo
  positions: Record<string, TablePos>
  highlighted: boolean
}) {
  const from = positions[fk.from_table]
  const to = positions[fk.to_table]
  if (!from || !to) return null

  // Self-referencing
  if (fk.from_table === fk.to_table) {
    const x = from.x + from.w
    const y1 = from.y + 18
    const y2 = from.y + from.h - 4
    return (
      <path
        d={`M ${x} ${y1} C ${x + 30} ${y1}, ${x + 30} ${y2}, ${x} ${y2}`}
        fill="none"
        stroke={highlighted ? '#eab308' : '#d1d5db'}
        strokeWidth={highlighted ? 2.5 : 1}
        strokeDasharray={highlighted ? '' : '4 2'}
        opacity={highlighted ? 1 : 0.5}
        markerEnd={highlighted ? 'url(#arrow-hl)' : 'url(#arrow)'}
      />
    )
  }

  const fromCx = from.x + from.w / 2
  const toCx = to.x + to.w / 2

  let x1: number, y1: number, x2: number, y2: number
  if (fromCx < toCx) {
    x1 = from.x + from.w; y1 = from.y + TABLE_HEADER_H / 2
    x2 = to.x; y2 = to.y + TABLE_HEADER_H / 2
  } else {
    x1 = from.x; y1 = from.y + TABLE_HEADER_H / 2
    x2 = to.x + to.w; y2 = to.y + TABLE_HEADER_H / 2
  }

  const mx = (x1 + x2) / 2

  return (
    <path
      d={`M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`}
      fill="none"
      stroke={highlighted ? '#eab308' : '#d1d5db'}
      strokeWidth={highlighted ? 2.5 : 1}
      strokeDasharray={highlighted ? '' : '4 2'}
      opacity={highlighted ? 1 : 0.5}
      markerEnd={highlighted ? 'url(#arrow-hl)' : 'url(#arrow)'}
    />
  )
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function SchemaExplorer() {
  const navigate = useNavigate()
  const containerRef = useRef<HTMLDivElement>(null)

  // Schema data — fetched from DB function
  const [tables, setTables] = useState<TableInfo[]>([])
  const [fks, setFks] = useState<FKInfo[]>([])
  const [rowCounts, setRowCounts] = useState<Record<string, number>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // UI state
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [hoveredTable, setHoveredTable] = useState<string | null>(null)
  const [search, setSearch] = useState('')

  // Pan/zoom
  const [viewBox, setViewBox] = useState({ x: 0, y: 0, w: 1600, h: 1200 })
  const [isPanning, setIsPanning] = useState(false)
  const panStart = useRef({ x: 0, y: 0, vx: 0, vy: 0 })

  // Fetch schema from DB function
  useEffect(() => {
    async function fetchSchema() {
      try {
        const { data, error: rpcError } = await supabase.rpc('get_schema_info')
        if (rpcError) throw rpcError

        const schema = data as {
          tables: TableInfo[]
          foreign_keys: FKInfo[]
          row_counts: RowCount[]
        }

        setTables(schema.tables ?? [])
        setFks(schema.foreign_keys ?? [])

        const counts: Record<string, number> = {}
        for (const rc of schema.row_counts ?? []) {
          counts[rc.table_name] = rc.count
        }
        setRowCounts(counts)
      } catch (err: unknown) {
        setError(err instanceof Error ? err.message : String(err))
      } finally {
        setLoading(false)
      }
    }
    fetchSchema()
  }, [])

  // Layout
  const { positions, totalW, totalH } = useMemo(
    () => computeLayout(tables, expanded),
    [tables, expanded]
  )

  // Set initial viewBox when layout computed
  useEffect(() => {
    if (totalW > 0 && totalH > 0) {
      setViewBox({ x: 0, y: 0, w: Math.max(totalW, 1200), h: Math.max(totalH, 800) })
    }
  }, [totalW, totalH])

  // Toggle expand
  const toggleTable = useCallback((name: string) => {
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }, [])

  // Hover highlighting
  const connectedTables = useMemo(() => {
    if (!hoveredTable) return new Set<string>()
    const set = new Set<string>()
    fks.forEach(fk => {
      if (fk.from_table === hoveredTable) set.add(fk.to_table)
      if (fk.to_table === hoveredTable) set.add(fk.from_table)
    })
    return set
  }, [hoveredTable, fks])

  const highlightedFKs = useMemo(() => {
    if (!hoveredTable) return new Set<number>()
    const set = new Set<number>()
    fks.forEach((fk, i) => {
      if (fk.from_table === hoveredTable || fk.to_table === hoveredTable) set.add(i)
    })
    return set
  }, [hoveredTable, fks])

  // Search filter
  const filteredNames = useMemo(() => {
    if (!search.trim()) return null
    const q = search.toLowerCase()
    return new Set(
      tables
        .filter(t =>
          t.table_name.includes(q) ||
          classifyDomain(t.table_name).includes(q) ||
          t.columns.some(c => c.name.includes(q))
        )
        .map(t => t.table_name)
    )
  }, [search, tables])

  // Zoom
  const handleWheel = useCallback((e: React.WheelEvent) => {
    e.preventDefault()
    const factor = e.deltaY > 0 ? 1.1 : 0.9
    setViewBox(prev => {
      const newW = prev.w * factor
      const newH = prev.h * factor
      const dx = (prev.w - newW) / 2
      const dy = (prev.h - newH) / 2
      return { x: prev.x + dx, y: prev.y + dy, w: newW, h: newH }
    })
  }, [])

  // Block native wheel
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const prevent = (e: WheelEvent) => e.preventDefault()
    el.addEventListener('wheel', prevent, { passive: false })
    return () => el.removeEventListener('wheel', prevent)
  }, [])

  // Pan
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if (e.button !== 0) return
    setIsPanning(true)
    panStart.current = { x: e.clientX, y: e.clientY, vx: viewBox.x, vy: viewBox.y }
  }, [viewBox])

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (!isPanning || !containerRef.current) return
    const rect = containerRef.current.getBoundingClientRect()
    const scaleX = viewBox.w / rect.width
    const scaleY = viewBox.h / rect.height
    const dx = (e.clientX - panStart.current.x) * scaleX
    const dy = (e.clientY - panStart.current.y) * scaleY
    setViewBox(prev => ({ ...prev, x: panStart.current.vx - dx, y: panStart.current.vy - dy }))
  }, [isPanning, viewBox.w, viewBox.h])

  const handleMouseUp = useCallback(() => setIsPanning(false), [])

  const resetZoom = useCallback(() => {
    setViewBox({ x: 0, y: 0, w: Math.max(totalW, 1200), h: Math.max(totalH, 800) })
  }, [totalW, totalH])

  const expandAll = useCallback(() => setExpanded(new Set(tables.map(t => t.table_name))), [tables])
  const collapseAll = useCallback(() => setExpanded(new Set()), [])

  // Domain labels for SVG
  const domainLabels = useMemo(() => {
    const seen = new Set<Domain>()
    const labels: { domain: Domain; x: number; y: number }[] = []
    for (const t of tables) {
      const d = classifyDomain(t.table_name)
      const pos = positions[t.table_name]
      if (pos && !seen.has(d)) {
        labels.push({ domain: d, x: pos.x, y: pos.y - 6 })
        seen.add(d)
      }
    }
    return labels
  }, [tables, positions])

  // Stats
  const totalRows = Object.values(rowCounts).reduce((s, v) => s + v, 0)

  if (loading) {
    return (
      <div className="flex items-center justify-center h-[calc(100vh-6rem)]">
        <div className="text-center">
          <div className="text-earth-400 text-lg mb-2">Loading schema...</div>
          <p className="text-earth-400 text-sm">Fetching tables, columns, and relationships from the database</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-[calc(100vh-6rem)]">
        <div className="text-center">
          <div className="text-red-600 text-lg mb-2">Failed to load schema</div>
          <p className="text-earth-500 text-sm max-w-md">{error}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="h-full flex flex-col -m-6">
      {/* Toolbar */}
      <div className="flex items-center gap-3 px-6 py-3 bg-white border-b border-earth-200 shrink-0">
        <h1 className="text-lg font-bold text-earth-900">Schema Explorer</h1>
        <span className="text-xs text-earth-400">
          {tables.length} tables &middot; {fks.length} FKs &middot; {totalRows.toLocaleString()} rows
        </span>
        <div className="flex-1" />
        <input
          type="text"
          placeholder="Search tables or columns..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="px-3 py-1.5 text-xs border border-earth-200 rounded-md bg-earth-50 w-56 focus:outline-none focus:ring-1 focus:ring-wine-500"
        />
        <button onClick={expandAll} className="px-2.5 py-1.5 text-xs bg-earth-100 text-earth-600 rounded hover:bg-earth-200">
          Expand All
        </button>
        <button onClick={collapseAll} className="px-2.5 py-1.5 text-xs bg-earth-100 text-earth-600 rounded hover:bg-earth-200">
          Collapse All
        </button>
        <button onClick={resetZoom} className="px-2.5 py-1.5 text-xs bg-earth-100 text-earth-600 rounded hover:bg-earth-200">
          Reset Zoom
        </button>
      </div>

      {/* Legend */}
      <div className="flex items-center gap-4 px-6 py-2 bg-earth-50 border-b border-earth-100 shrink-0 overflow-x-auto">
        {DOMAIN_ORDER.map(d => {
          const c = DOMAIN_COLORS[d]
          const count = tables.filter(t => classifyDomain(t.table_name) === d).length
          if (count === 0) return null
          return (
            <button
              key={d}
              onClick={() => setSearch(search === c.label.toLowerCase() ? '' : c.label.toLowerCase())}
              className="flex items-center gap-1.5 flex-shrink-0"
            >
              <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: c.header }} />
              <span className="text-[10px] text-earth-500 font-medium">{c.label} ({count})</span>
            </button>
          )
        })}
        <div className="flex-1" />
        <span className="text-[10px] text-earth-400 flex-shrink-0">
          Scroll to zoom &middot; Drag to pan &middot; Click name to browse &middot; {'\u25BC'} to expand
        </span>
      </div>

      {/* SVG */}
      <div
        ref={containerRef}
        className="flex-1 overflow-hidden bg-earth-50"
        style={{ cursor: isPanning ? 'grabbing' : 'grab' }}
      >
        <svg
          width="100%"
          height="100%"
          viewBox={`${viewBox.x} ${viewBox.y} ${viewBox.w} ${viewBox.h}`}
          onWheel={handleWheel}
          onMouseDown={handleMouseDown}
          onMouseMove={handleMouseMove}
          onMouseUp={handleMouseUp}
          onMouseLeave={handleMouseUp}
          style={{ userSelect: 'none' }}
        >
          <defs>
            <marker id="arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
              <polygon points="0 0, 8 3, 0 6" fill="#9ca3af" />
            </marker>
            <marker id="arrow-hl" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
              <polygon points="0 0, 8 3, 0 6" fill="#eab308" />
            </marker>
          </defs>

          {/* FK lines — dim first, highlighted on top */}
          {fks.map((fk, i) => {
            if (highlightedFKs.has(i)) return null
            if (filteredNames && !filteredNames.has(fk.from_table) && !filteredNames.has(fk.to_table)) return null
            const dimmed = hoveredTable && !highlightedFKs.has(i)
            return (
              <g key={i} opacity={dimmed ? 0.1 : 1}>
                <FKLine fk={fk} positions={positions} highlighted={false} />
              </g>
            )
          })}
          {fks.map((fk, i) => {
            if (!highlightedFKs.has(i)) return null
            return <FKLine key={`hl-${i}`} fk={fk} positions={positions} highlighted={true} />
          })}

          {/* Domain labels */}
          {domainLabels.map(dl => {
            const c = DOMAIN_COLORS[dl.domain]
            return (
              <g key={dl.domain}>
                <rect x={dl.x} y={dl.y - 14} width={10} height={10} rx={2} fill={c.header} />
                <text x={dl.x + 14} y={dl.y - 5} fill={c.header} fontSize={11} fontWeight={700} fontFamily="system-ui">
                  {c.label}
                </text>
              </g>
            )
          })}

          {/* Table nodes */}
          {tables.map(table => {
            const pos = positions[table.table_name]
            if (!pos) return null
            const dimmedBySearch = filteredNames && !filteredNames.has(table.table_name)
            const dimmedByHover = hoveredTable && hoveredTable !== table.table_name && !connectedTables.has(table.table_name)
            return (
              <g key={table.table_name} opacity={dimmedBySearch ? 0.15 : dimmedByHover ? 0.3 : 1}>
                <TableNode
                  table={table}
                  pos={pos}
                  rowCount={rowCounts[table.table_name] ?? null}
                  fks={fks}
                  highlighted={hoveredTable === table.table_name || connectedTables.has(table.table_name)}
                  expanded={expanded.has(table.table_name)}
                  onToggle={() => toggleTable(table.table_name)}
                  onHover={(h) => setHoveredTable(h ? table.table_name : null)}
                  onNav={() => navigate(`/dev/tables/${table.table_name}`)}
                />
              </g>
            )
          })}
        </svg>
      </div>
    </div>
  )
}

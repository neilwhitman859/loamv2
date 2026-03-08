import { useNavigate } from 'react-router-dom'
import ConfidenceBadge from './ConfidenceBadge'

export interface Column<T> {
  key: string
  label: string
  render?: (row: T) => React.ReactNode
  sortable?: boolean
  className?: string
}

interface Props<T> {
  data: T[]
  columns: Column<T>[]
  loading?: boolean
  search?: string
  onSearchChange?: (val: string) => void
  searchPlaceholder?: string
  detailPath?: (row: T) => string
  page?: number
  pageSize?: number
  totalCount?: number
  onPageChange?: (page: number) => void
  sortColumn?: string
  sortDirection?: 'asc' | 'desc'
  onSort?: (col: string) => void
}

export default function DataTable<T extends Record<string, unknown>>({
  data,
  columns,
  loading,
  search,
  onSearchChange,
  searchPlaceholder = 'Search...',
  detailPath,
  page = 0,
  pageSize = 50,
  totalCount,
  onPageChange,
  sortColumn,
  sortDirection,
  onSort,
}: Props<T>) {
  const navigate = useNavigate()
  const totalPages = totalCount != null ? Math.ceil(totalCount / pageSize) : null

  return (
    <div>
      {/* Search */}
      {onSearchChange && (
        <div className="mb-4">
          <input
            type="text"
            value={search || ''}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder={searchPlaceholder}
            className="w-full max-w-sm px-3 py-2 border border-earth-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-wine-500/30 focus:border-wine-500 bg-white"
          />
        </div>
      )}

      {/* Table */}
      <div className="bg-white rounded-lg border border-earth-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-earth-50 border-b border-earth-200">
                {columns.map((col) => (
                  <th
                    key={col.key}
                    className={`px-4 py-3 text-left font-semibold text-earth-700 ${
                      col.sortable !== false && onSort ? 'cursor-pointer hover:text-wine-700 select-none' : ''
                    } ${col.className || ''}`}
                    onClick={() => col.sortable !== false && onSort?.(col.key)}
                  >
                    <span className="inline-flex items-center gap-1">
                      {col.label}
                      {sortColumn === col.key && (
                        <span className="text-wine-500">{sortDirection === 'asc' ? '↑' : '↓'}</span>
                      )}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={columns.length} className="px-4 py-12 text-center text-earth-400">
                    Loading...
                  </td>
                </tr>
              ) : data.length === 0 ? (
                <tr>
                  <td colSpan={columns.length} className="px-4 py-12 text-center text-earth-400">
                    No results found
                  </td>
                </tr>
              ) : (
                data.map((row, i) => (
                    <tr
                      key={i}
                      className={`border-b border-earth-100 ${
                        detailPath ? 'hover:bg-wine-50/50 cursor-pointer' : 'hover:bg-earth-50'
                      } transition-colors`}
                      onClick={detailPath ? () => navigate(detailPath(row)) : undefined}
                    >
                      {columns.map((col) => (
                        <td key={col.key} className={`px-4 py-3 ${col.className || ''}`}>
                          {col.render ? col.render(row) : String(row[col.key] ?? '')}
                        </td>
                      ))}
                    </tr>
                  ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages != null && totalPages > 1 && onPageChange && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-earth-200 bg-earth-50/50">
            <span className="text-xs text-earth-500">
              Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, totalCount!)} of{' '}
              {totalCount!.toLocaleString()}
            </span>
            <div className="flex gap-1">
              <button
                onClick={() => onPageChange(page - 1)}
                disabled={page === 0}
                className="px-3 py-1 text-xs rounded border border-earth-300 bg-white hover:bg-earth-50 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                Prev
              </button>
              <span className="px-3 py-1 text-xs text-earth-600">
                {page + 1} / {totalPages}
              </span>
              <button
                onClick={() => onPageChange(page + 1)}
                disabled={page >= totalPages - 1}
                className="px-3 py-1 text-xs rounded border border-earth-300 bg-white hover:bg-earth-50 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                Next
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// Helper: confidence column renderer
export function confidenceRenderer(row: Record<string, unknown>) {
  const conf = row.confidence as number
  if (conf == null) return '—'
  return <ConfidenceBadge confidence={conf} />
}

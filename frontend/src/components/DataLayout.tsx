import { useState } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import GlobalSearch from './GlobalSearch'

const navItems = [
  { label: 'Dashboard', to: '/data' },
  { heading: 'Explore' },
  { label: 'Wines', to: '/data/wines' },
  { label: 'Producers', to: '/data/producers' },
  { label: 'Grapes', to: '/data/grapes' },
  { label: 'Appellations', to: '/data/appellations' },
  { label: 'Regions', to: '/data/regions' },
  { label: 'Countries', to: '/data/countries' },
  { heading: 'Developer' },
  { label: 'Schema', to: '/dev/schema' },
  { label: 'Tables', to: '/dev/tables' },
  { label: 'Strategy', to: '/dev/strategy' },
]

const buildDate = typeof __BUILD_TIMESTAMP__ !== 'undefined'
  ? new Date(__BUILD_TIMESTAMP__).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' })
  : null

export default function DataLayout() {
  const [menuOpen, setMenuOpen] = useState(false)
  const navContent = (
    <>
      <div className="flex-1 py-3 overflow-y-auto">
        {navItems.map((item, i) =>
          'heading' in item ? (
            <div key={i} className="px-5 pt-5 pb-1 text-[10px] font-semibold uppercase tracking-widest text-wine-200/60">
              {item.heading}
            </div>
          ) : (
            <NavLink
              key={item.to}
              to={item.to!}
              end={item.to === '/data'}
              onClick={() => setMenuOpen(false)}
              className={({ isActive }) =>
                `block px-5 py-2 text-sm transition-colors ${
                  isActive
                    ? 'bg-wine-700 text-white font-medium'
                    : 'text-wine-100/80 hover:bg-wine-800 hover:text-white'
                }`
              }
            >
              {item.label}
            </NavLink>
          )
        )}
      </div>
      <div className="p-4 border-t border-wine-700 text-[10px] text-wine-200/50">
        <div>Loam Data Explorer</div>
        {buildDate && <div className="mt-0.5">Deployed {buildDate}</div>}
      </div>
    </>
  )

  return (
    <div className="flex flex-col md:flex-row h-screen bg-earth-50">
      {/* Mobile header */}
      <div className="md:hidden bg-wine-900 text-white flex items-center justify-between px-4 py-3 flex-shrink-0">
        <div>
          <h1 className="text-lg font-semibold tracking-wide leading-tight">Loam</h1>
          <p className="text-[10px] text-wine-200">Data Explorer</p>
        </div>
        <button
          onClick={() => setMenuOpen(!menuOpen)}
          className="p-2 rounded hover:bg-wine-700 transition-colors"
          aria-label="Toggle menu"
        >
          {menuOpen ? (
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          ) : (
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
            </svg>
          )}
        </button>
      </div>

      {/* Mobile overlay menu */}
      {menuOpen && (
        <div className="md:hidden fixed inset-0 top-[52px] z-50 flex">
          <nav className="w-64 bg-wine-900 text-white flex flex-col h-full shadow-xl">
            {navContent}
          </nav>
          <div className="flex-1 bg-black/40" onClick={() => setMenuOpen(false)} />
        </div>
      )}

      {/* Desktop sidebar */}
      <nav className="hidden md:flex w-56 bg-wine-900 text-white flex-shrink-0 flex-col">
        <div className="p-5 border-b border-wine-700">
          <h1 className="text-lg font-semibold tracking-wide">Loam</h1>
          <p className="text-xs text-wine-200 mt-0.5">Data Explorer</p>
        </div>
        {navContent}
      </nav>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto">
        <div className="max-w-7xl mx-auto p-4 md:p-6">
          <div className="mb-6">
            <GlobalSearch />
          </div>
          <Outlet />
        </div>
      </main>
    </div>
  )
}

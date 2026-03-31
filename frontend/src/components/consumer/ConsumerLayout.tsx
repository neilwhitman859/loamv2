import { Outlet, Link, useLocation, useNavigate } from 'react-router-dom'
import { useState } from 'react'

export default function ConsumerLayout() {
  const location = useLocation()
  const navigate = useNavigate()
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)
  const isHome = location.pathname === '/'

  return (
    <div className="min-h-screen bg-earth-50 flex flex-col">
      {/* Header — compact, always visible */}
      {!isHome && (
        <header className="sticky top-0 z-50 bg-white/95 backdrop-blur-sm border-b border-earth-200">
          <div className="max-w-3xl mx-auto px-4 h-14 flex items-center gap-3">
            {/* Logo / Home link */}
            <Link to="/" className="font-display text-xl font-semibold text-wine-700 tracking-wide shrink-0">
              Loam
            </Link>

            {/* Inline search bar */}
            <form
              className="flex-1 relative"
              onSubmit={(e) => {
                e.preventDefault()
                const q = new FormData(e.currentTarget).get('q') as string
                if (q.trim()) navigate(`/search?q=${encodeURIComponent(q.trim())}`)
              }}
            >
              <input
                type="text"
                name="q"
                placeholder="Search wines, producers, regions..."
                className="w-full h-9 pl-9 pr-3 bg-earth-100 rounded-lg text-sm text-earth-800 placeholder:text-earth-400 border border-earth-200 focus:border-wine-400 focus:outline-none focus:ring-1 focus:ring-wine-400/30 transition-colors"
              />
              <svg className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-earth-400" fill="none" stroke="currentColor" strokeWidth={2} viewBox="0 0 24 24">
                <circle cx="11" cy="11" r="8" />
                <path d="m21 21-4.35-4.35" />
              </svg>
            </form>

            {/* Mobile menu toggle */}
            <button
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="md:hidden p-1.5 text-earth-500 hover:text-earth-700"
              aria-label="Menu"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" strokeWidth={2} viewBox="0 0 24 24">
                {mobileMenuOpen
                  ? <path d="M6 18L18 6M6 6l12 12" />
                  : <path d="M4 6h16M4 12h16M4 18h16" />
                }
              </svg>
            </button>
          </div>

          {/* Mobile nav dropdown */}
          {mobileMenuOpen && (
            <nav className="md:hidden border-t border-earth-100 bg-white px-4 py-3 space-y-1">
              <NavLink to="/explore" label="Explore" onClick={() => setMobileMenuOpen(false)} />
              <NavLink to="/regions" label="Regions" onClick={() => setMobileMenuOpen(false)} />
              <NavLink to="/grapes" label="Grapes" onClick={() => setMobileMenuOpen(false)} />
            </nav>
          )}
        </header>
      )}

      {/* Main content */}
      <main className="flex-1">
        <Outlet />
      </main>

      {/* Footer — minimal */}
      <footer className="border-t border-earth-200 bg-white">
        <div className="max-w-3xl mx-auto px-4 py-8 text-center">
          <Link to="/" className="font-display text-lg font-semibold text-wine-700 tracking-wide">
            Loam
          </Link>
          <p className="mt-1 text-xs text-earth-400">
            Wine intelligence. Place, grapes, people.
          </p>
          <div className="mt-4 flex justify-center gap-6 text-xs text-earth-400">
            <Link to="/about" className="hover:text-earth-600 transition-colors">About</Link>
            <Link to="/data" className="hover:text-earth-600 transition-colors">Data Explorer</Link>
          </div>
        </div>
      </footer>
    </div>
  )
}

function NavLink({ to, label, onClick }: { to: string; label: string; onClick?: () => void }) {
  const location = useLocation()
  const active = location.pathname.startsWith(to)
  return (
    <Link
      to={to}
      onClick={onClick}
      className={`block px-3 py-2 rounded-md text-sm font-medium transition-colors ${
        active
          ? 'text-wine-700 bg-wine-50'
          : 'text-earth-600 hover:text-earth-800 hover:bg-earth-50'
      }`}
    >
      {label}
    </Link>
  )
}

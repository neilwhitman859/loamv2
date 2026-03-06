import { NavLink, Outlet } from 'react-router-dom'

const navItems = [
  { label: 'Dashboard', to: '/' },
  { heading: 'Insights' },
  { label: 'Grapes', to: '/insights/grapes' },
  { label: 'Appellations', to: '/insights/appellations' },
  { label: 'Regions', to: '/insights/regions' },
  { label: 'Countries', to: '/insights/countries' },
  { heading: 'Data' },
  { label: 'Wines', to: '/data/wines' },
  { label: 'Producers', to: '/data/producers' },
]

export default function Layout() {
  return (
    <div className="flex h-screen bg-earth-50">
      {/* Sidebar */}
      <nav className="w-56 bg-wine-900 text-white flex-shrink-0 flex flex-col">
        <div className="p-5 border-b border-wine-700">
          <h1 className="text-lg font-semibold tracking-wide">Loam</h1>
          <p className="text-xs text-wine-200 mt-0.5">Wine Intelligence</p>
        </div>
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
                end={item.to === '/'}
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
          Loam v2 Data Explorer
        </div>
      </nav>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto">
        <div className="max-w-7xl mx-auto p-6">
          <Outlet />
        </div>
      </main>
    </div>
  )
}

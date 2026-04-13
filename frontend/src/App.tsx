import { Routes, Route, Link } from 'react-router-dom'

// Consumer pages
import ConsumerLayout from './components/consumer/ConsumerLayout'
import HomePage from './pages/consumer/HomePage'
import SearchPage from './pages/consumer/SearchPage'
import WinePage from './pages/consumer/WinePage'
import ProducerPage from './pages/consumer/ProducerPage'
import RegionPage from './pages/consumer/RegionPage'
import AppellationPage from './pages/consumer/AppellationPage'
import CountryPage from './pages/consumer/CountryPage'
import GrapePage from './pages/consumer/GrapePage'
import VineyardPage from './pages/consumer/VineyardPage'

// Data explorer (admin/dev)
import DataLayout from './components/DataLayout'
import DevLayout from './components/DevLayout'
import Dashboard from './pages/Dashboard'
import WinesList from './pages/data/WinesList'
import WineDetail from './pages/data/WineDetail'
import ProducersList from './pages/data/ProducersList'
import ProducerDetail from './pages/data/ProducerDetail'
import GrapesList from './pages/data/GrapesList'
import GrapeDetail from './pages/data/GrapeDetail'
import AppellationsList from './pages/data/AppellationsList'
import AppellationDetail from './pages/data/AppellationDetail'
import RegionsList from './pages/data/RegionsList'
import RegionDetail from './pages/data/RegionDetail'
import CountriesList from './pages/data/CountriesList'
import CountryDetail from './pages/data/CountryDetail'
import SearchResults from './pages/data/SearchResults'
import MapExplorer from './pages/data/MapExplorer'
import SchemaExplorer from './pages/SchemaExplorer'
import TableBrowser from './pages/TableBrowser'
import About from './pages/About'

function NotFoundPage() {
  return (
    <div className="max-w-3xl mx-auto px-4 py-16 text-center">
      <h1 className="font-display text-3xl font-semibold text-earth-900 mb-2">Page not found</h1>
      <p className="text-earth-500 mb-4">The page you're looking for doesn't exist.</p>
      <Link to="/" className="text-wine-600 text-sm hover:underline">Back to home</Link>
    </div>
  )
}

export default function App() {
  return (
    <Routes>
      {/* Consumer app — root-level, mobile-first */}
      <Route element={<ConsumerLayout />}>
        <Route path="/" element={<HomePage />} />
        <Route path="/search" element={<SearchPage />} />
        <Route path="/wine/:id" element={<WinePage />} />
        <Route path="/producer/:id" element={<ProducerPage />} />
        <Route path="/region/:id" element={<RegionPage />} />
        <Route path="/appellation/:id" element={<AppellationPage />} />
        <Route path="/country/:id" element={<CountryPage />} />
        <Route path="/grape/:id" element={<GrapePage />} />
        <Route path="/vineyard/:id" element={<VineyardPage />} />
        <Route path="/about" element={<About />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>

      {/* Data explorer — sidebar + max-w-7xl */}
      <Route element={<DataLayout />}>
        <Route path="/data" element={<Dashboard />} />
        <Route path="/data/wines" element={<WinesList />} />
        <Route path="/data/wines/:id" element={<WineDetail />} />
        <Route path="/data/producers" element={<ProducersList />} />
        <Route path="/data/producers/:id" element={<ProducerDetail />} />
        <Route path="/data/grapes" element={<GrapesList />} />
        <Route path="/data/grapes/:id" element={<GrapeDetail />} />
        <Route path="/data/appellations" element={<AppellationsList />} />
        <Route path="/data/appellations/:id" element={<AppellationDetail />} />
        <Route path="/data/regions" element={<RegionsList />} />
        <Route path="/data/regions/:id" element={<RegionDetail />} />
        <Route path="/data/countries" element={<CountriesList />} />
        <Route path="/data/countries/:id" element={<CountryDetail />} />
        <Route path="/data/search" element={<SearchResults />} />
        <Route path="/data/map" element={<MapExplorer />} />
      </Route>

      {/* Dev tools — sidebar + full-width */}
      <Route element={<DevLayout />}>
        <Route path="/dev/schema" element={<SchemaExplorer />} />
        <Route path="/dev/tables" element={<TableBrowser />} />
        <Route path="/dev/tables/:tableName" element={<TableBrowser />} />
        <Route path="/dev/about" element={<About />} />
      </Route>
    </Routes>
  )
}

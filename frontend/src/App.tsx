import { Routes, Route } from 'react-router-dom'
import DataLayout from './components/DataLayout'
import DevLayout from './components/DevLayout'
import LandingPage from './pages/LandingPage'
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
import SchemaExplorer from './pages/SchemaExplorer'
import TableBrowser from './pages/TableBrowser'

export default function App() {
  return (
    <Routes>
      {/* Landing page — standalone, no sidebar */}
      <Route path="/" element={<LandingPage />} />

      {/* Data section — sidebar + max-w-7xl */}
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
      </Route>

      {/* Dev section — sidebar + full-width */}
      <Route element={<DevLayout />}>
        <Route path="/dev/schema" element={<SchemaExplorer />} />
        <Route path="/dev/tables" element={<TableBrowser />} />
        <Route path="/dev/tables/:tableName" element={<TableBrowser />} />
      </Route>
    </Routes>
  )
}

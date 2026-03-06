import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import GrapeInsights from './pages/GrapeInsights'
import AppellationInsights from './pages/AppellationInsights'
import RegionInsights from './pages/RegionInsights'
import CountryInsights from './pages/CountryInsights'
import Wines from './pages/Wines'
import Producers from './pages/Producers'
import Detail from './pages/Detail'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/insights/grapes" element={<GrapeInsights />} />
        <Route path="/insights/appellations" element={<AppellationInsights />} />
        <Route path="/insights/regions" element={<RegionInsights />} />
        <Route path="/insights/countries" element={<CountryInsights />} />
        <Route path="/data/wines" element={<Wines />} />
        <Route path="/data/producers" element={<Producers />} />
        <Route path="/insights/:type/:id" element={<Detail />} />
      </Route>
    </Routes>
  )
}

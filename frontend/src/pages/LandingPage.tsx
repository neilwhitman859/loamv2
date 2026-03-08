import { Link } from 'react-router-dom'

export default function LandingPage() {
  return (
    <div className="min-h-screen bg-wine-900 flex flex-col items-center justify-center text-center px-6">
      <h1 className="text-5xl md:text-7xl font-bold text-white tracking-wide mb-4">Loam</h1>
      <p className="text-xl md:text-2xl text-wine-200 font-light mb-2">Wine Intelligence</p>
      <p className="text-lg text-wine-200/60 italic mb-12">Site is still decanting.</p>
      <Link
        to="/data"
        className="inline-block px-8 py-3 bg-white/10 hover:bg-white/20 text-white border border-white/20 rounded-lg text-sm font-medium tracking-wide transition-colors"
      >
        Explore the data
      </Link>
    </div>
  )
}

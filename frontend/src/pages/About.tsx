export default function About() {
  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold text-earth-900 mb-1">About Loam</h1>
      <p className="text-sm text-earth-500 mb-8">Wine intelligence, from the ground up</p>

      <div className="space-y-10">
        {/* The Problem */}
        <section>
          <p className="text-sm text-earth-700 leading-relaxed mb-3">
            Winemaking is one of the oldest conversations between human intention and the natural world. Every bottle is shaped by decisions — when to harvest, how to ferment, how long to age — made by people who balance science with intuition, tradition with experimentation. A great wine reflects the chemistry of its soil, the weather of its year, and the philosophy of the person who made it.
          </p>
          <p className="text-sm text-earth-700 leading-relaxed mb-3">
            Yet most wine data treats this complexity as an afterthought. Scores live in one place, prices in another, geographic context somewhere else entirely. The craft gets reduced to a number.
          </p>
          <p className="text-sm text-earth-700 leading-relaxed">
            Loam exists to restore that context. Every wine links to its producer, grapes, appellation, region, and country. Scores, prices, and AI-generated insights layer on top of this knowledge graph — not to replace the art of understanding wine, but to illuminate it.
          </p>
        </section>

        {/* Principles — the beliefs that drive how Loam is built, and what makes it different */}
        <section>
          <h2 className="text-lg font-semibold text-earth-900 mb-2">Principles</h2>
          <p className="text-sm text-earth-500 mb-4">The beliefs that drive how Loam is built</p>
          <div className="space-y-5">
            <div>
              <div className="flex items-baseline gap-2 mb-1">
                <span className="text-wine-700 font-bold text-sm">1.</span>
                <h3 className="text-sm font-semibold text-earth-900">Relationships over records</h3>
              </div>
              <p className="text-sm text-earth-700 leading-relaxed pl-5">
                A wine is not a row in a table — it's a node in a graph. Most wine databases are flat catalogs. Loam is a fully linked knowledge graph: navigate from a wine to its grape varieties, from a grape to all the appellations where it grows, from an appellation to the climate and soil that define it. The value is in the connections.
              </p>
            </div>
            <div>
              <div className="flex items-baseline gap-2 mb-1">
                <span className="text-wine-700 font-bold text-sm">2.</span>
                <h3 className="text-sm font-semibold text-earth-900">Context is everything</h3>
              </div>
              <p className="text-sm text-earth-700 leading-relaxed pl-5">
                A score without context is just a number. Wine is inseparable from place — and from the people who make it. Loam models the full geographic hierarchy from country to appellation, each level carrying its own climate data, soil profiles, and regulatory context. But it also connects to the human side: the producer's winemaking philosophy, their stylistic choices, their relationship with the land. That's what makes a rating meaningful.
              </p>
            </div>
            <div>
              <div className="flex items-baseline gap-2 mb-1">
                <span className="text-wine-700 font-bold text-sm">3.</span>
                <h3 className="text-sm font-semibold text-earth-900">Real data only</h3>
              </div>
              <p className="text-sm text-earth-700 leading-relaxed pl-5">
                No synthetic scores, no hallucinated tasting notes, no fabricated prices. Every score comes from a real critic or community source. Every price comes from a real merchant. Every data point traces back to a verifiable source. Gaps are acknowledged, not filled with guesses.
              </p>
            </div>
            <div>
              <div className="flex items-baseline gap-2 mb-1">
                <span className="text-wine-700 font-bold text-sm">4.</span>
                <h3 className="text-sm font-semibold text-earth-900">AI as amplifier, not author</h3>
              </div>
              <p className="text-sm text-earth-700 leading-relaxed pl-5">
                Every grape, appellation, region, and country has been analyzed by AI to generate overviews, tasting profiles, terroir descriptions, and food pairing suggestions. But this isn't generic content — each insight is grounded in the specific data Loam holds for that entity. AI helps surface patterns and connections across thousands of wines, but the art of making and appreciating wine remains fundamentally human.
              </p>
            </div>
            <div>
              <div className="flex items-baseline gap-2 mb-1">
                <span className="text-wine-700 font-bold text-sm">5.</span>
                <h3 className="text-sm font-semibold text-earth-900">Depth over breadth</h3>
              </div>
              <p className="text-sm text-earth-700 leading-relaxed pl-5">
                Better to have rich, linked data for the wines we cover than thin data for every wine that exists. Quality of understanding beats quantity of entries.
              </p>
            </div>
          </div>
        </section>

        {/* The Data Model — what the system actually contains */}
        <section>
          <h2 className="text-lg font-semibold text-earth-900 mb-2">The Data Model</h2>
          <p className="text-sm text-earth-500 mb-4">Five interconnected layers that form Loam's knowledge graph</p>
          <div className="space-y-3">
            <div className="flex gap-3 text-sm">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Catalog</span>
              <span className="text-earth-700">Wines, producers, grape varieties, and varietal categories — the foundational entities and their relationships</span>
            </div>
            <div className="flex gap-3 text-sm">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Geography</span>
              <span className="text-earth-700">Countries, regions, and appellations with hierarchical linkage, designation types, and terroir context</span>
            </div>
            <div className="flex gap-3 text-sm">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Scores</span>
              <span className="text-earth-700">Critic and community ratings aggregated per vintage from multiple sources</span>
            </div>
            <div className="flex gap-3 text-sm">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Prices</span>
              <span className="text-earth-700">Market pricing with merchant attribution across vintages</span>
            </div>
            <div className="flex gap-3 text-sm">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Insights</span>
              <span className="text-earth-700">AI-generated analysis at every level — grape profiles, appellation terroir, regional character, producer style</span>
            </div>
          </div>
        </section>

        {/* The Full Vision — what Loam will be when complete */}
        <section>
          <h2 className="text-lg font-semibold text-earth-900 mb-2">The Full Vision</h2>
          <p className="text-sm text-earth-700 leading-relaxed mb-3">
            Loam is being built to serve anyone who takes wine seriously — whether that's a collector evaluating a vintage, a sommelier building a list, or a curious drinker trying to understand why they like what they like. When complete, it will be the most contextually rich wine intelligence platform available.
          </p>
          <div className="space-y-2 text-sm text-earth-700 mb-4">
            <div className="flex gap-2">
              <span className="text-wine-600 shrink-0">-</span>
              <span><span className="font-semibold text-earth-800">Every wine with AI insights</span> — tasting profiles, food pairings, aging potential, and style analysis grounded in real data</span>
            </div>
            <div className="flex gap-2">
              <span className="text-wine-600 shrink-0">-</span>
              <span><span className="font-semibold text-earth-800">Vintage intelligence</span> — per-year weather analysis at the appellation level, connecting growing conditions to wine quality across regions</span>
            </div>
            <div className="flex gap-2">
              <span className="text-wine-600 shrink-0">-</span>
              <span><span className="font-semibold text-earth-800">Deep terroir mapping</span> — soil composition, elevation, water proximity, and microclimate data for every appellation</span>
            </div>
            <div className="flex gap-2">
              <span className="text-wine-600 shrink-0">-</span>
              <span><span className="font-semibold text-earth-800">Multi-source scoring</span> — aggregated critic and community ratings from across the industry, not just a single source</span>
            </div>
            <div className="flex gap-2">
              <span className="text-wine-600 shrink-0">-</span>
              <span><span className="font-semibold text-earth-800">Regulatory context</span> — the rules that define each appellation: allowed grapes, aging requirements, yield limits, and the stories behind them</span>
            </div>
          </div>
          <p className="text-sm text-earth-700 leading-relaxed">
            The goal is simple: when you look at any bottle, Loam should be able to tell you not just what it scored, but why it tastes the way it does — the grapes, the soil, the climate of that year, the winemaker's approach, and how it all fits together. Great wine is both art and science. Loam is built to honor both.
          </p>
        </section>

        {/* Built With */}
        <section>
          <h2 className="text-lg font-semibold text-earth-900 mb-2">Built With</h2>
          <p className="text-sm text-earth-700 leading-relaxed">
            PostgreSQL on Supabase, React + TypeScript frontend, and Claude AI for batch enrichment. The data foundation comes from open wine catalog sources, enriched with scraped market data and AI analysis.
          </p>
        </section>
      </div>
    </div>
  )
}

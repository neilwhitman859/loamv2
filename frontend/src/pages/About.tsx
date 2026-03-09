export default function About() {
  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold text-earth-900 mb-1">About Loam</h1>
      <p className="text-sm text-earth-500 mb-8">Wine intelligence, from the ground up</p>

      <div className="space-y-8">
        <section>
          <h2 className="text-lg font-semibold text-earth-900 mb-2">The Vision</h2>
          <p className="text-sm text-earth-700 leading-relaxed mb-3">
            Wine is one of the most complex consumer products in the world. A single bottle carries the influence of its grape varieties, the soil and climate of its origin, the philosophy of its maker, and the specific conditions of its vintage year. Yet most wine data is fragmented — scores live in one place, prices in another, geographic context somewhere else entirely.
          </p>
          <p className="text-sm text-earth-700 leading-relaxed">
            Loam connects all of it. Every wine links to its producer, grapes, appellation, region, and country. Scores, prices, and AI-generated insights layer on top of this knowledge graph to create a single, coherent picture of the wine world.
          </p>
        </section>

        <section>
          <h2 className="text-lg font-semibold text-earth-900 mb-2">What Makes Loam Different</h2>
          <div className="space-y-4">
            <div>
              <h3 className="text-sm font-semibold text-earth-800 mb-1">A Knowledge Graph, Not a List</h3>
              <p className="text-sm text-earth-700 leading-relaxed">
                Most wine databases are flat catalogs. Loam is a fully linked graph — navigate from a wine to its grape varieties, from a grape to all the appellations where it grows, from an appellation to the climate and soil that define it. Every relationship is explorable.
              </p>
            </div>
            <div>
              <h3 className="text-sm font-semibold text-earth-800 mb-1">AI-Enriched at Every Level</h3>
              <p className="text-sm text-earth-700 leading-relaxed">
                Every grape, appellation, region, and country has been analyzed by AI to generate overviews, tasting profiles, terroir descriptions, and food pairing suggestions. This isn't generic content — each insight is grounded in the specific data we hold for that entity.
              </p>
            </div>
            <div>
              <h3 className="text-sm font-semibold text-earth-800 mb-1">Real Data Only</h3>
              <p className="text-sm text-earth-700 leading-relaxed">
                Every score comes from a real critic or community source. Every price comes from a real merchant. We don't synthesize ratings or hallucinate tasting notes. If we don't have the data, we say so.
              </p>
            </div>
            <div>
              <h3 className="text-sm font-semibold text-earth-800 mb-1">Geographic Intelligence</h3>
              <p className="text-sm text-earth-700 leading-relaxed">
                Wine is inseparable from place. Loam models the full geographic hierarchy — from country to region to appellation — with each level carrying its own climate data, soil profiles, and regulatory context. Understanding where a wine comes from means understanding the wine itself.
              </p>
            </div>
          </div>
        </section>

        <section>
          <h2 className="text-lg font-semibold text-earth-900 mb-2">The Data Layers</h2>
          <ul className="space-y-3 text-sm text-earth-700">
            <li className="flex gap-3">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Catalog</span>
              <span>Wines, producers, grape varieties, and varietal categories — the foundational entities and their relationships</span>
            </li>
            <li className="flex gap-3">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Geography</span>
              <span>Countries, regions, and appellations with hierarchical linkage, designation types, and terroir context</span>
            </li>
            <li className="flex gap-3">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Scores</span>
              <span>Critic and community ratings aggregated per vintage from multiple sources</span>
            </li>
            <li className="flex gap-3">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Prices</span>
              <span>Market pricing with merchant attribution across vintages</span>
            </li>
            <li className="flex gap-3">
              <span className="text-wine-600 font-semibold shrink-0 w-24">Insights</span>
              <span>AI-generated analysis at every level — grape profiles, appellation terroir, regional character, producer style</span>
            </li>
          </ul>
        </section>

        <section>
          <h2 className="text-lg font-semibold text-earth-900 mb-2">Where It's Going</h2>
          <p className="text-sm text-earth-700 leading-relaxed mb-3">
            Loam is being built to serve anyone who takes wine seriously — whether that's a collector evaluating a vintage, a sommelier building a list, or a curious drinker trying to understand why they like what they like.
          </p>
          <p className="text-sm text-earth-700 leading-relaxed">
            The knowledge graph will continue to deepen: vintage-level weather analysis, soil composition mapping, regulatory data for every appellation, and richer scoring coverage across more sources. The goal is to make the full context behind every bottle accessible and understandable.
          </p>
        </section>

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

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const ANTHROPIC_API_KEY = Deno.env.get("ANTHROPIC_API_KEY");
const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

// FEATURE FLAG: On-demand Grade B enrichment is paused pending a quality fix.
// Session 10 audit (2026-04-10) found Grade B at 2.65/5 with widespread
// factual_error tags (91 errors across 20 sampled wines). The audit showed
// Sonnet/Haiku confidently confabulate facts (invented ABV, oak regimes,
// soil types, comparable SKUs). The fix (retrieval-grounded prompts +
// fact-check pass) is queued for a Phase 4 side branch. Until it lands,
// the Edge Function returns 503 so we don't write unreliable prose to DB.
//
// To re-enable: set env var ENRICHMENT_ENABLED=true on the function.
const ENRICHMENT_ENABLED = (Deno.env.get("ENRICHMENT_ENABLED") || "false").toLowerCase() === "true";

// Model IDs — centralized here, keep in sync with pipeline/lib/models.py
const SONNET_MODEL = "claude-sonnet-4-6";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey, x-client-info",
};

interface EnrichmentResult {
  hook: string;
  wine_summary: string;
  terroir_expression: string;
  vinification_summary: string;
  food_pairing: string;
  comparable_wines: string;
  style_profile: string;
  cellar_recommendation: string;
  aging_potential_years: number | null;
  drinking_window_min_years: number | null;
  drinking_window_max_years: number | null;
  sensory: {
    acidity: number;
    tannin: number;
    body: number;
    sweetness: number;
    alcohol: number;
    color_intensity: string;
    aroma_intensity: string;
    finish_length: string;
    complexity: string;
    quality_level: string;
  };
}

async function assembleContext(supabase: any, wineId: string) {
  // Wine + producer
  const { data: wine } = await supabase
    .from("wine_detail_view")
    .select("*")
    .eq("id", wineId)
    .single();

  if (!wine) throw new Error(`Wine ${wineId} not found`);

  // Grapes — fetch display_name (preferred) and name (fallback)
  const { data: grapes } = await supabase
    .from("wine_grapes")
    .select("percentage, grapes(name, display_name)")
    .eq("wine_id", wineId);

  // Scores
  const { data: scores } = await supabase
    .from("wine_vintage_scores")
    .select("score, score_scale, medal, publication:publications(name), critic, vintage_year, tasting_note")
    .eq("wine_id", wineId)
    .order("score", { ascending: false })
    .limit(10);

  // Vintages with depth data
  const { data: vintages } = await supabase
    .from("wine_vintages")
    .select("*")
    .eq("wine_id", wineId)
    .order("vintage_year", { ascending: false })
    .limit(5);

  // Appellation context
  let appellationInsight = null;
  if (wine.appellation_id) {
    const { data } = await supabase
      .from("appellation_insights")
      .select("ai_overview, ai_terroir, ai_climate, ai_style")
      .eq("appellation_id", wine.appellation_id)
      .single();
    appellationInsight = data;
  }

  // Region context
  let regionInsight = null;
  if (wine.region_id) {
    const { data } = await supabase
      .from("region_insights")
      .select("ai_overview, ai_terroir, ai_climate")
      .eq("region_id", wine.region_id)
      .single();
    regionInsight = data;
  }

  // Prices
  const { data: prices } = await supabase
    .from("wine_vintage_prices")
    .select("price_usd, price_original, currency, merchant_name, vintage_year")
    .eq("wine_id", wineId)
    .limit(5);

  // Farming certifications
  const { data: certs } = await supabase
    .from("wine_farming_certifications")
    .select("farming_certifications(name)")
    .eq("wine_id", wineId);

  // Food pairings (existing)
  const { data: pairings } = await supabase
    .from("wine_food_pairings")
    .select("food_categories(name)")
    .eq("wine_id", wineId);

  return { wine, grapes, scores, vintages, appellationInsight, regionInsight, prices, certs, pairings };
}

function buildPrompt(ctx: any): string {
  const { wine, grapes, scores, vintages, appellationInsight, regionInsight, prices, certs, pairings } = ctx;

  const grapeList = (grapes || []).map((g: any) => {
    // Prefer display_name (human-readable) over name (canonical key)
    const name = g.grapes?.display_name || g.grapes?.name || "Unknown";
    return g.percentage ? `${name} (${g.percentage}%)` : name;
  }).join(", ");

  const scoreLines = (scores || []).slice(0, 6).map((s: any) => {
    const pub = s.publication?.name || s.critic || "Unknown";
    if (s.medal) return `${pub}: ${s.medal} medal (${s.vintage_year || "NV"})`;
    return `${pub}: ${s.score}/${s.score_scale || 100} (${s.vintage_year || "NV"})${s.tasting_note ? " — " + s.tasting_note.slice(0, 150) : ""}`;
  }).join("\n");

  const vintageLines = (vintages || []).slice(0, 3).map((v: any) => {
    const parts = [`Vintage ${v.vintage_year || "NV"}`];
    if (v.abv) parts.push(`ABV ${v.abv}%`);
    if (v.ph) parts.push(`pH ${v.ph}`);
    if (v.ta_g_l) parts.push(`TA ${v.ta_g_l} g/L`);
    if (v.rs_g_l) parts.push(`RS ${v.rs_g_l} g/L`);
    if (v.fermentation_vessel) parts.push(`Fermented in ${v.fermentation_vessel}`);
    if (v.yeast_type) parts.push(`${v.yeast_type} yeast`);
    if (v.mlf !== null) parts.push(v.mlf ? "Full MLF" : "No MLF");
    if (v.aging_vessel) parts.push(`Aged in ${v.aging_vessel}`);
    if (v.duration_in_oak_months) parts.push(`${v.duration_in_oak_months} months oak`);
    if (v.oak_origin) parts.push(`${v.oak_origin} oak`);
    if (v.new_oak_pct) parts.push(`${v.new_oak_pct}% new oak`);
    if (v.closure) parts.push(`Closure: ${v.closure}`);
    if (v.cases_produced) parts.push(`${v.cases_produced} cases`);
    return parts.join(", ");
  }).join("\n");

  const priceLines = (prices || []).map((p: any) => {
    const amt = p.price_usd ? `$${p.price_usd}` : `${p.price_original} ${p.currency}`;
    return `${amt} (${p.merchant_name}, ${p.vintage_year || "NV"})`;
  }).join(", ");

  const certList = (certs || []).map((c: any) => c.farming_certifications?.name).filter(Boolean).join(", ");
  const pairingList = (pairings || []).map((p: any) => p.food_categories?.name).filter(Boolean).join(", ");

  let prompt = `You are a wine expert writing for Loam, a wine intelligence platform. Your voice is knowledgeable but approachable — like a sommelier friend who explains the "why" behind everything. Be specific (name soils, climate patterns, geological formations). Connect place to taste. Have a point of view. No generic filler, no sommelier theater, no vague hedging.

Enrich this wine with a complete profile. Return ONLY valid JSON matching the schema below.

## Wine Identity
- Name: ${wine.name}
- Producer: ${wine.producer_name || "Unknown"}
- Country: ${wine.country_name || "Unknown"}
- Region: ${wine.region_name || "Unknown"}
- Appellation: ${wine.appellation_name || "Unknown"}
- Type: ${wine.wine_type || "table"} ${wine.color || ""} ${wine.effervescence || "still"}
- Grapes: ${grapeList || "Unknown"}
- Sweetness: ${wine.sweetness_level || "Unknown"}`;

  if (wine.soil_description) prompt += `\n- Soil: ${wine.soil_description}`;
  if (wine.elevation_m) prompt += `\n- Elevation: ${wine.elevation_m}m`;
  if (wine.aspect) prompt += `\n- Aspect: ${wine.aspect}`;
  if (wine.training_method) prompt += `\n- Training: ${wine.training_method}`;
  if (wine.vine_density_per_ha) prompt += `\n- Vine density: ${wine.vine_density_per_ha}/ha`;
  if (wine.vine_age_description) prompt += `\n- Vine age: ${wine.vine_age_description}`;
  if (wine.vinification_notes) prompt += `\n- Vinification: ${wine.vinification_notes.slice(0, 500)}`;
  if (wine.description) prompt += `\n- Tasting: ${wine.description.slice(0, 500)}`;
  if (certList) prompt += `\n- Certifications: ${certList}`;

  if (vintageLines) prompt += `\n\n## Vintage Data\n${vintageLines}`;
  if (scoreLines) prompt += `\n\n## Critic Scores\n${scoreLines}`;
  if (priceLines) prompt += `\n\n## Prices\n${priceLines}`;

  if (appellationInsight) {
    prompt += `\n\n## Appellation Context`;
    if (appellationInsight.ai_terroir) prompt += `\nTerroir: ${appellationInsight.ai_terroir.slice(0, 400)}`;
    if (appellationInsight.ai_climate) prompt += `\nClimate: ${appellationInsight.ai_climate.slice(0, 300)}`;
    if (appellationInsight.ai_style) prompt += `\nStyle: ${appellationInsight.ai_style.slice(0, 300)}`;
  }

  if (regionInsight) {
    prompt += `\n\n## Region Context`;
    if (regionInsight.ai_terroir) prompt += `\nTerroir: ${regionInsight.ai_terroir.slice(0, 300)}`;
    if (regionInsight.ai_climate) prompt += `\nClimate: ${regionInsight.ai_climate.slice(0, 300)}`;
  }

  if (pairingList) prompt += `\n\n## Existing Food Pairings\n${pairingList}`;

  prompt += `\n\n## Output Format
Return ONLY a JSON object (no markdown, no backticks) with these fields:
{
  "hook": "2-3 sentences. The 30-second story — what makes this wine worth knowing about.",
  "wine_summary": "1-2 paragraphs. The rich story: why this wine matters, what makes the producer distinctive, how terroir shapes the wine. Be specific.",
  "terroir_expression": "1 paragraph. How the specific soil, climate, and site express themselves in this wine. Connect geology to glass.",
  "vinification_summary": "1 paragraph. How the wine is made and why those choices matter. Explain technical concepts briefly.",
  "food_pairing": "3-5 specific pairings with brief reasoning. Name real dishes, not categories. Include casual and fine dining.",
  "comparable_wines": "2-3 similar wines with reasoning. Format: 'Wine Name — why it's comparable.'",
  "style_profile": "One phrase, e.g., 'Full-bodied dry red with firm tannins and earthy depth'",
  "cellar_recommendation": "1-2 sentences on when to drink and storage advice.",
  "aging_potential_years": null or integer (total years from vintage this wine can age),
  "drinking_window_min_years": null or integer (minimum years from vintage before drinking),
  "drinking_window_max_years": null or integer (maximum years from vintage before it fades),
  "sensory": {
    "acidity": 1-5 integer (1=low, 5=high),
    "tannin": 1-5 integer (1=none, 5=very high; 1 for whites),
    "body": 1-5 integer (1=light, 5=full),
    "sweetness": 1-5 integer (1=bone dry, 5=very sweet),
    "alcohol": 1-5 integer (1=low <11%, 5=high >15%),
    "color_intensity": "pale|medium|deep",
    "aroma_intensity": "light|medium|pronounced",
    "finish_length": "short|medium|long",
    "complexity": "simple|moderate|complex",
    "quality_level": "acceptable|good|very_good|outstanding|exceptional"
  }
}`;

  return prompt;
}

async function writeResults(supabase: any, wineId: string, result: EnrichmentResult, vintageYear: number) {
  const now = new Date().toISOString();
  const refreshAfter = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString();

  // Upsert wine_insights
  await supabase.from("wine_insights").upsert({
    wine_id: wineId,
    ai_hook: result.hook,
    ai_wine_summary: result.wine_summary,
    ai_terroir_expression: result.terroir_expression,
    ai_vinification_summary: result.vinification_summary,
    ai_food_pairing: result.food_pairing,
    ai_comparable_wines: result.comparable_wines,
    ai_style_profile: result.style_profile,
    ai_cellar_recommendation: result.cellar_recommendation,
    typical_aging_potential_years: result.aging_potential_years,
    typical_drinking_window_min_years: result.drinking_window_min_years,
    typical_drinking_window_max_years: result.drinking_window_max_years,
    enrichment_tier: "B",
    confidence: 0.85,
    enriched_at: now,
    refresh_after: refreshAfter,
  }, { onConflict: "wine_id" });

  // Upsert wine_vintage_tasting_insights
  if (result.sensory) {
    await supabase.from("wine_vintage_tasting_insights").upsert({
      wine_id: wineId,
      vintage_year: vintageYear,
      sensory_acidity: result.sensory.acidity,
      sensory_tannin: result.sensory.tannin,
      sensory_body: result.sensory.body,
      sensory_sweetness: result.sensory.sweetness,
      sensory_alcohol: result.sensory.alcohol,
      color_intensity: result.sensory.color_intensity,
      aroma_intensity: result.sensory.aroma_intensity,
      finish_length: result.sensory.finish_length,
      complexity: result.sensory.complexity,
      quality_level: result.sensory.quality_level,
      confidence: 0.8,
      enriched_at: now,
      refresh_after: refreshAfter,
    }, { onConflict: "wine_id,vintage_year" });
  }

  // Update wine data_grade
  await supabase.from("wines").update({ data_grade: "B" }).eq("id", wineId);
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  // Feature flag check — respond 503 if enrichment is disabled
  if (!ENRICHMENT_ENABLED) {
    return new Response(JSON.stringify({
      error: "enrichment_paused",
      message: "On-demand Grade B enrichment is paused pending the quality fix. Session 10 audit (2026-04-10) found Grade B at 2.65/5 with widespread factual errors. Re-enable by setting ENRICHMENT_ENABLED=true on the function after the fix lands.",
      session: "phase_4_enrichment_quality_fix",
    }), {
      status: 503,
      headers: { ...corsHeaders, "Content-Type": "application/json", "Retry-After": "86400" },
    });
  }

  if (!ANTHROPIC_API_KEY) {
    return new Response(JSON.stringify({ error: "ANTHROPIC_API_KEY not configured" }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  try {
    const { wine_id } = await req.json();
    if (!wine_id) {
      return new Response(JSON.stringify({ error: "wine_id required" }), {
        status: 400,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // Assemble context
    const ctx = await assembleContext(supabase, wine_id);
    const prompt = buildPrompt(ctx);

    // Determine most recent vintage year for tasting insights
    const vintageYear = (ctx.vintages && ctx.vintages.length > 0)
      ? ctx.vintages[0].vintage_year || 0
      : 0;

    // Call Claude Sonnet
    const startTime = Date.now();
    const resp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: SONNET_MODEL,
        max_tokens: 2000,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const data = await resp.json();
    const latencyMs = Date.now() - startTime;

    if (!data.content?.[0]?.text) {
      throw new Error(`Empty response from Claude: ${JSON.stringify(data)}`);
    }

    // Parse JSON response
    const rawText = data.content[0].text.trim();
    // Strip markdown code fences if present
    const jsonText = rawText.replace(/^```json\n?/, "").replace(/\n?```$/, "");
    const result: EnrichmentResult = JSON.parse(jsonText);

    // Write results to DB
    await writeResults(supabase, wine_id, result, vintageYear);

    // Log enrichment
    const inputTokens = data.usage?.input_tokens || 0;
    const outputTokens = data.usage?.output_tokens || 0;
    const costUsd = (inputTokens * 3 / 1_000_000) + (outputTokens * 15 / 1_000_000);

    await supabase.from("enrichment_log").insert({
      entity_type: "wine",
      entity_id: wine_id,
      enrichment_type: "grade_b",
      model: SONNET_MODEL,
      prompt_template: "enrich-wine-v1",
      input_tokens: inputTokens,
      output_tokens: outputTokens,
      cost_usd: costUsd,
      fields_updated: ["wine_insights", "wine_vintage_tasting_insights", "data_grade"],
      status: "completed",
      attempts: 1,
    });

    return new Response(JSON.stringify({
      success: true,
      wine_id,
      data_grade: "B",
      latency_ms: latencyMs,
      cost_usd: Math.round(costUsd * 10000) / 10000,
      hook: result.hook,
      style_profile: result.style_profile,
    }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });

  } catch (err: any) {
    // Log error
    try {
      const body = await req.clone().json().catch(() => ({}));
      await supabase.from("enrichment_log").insert({
        entity_type: "wine",
        entity_id: body.wine_id || null,
        enrichment_type: "grade_b",
        model: SONNET_MODEL,
        status: "error",
        error_message: err.message?.slice(0, 500),
        attempts: 1,
      });
    } catch { /* ignore logging errors */ }

    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});

import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = "https://vgbppjhmvbggfjztzobl.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZnYnBwamhtdmJnZ2ZqenR6b2JsIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjU4NTU0NiwiZXhwIjoyMDg4MTYxNTQ2fQ.ubAQ3dlxurKTE5IZGMwjMzYegBfxA4sF6Tvs2yzKu5c";
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

async function main() {
  try {
    // Try to find source_types
    let { data: sources, error } = await sb.from("source_types").select("*");
    if (error) {
      console.log("source_types error:", error.message);
      console.log("Trying to find all tables...");
      
      // Query information_schema
      const { data: tables, error: e2 } = await sb.rpc('get_tables', {schema: 'public'}).catch(err => ({ error: err }));
      console.log("Result:", tables, e2);
    } else {
      console.log("Found source_types:");
      sources.forEach(s => console.log(`  - ${s.type_name} (id: ${s.id})`));
    }
    
    // Check wines table structure more deeply
    console.log("\n=== WINES TABLE STRUCTURE ===");
    const { data: wines } = await sb.from("wines").select("*").limit(1);
    if (wines && wines.length === 0) {
      console.log("Wines table is empty, but schema exists");
      // Try to get column info via RPC or direct query
      const { data: cols } = await sb.rpc('get_columns', { table_name: 'wines' }).catch(err => ({ data: null }));
      if (cols) {
        console.log("Columns:", JSON.stringify(cols, null, 2));
      } else {
        console.log("Can't query columns via RPC, but table exists");
      }
    }
    
    // Get producers table structure
    console.log("\n=== PRODUCERS TABLE STRUCTURE ===");
    const { data: prods } = await sb.from("producers").select("*").limit(1);
    if (prods && prods.length === 0) {
      console.log("Producers table is empty");
    }
    
    // Check wine_vintages
    console.log("\n=== WINE_VINTAGES TABLE STRUCTURE ===");
    const { data: vints } = await sb.from("wine_vintages").select("*").limit(1);
    if (vints && vints.length === 0) {
      console.log("Wine_vintages table is empty");
    }
    
  } catch (err) {
    console.error("Fatal:", err.message);
  }
}

main();

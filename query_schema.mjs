import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = "https://vgbppjhmvbggfjztzobl.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZnYnBwamhtdmJnZ2ZqenR6b2JsIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjU4NTU0NiwiZXhwIjoyMDg4MTYxNTQ2fQ.ubAQ3dlxurKTE5IZGMwjMzYegBfxA4sF6Tvs2yzKu5c";
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

async function main() {
  try {
    // Get table info by checking a few rows from each key table
    const tables = ["wines", "producers", "wine_vintages", "varietal_categories"];
    
    for (const table of tables) {
      console.log(`\n=== ${table.toUpperCase()} ===`);
      const { data, error } = await sb.from(table).select("*").limit(1);
      
      if (error) {
        console.error(`Error: ${error.message}`);
        continue;
      }
      
      if (data && data.length > 0) {
        const cols = Object.keys(data[0]);
        console.log("Columns:", cols.join(", "));
        console.log("Sample row:");
        console.log(JSON.stringify(data[0], null, 2));
      } else {
        console.log("(no rows)");
      }
    }
    
    // Get all varietal_categories for a full list
    console.log("\n=== ALL VARIETAL_CATEGORIES ===");
    const { data: vcats } = await sb.from("varietal_categories").select("*").limit(200);
    if (vcats) {
      console.log(`Found ${vcats.length} categories:`);
      vcats.forEach(v => {
        console.log(`  - ${v.name} (id: ${v.id}, type: ${v.type}, color: ${v.color})`);
      });
    }
  } catch (err) {
    console.error("Fatal:", err.message);
  }
}

main();

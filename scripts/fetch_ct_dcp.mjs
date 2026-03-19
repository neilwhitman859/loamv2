#!/usr/bin/env node
/**
 * Connecticut DCP OpenAccess Wine Price List Scraper
 *
 * Source: biznet.ct.gov/DCPOpenAccess/LiquorControl
 * Value:  UPC ↔ COLA(TTB ID#) bridge — the "Rosetta Stone" for identity matching
 * Fields: Item#, ETS Type, UPC, COLA(TTB ID#), Brand Name, Description, Vintage, Alc%, Size, BPC, Price, Origin
 *
 * Architecture:
 *   Phase 1: Puppeteer collects supplier GUIDs from the index page
 *   Phase 2: Downloads each supplier's PDF price list
 *   Phase 3: Parses PDFs with pdf-parse to extract tabular wine data
 *
 * Output: data/imports/ct_dcp_wines.json
 *
 * Usage:
 *   node scripts/fetch_ct_dcp.mjs                    # Full run
 *   node scripts/fetch_ct_dcp.mjs --month February --year 2026
 *   node scripts/fetch_ct_dcp.mjs --guids-only       # Just collect GUIDs, don't download PDFs
 *   node scripts/fetch_ct_dcp.mjs --resume            # Resume from last checkpoint
 *   node scripts/fetch_ct_dcp.mjs --chrome-profile    # Use your Chrome profile cookies (CLOSE CHROME FIRST!)
 */

import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, '..', 'data', 'imports');
const PDF_DIR = path.join(DATA_DIR, 'ct_dcp_pdfs');
const GUIDS_FILE = path.join(DATA_DIR, 'ct_dcp_guids.json');
const OUTPUT_FILE = path.join(DATA_DIR, 'ct_dcp_wines.json');
const CHECKPOINT_FILE = path.join(DATA_DIR, 'ct_dcp_checkpoint.json');

const BASE_URL = 'https://biznet.ct.gov/DCPOpenAccess/LiquorControl';
const INDEX_URL = `${BASE_URL}/ItemList.aspx`;
const DISPLAY_URL = `${BASE_URL}/DisplayItem.aspx?ItemID=`;

const DELAY_MS = 2000; // polite delay between PDF downloads
const MONTH = process.argv.includes('--month') ? process.argv[process.argv.indexOf('--month') + 1] : 'February';
const YEAR = process.argv.includes('--year') ? process.argv[process.argv.indexOf('--year') + 1] : '2026';
const GUIDS_ONLY = process.argv.includes('--guids-only');
const RESUME = process.argv.includes('--resume');
const USE_CHROME_PROFILE = process.argv.includes('--chrome-profile');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================
// Phase 1: Collect supplier GUIDs from the index page
// ============================================================
async function collectGUIDs(browser) {
  console.log(`\n=== Phase 1: Collecting supplier GUIDs (${MONTH} ${YEAR}) ===`);

  const page = await browser.newPage();
  // Set a realistic user agent to avoid bot detection
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36');
  await page.goto(INDEX_URL, { waitUntil: 'networkidle2', timeout: 30000 });

  // Debug: check if we got blocked
  const pageTitle = await page.title();
  const bodyText = await page.evaluate(() => document.body.innerText.substring(0, 200));
  console.log(`  Page title: "${pageTitle}"`);
  if (bodyText.includes('rejected') || bodyText.includes('blocked')) {
    console.error('  BLOCKED by WAF. Page content:', bodyText);
    throw new Error('Blocked by BITS BOT WAF');
  }

  // Select month and year via ASP.NET postback
  // The dropdowns are: #ddlPostMonth and #ddlPostYear
  await page.select('#ddlPostMonth', MONTH);
  await sleep(4000); // wait for ASP.NET postback to complete

  // Check if year needs changing
  const currentYear = await page.$eval('#ddlPostYear', el => el.value);
  if (currentYear !== YEAR) {
    await page.select('#ddlPostYear', YEAR);
    await sleep(4000);
  }

  await page.waitForSelector('a[href*="DisplayItem"]', { timeout: 15000 }).catch(() => {
    console.log('  Warning: No DisplayItem links found after month selection');
  });

  // Extract all supplier links
  const suppliers = await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll('a[href*="DisplayItem"]'));
    return links.map(a => {
      const guid = a.href.match(/ItemID=([a-f0-9-]+)/i)?.[1];
      const tr = a.closest('tr');
      const tds = tr ? Array.from(tr.querySelectorAll('td')) : [];
      const company = tds.length > 1 ? tds[1].innerText.split('\n')[0].trim() : '';
      const address = tds.length > 2 ? tds[2].innerText.replace(/\n/g, ', ').trim() : '';

      // Determine if this is a Supplier or Wholesaler based on section headers
      let section = 'unknown';
      let el = tr;
      while (el) {
        const text = el.innerText || '';
        if (text.includes('Suppliers') && !text.includes('Wholesalers')) { section = 'supplier'; break; }
        if (text.includes('Wholesalers')) { section = 'wholesaler'; break; }
        el = el.previousElementSibling;
      }

      return { guid, company, address, section };
    }).filter(s => s.guid);
  });

  console.log(`  Found ${suppliers.length} price list links`);

  // Save GUIDs
  const guidData = {
    month: MONTH,
    year: YEAR,
    collected_at: new Date().toISOString(),
    total: suppliers.length,
    suppliers
  };

  fs.writeFileSync(GUIDS_FILE, JSON.stringify(guidData, null, 2));
  console.log(`  Saved to ${GUIDS_FILE}`);

  await page.close();
  return suppliers;
}

// ============================================================
// Phase 2: Download PDFs
// ============================================================
async function downloadPDFs(browser, suppliers) {
  console.log(`\n=== Phase 2: Downloading ${suppliers.length} PDFs ===`);

  if (!fs.existsSync(PDF_DIR)) fs.mkdirSync(PDF_DIR, { recursive: true });

  // Load checkpoint for resume
  let completed = new Set();
  if (RESUME && fs.existsSync(CHECKPOINT_FILE)) {
    const cp = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8'));
    completed = new Set(cp.completed || []);
    console.log(`  Resuming: ${completed.size} already downloaded`);
  }

  const page = await browser.newPage();

  // Configure PDF download behavior
  const client = await page.createCDPSession();
  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: PDF_DIR
  });

  let downloaded = 0;
  let errors = 0;

  for (const supplier of suppliers) {
    if (completed.has(supplier.guid)) continue;

    try {
      const pdfPath = path.join(PDF_DIR, `${supplier.guid}.pdf`);

      // Navigate to the DisplayItem page — it serves a PDF
      const response = await page.goto(`${DISPLAY_URL}${supplier.guid}`, {
        waitUntil: 'networkidle2',
        timeout: 30000
      });

      // Check if response is a PDF
      const contentType = response?.headers()['content-type'] || '';

      if (contentType.includes('pdf') || contentType.includes('octet-stream')) {
        // Direct PDF download - use fetch to save
        const pdfBuffer = await response.buffer();
        // Check for WAF "Request Rejected" block page
        const headerStr = pdfBuffer.toString('utf8', 0, 200);
        if (headerStr.includes('Request Rejected')) {
          console.error(`  WAF BLOCKED: ${supplier.company} — got "Request Rejected" PDF`);
          errors++;
          await sleep(DELAY_MS * 3);
          continue;
        }
        fs.writeFileSync(pdfPath, pdfBuffer);
      } else {
        // Page might embed the PDF or render HTML
        // Try to get the PDF URL from the page
        const pdfUrl = await page.evaluate(() => {
          const embed = document.querySelector('embed[type="application/pdf"], object[type="application/pdf"], iframe[src*=".pdf"]');
          return embed?.src || embed?.data || null;
        });

        if (pdfUrl) {
          // Fetch the PDF directly
          const pdfResponse = await page.goto(pdfUrl, { waitUntil: 'networkidle2', timeout: 30000 });
          const pdfBuffer = await pdfResponse.buffer();
          fs.writeFileSync(pdfPath, pdfBuffer);
        } else {
          // Try printing the page to PDF as fallback
          await page.pdf({ path: pdfPath, format: 'Letter' });
        }
      }

      completed.add(supplier.guid);
      downloaded++;

      if (downloaded % 20 === 0) {
        console.log(`  Downloaded ${downloaded}/${suppliers.length - completed.size + downloaded} (${supplier.company})`);
        // Save checkpoint
        fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify({
          completed: [...completed],
          last_update: new Date().toISOString()
        }));
      }

      await sleep(DELAY_MS);

    } catch (err) {
      console.error(`  ERROR downloading ${supplier.company} (${supplier.guid}): ${err.message}`);
      errors++;
      // Continue to next
      await sleep(DELAY_MS * 2);
    }
  }

  // Final checkpoint
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify({
    completed: [...completed],
    last_update: new Date().toISOString()
  }));

  console.log(`  Downloaded: ${downloaded}, Errors: ${errors}, Total complete: ${completed.size}`);
  await page.close();
  return [...completed];
}

// ============================================================
// Phase 3: Parse PDFs
// ============================================================
async function parsePDFs(completedGuids, suppliers) {
  console.log(`\n=== Phase 3: Parsing ${completedGuids.length} PDFs ===`);

  let PDFParseClass;
  try {
    const mod = await import('pdf-parse');
    PDFParseClass = mod.PDFParse || mod.default;
    if (!PDFParseClass) throw new Error('Could not find PDFParse export');
  } catch (e) {
    console.error('pdf-parse not available:', e.message);
    console.log('Skipping Phase 3. PDFs are saved in', PDF_DIR);
    return [];
  }

  const allWines = [];
  let parsed = 0;
  let errors = 0;

  const supplierMap = new Map(suppliers.map(s => [s.guid, s]));

  for (const guid of completedGuids) {
    const pdfPath = path.join(PDF_DIR, `${guid}.pdf`);
    if (!fs.existsSync(pdfPath)) continue;

    try {
      const dataBuffer = fs.readFileSync(pdfPath);
      let text = '';

      if (typeof PDFParseClass === 'function' && PDFParseClass.prototype) {
        // v2 API: class-based
        const parser = new PDFParseClass();
        const result = await parser.parseBuffer(dataBuffer);
        text = result.text || '';
      } else if (typeof PDFParseClass === 'function') {
        // v1 API: function-based
        const result = await PDFParseClass(dataBuffer);
        text = result.text || '';
      } else {
        throw new Error('Unknown pdf-parse API');
      }

      // Parse the tabular data from the PDF text
      // Columns: Item # | ETS Type | UPC | COLA(TTB ID#) | Brand Name | Description | Vintage | Alc % | Size | BPC | Price | Origin | Notes
      const lines = text.split('\n').map(l => l.trim()).filter(Boolean);

      // Find header line to identify column positions
      const headerIdx = lines.findIndex(l =>
        l.includes('Item') && l.includes('UPC') && l.includes('COLA')
      );

      if (headerIdx === -1) {
        // No wine data header found - might be spirits or empty
        continue;
      }

      // Parse data lines after header
      const supplier = supplierMap.get(guid);
      const companyName = supplier?.company || 'Unknown';

      for (let i = headerIdx + 1; i < lines.length; i++) {
        const line = lines[i];
        if (!line || line.startsWith('Page') || line.includes('Price List')) continue;

        // Try to extract UPC and COLA from the line
        // UPC patterns: 12-digit (080887 49396 6) or with dashes (0-80887-49492-5)
        const upcMatch = line.match(/\b(\d[\d\s-]{10,15}\d)\b/);
        // COLA/TTB ID pattern: typically 14-digit numbers
        const colaMatch = line.match(/\b(\d{11,15})\b/);

        if (upcMatch || colaMatch) {
          // This line likely has product data
          // Parse as best we can from the space-delimited text
          const wine = {
            raw_line: line,
            supplier: companyName,
            supplier_guid: guid,
            upc: upcMatch ? upcMatch[1].replace(/[\s-]/g, '') : null,
            cola_ttb_id: null,
            brand: null,
            description: null,
            vintage: null,
            abv: null,
            size: null,
            price: null,
          };

          // Try to extract vintage (4-digit year 19xx or 20xx)
          const vintageMatch = line.match(/\b(19\d{2}|20[0-2]\d)\b/);
          if (vintageMatch) wine.vintage = parseInt(vintageMatch[1]);

          // Try to extract ABV (number followed by decimal, typically 5-20)
          const abvMatch = line.match(/\b(\d{1,2}\.\d{1,2})\b/);
          if (abvMatch) {
            const val = parseFloat(abvMatch[1]);
            if (val >= 5 && val <= 25) wine.abv = val;
          }

          // Try to extract price (number like 66.00, 72.00)
          const priceMatch = line.match(/\b(\d{1,4}\.\d{2})\b/g);
          if (priceMatch) {
            // Last decimal number is usually the price
            wine.price = parseFloat(priceMatch[priceMatch.length - 1]);
          }

          // Size (750ML, 1.5L, etc.)
          const sizeMatch = line.match(/\b(\d+(?:\.\d+)?)\s*(ML|L)\b/i);
          if (sizeMatch) wine.size = sizeMatch[0].toUpperCase();

          // COLA - longer numeric sequences that aren't UPC
          const allNums = line.match(/\b\d{11,15}\b/g);
          if (allNums) {
            for (const num of allNums) {
              const clean = num.replace(/[\s-]/g, '');
              if (clean !== wine.upc && clean.length >= 11) {
                wine.cola_ttb_id = clean;
                break;
              }
            }
          }

          allWines.push(wine);
        }
      }

      parsed++;
      if (parsed % 50 === 0) {
        console.log(`  Parsed ${parsed} PDFs, ${allWines.length} wine rows found`);
      }

    } catch (err) {
      errors++;
      if (errors <= 5) console.error(`  Parse error for ${guid}: ${err.message}`);
    }
  }

  console.log(`  Parsed: ${parsed}, Errors: ${errors}, Wine rows: ${allWines.length}`);
  return allWines;
}

// ============================================================
// Main
// ============================================================
async function main() {
  console.log('=== Connecticut DCP Wine Price List Scraper ===');
  console.log(`Month: ${MONTH} ${YEAR}`);
  console.log(`Output: ${OUTPUT_FILE}`);

  // Chrome profile mode: uses your real browser cookies to bypass WAF
  // IMPORTANT: Close Chrome completely before running with --chrome-profile
  // Use a COPY of the Chrome profile so it works even while Chrome is open
  const chromeProfileDir = path.join(__dirname, '..', '.chrome-temp-profile');
  const launchOptions = USE_CHROME_PROFILE
    ? {
        headless: 'new',
        executablePath: 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
        userDataDir: chromeProfileDir,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-extensions']
      }
    : {
        headless: 'new',
        args: ['--no-sandbox', '--disable-setuid-sandbox']
      };

  if (USE_CHROME_PROFILE) {
    console.log(`  Using Chrome profile: ${chromeProfileDir}`);
    console.log('  ⚠️  Make sure Chrome is COMPLETELY closed!');
  }

  const browser = await puppeteer.launch(launchOptions);

  try {
    // Phase 1: Collect GUIDs
    let suppliers;
    if (RESUME && fs.existsSync(GUIDS_FILE)) {
      const saved = JSON.parse(fs.readFileSync(GUIDS_FILE, 'utf8'));
      suppliers = saved.suppliers;
      console.log(`  Loaded ${suppliers.length} GUIDs from cache`);
    } else {
      suppliers = await collectGUIDs(browser);
    }

    if (GUIDS_ONLY) {
      console.log('\n--guids-only: stopping after Phase 1');
      await browser.close();
      return;
    }

    // Phase 2: Download PDFs
    const completedGuids = await downloadPDFs(browser, suppliers);

    await browser.close();

    // Phase 3: Parse PDFs
    const wines = await parsePDFs(completedGuids, suppliers);

    // Stats
    const stats = {
      total_wines: wines.length,
      has_upc: wines.filter(w => w.upc).length,
      has_cola: wines.filter(w => w.cola_ttb_id).length,
      has_both: wines.filter(w => w.upc && w.cola_ttb_id).length,
      has_vintage: wines.filter(w => w.vintage).length,
      has_abv: wines.filter(w => w.abv).length,
      has_price: wines.filter(w => w.price).length,
      unique_suppliers: new Set(wines.map(w => w.supplier)).size,
    };

    console.log('\n=== RESULTS ===');
    console.log(`Total wine rows: ${stats.total_wines}`);
    console.log(`Has UPC: ${stats.has_upc}`);
    console.log(`Has COLA/TTB ID: ${stats.has_cola}`);
    console.log(`Has BOTH UPC + COLA: ${stats.has_both} ⭐`);
    console.log(`Has vintage: ${stats.has_vintage}`);
    console.log(`Has ABV: ${stats.has_abv}`);
    console.log(`Unique suppliers: ${stats.unique_suppliers}`);

    // Save output
    const output = {
      metadata: {
        source: 'Connecticut DCP Liquor Control Posted Prices',
        url: 'https://biznet.ct.gov/DCPOpenAccess/LiquorControl/ItemList.aspx',
        month: MONTH,
        year: YEAR,
        fetched_at: new Date().toISOString(),
        stats
      },
      wines
    };

    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2));
    console.log(`\nSaved to ${OUTPUT_FILE}`);

  } catch (err) {
    console.error('Fatal error:', err);
    await browser.close();
    process.exit(1);
  }
}

main();

// TTB COLA Chrome Scraper — paste this into DevTools Console on TTB search page
// It fetches batches of TTB IDs from the local Python server, scrapes detail pages,
// and sends extracted data back. Runs until all records are processed.

(async () => {
    const SERVER = 'http://localhost:8765';
    const CONCURRENCY = 20;
    const DETAIL_URL = 'https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicDisplaySearchAdvanced&ttbid=';

    function extractFields(html) {
        const fields = {};

        function getField(labelPattern) {
            const re = new RegExp('<strong>' + labelPattern + '[^<]*</strong>(.*?)</td>', 'si');
            const m = html.match(re);
            if (!m) return null;
            let text = m[1].replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&#(\d+);/g, (_, n) => String.fromCharCode(n)).replace(/\s+/g, ' ').trim();
            if (!text || text === 'N/A') return null;
            return text;
        }

        fields.grape_varietals = getField('Grape Varietal');
        fields.wine_vintage = getField('Wine Vintage');
        fields.wine_appellation = getField('(?:Wine )?Appellation');
        fields.abv = getField('Alcohol Content');

        // Phone (no <strong> wrapper)
        const phoneM = html.match(/Phone Number:[&nbsp;\s]*([\d()\-\s]+)/);
        if (phoneM) fields.phone = phoneM[1].trim();

        // Email
        const emailM = html.match(/Email[^:]*:[&nbsp;\s]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
        if (emailM) fields.email = emailM[1];

        // Applicant from Plant Registry section
        const permitM = html.match(/Plant Registry.*?Principal Place of Business.*?<\/strong>\s*(?:<\/td>\s*<td[^>]*>\s*)?(.*?)(?:<\/td>|<hr|While the Alcohol)/si);
        if (permitM) {
            const parts = permitM[1].split(/<br\s*\/?>/i)
                .map(p => p.replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').trim())
                .filter(p => p);
            const permitRe = /^[A-Z]{2}-[A-Z]-\d+$/;
            let foundName = false;
            for (const p of parts) {
                if (permitRe.test(p)) continue;
                if (!foundName) { fields.applicant_name = p; foundName = true; continue; }
                const csz = p.match(/^(.+?),?\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
                if (csz) {
                    fields.applicant_city = csz[1].replace(/,$/, '').trim();
                    fields.applicant_state = csz[2];
                    fields.applicant_zip = csz[3];
                    break;
                } else if (!fields.applicant_address) {
                    fields.applicant_address = p;
                }
            }
        }

        // Image IDs from imageWindow() calls
        const imageIds = [...html.matchAll(/imageWindow\(['"]?(\d+)['"]?\)/g)].map(m => m[1]);
        if (imageIds.length > 0) fields.image_ids = imageIds;

        // Remove null/undefined values
        return Object.fromEntries(Object.entries(fields).filter(([k, v]) => v != null));
    }

    let totalProcessed = 0;
    let totalErrors = 0;
    let running = true;
    let batchNum = 0;

    console.log('%c[TTB Scraper] Starting...', 'color: green; font-weight: bold');

    while (running) {
        // Get batch from server
        let batch;
        try {
            const batchResp = await fetch(SERVER + '/batch');
            batch = await batchResp.json();
        } catch (e) {
            console.log('%c[TTB Scraper] Server unreachable, retrying in 5s...', 'color: red');
            await new Promise(r => setTimeout(r, 5000));
            continue;
        }

        if (batch.length === 0) {
            console.log('%c[TTB Scraper] No more records. Done!', 'color: green; font-weight: bold');
            running = false;
            break;
        }

        batchNum++;
        const results = [];
        const errors = [];

        // Process with concurrency limit
        for (let i = 0; i < batch.length; i += CONCURRENCY) {
            const chunk = batch.slice(i, i + CONCURRENCY);
            const promises = chunk.map(async (rec) => {
                try {
                    const resp = await fetch(DETAIL_URL + rec.ttb_id);
                    const html = await resp.text();

                    if (!html.includes('Application Detail')) {
                        errors.push({ ttb_id: rec.ttb_id, error: 'waf_or_error' });
                        return;
                    }

                    const fields = extractFields(html);
                    results.push({
                        ttb_id: rec.ttb_id,
                        brand: rec.brand,
                        era: rec.era,
                        fields: fields,
                    });
                } catch (e) {
                    errors.push({ ttb_id: rec.ttb_id, error: e.message });
                }
            });

            await Promise.all(promises);
        }

        // Send results to server
        try {
            await fetch(SERVER + '/results', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ results, errors }),
            });
        } catch (e) {
            console.log('%c[TTB Scraper] Failed to send results, retrying...', 'color: red');
            await new Promise(r => setTimeout(r, 2000));
            continue;
        }

        totalProcessed += results.length;
        totalErrors += errors.length;

        if (batchNum % 10 === 0) {
            console.log(`%c[TTB Scraper] Batch ${batchNum}: ${totalProcessed.toLocaleString()} processed, ${totalErrors} errors`, 'color: cyan');
        }
    }

    console.log(`%c[TTB Scraper] Complete! ${totalProcessed.toLocaleString()} processed, ${totalErrors} errors`, 'color: green; font-weight: bold');
})();

// TTB COLA Printable Page Scraper — paste into DevTools Console on TTB search page
// Extracts ALL form fields from BOTH old and new form versions.
// Server: python -m pipeline.fetch.ttb_printable_scraper --concurrency 15

(async () => {
    const SERVER = 'http://localhost:8766';
    const CONCURRENCY = 25;
    const PRINT_URL = 'https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicFormDisplay&ttbid=';

    function extractFields(html) {
        const fields = {};

        // Generic field extractor for <div class="label">PATTERN</div> ... <div class="data">VALUE</div>
        function getField(labelPattern) {
            const re = new RegExp(
                '<div\\s+class="(?:bold)?label">.*?' + labelPattern + '.*?</div>' +
                '\\s*(?:<br\\s*/?>|\\s)*' +
                '<div\\s+class="data">\\s*(.*?)\\s*</div>',
                'si'
            );
            const m = html.match(re);
            if (!m) return null;
            let text = m[1].replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ')
                .replace(/&amp;/g, '&').replace(/&#(\d+);/g, (_, n) => String.fromCharCode(n))
                .replace(/\s+/g, ' ').trim();
            if (!text || text === '\u00A0' || text === 'N/A' || text === '') return null;
            return text;
        }

        // Detect form version: old form has "NET CONTENTS" and "ALCOHOL CONTENT"
        const isOldForm = html.includes('NET CONTENTS') || html.includes('ALCOHOL CONTENT');

        // === FIELDS PRESENT ON BOTH FORMS ===
        fields.rep_id_no = getField('REP\\.?\\s*ID');
        fields.permit_number = getField('PLANT REGISTRY');
        fields.source_of_product = getField('SOURCE OF PRODUCT');
        fields.serial_number = getField('SERIAL NUMBER');
        fields.class_type_desc = getField('TYPE OF PRODUCT');
        fields.brand_name = getField('BRAND NAME');
        fields.fanciful_name = getField('FANCIFUL NAME');
        fields.mailing_address = getField('MAILING ADDRESS');
        fields.grape_varietals = getField('GRAPE VARIETAL');
        fields.qualifications = getField('QUALIFICATIONS');
        fields.expiration_date = getField('EXPIRATION DATE');

        // === FIELD 8: Full applicant block with DBA/tradename ===
        fields.applicant_dba = getField('NAME AND ADDRESS OF APPLICANT');

        // === CT and OR codes (top of form) ===
        const ctMatch = html.match(/<div\s+class="(?:bold)?label">\s*CT\s*<\/div>\s*(?:<br\s*\/?>|\s)*<div\s+class="data">\s*(.*?)\s*<\/div>/si);
        if (ctMatch) {
            const ct = ctMatch[1].replace(/<[^>]+>/g, '').trim();
            if (ct && ct !== '\u00A0') fields.ct_code = ct;
        }
        const orMatch = html.match(/<div\s+class="(?:bold)?label">\s*OR\s*<\/div>\s*(?:<br\s*\/?>|\s)*<div\s+class="data">\s*(.*?)\s*<\/div>/si);
        if (orMatch) {
            const or_val = orMatch[1].replace(/<[^>]+>/g, '').trim();
            if (or_val && or_val !== '\u00A0') fields.or_code = or_val;
        }

        if (isOldForm) {
            // === OLD FORM (pre-~2013) — different field numbers ===
            fields.formula = getField('FORMULA/SOP');
            if (!fields.formula) fields.formula = getField('FORMULA');
            fields.lab_no = getField('LAB\\.?\\s*NO');
            fields.net_contents = getField('NET CONTENTS');
            fields.abv = getField('ALCOHOL CONTENT');
            fields.wine_appellation = getField('WINE APPELLATION');
            if (!fields.wine_appellation) fields.wine_appellation = getField('14\\.\\s*WINE APPELLATION');
            fields.wine_vintage = getField('WINE VINTAGE');
            if (!fields.wine_vintage) fields.wine_vintage = getField('15\\.\\s*WINE VINTAGE');
            fields.phone = getField('PHONE NUMBER');
            fields.fax_number = getField('FAX NUMBER');
            fields.email = getField('EMAIL');
            fields.type_of_application = getField('TYPE OF APPLICATION');
            fields.date_of_application = getField('DATE OF APPLICATION');
            fields.applicant_name = getField('PRINT NAME OF APPLICANT');
            fields.approval_date = getField('DATE ISSUED');
        } else {
            // === NEW FORM (post-~2013) — standard field numbers ===
            fields.formula = getField('9\\.\\s*FORMULA') || getField('FORMULA');
            fields.wine_appellation = getField('WINE APPELLATION');
            fields.phone = getField('PHONE NUMBER');
            fields.email = getField('EMAIL');
            fields.type_of_application = getField('TYPE OF APPLICATION');
            fields.date_of_application = getField('DATE OF APPLICATION');
            fields.applicant_name = getField('PRINT NAME OF APPLICANT');
            fields.approval_date = getField('DATE ISSUED');
            // Field 15 (new form — checkbox area)
            fields.field_15 = getField('15\\.');
        }

        // === APPLICANT ADDRESS (from permit/plant registry block) ===
        const addrBlock = html.match(/PLANT REGISTRY.*?<div\s+class="data">([\s\S]*?)<\/div>/i);
        if (addrBlock) {
            const text = addrBlock[1].replace(/<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, '')
                .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').trim();
            const lines = text.split('\n').map(s => s.trim()).filter(s => s);
            for (const line of lines) {
                const csz = line.match(/^(.+?),?\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
                if (csz) {
                    fields.applicant_city = csz[1].replace(/,$/, '').trim();
                    fields.applicant_state = csz[2];
                    fields.applicant_zip = csz[3];
                    break;
                }
                if (!fields.applicant_address && /^\d+\s/.test(line)) {
                    fields.applicant_address = line;
                }
            }
        }

        // === LABEL DIMENSIONS ===
        // "Actual Dimensions: 4.3 inches W X 3.75 inches H"
        const dimMatches = [...html.matchAll(/Actual Dimensions:\s*([\d.]+\s*inches?\s*W\s*X\s*[\d.]+\s*inches?\s*H)/gi)];
        if (dimMatches.length > 0) {
            fields.label_dimensions = dimMatches.map(m => m[1].trim()).join('; ');
        }

        // Label type + dimensions (e.g. "Brand (front) or keg collar: 4.3 inches W X 3.75 inches H")
        const labelTypeMatches = [...html.matchAll(/((?:Brand|Back|Strip|Neck|Keg)[^<]*?)(?:<[^>]*>)*\s*Actual Dimensions:\s*([\d.]+\s*inches?\s*W\s*X\s*[\d.]+\s*inches?\s*H)/gi)];
        if (labelTypeMatches.length > 0) {
            fields.label_details = labelTypeMatches.map(m =>
                m[1].replace(/<[^>]+>/g, '').trim() + ': ' + m[2].trim()
            ).join('; ');
        }

        // Bottle capacity
        const capMatch = html.match(/(?:total\s+)?(?:bottle\s+)?capacity[:\s]*([^<]+)/i);
        if (capMatch) {
            const c = capMatch[1].replace(/&nbsp;/g, ' ').trim();
            if (c && c !== '\u00A0') fields.total_bottle_capacity = c;
        }

        // === IMAGE IDs ===
        const imageIds = [...html.matchAll(/imageWindow\(['"]?(\d+)['"]?\)/g)].map(m => m[1]);
        if (imageIds.length > 0) fields.image_ids = imageIds;

        // Remove null/undefined values
        return Object.fromEntries(Object.entries(fields).filter(([k, v]) => v != null));
    }

    let totalProcessed = 0;
    let totalErrors = 0;
    let totalWaf = 0;
    let totalOldForm = 0;
    let totalNewForm = 0;
    let running = true;
    let batchNum = 0;

    console.log('%c[TTB Printable] Starting — both old + new form versions...', 'color: green; font-weight: bold');

    while (running) {
        let batch;
        try {
            const batchResp = await fetch(SERVER + '/batch');
            batch = await batchResp.json();
        } catch (e) {
            console.log('%c[TTB Printable] Server unreachable, retrying in 5s...', 'color: red');
            await new Promise(r => setTimeout(r, 5000));
            continue;
        }

        if (batch.length === 0) {
            console.log('%c[TTB Printable] No more records. Done!', 'color: green; font-weight: bold');
            running = false;
            break;
        }

        batchNum++;
        const results = [];
        const errors = [];

        for (let i = 0; i < batch.length; i += CONCURRENCY) {
            const chunk = batch.slice(i, i + CONCURRENCY);
            const promises = chunk.map(async (rec) => {
                try {
                    const resp = await fetch(PRINT_URL + rec.ttb_id);
                    const html = await resp.text();

                    // WAF detection
                    if (html.length < 5000 || html.includes('bobcmn') || html.includes('UiwV')) {
                        errors.push({ ttb_id: rec.ttb_id, error: 'waf' });
                        totalWaf++;
                        return;
                    }

                    if (!html.includes('class="data"')) {
                        errors.push({ ttb_id: rec.ttb_id, error: 'no_form' });
                        return;
                    }

                    const fields = extractFields(html);

                    // Track form version
                    if (html.includes('NET CONTENTS') || html.includes('ALCOHOL CONTENT')) {
                        totalOldForm++;
                    } else {
                        totalNewForm++;
                    }

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

        try {
            await fetch(SERVER + '/results', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ results, errors }),
            });
        } catch (e) {
            console.log('%c[TTB Printable] Failed to send results, retrying...', 'color: red');
            await new Promise(r => setTimeout(r, 2000));
            continue;
        }

        totalProcessed += results.length;
        totalErrors += errors.length;

        if (batchNum % 10 === 0) {
            const wafStr = totalWaf > 0 ? ` [WAF: ${totalWaf}]` : '';
            console.log(
                `%c[TTB Printable] Batch ${batchNum}: ${totalProcessed.toLocaleString()} processed (old:${totalOldForm} new:${totalNewForm}), ${totalErrors} errors${wafStr}`,
                'color: cyan'
            );
        }
    }

    console.log(
        `%c[TTB Printable] Complete! ${totalProcessed.toLocaleString()} processed (old:${totalOldForm} new:${totalNewForm}), ${totalErrors} errors, ${totalWaf} WAF blocks`,
        'color: green; font-weight: bold'
    );
})();

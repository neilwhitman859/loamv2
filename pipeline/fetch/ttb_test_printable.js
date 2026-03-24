// Quick test: paste this into Chrome DevTools Console on TTB search page
// Tests if publicFormDisplay (printable version) is accessible via fetch()

(async () => {
    const ids = ['25014001000640', '24068001000639', '23150001000385', '22236001000476', '21108001000123'];

    console.log('%c[TEST] Testing printable version access...', 'color: yellow; font-weight: bold');

    for (const id of ids) {
        // Test 1: Printable version
        try {
            const r1 = await fetch('https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicFormDisplay&ttbid=' + id);
            const h1 = await r1.text();
            const hasData = h1.includes('class="data"');
            const hasError = h1.includes('Error Message');
            const hasWAF = h1.includes('bobcmn');
            const hasAppellation = h1.toLowerCase().includes('appellation');
            const hasABV = h1.toLowerCase().includes('alcohol content');
            console.log(`%c[PRINT] ${id}: ${h1.length}b | data=${hasData} error=${hasError} waf=${hasWAF} appellation=${hasAppellation} abv=${hasABV}`,
                hasData ? 'color: green' : 'color: red');
        } catch(e) {
            console.log(`%c[PRINT] ${id}: FETCH FAILED - ${e.message}`, 'color: red');
        }

        // Test 2: Regular detail (for comparison)
        try {
            const r2 = await fetch('https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicDisplaySearchBasic&ttbid=' + id);
            const h2 = await r2.text();
            const hasDetail = h2.includes('Application Detail');
            const hasAppellation2 = h2.toLowerCase().includes('appellation');
            const hasABV2 = h2.toLowerCase().includes('alcohol content');
            console.log(`%c[DETAIL] ${id}: ${h2.length}b | detail=${hasDetail} appellation=${hasAppellation2} abv=${hasABV2}`,
                hasDetail ? 'color: green' : 'color: orange');
        } catch(e) {
            console.log(`%c[DETAIL] ${id}: FETCH FAILED - ${e.message}`, 'color: red');
        }
    }

    console.log('%c[TEST] Done! Check results above.', 'color: yellow; font-weight: bold');
    console.log('%cIf PRINT lines show data=true, we can scrape printable versions!', 'color: cyan');
    console.log('%cIf PRINT lines show error=true or waf=true, printable is blocked.', 'color: cyan');
})();

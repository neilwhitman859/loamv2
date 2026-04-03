// TTB Cookie Relay — paste into Chrome DevTools Console on any ttbonline.gov page
// Sends browser cookies to localhost Python server every 30 seconds.
// The image downloader uses these cookies to bypass Shape Security WAF.

(async () => {
    const SERVER = 'http://localhost:8766';
    const INTERVAL = 30000; // 30 seconds

    // MessageChannel delay — not throttled in background tabs
    function delay(ms) {
        return new Promise(resolve => {
            const start = performance.now();
            const ch = new MessageChannel();
            ch.port1.onmessage = () => {
                if (performance.now() - start >= ms) { ch.port1.close(); resolve(); }
                else ch.port2.postMessage('');
            };
            ch.port2.postMessage('');
        });
    }

    async function sendCookies() {
        try {
            const resp = await fetch(SERVER + '/cookies', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ cookies: document.cookie }),
            });
            const data = await resp.json();
            return data;
        } catch (e) {
            return null;
        }
    }

    console.log('%c[TTB Cookie Relay] Starting — sending cookies every 30s to ' + SERVER, 'color: green; font-weight: bold');

    // Send immediately
    let result = await sendCookies();
    if (result) {
        console.log('%c[TTB Cookie Relay] ✓ Connected — ' + (result.cookie_count || 0) + ' cookies sent', 'color: green');
    } else {
        console.log('%c[TTB Cookie Relay] ✗ Server not reachable at ' + SERVER + '. Start the downloader first.', 'color: red; font-weight: bold');
    }

    // Keep sending
    while (true) {
        await delay(INTERVAL);
        result = await sendCookies();
        if (result && result.status === 'stopped') {
            console.log('%c[TTB Cookie Relay] Server says download complete. Stopping.', 'color: green; font-weight: bold');
            break;
        }
        if (result) {
            console.log(`%c[TTB Cookie Relay] ✓ Refreshed (${result.cookie_count} cookies, ${result.downloads_ok || 0} downloaded)`, 'color: gray');
        }
    }
})();

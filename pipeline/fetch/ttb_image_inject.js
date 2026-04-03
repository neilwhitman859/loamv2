// TTB COLA Image Downloader — paste into Chrome DevTools Console on any ttbonline.gov page
// Fetches batches of image URLs from localhost Python server, downloads them via Chrome's
// fetch() (which includes HttpOnly cookies automatically), converts to base64, and sends
// the image data back to the server for saving to disk.

(async () => {
    const SERVER = 'http://localhost:8766';
    const CONCURRENCY = 20;

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

    function arrayBufferToBase64(buffer) {
        const bytes = new Uint8Array(buffer);
        let binary = '';
        for (let i = 0; i < bytes.length; i += 8192) {
            const chunk = bytes.subarray(i, Math.min(i + 8192, bytes.length));
            binary += String.fromCharCode.apply(null, chunk);
        }
        return btoa(binary);
    }

    let totalProcessed = 0;
    let totalOk = 0;
    let totalErrors = 0;
    let batchNum = 0;
    let running = true;

    console.log('%c[TTB Images] Starting image downloader...', 'color: green; font-weight: bold');

    while (running) {
        // Get batch of image tasks from server
        let batch;
        try {
            const resp = await fetch(SERVER + '/batch');
            batch = await resp.json();
        } catch (e) {
            console.log('%c[TTB Images] Server unreachable, retrying in 5s...', 'color: red');
            await delay(5000);
            continue;
        }

        if (!batch || batch.length === 0) {
            console.log('%c[TTB Images] No more images. Done!', 'color: green; font-weight: bold');
            console.log(`%c[TTB Images] Total: ${totalOk.toLocaleString()} downloaded, ${totalErrors.toLocaleString()} errors`, 'color: green');
            running = false;
            break;
        }

        batchNum++;
        const results = [];
        const errors = [];

        // Process with concurrency limit
        for (let i = 0; i < batch.length; i += CONCURRENCY) {
            const chunk = batch.slice(i, Math.min(i + CONCURRENCY, batch.length));
            const promises = chunk.map(async (task) => {
                try {
                    const resp = await fetch(task.url);

                    if (!resp.ok) {
                        errors.push({ task_id: task.id, error: `HTTP ${resp.status}` });
                        return;
                    }

                    const contentType = resp.headers.get('content-type') || '';

                    // Check for HTML response (WAF block)
                    if (contentType.includes('text/html')) {
                        errors.push({ task_id: task.id, error: 'html' });
                        return;
                    }

                    const buffer = await resp.arrayBuffer();

                    // Skip tiny responses
                    if (buffer.byteLength < 500) {
                        errors.push({ task_id: task.id, error: 'small', size: buffer.byteLength });
                        return;
                    }

                    const b64 = arrayBufferToBase64(buffer);
                    results.push({
                        task_id: task.id,
                        data: b64,
                        size: buffer.byteLength,
                        content_type: contentType,
                    });
                } catch (e) {
                    errors.push({ task_id: task.id, error: e.message || 'fetch_error' });
                }
            });

            await Promise.all(promises);

            // Small delay between chunks to avoid overwhelming the connection
            if (i + CONCURRENCY < batch.length) {
                await delay(50);
            }
        }

        // Send results back to server
        try {
            const payload = JSON.stringify({ results, errors });
            await fetch(SERVER + '/results', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: payload,
            });
        } catch (e) {
            console.log('%c[TTB Images] Failed to send results, retrying...', 'color: red');
            await delay(2000);
            continue;
        }

        totalProcessed += results.length + errors.length;
        totalOk += results.length;
        totalErrors += errors.length;

        if (batchNum % 5 === 0) {
            console.log(
                `%c[TTB Images] Batch ${batchNum}: ${totalOk.toLocaleString()} downloaded, ${totalErrors.toLocaleString()} errors`,
                'color: cyan'
            );
        }
    }
})();

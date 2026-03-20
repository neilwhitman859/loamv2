// Tiny HTTP server to receive scraped data from the browser
import { createServer } from 'http';
import { appendFileSync } from 'fs';

const OUTPUT_FILE = 'totalwine_lexington_green.jsonl';

const server = createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method === 'POST' && req.url === '/append') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      appendFileSync(OUTPUT_FILE, body + '\n');
      const lines = body.split('\n').length;
      console.log(`Received ${lines} lines`);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, lines }));
    });
    return;
  }

  if (req.url === '/done') {
    res.writeHead(200);
    res.end('done');
    console.log('All data received. Shutting down.');
    setTimeout(() => process.exit(0), 500);
    return;
  }

  res.writeHead(404);
  res.end('not found');
});

server.listen(9876, () => {
  console.log('Receiver listening on http://localhost:9876');
});

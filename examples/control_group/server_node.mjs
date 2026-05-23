import http from 'node:http';
import { URL } from 'node:url';

class RequestCounter {
  constructor() {
    this.active_requests = 0;
    this.max_active_requests = 0;
    this.completed_requests = 0;
  }
  request_started() {
    this.active_requests += 1;
    this.max_active_requests = Math.max(this.max_active_requests, this.active_requests);
  }
  request_finished() {
    this.active_requests -= 1;
    this.completed_requests += 1;
  }
  reset() {
    this.active_requests = 0;
    this.max_active_requests = 0;
    this.completed_requests = 0;
  }
  snapshot() {
    return {
      active_requests: this.active_requests,
      max_active_requests: this.max_active_requests,
      completed_requests: this.completed_requests,
    };
  }
}

const counter = new RequestCounter();

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (url.pathname === '/work' && req.method === 'GET') {
      const request_id = Number(url.searchParams.get('request_id')) || 0;
      const sleep_seconds = Number(url.searchParams.get('sleep_seconds')) || 1;

      counter.request_started();
      try {
        const started = Date.now();
        await sleep(Math.max(0, Math.floor(sleep_seconds * 1000)));
        const elapsed = (Date.now() - started) / 1000;
        const body = JSON.stringify({ request_id, status: 'OK', sleep_seconds, elapsed_seconds: elapsed });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(body);
      } finally {
        counter.request_finished();
      }
      return;
    }

    if (url.pathname === '/metrics' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(counter.snapshot()));
      return;
    }

    if (url.pathname === '/reset' && req.method === 'POST') {
      counter.reset();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'reset' }));
      return;
    }

    res.writeHead(404);
    res.end('Not found');
  } catch (err) {
    res.writeHead(500);
    res.end(String(err));
  }
});

const PORT = Number(process.env.PORT || 8888);
server.listen(PORT, '127.0.0.1', () => {
  console.log(`node control server listening on http://127.0.0.1:${PORT}`);
});

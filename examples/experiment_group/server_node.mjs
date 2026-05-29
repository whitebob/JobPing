import http from 'node:http';
import { URL } from 'node:url';

import { jp } from '../../packages/js/index.mjs';

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

const BROKER_URL = process.env.BROKER_URL || 'http://127.0.0.1:8890';

jp.configure({ brokerPort: 0, peerBrokers: [BROKER_URL] });

// Server-side wrap: if the request carries a job-id header the handler is
// deferred and a job_ref is returned immediately; otherwise it runs inline.
const doWork = jp.wrap(async (req, request_id, sleep_seconds) => {
  const started = Date.now();
  await sleep(Math.max(0, Math.floor(sleep_seconds * 1000)));
  const elapsed = (Date.now() - started) / 1000;
  return { request_id, status: 'OK', sleep_seconds, elapsed_seconds: elapsed };
});

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
}

const server = http.createServer(async (req, res) => {
  setCors(res);

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (url.pathname === '/work' && req.method === 'GET') {
      const request_id = Number(url.searchParams.get('request_id')) || 0;
      const sleep_seconds = Number(url.searchParams.get('sleep_seconds')) || 1;

      counter.request_started();
      try {
        const body = JSON.stringify(await doWork(req, request_id, sleep_seconds));
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

const PORT = Number(process.env.PORT || 8887);
await jp.startBroker();
server.listen(PORT, '127.0.0.1', () => {
  console.log(`node experiment server listening on http://127.0.0.1:${PORT} broker=${BROKER_URL}`);
});

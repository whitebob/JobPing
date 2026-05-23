import http from 'node:http';
import { URL } from 'node:url';

import * as jp from '../../packages/js/index.mjs';

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

// Setup JobPing endpoint proxy backed by in-memory queue + WS transport to broker
const BROKER_URL = process.env.BROKER_URL || 'http://127.0.0.1:8890';
let transport;
try {
  transport = new jp.TransportLayerWS({ url: BROKER_URL });
} catch (e) {
  console.error('Failed to create jp.TransportLayerWS, is socket.io-client installed?', e);
  process.exit(1);
}

const endpointProxy = new jp.EndpointProxy({
  stateSync: new jp.StateSync({ transportLayer: transport }),
  resultHandoff: new jp.ResultHandoff({ transportLayer: transport }),
  queue: new jp.JPItemQueueInMemory(new jp.EnvelopeEndpointInMemory()),
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

      // detect jobping header
      const jobHeader = req.headers[jp.JOBPING_JOB_ID_HEADER] || req.headers[jp.JOBPING_JOB_ID_HEADER.toLowerCase()];
      if (jobHeader) {
        const jobId = String(jobHeader);
        // Offer, defer and fulfill later
        endpointProxy.offer(jobId);
        endpointProxy.defer(jobId);
        endpointProxy.fulfillLater(jobId, async () => {
          const started = Date.now();
          await sleep(Math.max(0, Math.floor(sleep_seconds * 1000)));
          const elapsed = (Date.now() - started) / 1000;
          return { request_id, status: 'OK', sleep_seconds, elapsed_seconds: elapsed };
        }).catch((e) => {
          console.error('fulfillLater error', e);
        });
        counter.request_started();
        // Return job ref immediately
        const body = JSON.stringify(endpointProxy.makeJobRef(jobId));
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(body);
        counter.request_finished();
        return;
      }

      // No jobping header: behave like control
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

const PORT = Number(process.env.PORT || 8887);
server.listen(PORT, '127.0.0.1', () => {
  console.log(`node experiment server listening on http://127.0.0.1:${PORT} broker=${BROKER_URL}`);
});

//const http = require('http');
const express = require('express');
const mysql = require('mysq12/promise');

class TokenBucket {
    constructor(requestsPerSecond = 10, burstSize = 20){
        this.requestsPerSecond = requestsPerSecond;
        this.burstSize = burstSize;
        this.tokens = burstSize;
        this.lastRefill = Date.now();
    }
    refill(){
        const now = Date.now();
        const secondsElapsed = (now - this.lastRefill) / 1000;
        this.tokens = Math.min(this.burstSize, this.tokens + secondsElapsed * this.requestsPerSecond);
        this.lastRefill = now;
    }
    consume(){
        this.refill();
        if(this.tokens >=1){
            this.tokens -= 1;
            return true;
        }
        return false;
    }
    getStatus() {
        return {
            limit: this.requestsPerSecond,
            remaining: Math.floor(this.tokens),
            resetInMs: Math.ceil((1 - (this.tokens % 1)) / this.requestsPerSecond * 1000),
        }; 
    }
}
const buckets = new Map();
const getBucket = (key, config = {}) => {
    if (!buckets.has(key)) buckets.set(key, new TokenBucket(
        config.requestsPerSecond || 10,
        config.burstSize || 20
    ));
    return buckets.get(key);
};

// ─── HTTP Server ──────────────────────────────────────────────────────────────
// http.createServer((req, res) => {

//   const bucket  = getBucket(req.headers['x-client-id'] || 'anonymous');
//   const allowed = bucket.consume();
//   const status  = bucket.getStatus();

//   res.setHeader('Content-Type',          'application/json');
//   res.setHeader('X-RateLimit-Limit',     status.limit);
//   res.setHeader('X-RateLimit-Remaining', status.remaining);
//   res.setHeader('X-RateLimit-Reset',     status.resetInMs);

//   res.writeHead(allowed ? 200 : 429);
//   res.end(JSON.stringify({
//     status:   allowed ? 'ALLOW' : 'DENY',
//     client:   req.headers['x-client-id'] || 'anonymous',
//     tokens:   status.remaining,
//     retryInMs: allowed ? null : status.resetInMs,
//   }));

// }).listen(3000, () => console.log('Rate limiter running on http://localhost:3000'));

const app = express();
app.use(express.json());
 
// ─── Rate Limit Middleware ────────────────────────────────────────────────────
const rateLimiter = (req, res, next) => {
  const clientKey = req.headers['x-client-id'] || 'anonymous';
  const bucket    = getBucket(clientKey);
  const allowed   = bucket.consume();
  const status    = bucket.getStatus();
 
  res.setHeader('X-RateLimit-Limit',     status.limit);
  res.setHeader('X-RateLimit-Remaining', status.remaining);
  res.setHeader('X-RateLimit-Reset',     status.resetInMs);
 
  if (!allowed) {
    return res.status(429).json({
      status:    'DENY',
      message:   'Too many requests, slow down',
      client:    clientKey,
      retryInMs: status.resetInMs,
    });
  }
 
  next();
};
 
app.use(rateLimiter);
 
// ─── Admin Endpoint — configure a client's limits ────────────────────────────
app.post('/admin/config', (req, res) => {
  const { clientKey, requestsPerSecond, burstSize } = req.body;
 
  if (!clientKey) {
    return res.status(400).json({ error: 'clientKey is required' });
  }
 
  buckets.set(clientKey, new TokenBucket(
    requestsPerSecond || 10,
    burstSize         || 20
  ));
 
  res.json({
    message:           'Config updated',
    clientKey,
    requestsPerSecond: requestsPerSecond || 10,
    burstSize:         burstSize         || 20,
  });
});
 
// ─── Main API Endpoint ────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  const clientKey = req.headers['x-client-id'] || 'anonymous';
  const status    = getBucket(clientKey).getStatus();
 
  res.json({
    status:  'ALLOW',
    message: 'Request accepted',
    client:  clientKey,
    tokens:  status.remaining,
  });
});
 
// ─── Start Server ─────────────────────────────────────────────────────────────
app.listen(3000, () => {
  console.log('Rate limiter running on http://localhost:3000');
  console.log('Test: curl -H "x-client-id: user-1" http://localhost:3000');
  console.log('Admin: POST http://localhost:3000/admin/config');
});

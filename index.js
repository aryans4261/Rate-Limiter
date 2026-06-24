const express = require('express');
const mysql   = require('mysql2/promise');

class TokenBucket {
  constructor(requestsPerSecond = 10, burstSize = 20) {
    this.requestsPerSecond = requestsPerSecond;
    this.burstSize         = burstSize;
    this.tokens            = burstSize;
    this.lastRefill        = Date.now();
  }

  refill() {
    const now            = Date.now();
    const secondsElapsed = (now - this.lastRefill) / 1000;
    this.tokens          = Math.min(this.burstSize, this.tokens + secondsElapsed * this.requestsPerSecond);
    this.lastRefill      = now;
  }

  consume() {
    this.refill();
    if (this.tokens >= 1) { this.tokens -= 1; return true; }
    return false;
  }

  getStatus() {
    return {
      limit:     this.requestsPerSecond,
      remaining: Math.floor(this.tokens),
      resetInMs: Math.ceil((1 - (this.tokens % 1)) / this.requestsPerSecond * 1000),
    };
  }
}

let db;

async function connectDB() {
  db = await mysql.createConnection({
    host:     'localhost',
    user:     'root',
    password: 'Ary0612an#',       
    database: 'rate_limiter',
  });
  console.log('Connected to MySQL');
}

async function getBucket(clientKey) {

  // try to find existing row for this client
  const [rows] = await db.execute(
    'SELECT * FROM rate_limit_buckets WHERE client_key = ?',
    [clientKey]
  );

  if (rows.length > 0) {
    // Client exists-load their saved state
    const row    = rows[0];
    const bucket = new TokenBucket(row.requests_per_second, row.burst_size);
    bucket.tokens     = row.tokens;
    bucket.lastRefill = Number(row.last_refill);
    return bucket;
  } else {
    // Brand new client-a fresh bucket and save on MySQL
    const bucket = new TokenBucket();
    await db.execute(
      'INSERT INTO rate_limit_buckets (client_key, tokens, last_refill, requests_per_second, burst_size) VALUES (?, ?, ?, ?, ?)',
      [clientKey, bucket.tokens, bucket.lastRefill, bucket.requestsPerSecond, bucket.burstSize]
    );
    return bucket;
  }
}

// Save bucket state back to MySQL after every request
async function saveBucket(clientKey, bucket) {
  await db.execute(
    'UPDATE rate_limit_buckets SET tokens = ?, last_refill = ? WHERE client_key = ?',
    [bucket.tokens, bucket.lastRefill, clientKey]
  );
}


const app = express();
app.use(express.json());

const rateLimiter = async (req, res, next) => {
  const clientKey = req.headers['x-client-id'] || 'anonymous';

  const bucket  = await getBucket(clientKey);
  const allowed = bucket.consume();
  const status  = bucket.getStatus();

  await saveBucket(clientKey, bucket);

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


app.post('/admin/config', async (req, res) => {
  const { clientKey, requestsPerSecond, burstSize } = req.body;

  if (!clientKey) {
    return res.status(400).json({ error: 'clientKey is required' });
  }

  const rps   = requestsPerSecond || 10;
  const burst = burstSize         || 20;


  await db.execute(
    `INSERT INTO rate_limit_buckets (client_key, tokens, last_refill, requests_per_second, burst_size)
     VALUES (?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE requests_per_second = ?, burst_size = ?, tokens = ?, last_refill = ?`,
    [clientKey, burst, Date.now(), rps, burst, rps, burst, burst, Date.now()]
  );

  res.json({
    message:            'Config updated',
    clientKey,
    requestsPerSecond:  rps,
    burstSize:          burst,
  });
});


app.get('/', async (req, res) => {
  const clientKey = req.headers['x-client-id'] || 'anonymous';
  const bucket    = await getBucket(clientKey);
  const status    = bucket.getStatus();

  res.json({
    status:  'ALLOW',
    message: 'Request accepted',
    client:  clientKey,
    tokens:  status.remaining,
  });
});


connectDB().then(() => {
  app.listen(3000, () => {
    console.log('Rate limiter running on http://localhost:3000');
    console.log('MySQL is now storing all bucket state');
  });
}).catch(err => {
  console.error('Could not connect to MySQL:', err.message);
});
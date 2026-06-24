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

//(Phase 4 -race condition safe) 
async function getBucket(clientKey) {
  // Step 1: Start transaction
  await db.beginTransaction();

  try {
    // Step 2: SELECT FOR UPDATE locks this row so no other request can touch it
    const [rows] = await db.execute(
      'SELECT * FROM rate_limit_buckets WHERE client_key = ? FOR UPDATE',
      [clientKey]
    );

    let bucket;

    if (rows.length > 0) {
      // Client existsload their saved state
      const row = rows[0];
      bucket = new TokenBucket(row.requests_per_second, row.burst_size);
      bucket.tokens     = row.tokens;
      bucket.lastRefill = Number(row.last_refill);
    } else {
      //create fresh bucket
      bucket = new TokenBucket();
      await db.execute(
        'INSERT INTO rate_limit_buckets (client_key, tokens, last_refill, requests_per_second, burst_size) VALUES (?, ?, ?, ?, ?)',
        [clientKey, bucket.tokens, bucket.lastRefill, bucket.requestsPerSecond, bucket.burstSize]
      );
    }

    // Step 3 Consume token
    const allowed = bucket.consume();

    // Step 4:Save updated token count back to MySQL
    await db.execute(
      'UPDATE rate_limit_buckets SET tokens = ?, last_refill = ? WHERE client_key = ?',
      [bucket.tokens, bucket.lastRefill, clientKey]
    );

    // Step 5: Commit -releases the lock
    await db.commit();

    return { bucket, allowed };

  } catch (err) {
    // If anything goes wrong, rollback- undoes everything
    await db.rollback();
    throw err;
  }
}

const app = express();
app.use(express.json());


const rateLimiter = async (req, res, next) => {
  const clientKey = req.headers['x-client-id'] || 'anonymous';

  try {
    const { bucket, allowed } = await getBucket(clientKey);
    const status = bucket.getStatus();

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

  } catch (err) {
    console.error('Rate limiter error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
};

app.use(rateLimiter);

app.post('/admin/config', async (req, res) => {
  const { clientKey, requestsPerSecond, burstSize } = req.body;

  if (!clientKey) {
    return res.status(400).json({ error: 'clientKey is required' });
  }

  const rps   = requestsPerSecond || 10;
  const burst = burstSize         || 20;

  try {
    await db.execute(
      `INSERT INTO rate_limit_buckets (client_key, tokens, last_refill, requests_per_second, burst_size)
       VALUES (?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE requests_per_second = ?, burst_size = ?, tokens = ?, last_refill = ?`,
      [clientKey, burst, Date.now(), rps, burst, rps, burst, burst, Date.now()]
    );

    res.json({ message: 'Config updated', clientKey, requestsPerSecond: rps, burstSize: burst });

  } catch (err) {
    res.status(500).json({ error: 'Failed to update config' });
  }
});

app.get('/', async (req, res) => {
  const clientKey = req.headers['x-client-id'] || 'anonymous';
  const [rows]    = await db.execute(
    'SELECT * FROM rate_limit_buckets WHERE client_key = ?',
    [clientKey]
  );

  res.json({
    status:  'ALLOW',
    message: 'Request accepted',
    client:  clientKey,
    tokens:  rows.length > 0 ? Math.floor(rows[0].tokens) : 20,
  });
});


connectDB().then(() => {
  app.listen(3000, () => {
    console.log('Rate limiter running on http://localhost:3000');
    console.log('Phase 4 - concurrency safe with MySQL transactions');
  });
}).catch(err => {
  console.error('Could not connect to MySQL:', err.message);
});
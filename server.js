/*
 * Mock Data Generator API (v4) - Patched: Kafka removed (Postgres + files + auto-generator)
 * - Dimension-first inserts (merchants/accounts) so facts (payins/payouts) reference DB-backed dims
 * - Clean, single-file server.js for local usage (no Kafka/EventHub)
 */

import express from "express";
import cors from "cors";
import dayjs from "dayjs";
import fs from "fs-extra";
import dotenv from "dotenv";
import pLimit from "p-limit"; // for throttled concurrency
import pkg from "pg"; // pg client
import rateLimit from "express-rate-limit";
import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";

const { Pool } = pkg;

// --- Local imports (generator helpers from your project)
import { generateMerchants } from "./generators/merchant.js";
import { generateAccounts } from "./generators/account.js";
import { generatePayins } from "./generators/payin.js";
import { generatePayouts } from "./generators/payout.js";
import { writeJsonAndCsv } from "./utils/helpers.js";

dotenv.config();
const app = express();

// --- Rate Limiting (Throttling) - Max 100 requests per second
const limiter = rateLimit({
  windowMs: 1000, // 1 second
  max: 100, // Limit each IP to 100 requests per windowMs
  message: { error: "Too many requests. Limit is 100 requests per second." },
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(limiter);
app.use(cors());
app.use(express.json());

// --- Environment Config
const PORT = +process.env.PORT || 4000;

// Postgres envs (defaults)
const PG_HOST = process.env.PG_HOST || "localhost";
const PG_PORT = +process.env.PG_PORT || 5432;
const PG_USER = process.env.PG_USER || "postgres";
const PG_PASSWORD = process.env.PG_PASSWORD || "postgres";
const PG_DB = process.env.PG_DB || "payments";

// --- Postgres pool (Supports DATABASE_URL for Railway and cloud databases)
const pgPool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DATABASE_URL.includes("localhost") || process.env.DATABASE_URL.includes("127.0.0.1")
        ? false
        : { rejectUnauthorized: false },
      max: 10,
    })
  : new Pool({
      host: PG_HOST,
      port: PG_PORT,
      user: PG_USER,
      password: PG_PASSWORD,
      database: PG_DB,
      max: 10,
    });

// --- Globals
let merchants = [];
let accounts = [];
let autoInsertInterval = null; // reference to interval

/** --------------------------------------------------------
 *  Helper: Initialize Database Schema (Tables)
 * ------------------------------------------------------- */
async function initializeDatabaseSchema() {
  const client = await pgPool.connect();
  try {
    console.log("⚙️ Initializing database tables if they do not exist...");
    
    // Create merchants table
    await client.query(`
      CREATE TABLE IF NOT EXISTS merchants (
        id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        alias VARCHAR(255),
        category VARCHAR(255),
        contact_name VARCHAR(255),
        contact_email VARCHAR(255),
        contact_mobile VARCHAR(255),
        limit_amount NUMERIC,
        active_since DATE,
        kyc_status VARCHAR(50),
        enabled BOOLEAN,
        created_on TIMESTAMP,
        updated_on TIMESTAMP
      )
    `);

    // Create accounts table
    await client.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        id VARCHAR(255) PRIMARY KEY,
        bank VARCHAR(255),
        acc_name VARCHAR(255),
        acc_num VARCHAR(255),
        ifsc VARCHAR(50),
        vpa VARCHAR(255),
        zone VARCHAR(100),
        max_limit NUMERIC,
        avg_ticket_size NUMERIC,
        enabled BOOLEAN,
        created_on TIMESTAMP,
        updated_on TIMESTAMP
      )
    `);

    // Create payins table
    await client.query(`
      CREATE TABLE IF NOT EXISTS payins (
        id VARCHAR(255) PRIMARY KEY,
        seq_num BIGINT,
        merchant_id VARCHAR(255) REFERENCES merchants(id),
        request_id VARCHAR(255),
        customer_name VARCHAR(255),
        customer_mobile VARCHAR(255),
        customer_email VARCHAR(255),
        txn_amount NUMERIC,
        txn_type VARCHAR(50),
        callback_url TEXT,
        order_code VARCHAR(255),
        receiving_vpa VARCHAR(255),
        bank VARCHAR(255),
        zone VARCHAR(100),
        utr VARCHAR(255),
        processed_amount NUMERIC,
        status VARCHAR(50),
        created_on TIMESTAMP,
        updated_on TIMESTAMP,
        raw JSONB
      )
    `);

    // Create payouts table
    await client.query(`
      CREATE TABLE IF NOT EXISTS payouts (
        id VARCHAR(255) PRIMARY KEY,
        seq_num BIGINT,
        merchant_id VARCHAR(255) REFERENCES merchants(id),
        txn_amount NUMERIC,
        txn_type VARCHAR(50),
        bank VARCHAR(255),
        ifsc VARCHAR(50),
        account_number VARCHAR(255),
        status VARCHAR(50),
        requested_amount NUMERIC,
        processed_amount NUMERIC,
        utr VARCHAR(255),
        created_on TIMESTAMP,
        updated_on TIMESTAMP,
        raw JSONB
      )
    `);

    console.log("✅ Database tables checked/created successfully.");
  } catch (err) {
    console.error("❌ Database schema initialization failed:", err.message);
  } finally {
    client.release();
  }
}

/** --------------------------------------------------------
 *  Helper: Seed dimension tables
 *  - now async and ensures DB upsert before facts are created
 * ------------------------------------------------------- */
async function ensureSeed(date) {
  if (!merchants?.length) {
    merchants = generateMerchants([], date);
    try {
      await insertIntoPostgres("merchants", merchants);
    } catch (e) {
      console.warn("ensureSeed: merchants upsert failed:", e.message);
    }
  }

  if (!accounts?.length) {
    accounts = generateAccounts([], date);
    try {
      await insertIntoPostgres("accounts", accounts);
    } catch (e) {
      console.warn("ensureSeed: accounts upsert failed:", e.message);
    }
  }
}

/** --------------------------------------------------------
 *  Helper: Reload dimension tables from Postgres
 *  - ensures the latest DB-backed rows are used by generators
 * ------------------------------------------------------- */
async function reloadDimensionTables() {
  const client = await pgPool.connect();
  try {
    const m = await client.query(
      `SELECT id, name, alias, category, contact_name, contact_email, contact_mobile, limit_amount AS limit, active_since, kyc_status, enabled, created_on, updated_on FROM merchants`,
    );
    merchants = m.rows.map((r) => ({
      id: r.id,
      name: r.name,
      alias: r.alias,
      category: r.category,
      contactName: r.contact_name,
      contactEmail: r.contact_email,
      contactMobile: r.contact_mobile,
      limit: r.limit,
      activeSince: r.active_since
        ? dayjs(r.active_since).format("YYYY-MM-DD")
        : null,
      kycStatus: r.kyc_status,
      enabled: r.enabled,
      createdOn: r.created_on,
      updatedOn: r.updated_on,
    }));

    const a = await client.query(
      `SELECT id, bank, acc_name, acc_num, ifsc, vpa, zone, max_limit AS maxLimit, avg_ticket_size AS avgTicketSize, enabled, created_on, updated_on FROM accounts`,
    );
    accounts = a.rows.map((r) => ({
      id: r.id,
      bank: r.bank,
      accName: r.acc_name,
      accNum: r.acc_num,
      ifsc: r.ifsc,
      vpa: r.vpa,
      zone: r.zone,
      maxLimit: r.maxlimit ?? r.maxLimit ?? null,
      avgTicketSize: r.avg_ticket_size ?? r.avgTicketSize ?? null,
      enabled: r.enabled,
      createdOn: r.created_on,
      updatedOn: r.updated_on,
    }));
  } catch (err) {
    console.warn("reloadDimensionTables error:", err.message);
  } finally {
    client.release();
  }
}

/** --------------------------------------------------------
 *  Postgres helper: upsert rows into given table
 *  - handles JSONB fields for payins/payouts
 * ------------------------------------------------------- */
async function insertIntoPostgres(table, rows) {
  if (!rows || !rows.length) return;
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      if (table === "merchants") {
        const q = `
            INSERT INTO merchants (id, name, alias, category, contact_name, contact_email, contact_mobile, limit_amount, active_since, kyc_status, enabled, created_on, updated_on)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (id) DO UPDATE SET
              name=EXCLUDED.name, alias=EXCLUDED.alias, category=EXCLUDED.category,
              contact_name=EXCLUDED.contact_name, contact_email=EXCLUDED.contact_email, contact_mobile=EXCLUDED.contact_mobile,
              limit_amount=EXCLUDED.limit_amount, active_since=EXCLUDED.active_since, kyc_status=EXCLUDED.kyc_status,
              enabled=EXCLUDED.enabled, updated_on=EXCLUDED.updated_on;
          `;
        await client.query(q, [
          r.id,
          r.name ?? null,
          r.alias ?? null,
          r.category ?? null,
          r.contactName ?? null,
          r.contactEmail ?? null,
          r.contactMobile ?? null,
          r.limit ?? null,
          r.activeSince ?? null,
          r.kycStatus ?? null,
          r.enabled ?? null,
          r.createdOn ?? null,
          r.updatedOn ?? null,
        ]);
      } else if (table === "accounts") {
        const q = `
            INSERT INTO accounts (id, bank, acc_name, acc_num, ifsc, vpa, zone, max_limit, avg_ticket_size, enabled, created_on, updated_on)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (id) DO UPDATE SET
              bank=EXCLUDED.bank, acc_name=EXCLUDED.acc_name, acc_num=EXCLUDED.acc_num,
              ifsc=EXCLUDED.ifsc, vpa=EXCLUDED.vpa, zone=EXCLUDED.zone,
              max_limit=EXCLUDED.max_limit, avg_ticket_size=EXCLUDED.avg_ticket_size, enabled=EXCLUDED.enabled,
              updated_on=EXCLUDED.updated_on;
          `;
        await client.query(q, [
          r.id,
          r.bank ?? null,
          r.accName ?? r.acc_name ?? null,
          r.accNum ?? r.acc_num ?? null,
          r.ifsc ?? null,
          r.vpa ?? null,
          r.zone ?? null,
          r.maxLimit ?? null,
          r.avgTicketSize ?? null,
          r.enabled ?? null,
          r.createdOn ?? null,
          r.updatedOn ?? null,
        ]);
      } else if (table === "payins") {
        const q = `
      INSERT INTO payins (
        id, seq_num, merchant_id, request_id,

        customer_name, customer_mobile, customer_email,

        txn_amount, txn_type, callback_url,

        order_code, receiving_vpa, bank, zone, utr,
        processed_amount, status, created_on, updated_on,

        raw
      )
      VALUES (
        $1, $2, $3, $4,

        $5, $6, $7,

        $8, $9, $10,

        $11, $12, $13, $14, $15,
        $16, $17, $18, $19,

        $20::jsonb
      )
      ON CONFLICT (id) DO UPDATE SET
        status = EXCLUDED.status,
        processed_amount = EXCLUDED.processed_amount,
        updated_on = EXCLUDED.updated_on;
    `;

        await client.query(q, [
          r.id,
          r.seqNum ?? null,
          r.merchantId ?? null,
          r.requestId ?? null,

          r.customer?.name ?? null,
          r.customer?.mobile ?? null,
          r.customer?.email ?? null,

          r.transaction?.amount ?? null,
          r.transaction?.type ?? null,
          r.transaction?.callbackUrl ?? null,

          r.orderCode ?? null,
          r.receivingVpa ?? null,
          r.bank ?? null,
          r.zone ?? null,
          r.utr ?? null,

          r.processedAmount ?? null,
          r.status ?? null,
          r.createdOn ?? null,
          r.updatedOn ?? null,

          JSON.stringify(r),
        ]);
      } else if (table === "payouts") {
        const q = `
      INSERT INTO payouts (
        id, seq_num, merchant_id,

        txn_amount, txn_type,

        bank, ifsc, account_number,

        status, requested_amount, processed_amount, utr,
        created_on, updated_on,

        raw
      )
      VALUES (
        $1, $2, $3,

        $4, $5,

        $6, $7, $8,

        $9, $10, $11, $12,
        $13, $14,

        $15::jsonb
      )
      ON CONFLICT (id) DO UPDATE SET
        status = EXCLUDED.status,
        processed_amount = EXCLUDED.processed_amount,
        updated_on = EXCLUDED.updated_on;
    `;

        await client.query(q, [
          r.id,
          r.seqNum ?? null,
          r.merchantId ?? null,

          r.transaction?.amount ?? null,
          r.transaction?.type ?? null,

          r.account?.bank ?? null,
          r.account?.ifsc ?? null,
          r.account?.account ?? null,

          r.status ?? null,
          r.requestedAmount ?? null,
          r.processedAmount ?? null,
          r.utr ?? null,

          r.createdOn ?? null,
          r.updatedOn ?? null,

          JSON.stringify(r),
        ]);
      } else {
        // generic fallback: try to insert raw JSON into a `raw_events` table if exists
        const q = `INSERT INTO raw_events(payload) VALUES($1::jsonb)`;
        await client.query(q, [JSON.stringify(r)]);
      }
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Postgres insert error:", err.message);
    throw err;
  } finally {
    client.release();
  }
}

/** --------------------------------------------------------
 *  Routes
 * ------------------------------------------------------- */

// --- Authentication Configurations & Middleware
const JWT_SECRET = process.env.JWT_SECRET || "mock_data_secret_key_12345";
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || "admin";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "admin123";

// Middleware to verify JWT tokens on protected routes
function authenticateJWT(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    return res.status(401).json({ error: "Access denied. No token provided." });
  }

  const token = authHeader.split(" ")[1]; // Format: Bearer <token>
  if (!token) {
    return res.status(401).json({ error: "Access denied. Invalid token format." });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    return res.status(403).json({ error: "Invalid or expired token." });
  }
}

// POST /login: Authenticates admin credentials and issues a JWT token
app.post("/login", async (req, res) => {
  const { username, password } = req.body;

  if (!username || !password) {
    return res.status(400).json({ error: "Username and password are required." });
  }

  let isPasswordValid = false;
  if (username === ADMIN_USERNAME) {
    // If ADMIN_PASSWORD in environment is a bcrypt hash, verify using bcrypt. Otherwise, plain text match.
    if (ADMIN_PASSWORD.startsWith("$2a$") || ADMIN_PASSWORD.startsWith("$2b$")) {
      isPasswordValid = await bcrypt.compare(password, ADMIN_PASSWORD);
    } else {
      isPasswordValid = password === ADMIN_PASSWORD;
    }
  }

  if (!isPasswordValid) {
    return res.status(401).json({ error: "Invalid username or password." });
  }

  // Issue token valid for 24 hours
  const token = jwt.sign({ username }, JWT_SECRET, { expiresIn: "24h" });
  return res.json({ token, expires_in: "24h" });
});


// Health check
app.get("/health", (_, res) =>
  res.json({
    status: "ok",
    date: new Date().toISOString(),
    postgres: !!pgPool,
  }),
);

// Generate Payins
app.get("/generate/payin", authenticateJWT, async (req, res) => {
  const count = +req.query.count || 10;
  const extended = req.query.extended === "true";
  const date = dayjs().format("YYYY-MM-DD");

  await ensureSeed(date);
  await reloadDimensionTables();

  const payins = generatePayins(date, merchants, accounts, count, extended);
  const dir = `./data/${date}`;
  await fs.ensureDir(dir);
  await writeJsonAndCsv(dir, `payins_${Date.now()}`, payins);

  // Save to Postgres (so Debezium can capture)
  try {
    await insertIntoPostgres("payins", payins);
  } catch (err) {
    console.error("Error inserting payins into Postgres:", err.message);
  }

  res.json({
    count,
    postgres: true,
    data: payins,
  });
});

// Generate Payouts
app.get("/generate/payout", authenticateJWT, async (req, res) => {
  const count = +req.query.count || 10;
  const extended = req.query.extended === "true";
  const date = dayjs().format("YYYY-MM-DD");

  await ensureSeed(date);
  await reloadDimensionTables();

  const payouts = generatePayouts(date, merchants, accounts, count, extended);
  const dir = `./data/${date}`;
  await fs.ensureDir(dir);
  await writeJsonAndCsv(dir, `payouts_${Date.now()}`, payouts);

  // Save to Postgres
  try {
    await insertIntoPostgres("payouts", payouts);
  } catch (err) {
    console.error("Error inserting payouts into Postgres:", err.message);
  }

  res.json({
    count,
    postgres: true,
    data: payouts,
  });
});

// Dimension data
app.get("/generate/merchant", authenticateJWT, async (_, res) => {
  const date = dayjs().format("YYYY-MM-DD");
  merchants = generateMerchants(merchants, date);
  // upsert merchants to Postgres so Debezium can pick dimension updates
  try {
    await insertIntoPostgres("merchants", merchants);
  } catch (err) {
    console.error("Error inserting merchants into Postgres:", err.message);
  }
  res.json({ count: merchants.length, data: merchants });
});

// app.get("/generate/account", async (_, res) => {
//   const date = dayjs().format("YYYY-MM-DD");
//   accounts = generateAccounts(accounts, date);
//   try {
//     await insertIntoPostgres("accounts", accounts);
//   } catch (err) {
//     console.error("Error inserting accounts into Postgres:", err.message);
//   }
//   res.json({ count: accounts.length, data: accounts });
// });
app.get("/generate/account", authenticateJWT, async (req, res) => {
  const date = dayjs().format("YYYY-MM-DD");
  const count = +req.query.count || 10;

  // force-generate new accounts
  const newAccounts = generateAccounts([], date, count);

  try {
    await insertIntoPostgres("accounts", newAccounts);
  } catch (err) {
    console.error("Error inserting accounts:", err.message);
  }

  res.json({
    requested: count,
    inserted: newAccounts.length,
    data: newAccounts,
  });
});

// Generate historical data
app.get("/generate/history", authenticateJWT, async (req, res) => {
  const from = req.query.from || "2025-08-20";
  const to = req.query.to || dayjs().format("YYYY-MM-DD");
  const stream = req.query.stream === "true"; // retained but no-op
  const extended = req.query.extended === "true";

  const start = dayjs(from);
  const end = dayjs(to);
  const limit = pLimit(2); // limit concurrent days

  console.log(`🕐 Generating data from ${from} → ${to}`);

  merchants = [];
  accounts = [];

  const tasks = [];
  for (let d = start; d.isBefore(end) || d.isSame(end); d = d.add(1, "day")) {
    const date = d.format("YYYY-MM-DD");
    const dir = `./data/${date}`;
    await fs.ensureDir(dir);

    merchants = generateMerchants(merchants, date);
    accounts = generateAccounts(accounts, date);

    // upsert dims
    try {
      await insertIntoPostgres("merchants", merchants);
      await insertIntoPostgres("accounts", accounts);
    } catch (err) {
      console.error("Historical upsert error:", err.message);
    }

    const daysPassed = d.diff(start, "day");
    const payinCount = 2000 + daysPassed * 100;
    const payoutCount = 1000 + daysPassed * 50;

    tasks.push(
      limit(async () => {
        // reload dims to ensure generators use DB-canonical shapes
        await reloadDimensionTables();

        const payins = generatePayins(
          date,
          merchants,
          accounts,
          payinCount,
          extended,
        );
        const payouts = generatePayouts(
          date,
          merchants,
          accounts,
          payoutCount,
          extended,
        );

        await writeJsonAndCsv(dir, "payins", payins);
        await writeJsonAndCsv(dir, "payouts", payouts);

        // write to postgres
        try {
          await insertIntoPostgres("payins", payins);
          await insertIntoPostgres("payouts", payouts);
        } catch (err) {
          console.error("Historical insert error:", err.message);
        }

        console.log(`✅ ${date}: ${payinCount} payins, ${payoutCount} payouts`);
      }),
    );
  }

  await Promise.all(tasks);
  res.json({ message: `✅ History generated ${from} → ${to}` });
});
app.get("/payins", authenticateJWT, async (req, res) => {
  const PAGE_SIZE = 5000;
  const page = Math.max(+req.query.page || 1, 1);
  const offset = (page - 1) * PAGE_SIZE;

  const client = await pgPool.connect();
  try {
    // 1️⃣ Get total count
    const countResult = await client.query(`SELECT COUNT(*) FROM payins`);
    const totalRows = Number(countResult.rows[0].count);

    // 2️⃣ Fetch paginated data
    const dataResult = await client.query(
      `
      SELECT *
      FROM payins
      ORDER BY created_on DESC
      LIMIT $1 OFFSET $2
      `,
      [PAGE_SIZE, offset],
    );

    // 3️⃣ Pagination info
    const totalPages = Math.ceil(totalRows / PAGE_SIZE);
    const hasNextPage = page < totalPages;
    const hasPrevPage = page > 1;

    res.json({
      meta: {
        totalRows,
        pageSize: PAGE_SIZE,
        currentPage: page,
        totalPages,
        hasNextPage,
        hasPrevPage,
        nextPage: hasNextPage ? `/payins?page=${page + 1}` : null,
        prevPage: hasPrevPage ? `/payins?page=${page - 1}` : null,
      },
      count: dataResult.rowCount,
      data: dataResult.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.get("/payouts", authenticateJWT, async (req, res) => {
  const PAGE_SIZE = 5000;
  const page = Math.max(+req.query.page || 1, 1);
  const offset = (page - 1) * PAGE_SIZE;

  const client = await pgPool.connect();
  try {
    const totalResult = await client.query(`SELECT COUNT(*) FROM payouts`);
    const totalRows = Number(totalResult.rows[0].count);
    const totalPages = Math.ceil(totalRows / PAGE_SIZE);

    const dataResult = await client.query(
      `
      SELECT *
      FROM payouts
      ORDER BY created_on DESC
      LIMIT $1 OFFSET $2
      `,
      [PAGE_SIZE, offset],
    );

    res.json({
      meta: {
        totalRows,
        pageSize: PAGE_SIZE,
        currentPage: page,
        totalPages,
        hasNextPage: page < totalPages,
        hasPrevPage: page > 1,
        nextPage: page < totalPages ? `/payouts?page=${page + 1}` : null,
        prevPage: page > 1 ? `/payouts?page=${page - 1}` : null,
      },
      count: dataResult.rowCount,
      data: dataResult.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.get("/accounts", authenticateJWT, async (req, res) => {
  const PAGE_SIZE = 5000;
  const page = Math.max(+req.query.page || 1, 1);
  const offset = (page - 1) * PAGE_SIZE;

  const client = await pgPool.connect();
  try {
    const totalResult = await client.query(`SELECT COUNT(*) FROM accounts`);
    const totalRows = Number(totalResult.rows[0].count);
    const totalPages = Math.ceil(totalRows / PAGE_SIZE);

    const dataResult = await client.query(
      `
      SELECT *
      FROM accounts
      ORDER BY created_on DESC
      LIMIT $1 OFFSET $2
      `,
      [PAGE_SIZE, offset],
    );

    res.json({
      meta: {
        totalRows,
        pageSize: PAGE_SIZE,
        currentPage: page,
        totalPages,
        hasNextPage: page < totalPages,
        hasPrevPage: page > 1,
        nextPage: page < totalPages ? `/accounts?page=${page + 1}` : null,
        prevPage: page > 1 ? `/accounts?page=${page - 1}` : null,
      },
      count: dataResult.rowCount,
      data: dataResult.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.get("/merchants", authenticateJWT, async (req, res) => {
  const PAGE_SIZE = 5000;
  const page = Math.max(+req.query.page || 1, 1);
  const offset = (page - 1) * PAGE_SIZE;

  const client = await pgPool.connect();
  try {
    const totalResult = await client.query(`SELECT COUNT(*) FROM merchants`);
    const totalRows = Number(totalResult.rows[0].count);
    const totalPages = Math.ceil(totalRows / PAGE_SIZE);

    const dataResult = await client.query(
      `
      SELECT *
      FROM merchants
      ORDER BY created_on DESC
      LIMIT $1 OFFSET $2
      `,
      [PAGE_SIZE, offset],
    );

    res.json({
      meta: {
        totalRows,
        pageSize: PAGE_SIZE,
        currentPage: page,
        totalPages,
        hasNextPage: page < totalPages,
        hasPrevPage: page > 1,
        nextPage: page < totalPages ? `/merchants?page=${page + 1}` : null,
        prevPage: page > 1 ? `/merchants?page=${page - 1}` : null,
      },
      count: dataResult.rowCount,
      data: dataResult.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

/** --------------------------------------------------------
 *  Auto-generator every second (1 payin + 1 payout)
 *  - you can change counts or disable by env var AUTO_GENERATE=false
 * ------------------------------------------------------- */
function startAutoGenerator() {
  const enabled = process.env.AUTO_GENERATE !== "false";
  const intervalMs = +process.env.AUTO_INTERVAL_MS || 10000000;

  if (!enabled) {
    console.log("⚠️ Auto generator disabled by env AUTO_GENERATE=false");
    return;
  }

  // Already running?
  if (autoInsertInterval) return;

  autoInsertInterval = setInterval(async () => {
    try {
      const date = dayjs().format("YYYY-MM-DD");
      await ensureSeed(date);
      await reloadDimensionTables();

      const payins = generatePayins(date, merchants, accounts, 1, false);
      const payouts = generatePayouts(date, merchants, accounts, 1, false);

      // insert into payments DB - Debezium will pick it up
      await insertIntoPostgres("payins", payins);
      await insertIntoPostgres("payouts", payouts);

      // write to files for traceability
      const dir = `./data/${date}`;
      await fs.ensureDir(dir);
      await writeJsonAndCsv(dir, `auto_payins_${Date.now()}`, payins);
      await writeJsonAndCsv(dir, `auto_payouts_${Date.now()}`, payouts);

      console.log(
        "⏱ auto-generated 1 payin + 1 payout and inserted into Postgres",
      );
    } catch (err) {
      console.error("Auto generator error:", err.message);
    }
  }, intervalMs);
}

/** --------------------------------------------------------
 *  Graceful shutdown
 * ------------------------------------------------------- */
async function shutdown() {
  console.log("\n🛑 Shutting down gracefully...");
  if (autoInsertInterval) clearInterval(autoInsertInterval);
  try {
    await pgPool.end();
    console.log("✅ Postgres pool closed");
  } catch (e) {
    console.warn("Postgres pool end error:", e.message);
  }
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

/** --------------------------------------------------------
 *  Init and Start
 * ------------------------------------------------------- */
(async () => {
  try {
    await initializeDatabaseSchema();
  } catch (err) {
    console.error("❌ Pre-startup database initialization failed:", err.message);
  }
  startAutoGenerator();

  app.listen(PORT, () => {
    console.log(`🚀 Mock Data API running on http://localhost:${PORT}`);
  });
})();

// /**
//  * Mock Data Generator API (v4) - Patched for dimension-first inserts and DB-backed generators
//  */

// import express from "express";
// import cors from "cors";
// import dayjs from "dayjs";
// import fs from "fs-extra";
// import dotenv from "dotenv";
// import { Kafka } from "kafkajs";
// import pLimit from "p-limit"; // for throttled concurrency
// import pkg from "pg"; // pg client
// const { Pool } = pkg;

// // --- Local imports
// import { generateMerchants } from "./generators/merchant.js";
// import { generateAccounts } from "./generators/account.js";
// import { generatePayins } from "./generators/payin.js";
// import { generatePayouts } from "./generators/payout.js";
// import { writeJsonAndCsv } from "./utils/helpers.js";

// dotenv.config();
// const app = express();
// app.use(cors());
// app.use(express.json());

// // --- Environment Config
// const PORT = +process.env.PORT || 4000;
// const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "mock-generator-v4";
// const KAFKA_BROKERS = process.env.KAFKA_BROKERS?.split(",") ?? [];
// const EVENTHUB_NAMESPACE = process.env.EVENTHUB_NAMESPACE;
// const EVENTHUB_CONNSTRING = process.env.EVENTHUB_CONNSTRING;

// // Postgres envs (defaults)
// const PG_HOST = process.env.PG_HOST || "localhost";
// const PG_PORT = +process.env.PG_PORT || 5432;
// const PG_USER = process.env.PG_USER || "postgres";
// const PG_PASSWORD = process.env.PG_PASSWORD || "postgres";
// const PG_DB = process.env.PG_DB || "payments";

// // --- Postgres pool
// const pgPool = new Pool({
//   host: PG_HOST,
//   port: PG_PORT,
//   user: PG_USER,
//   password: PG_PASSWORD,
//   database: PG_DB,
//   max: 10,
// });

// // --- Globals
// let kafkaProducer = null;
// let kafkaEnabled = false;
// let merchants = [];
// let accounts = [];
// let autoInsertInterval = null; // reference to interval

// /** --------------------------------------------------------
//  *  Kafka Initialization
//  * ------------------------------------------------------- */
// async function initKafka() {
//   try {
//     if (EVENTHUB_NAMESPACE && EVENTHUB_CONNSTRING) {
//       console.log("⚙️ Initializing Azure EventHub as Kafka...");
//       const kafka = new Kafka({
//         clientId: KAFKA_CLIENT_ID,
//         brokers: [`${EVENTHUB_NAMESPACE}.servicebus.windows.net:9093`],
//         ssl: true,
//         sasl: {
//           mechanism: "plain",
//           username: "$ConnectionString",
//           password: EVENTHUB_CONNSTRING,
//         },
//       });
//       kafkaProducer = kafka.producer();
//       await kafkaProducer.connect();
//       kafkaEnabled = true;
//       console.log("✅ Connected to Azure EventHub via Kafka protocol");
//     } else if (KAFKA_BROKERS.length > 0) {
//       console.log("⚙️ Connecting to local Kafka...");
//       const kafka = new Kafka({
//         clientId: KAFKA_CLIENT_ID,
//         brokers: KAFKA_BROKERS,
//       });
//       kafkaProducer = kafka.producer();
//       await kafkaProducer.connect();
//       kafkaEnabled = true;
//       console.log("✅ Connected to local Kafka:", KAFKA_BROKERS.join(", "));
//     } else {
//       console.log("⚠️ No Kafka or EventHub config — running in file-only mode");
//     }
//   } catch (err) {
//     console.error("❌ Kafka init failed, fallback to file mode:", err.message);
//   }
// }

// /** --------------------------------------------------------
//  *  Kafka Producer Utility
//  * ------------------------------------------------------- */
// async function produce(topic, records) {
//   if (!kafkaEnabled || !kafkaProducer || !records?.length) return;
//   try {
//     const messages = records.map((r) => ({
//       key: String(r.id || r.seqNum || Date.now()),
//       value: JSON.stringify(r),
//     }));
//     await kafkaProducer.send({ topic, messages });
//     console.log(`📤 ${records.length} messages sent to ${topic}`);
//   } catch (err) {
//     console.error(`❌ Kafka produce failed for ${topic}:`, err.message);
//   }
// }

// /** --------------------------------------------------------
//  *  Helper: Seed dimension tables
//  *  - now async and ensures DB upsert before facts are created
//  * ------------------------------------------------------- */
// async function ensureSeed(date) {
//   if (!merchants?.length) {
//     merchants = generateMerchants([], date);
//     try {
//       await insertIntoPostgres("merchants", merchants);
//     } catch (e) {
//       console.warn("ensureSeed: merchants upsert failed:", e.message);
//     }
//   }

//   if (!accounts?.length) {
//     accounts = generateAccounts([], date);
//     try {
//       await insertIntoPostgres("accounts", accounts);
//     } catch (e) {
//       console.warn("ensureSeed: accounts upsert failed:", e.message);
//     }
//   }
// }

// /** --------------------------------------------------------
//  *  Helper: Reload dimension tables from Postgres
//  *  - ensures the latest DB-backed rows are used by generators
//  * ------------------------------------------------------- */
// async function reloadDimensionTables() {
//   const client = await pgPool.connect();
//   try {
//     const m = await client.query(
//       "SELECT id, name, alias, category, contact_name, contact_email, contact_mobile, limit_amount AS limit, active_since, kyc_status, enabled, created_on, updated_on FROM merchants"
//     );
//     merchants = m.rows.map((r) => {
//       // map DB column names to the expected in-memory shape used by generators
//       return {
//         id: r.id,
//         name: r.name,
//         alias: r.alias,
//         category: r.category,
//         contactName: r.contact_name,
//         contactEmail: r.contact_email,
//         contactMobile: r.contact_mobile,
//         limit: r.limit,
//         activeSince: r.active_since
//           ? dayjs(r.active_since).format("YYYY-MM-DD")
//           : null,
//         kycStatus: r.kyc_status,
//         enabled: r.enabled,
//         createdOn: r.created_on,
//         updatedOn: r.updated_on,
//       };
//     });

//     const a = await client.query(
//       "SELECT id, bank, acc_name, acc_num, ifsc, vpa, zone, max_limit AS maxLimit, avg_ticket_size AS avgTicketSize, enabled, created_on, updated_on FROM accounts"
//     );
//     accounts = a.rows.map((r) => {
//       return {
//         id: r.id,
//         bank: r.bank,
//         accName: r.acc_name,
//         accNum: r.acc_num,
//         ifsc: r.ifsc,
//         vpa: r.vpa,
//         zone: r.zone,
//         maxLimit: r.maxlimit ?? r.maxLimit ?? null,
//         avgTicketSize: r.avg_ticket_size ?? r.avgTicketSize ?? null,
//         enabled: r.enabled,
//         createdOn: r.created_on,
//         updatedOn: r.updated_on,
//       };
//     });
//   } catch (err) {
//     console.warn("reloadDimensionTables error:", err.message);
//   } finally {
//     client.release();
//   }
// }

// /** --------------------------------------------------------
//  *  Postgres helper: upsert rows into given table
//  *  - handles JSONB fields for payins/payouts
//  * ------------------------------------------------------- */
// async function insertIntoPostgres(table, rows) {
//   if (!rows || !rows.length) return;
//   const client = await pgPool.connect();
//   try {
//     await client.query("BEGIN");
//     for (const r of rows) {
//       if (table === "merchants") {
//         const q = `
//           INSERT INTO merchants (id, name, alias, category, contact_name, contact_email, contact_mobile, limit_amount, active_since, kyc_status, enabled, created_on, updated_on)
//           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
//           ON CONFLICT (id) DO UPDATE SET
//             name=EXCLUDED.name, alias=EXCLUDED.alias, category=EXCLUDED.category,
//             contact_name=EXCLUDED.contact_name, contact_email=EXCLUDED.contact_email, contact_mobile=EXCLUDED.contact_mobile,
//             limit_amount=EXCLUDED.limit_amount, active_since=EXCLUDED.active_since, kyc_status=EXCLUDED.kyc_status,
//             enabled=EXCLUDED.enabled, updated_on=EXCLUDED.updated_on;
//         `;
//         await client.query(q, [
//           r.id,
//           r.name ?? null,
//           r.alias ?? null,
//           r.category ?? null,
//           r.contactName ?? null,
//           r.contactEmail ?? null,
//           r.contactMobile ?? null,
//           r.limit ?? null,
//           r.activeSince ?? null,
//           r.kycStatus ?? null,
//           r.enabled ?? null,
//           r.createdOn ?? null,
//           r.updatedOn ?? null,
//         ]);
//       } else if (table === "accounts") {
//         const q = `
//           INSERT INTO accounts (id, bank, acc_name, acc_num, ifsc, vpa, zone, max_limit, avg_ticket_size, enabled, created_on, updated_on)
//           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
//           ON CONFLICT (id) DO UPDATE SET
//             bank=EXCLUDED.bank, acc_name=EXCLUDED.acc_name, acc_num=EXCLUDED.acc_num,
//             ifsc=EXCLUDED.ifsc, vpa=EXCLUDED.vpa, zone=EXCLUDED.zone,
//             max_limit=EXCLUDED.max_limit, avg_ticket_size=EXCLUDED.avg_ticket_size, enabled=EXCLUDED.enabled,
//             updated_on=EXCLUDED.updated_on;
//         `;
//         await client.query(q, [
//           r.id,
//           r.bank ?? null,
//           r.accName ?? r.acc_name ?? null,
//           r.accNum ?? r.acc_num ?? null,
//           r.ifsc ?? null,
//           r.vpa ?? null,
//           r.zone ?? null,
//           r.maxLimit ?? null,
//           r.avgTicketSize ?? null,
//           r.enabled ?? null,
//           r.createdOn ?? null,
//           r.updatedOn ?? null,
//         ]);
//       } else if (table === "payins") {
//         const q = `
//     INSERT INTO payins (
//       id, seq_num, merchant_id, request_id,

//       customer_name, customer_mobile, customer_email,

//       txn_amount, txn_type, callback_url,

//       order_code, receiving_vpa, bank, zone, utr,
//       processed_amount, status, created_on, updated_on,

//       raw
//     )
//     VALUES (
//       $1, $2, $3, $4,

//       $5, $6, $7,

//       $8, $9, $10,

//       $11, $12, $13, $14, $15,
//       $16, $17, $18, $19,

//       $20::jsonb
//     )
//     ON CONFLICT (id) DO UPDATE SET
//       status = EXCLUDED.status,
//       processed_amount = EXCLUDED.processed_amount,
//       updated_on = EXCLUDED.updated_on;
//   `;

//         await client.query(q, [
//           r.id,
//           r.seqNum ?? null,
//           r.merchantId ?? null,
//           r.requestId ?? null,

//           r.customer?.name ?? null,
//           r.customer?.mobile ?? null,
//           r.customer?.email ?? null,

//           r.transaction?.amount ?? null,
//           r.transaction?.type ?? null,
//           r.transaction?.callbackUrl ?? null,

//           r.orderCode ?? null,
//           r.receivingVpa ?? null,
//           r.bank ?? null,
//           r.zone ?? null,
//           r.utr ?? null,

//           r.processedAmount ?? null,
//           r.status ?? null,
//           r.createdOn ?? null,
//           r.updatedOn ?? null,

//           JSON.stringify(r),
//         ]);
//       } else if (table === "payouts") {
//         const q = `
//     INSERT INTO payouts (
//       id, seq_num, merchant_id,

//       txn_amount, txn_type,

//       bank, ifsc, account_number,

//       status, requested_amount, processed_amount, utr,
//       created_on, updated_on,

//       raw
//     )
//     VALUES (
//       $1, $2, $3,

//       $4, $5,

//       $6, $7, $8,

//       $9, $10, $11, $12,
//       $13, $14,

//       $15::jsonb
//     )
//     ON CONFLICT (id) DO UPDATE SET
//       status = EXCLUDED.status,
//       processed_amount = EXCLUDED.processed_amount,
//       updated_on = EXCLUDED.updated_on;
//   `;

//         await client.query(q, [
//           r.id,
//           r.seqNum ?? null,
//           r.merchantId ?? null,

//           r.transaction?.amount ?? null,
//           r.transaction?.type ?? null,

//           r.account?.bank ?? null,
//           r.account?.ifsc ?? null,
//           r.account?.account ?? null,

//           r.status ?? null,
//           r.requestedAmount ?? null,
//           r.processedAmount ?? null,
//           r.utr ?? null,

//           r.createdOn ?? null,
//           r.updatedOn ?? null,

//           JSON.stringify(r),
//         ]);
//       } else {
//         // generic fallback: try to insert raw JSON into a `raw_events` table if exists
//         const q = `
//           INSERT INTO raw_events(payload) VALUES($1::jsonb)
//         `;
//         await client.query(q, [JSON.stringify(r)]);
//       }
//     }
//     await client.query("COMMIT");
//   } catch (err) {
//     await client.query("ROLLBACK");
//     console.error("❌ Postgres insert error:", err.message);
//     throw err;
//   } finally {
//     client.release();
//   }
// }

// /** --------------------------------------------------------
//  *  Routes
//  * ------------------------------------------------------- */

// // Health check
// app.get("/health", (_, res) =>
//   res.json({
//     status: "ok",
//     date: new Date().toISOString(),
//     kafka: kafkaEnabled,
//     postgres: !!pgPool,
//   })
// );

// // Generate Payins
// app.get("/generate/payin", async (req, res) => {
//   const count = +req.query.count || 10;
//   const extended = req.query.extended === "true";
//   const stream = req.query.stream === "true";
//   const date = dayjs().format("YYYY-MM-DD");

//   await ensureSeed(date);
//   await reloadDimensionTables();

//   const payins = generatePayins(date, merchants, accounts, count, extended);
//   const dir = `./data/${date}`;
//   await fs.ensureDir(dir);
//   await writeJsonAndCsv(dir, `payins_${Date.now()}`, payins);

//   // Save to Postgres (so Debezium can capture)
//   try {
//     await insertIntoPostgres("payins", payins);
//   } catch (err) {
//     console.error("Error inserting payins into Postgres:", err.message);
//   }

//   if (stream) await produce("payin-events", payins);

//   res.json({
//     count,
//     postgres: true,
//     kafka: kafkaEnabled && stream,
//     data: payins.slice(0, 5),
//   });
// });

// // Generate Payouts
// app.get("/generate/payout", async (req, res) => {
//   const count = +req.query.count || 10;
//   const extended = req.query.extended === "true";
//   const stream = req.query.stream === "true";
//   const date = dayjs().format("YYYY-MM-DD");

//   await ensureSeed(date);
//   await reloadDimensionTables();

//   const payouts = generatePayouts(date, merchants, accounts, count, extended);
//   const dir = `./data/${date}`;
//   await fs.ensureDir(dir);
//   await writeJsonAndCsv(dir, `payouts_${Date.now()}`, payouts);

//   // Save to Postgres
//   try {
//     await insertIntoPostgres("payouts", payouts);
//   } catch (err) {
//     console.error("Error inserting payouts into Postgres:", err.message);
//   }

//   if (stream) await produce("payout-events", payouts);

//   res.json({
//     count,
//     postgres: true,
//     kafka: kafkaEnabled && stream,
//     data: payouts.slice(0, 5),
//   });
// });

// // Dimension data
// app.get("/generate/merchant", async (_, res) => {
//   const date = dayjs().format("YYYY-MM-DD");
//   merchants = generateMerchants(merchants, date);
//   // upsert merchants to Postgres so Debezium can pick dimension updates
//   try {
//     await insertIntoPostgres("merchants", merchants);
//   } catch (err) {
//     console.error("Error inserting merchants into Postgres:", err.message);
//   }
//   res.json({ count: merchants.length, data: merchants });
// });

// app.get("/generate/account", async (_, res) => {
//   const date = dayjs().format("YYYY-MM-DD");
//   accounts = generateAccounts(accounts, date);
//   try {
//     await insertIntoPostgres("accounts", accounts);
//   } catch (err) {
//     console.error("Error inserting accounts into Postgres:", err.message);
//   }
//   res.json({ count: accounts.length, data: accounts });
// });

// // Generate historical data
// app.get("/generate/history", async (req, res) => {
//   const from = req.query.from || "2025-08-20";
//   const to = req.query.to || dayjs().format("YYYY-MM-DD");
//   const stream = req.query.stream === "true";
//   const extended = req.query.extended === "true";

//   const start = dayjs(from);
//   const end = dayjs(to);
//   const limit = pLimit(2); // limit concurrent days

//   console.log(`🕐 Generating data from ${from} → ${to}`);

//   merchants = [];
//   accounts = [];

//   const tasks = [];
//   for (let d = start; d.isBefore(end) || d.isSame(end); d = d.add(1, "day")) {
//     const date = d.format("YYYY-MM-DD");
//     const dir = `./data/${date}`;
//     await fs.ensureDir(dir);

//     merchants = generateMerchants(merchants, date);
//     accounts = generateAccounts(accounts, date);

//     // upsert dims
//     try {
//       await insertIntoPostgres("merchants", merchants);
//       await insertIntoPostgres("accounts", accounts);
//     } catch (err) {
//       console.error("Historical upsert error:", err.message);
//     }

//     const daysPassed = d.diff(start, "day");
//     const payinCount = 2000 + daysPassed * 100;
//     const payoutCount = 1000 + daysPassed * 50;

//     tasks.push(
//       limit(async () => {
//         // reload dims to ensure generators use DB-canonical shapes
//         await reloadDimensionTables();

//         const payins = generatePayins(
//           date,
//           merchants,
//           accounts,
//           payinCount,
//           extended
//         );
//         const payouts = generatePayouts(
//           date,
//           merchants,
//           accounts,
//           payoutCount,
//           extended
//         );

//         await writeJsonAndCsv(dir, "payins", payins);
//         await writeJsonAndCsv(dir, "payouts", payouts);

//         // write to postgres
//         try {
//           await insertIntoPostgres("payins", payins);
//           await insertIntoPostgres("payouts", payouts);
//         } catch (err) {
//           console.error("Historical insert error:", err.message);
//         }

//         if (stream) {
//           await produce("payin-events", payins.slice(0, 5000));
//           await produce("payout-events", payouts.slice(0, 5000));
//         }

//         console.log(`✅ ${date}: ${payinCount} payins, ${payoutCount} payouts`);
//       })
//     );
//   }

//   await Promise.all(tasks);
//   res.json({
//     message: `✅ History generated ${from} → ${to}`,
//     kafka: kafkaEnabled && stream,
//   });
// });

// /** --------------------------------------------------------
//  *  Auto-generator every second (1 payin + 1 payout)
//  *  - you can change counts or disable by env var AUTO_GENERATE=false
//  * ------------------------------------------------------- */
// function startAutoGenerator() {
//   const enabled = process.env.AUTO_GENERATE !== "false";
//   const intervalMs = +process.env.AUTO_INTERVAL_MS || 1000;

//   if (!enabled) {
//     console.log("⚠️ Auto generator disabled by env AUTO_GENERATE=false");
//     return;
//   }

//   // Already running?
//   if (autoInsertInterval) return;

//   autoInsertInterval = setInterval(async () => {
//     try {
//       const date = dayjs().format("YYYY-MM-DD");
//       await ensureSeed(date);
//       await reloadDimensionTables();

//       const payins = generatePayins(date, merchants, accounts, 1, false);
//       const payouts = generatePayouts(date, merchants, accounts, 1, false);

//       // insert into payments DB - Debezium will pick it up
//       await insertIntoPostgres("payins", payins);
//       await insertIntoPostgres("payouts", payouts);

//       // optional: also push to kafka if enabled
//       if (kafkaEnabled) {
//         await produce("payin-events", payins);
//         await produce("payout-events", payouts);
//       }

//       // write to files for traceability
//       const dir = `./data/${date}`;
//       await fs.ensureDir(dir);
//       await writeJsonAndCsv(dir, `auto_payins_${Date.now()}`, payins);
//       await writeJsonAndCsv(dir, `auto_payouts_${Date.now()}`, payouts);

//       console.log(
//         "⏱ auto-generated 1 payin + 1 payout and inserted into Postgres"
//       );
//     } catch (err) {
//       console.error("Auto generator error:", err.message);
//     }
//   }, intervalMs);
// }

// /** --------------------------------------------------------
//  *  Graceful shutdown
//  * ------------------------------------------------------- */
// async function shutdown() {
//   console.log("\\n🛑 Shutting down gracefully...");
//   if (autoInsertInterval) clearInterval(autoInsertInterval);
//   if (kafkaProducer) {
//     try {
//       await kafkaProducer.disconnect();
//     } catch (e) {
//       console.warn("Kafka disconnect error:", e.message);
//     }
//   }
//   try {
//     await pgPool.end();
//     console.log("✅ Postgres pool closed");
//   } catch (e) {
//     console.warn("Postgres pool end error:", e.message);
//   }
//   process.exit(0);
// }

// process.on("SIGINT", shutdown);
// process.on("SIGTERM", shutdown);

// /** --------------------------------------------------------
//  *  Init and Start
//  * ------------------------------------------------------- */
// await initKafka();
// startAutoGenerator();

// app.listen(PORT, () => {
//   console.log(`🚀 Mock Data API v4 running on http://localhost:${PORT}`);
// });

// /**

//  * Mock Data Generator API (v4) - Updated for Postgres CDC pipeline
//  */

// import express from "express";
// import cors from "cors";
// import dayjs from "dayjs";
// import fs from "fs-extra";
// import dotenv from "dotenv";
// import { Kafka } from "kafkajs";
// import pLimit from "p-limit"; // for throttled concurrency
// import pkg from "pg"; // pg client
// const { Pool } = pkg;

// // --- Local imports
// import { generateMerchants } from "./generators/merchant.js";
// import { generateAccounts } from "./generators/account.js";
// import { generatePayins } from "./generators/payin.js";
// import { generatePayouts } from "./generators/payout.js";
// import { writeJsonAndCsv } from "./utils/helpers.js";

// dotenv.config();
// const app = express();
// app.use(cors());
// app.use(express.json());

// // --- Environment Config
// const PORT = +process.env.PORT || 4000;
// const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "mock-generator-v4";
// const KAFKA_BROKERS = process.env.KAFKA_BROKERS?.split(",") ?? [];
// const EVENTHUB_NAMESPACE = process.env.EVENTHUB_NAMESPACE;
// const EVENTHUB_CONNSTRING = process.env.EVENTHUB_CONNSTRING;

// // Postgres envs (defaults)
// const PG_HOST = process.env.PG_HOST || "localhost";
// const PG_PORT = +process.env.PG_PORT || 5432;
// const PG_USER = process.env.PG_USER || "postgres";
// const PG_PASSWORD = process.env.PG_PASSWORD || "postgres";
// const PG_DB = process.env.PG_DB || "payments";

// // --- Postgres pool
// const pgPool = new Pool({
//   host: PG_HOST,
//   port: PG_PORT,
//   user: PG_USER,
//   password: PG_PASSWORD,
//   database: PG_DB,
//   max: 10,
// });

// // --- Globals
// let kafkaProducer = null;
// let kafkaEnabled = false;
// let merchants = [];
// let accounts = [];
// let autoInsertInterval = null; // reference to interval

// /** --------------------------------------------------------
//  *  Kafka Initialization
//  * ------------------------------------------------------- */
// async function initKafka() {
//   try {
//     if (EVENTHUB_NAMESPACE && EVENTHUB_CONNSTRING) {
//       console.log("⚙️ Initializing Azure EventHub as Kafka...");
//       const kafka = new Kafka({
//         clientId: KAFKA_CLIENT_ID,
//         brokers: [`${EVENTHUB_NAMESPACE}.servicebus.windows.net:9093`],
//         ssl: true,
//         sasl: {
//           mechanism: "plain",
//           username: "$ConnectionString",
//           password: EVENTHUB_CONNSTRING,
//         },
//       });
//       kafkaProducer = kafka.producer();
//       await kafkaProducer.connect();
//       kafkaEnabled = true;
//       console.log("✅ Connected to Azure EventHub via Kafka protocol");
//     } else if (KAFKA_BROKERS.length > 0) {
//       console.log("⚙️ Connecting to local Kafka...");
//       const kafka = new Kafka({
//         clientId: KAFKA_CLIENT_ID,
//         brokers: KAFKA_BROKERS,
//       });
//       kafkaProducer = kafka.producer();
//       await kafkaProducer.connect();
//       kafkaEnabled = true;
//       console.log("✅ Connected to local Kafka:", KAFKA_BROKERS.join(", "));
//     } else {
//       console.log("⚠️ No Kafka or EventHub config — running in file-only mode");
//     }
//   } catch (err) {
//     console.error("❌ Kafka init failed, fallback to file mode:", err.message);
//   }
// }

// /** --------------------------------------------------------
//  *  Kafka Producer Utility
//  * ------------------------------------------------------- */
// async function produce(topic, records) {
//   if (!kafkaEnabled || !kafkaProducer || !records?.length) return;
//   try {
//     const messages = records.map((r) => ({
//       key: String(r.id || r.seqNum || Date.now()),
//       value: JSON.stringify(r),
//     }));
//     await kafkaProducer.send({ topic, messages });
//     console.log(`📤 ${records.length} messages sent to ${topic}`);
//   } catch (err) {
//     console.error(`❌ Kafka produce failed for ${topic}:`, err.message);
//   }
// }

// /** --------------------------------------------------------
//  *  Helper: Seed dimension tables
//  * ------------------------------------------------------- */
// function ensureSeed(date) {
//   if (!merchants?.length) merchants = generateMerchants([], date);
//   if (!accounts?.length) accounts = generateAccounts([], date);
// }

// /** --------------------------------------------------------
//  *  Postgres helper: upsert rows into given table
//  *  - handles JSONB fields for payins/payouts
//  * ------------------------------------------------------- */
// async function insertIntoPostgres(table, rows) {
//   if (!rows || !rows.length) return;
//   const client = await pgPool.connect();
//   try {
//     await client.query("BEGIN");
//     for (const r of rows) {
//       if (table === "merchants") {
//         const q = `
//           INSERT INTO merchants (id, name, alias, category, contactName, contactEmail, contactMobile, limit, activeSince, kycStatus, enabled, createdOn, updatedOn)
//           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
//           ON CONFLICT (id) DO UPDATE SET
//             name=EXCLUDED.name, alias=EXCLUDED.alias, category=EXCLUDED.category,
//             contactName=EXCLUDED.contactName, contactEmail=EXCLUDED.contactEmail, contactMobile=EXCLUDED.contactMobile,
//             limit=EXCLUDED.limit, activeSince=EXCLUDED.activeSince, kycStatus=EXCLUDED.kycStatus,
//             enabled=EXCLUDED.enabled, updatedOn=EXCLUDED.updatedOn;
//         `;
//         await client.query(q, [
//           r.id,
//           r.name ?? null,
//           r.alias ?? null,
//           r.category ?? null,
//           r.contactName ?? null,
//           r.contactEmail ?? null,
//           r.contactMobile ?? null,
//           r.limit ?? null,
//           r.activeSince ?? null,
//           r.kycStatus ?? null,
//           r.enabled ?? null,
//           r.createdOn ?? null,
//           r.updatedOn ?? null,
//         ]);
//       } else if (table === "accounts") {
//         const q = `
//           INSERT INTO accounts (id, bank, accName, accNum, ifsc, vpa, zone, maxLimit, avgTicketSize, enabled, createdOn, updatedOn)
//           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
//           ON CONFLICT (id) DO UPDATE SET
//             bank=EXCLUDED.bank, accName=EXCLUDED.accName, accNum=EXCLUDED.accNum,
//             ifsc=EXCLUDED.ifsc, vpa=EXCLUDED.vpa, zone=EXCLUDED.zone,
//             maxLimit=EXCLUDED.maxLimit, avgTicketSize=EXCLUDED.avgTicketSize, enabled=EXCLUDED.enabled,
//             updatedOn=EXCLUDED.updatedOn;
//         `;
//         await client.query(q, [
//           r.id,
//           r.bank ?? null,
//           r.accName ?? null,
//           r.accNum ?? null,
//           r.ifsc ?? null,
//           r.vpa ?? null,
//           r.zone ?? null,
//           r.maxLimit ?? null,
//           r.avgTicketSize ?? null,
//           r.enabled ?? null,
//           r.createdOn ?? null,
//           r.updatedOn ?? null,
//         ]);
//       } else if (table === "payins") {
//         const q = `
//     INSERT INTO payins (
//       id, seq_num, merchant_id, request_id,

//       customer_name, customer_mobile, customer_email,

//       txn_amount, txn_type, callback_url,

//       order_code, receiving_vpa, bank, zone, utr,
//       processed_amount, status, created_on, updated_on,

//       raw
//     )
//     VALUES (
//       $1, $2, $3, $4,

//       $5, $6, $7,

//       $8, $9, $10,

//       $11, $12, $13, $14, $15,
//       $16, $17, $18, $19,

//       $20::jsonb
//     )
//     ON CONFLICT (id) DO UPDATE SET
//       status = EXCLUDED.status,
//       processed_amount = EXCLUDED.processed_amount,
//       updated_on = EXCLUDED.updated_on;
//   `;

//         await client.query(q, [
//           r.id,
//           r.seqNum,
//           r.merchantId,
//           r.requestId,

//           r.customer?.name ?? null,
//           r.customer?.mobile ?? null,
//           r.customer?.email ?? null,

//           r.transaction?.amount ?? null,
//           r.transaction?.type ?? null,
//           r.transaction?.callbackUrl ?? null,

//           r.orderCode ?? null,
//           r.receivingVpa ?? null,
//           r.bank ?? null,
//           r.zone ?? null,
//           r.utr ?? null,

//           r.processedAmount ?? null,
//           r.status ?? null,
//           r.createdOn ?? null,
//           r.updatedOn ?? null,

//           JSON.stringify(r),
//         ]);
//       } else if (table === "payouts") {
//         const q = `
//     INSERT INTO payouts (
//       id, seq_num, merchant_id,

//       txn_amount, txn_type,

//       bank, ifsc, account_number,

//       status, requested_amount, processed_amount, utr,
//       created_on, updated_on,

//       raw
//     )
//     VALUES (
//       $1, $2, $3,

//       $4, $5,

//       $6, $7, $8,

//       $9, $10, $11, $12,
//       $13, $14,

//       $15::jsonb
//     )
//     ON CONFLICT (id) DO UPDATE SET
//       status = EXCLUDED.status,
//       processed_amount = EXCLUDED.processed_amount,
//       updated_on = EXCLUDED.updated_on;
//   `;

//         await client.query(q, [
//           r.id,
//           r.seqNum,
//           r.merchantId,

//           r.transaction?.amount ?? null,
//           r.transaction?.type ?? null,

//           r.account?.bank ?? null,
//           r.account?.ifsc ?? null,
//           r.account?.account ?? null,

//           r.status ?? null,
//           r.requestedAmount ?? null,
//           r.processedAmount ?? null,
//           r.utr ?? null,

//           r.createdOn ?? null,
//           r.updatedOn ?? null,

//           JSON.stringify(r),
//         ]);
//       } else {
//         // generic fallback: try to insert raw JSON into a `raw_events` table if exists
//         const q = `
//           INSERT INTO raw_events(payload) VALUES($1::jsonb)
//         `;
//         await client.query(q, [JSON.stringify(r)]);
//       }
//     }
//     await client.query("COMMIT");
//   } catch (err) {
//     await client.query("ROLLBACK");
//     console.error("❌ Postgres insert error:", err.message);
//   } finally {
//     client.release();
//   }
// }

// /** --------------------------------------------------------
//  *  Routes
//  * ------------------------------------------------------- */

// // Health check
// app.get("/health", (_, res) =>
//   res.json({
//     status: "ok",
//     date: new Date().toISOString(),
//     kafka: kafkaEnabled,
//     postgres: !!pgPool,
//   })
// );

// // Generate Payins
// app.get("/generate/payin", async (req, res) => {
//   const count = +req.query.count || 10;
//   const extended = req.query.extended === "true";
//   const stream = req.query.stream === "true";
//   const date = dayjs().format("YYYY-MM-DD");

//   ensureSeed(date);
//   const payins = generatePayins(date, merchants, accounts, count, extended);
//   const dir = `./data/${date}`;
//   await fs.ensureDir(dir);
//   await writeJsonAndCsv(dir, `payins_${Date.now()}`, payins);

//   // Save to Postgres (so Debezium can capture)
//   try {
//     await insertIntoPostgres("payins", payins);
//   } catch (err) {
//     console.error("Error inserting payins into Postgres:", err.message);
//   }

//   if (stream) await produce("payin-events", payins);

//   res.json({
//     count,
//     postgres: true,
//     kafka: kafkaEnabled && stream,
//     data: payins.slice(0, 5),
//   });
// });

// // Generate Payouts
// app.get("/generate/payout", async (req, res) => {
//   const count = +req.query.count || 10;
//   const extended = req.query.extended === "true";
//   const stream = req.query.stream === "true";
//   const date = dayjs().format("YYYY-MM-DD");

//   ensureSeed(date);
//   const payouts = generatePayouts(date, merchants, accounts, count, extended);
//   const dir = `./data/${date}`;
//   await fs.ensureDir(dir);
//   await writeJsonAndCsv(dir, `payouts_${Date.now()}`, payouts);

//   // Save to Postgres
//   try {
//     await insertIntoPostgres("payouts", payouts);
//   } catch (err) {
//     console.error("Error inserting payouts into Postgres:", err.message);
//   }

//   if (stream) await produce("payout-events", payouts);

//   res.json({
//     count,
//     postgres: true,
//     kafka: kafkaEnabled && stream,
//     data: payouts.slice(0, 5),
//   });
// });

// // Dimension data
// app.get("/generate/merchant", async (_, res) => {
//   const date = dayjs().format("YYYY-MM-DD");
//   merchants = generateMerchants(merchants, date);
//   // upsert merchants to Postgres so Debezium can pick dimension updates
//   try {
//     await insertIntoPostgres("merchants", merchants);
//   } catch (err) {
//     console.error("Error inserting merchants into Postgres:", err.message);
//   }
//   res.json({ count: merchants.length, data: merchants });
// });

// app.get("/generate/account", async (_, res) => {
//   const date = dayjs().format("YYYY-MM-DD");
//   accounts = generateAccounts(accounts, date);
//   try {
//     await insertIntoPostgres("accounts", accounts);
//   } catch (err) {
//     console.error("Error inserting accounts into Postgres:", err.message);
//   }
//   res.json({ count: accounts.length, data: accounts });
// });

// // Generate historical data
// app.get("/generate/history", async (req, res) => {
//   const from = req.query.from || "2025-08-20";
//   const to = req.query.to || dayjs().format("YYYY-MM-DD");
//   const stream = req.query.stream === "true";
//   const extended = req.query.extended === "true";

//   const start = dayjs(from);
//   const end = dayjs(to);
//   const limit = pLimit(2); // limit concurrent days

//   console.log(`🕐 Generating data from ${from} → ${to}`);

//   merchants = [];
//   accounts = [];

//   const tasks = [];
//   for (let d = start; d.isBefore(end) || d.isSame(end); d = d.add(1, "day")) {
//     const date = d.format("YYYY-MM-DD");
//     const dir = `./data/${date}`;
//     await fs.ensureDir(dir);

//     merchants = generateMerchants(merchants, date);
//     accounts = generateAccounts(accounts, date);

//     const daysPassed = d.diff(start, "day");
//     const payinCount = 2000 + daysPassed * 100;
//     const payoutCount = 1000 + daysPassed * 50;

//     tasks.push(
//       limit(async () => {
//         const payins = generatePayins(
//           date,
//           merchants,
//           accounts,
//           payinCount,
//           extended
//         );
//         const payouts = generatePayouts(
//           date,
//           merchants,
//           accounts,
//           payoutCount,
//           extended
//         );

//         await writeJsonAndCsv(dir, "payins", payins);
//         await writeJsonAndCsv(dir, "payouts", payouts);

//         // write to postgres
//         try {
//           await insertIntoPostgres("payins", payins);
//           await insertIntoPostgres("payouts", payouts);
//         } catch (err) {
//           console.error("Historical insert error:", err.message);
//         }

//         if (stream) {
//           await produce("payin-events", payins.slice(0, 5000));
//           await produce("payout-events", payouts.slice(0, 5000));
//         }

//         console.log(`✅ ${date}: ${payinCount} payins, ${payoutCount} payouts`);
//       })
//     );
//   }

//   await Promise.all(tasks);
//   res.json({
//     message: `✅ History generated ${from} → ${to}`,
//     kafka: kafkaEnabled && stream,
//   });
// });

// /** --------------------------------------------------------
//  *  Auto-generator every second (1 payin + 1 payout)
//  *  - you can change counts or disable by env var AUTO_GENERATE=false
//  * ------------------------------------------------------- */
// function startAutoGenerator() {
//   const enabled = process.env.AUTO_GENERATE !== "false";
//   const intervalMs = +process.env.AUTO_INTERVAL_MS || 1000;

//   if (!enabled) {
//     console.log("⚠️ Auto generator disabled by env AUTO_GENERATE=false");
//     return;
//   }

//   // Already running?
//   if (autoInsertInterval) return;

//   autoInsertInterval = setInterval(async () => {
//     try {
//       const date = dayjs().format("YYYY-MM-DD");
//       ensureSeed(date);

//       const payins = generatePayins(date, merchants, accounts, 1, false);
//       const payouts = generatePayouts(date, merchants, accounts, 1, false);

//       // insert into payments DB - Debezium will pick it up
//       await insertIntoPostgres("payins", payins);
//       await insertIntoPostgres("payouts", payouts);

//       // optional: also push to kafka if enabled
//       if (kafkaEnabled) {
//         await produce("payin-events", payins);
//         await produce("payout-events", payouts);
//       }

//       // write to files for traceability
//       const dir = `./data/${date}`;
//       await fs.ensureDir(dir);
//       await writeJsonAndCsv(dir, `auto_payins_${Date.now()}`, payins);
//       await writeJsonAndCsv(dir, `auto_payouts_${Date.now()}`, payouts);

//       console.log(
//         "⏱ auto-generated 1 payin + 1 payout and inserted into Postgres"
//       );
//     } catch (err) {
//       console.error("Auto generator error:", err.message);
//     }
//   }, intervalMs);
// }

// /** --------------------------------------------------------
//  *  Graceful shutdown
//  * ------------------------------------------------------- */
// async function shutdown() {
//   console.log("\n🛑 Shutting down gracefully...");
//   if (autoInsertInterval) clearInterval(autoInsertInterval);
//   if (kafkaProducer) {
//     try {
//       await kafkaProducer.disconnect();
//     } catch (e) {
//       console.warn("Kafka disconnect error:", e.message);
//     }
//   }
//   try {
//     await pgPool.end();
//     console.log("✅ Postgres pool closed");
//   } catch (e) {
//     console.warn("Postgres pool end error:", e.message);
//   }
//   process.exit(0);
// }

// process.on("SIGINT", shutdown);
// process.on("SIGTERM", shutdown);

// /** --------------------------------------------------------
//  *  Init and Start
//  * ------------------------------------------------------- */
// await initKafka();
// startAutoGenerator();

// app.listen(PORT, () => {
//   console.log(`🚀 Mock Data API v4 running on http://localhost:${PORT}`);
// });

// /**
//  * Mock Data Generator API (v4)
//  * ------------------------------------
//  * Features:
//  * ✅ Robust Kafka connection (local + Azure Event Hub)
//  * ✅ Modular route generation
//  * ✅ Smart historical replay with batching
//  * ✅ Environment-driven configuration
//  * ✅ Graceful shutdown support
//  */

// import express from "express";
// import cors from "cors";
// import dayjs from "dayjs";
// import fs from "fs-extra";
// import dotenv from "dotenv";
// import { Kafka } from "kafkajs";
// import pLimit from "p-limit"; // for throttled concurrency

// // --- Local imports
// import { generateMerchants } from "./generators/merchant.js";
// import { generateAccounts } from "./generators/account.js";
// import { generatePayins } from "./generators/payin.js";
// import { generatePayouts } from "./generators/payout.js";
// import { writeJsonAndCsv } from "./utils/helpers.js";

// dotenv.config();
// const app = express();
// app.use(cors());
// app.use(express.json());

// // --- Environment Config
// const PORT = +process.env.PORT || 4000;
// const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "mock-generator-v4";
// const KAFKA_BROKERS = process.env.KAFKA_BROKERS?.split(",") ?? [];
// const EVENTHUB_NAMESPACE = process.env.EVENTHUB_NAMESPACE;
// const EVENTHUB_CONNSTRING = process.env.EVENTHUB_CONNSTRING;

// // --- Globals
// let kafkaProducer = null;
// let kafkaEnabled = false;
// let merchants = [];
// let accounts = [];

// /** --------------------------------------------------------
//  *  Kafka Initialization
//  * ------------------------------------------------------- */
// async function initKafka() {
//   try {
//     if (EVENTHUB_NAMESPACE && EVENTHUB_CONNSTRING) {
//       console.log("⚙️ Initializing Azure EventHub as Kafka...");
//       const kafka = new Kafka({
//         clientId: KAFKA_CLIENT_ID,
//         brokers: [`${EVENTHUB_NAMESPACE}.servicebus.windows.net:9093`],
//         ssl: true,
//         sasl: {
//           mechanism: "plain",
//           username: "$ConnectionString",
//           password: EVENTHUB_CONNSTRING,
//         },
//       });
//       kafkaProducer = kafka.producer();
//       await kafkaProducer.connect();
//       kafkaEnabled = true;
//       console.log("✅ Connected to Azure EventHub via Kafka protocol");
//     } else if (KAFKA_BROKERS.length > 0) {
//       console.log("⚙️ Connecting to local Kafka...");
//       const kafka = new Kafka({
//         clientId: KAFKA_CLIENT_ID,
//         brokers: KAFKA_BROKERS,
//       });
//       kafkaProducer = kafka.producer();
//       await kafkaProducer.connect();
//       kafkaEnabled = true;
//       console.log("✅ Connected to local Kafka:", KAFKA_BROKERS.join(", "));
//     } else {
//       console.log("⚠️ No Kafka or EventHub config — running in file-only mode");
//     }
//   } catch (err) {
//     console.error("❌ Kafka init failed, fallback to file mode:", err.message);
//   }
// }

// /** --------------------------------------------------------
//  *  Kafka Producer Utility
//  * ------------------------------------------------------- */
// async function produce(topic, records) {
//   if (!kafkaEnabled || !kafkaProducer || !records?.length) return;
//   try {
//     const messages = records.map((r) => ({
//       key: String(r.id || r.seqNum || Date.now()),
//       value: JSON.stringify(r),
//     }));
//     await kafkaProducer.send({ topic, messages });
//     console.log(`📤 ${records.length} messages sent to ${topic}`);
//   } catch (err) {
//     console.error(`❌ Kafka produce failed for ${topic}:`, err.message);
//   }
// }

// /** --------------------------------------------------------
//  *  Helper: Seed dimension tables
//  * ------------------------------------------------------- */
// function ensureSeed(date) {
//   if (!merchants?.length) merchants = generateMerchants([], date);
//   if (!accounts?.length) accounts = generateAccounts([], date);
// }

// /** --------------------------------------------------------
//  *  Routes
//  * ------------------------------------------------------- */

// // Health check
// app.get("/health", (_, res) =>
//   res.json({
//     status: "ok",
//     date: new Date().toISOString(),
//     kafka: kafkaEnabled,
//   })
// );

// // Generate Payins
// app.get("/generate/payin", async (req, res) => {
//   const count = +req.query.count || 10;
//   const extended = req.query.extended === "true";
//   const stream = req.query.stream === "true";
//   const date = dayjs().format("YYYY-MM-DD");

//   ensureSeed(date);
//   const payins = generatePayins(date, merchants, accounts, count, extended);
//   const dir = `./data/${date}`;
//   await fs.ensureDir(dir);
//   await writeJsonAndCsv(dir, `payins_${Date.now()}`, payins);

//   if (stream) await produce("payin-events", payins);

//   res.json({ count, kafka: kafkaEnabled && stream, data: payins.slice(0, 5) });
// });

// // Generate Payouts
// app.get("/generate/payout", async (req, res) => {
//   const count = +req.query.count || 10;
//   const extended = req.query.extended === "true";
//   const stream = req.query.stream === "true";
//   const date = dayjs().format("YYYY-MM-DD");

//   ensureSeed(date);
//   const payouts = generatePayouts(date, merchants, accounts, count, extended);
//   const dir = `./data/${date}`;
//   await fs.ensureDir(dir);
//   await writeJsonAndCsv(dir, `payouts_${Date.now()}`, payouts);

//   if (stream) await produce("payout-events", payouts);

//   res.json({ count, kafka: kafkaEnabled && stream, data: payouts.slice(0, 5) });
// });

// // Dimension data
// app.get("/generate/merchant", (_, res) => {
//   const date = dayjs().format("YYYY-MM-DD");
//   merchants = generateMerchants(merchants, date);
//   res.json({ count: merchants.length, data: merchants });
// });

// app.get("/generate/account", (_, res) => {
//   const date = dayjs().format("YYYY-MM-DD");
//   accounts = generateAccounts(accounts, date);
//   res.json({ count: accounts.length, data: accounts });
// });

// // Generate historical data
// app.get("/generate/history", async (req, res) => {
//   const from = req.query.from || "2025-08-20";
//   const to = req.query.to || dayjs().format("YYYY-MM-DD");
//   const stream = req.query.stream === "true";
//   const extended = req.query.extended === "true";

//   const start = dayjs(from);
//   const end = dayjs(to);
//   const limit = pLimit(2); // limit concurrent days

//   console.log(`🕐 Generating data from ${from} → ${to}`);

//   merchants = [];
//   accounts = [];

//   const tasks = [];
//   for (let d = start; d.isBefore(end) || d.isSame(end); d = d.add(1, "day")) {
//     const date = d.format("YYYY-MM-DD");
//     const dir = `./data/${date}`;
//     await fs.ensureDir(dir);

//     merchants = generateMerchants(merchants, date);
//     accounts = generateAccounts(accounts, date);

//     const daysPassed = d.diff(start, "day");
//     const payinCount = 2000 + daysPassed * 100;
//     const payoutCount = 1000 + daysPassed * 50;

//     tasks.push(
//       limit(async () => {
//         const payins = generatePayins(
//           date,
//           merchants,
//           accounts,
//           payinCount,
//           extended
//         );
//         const payouts = generatePayouts(
//           date,
//           merchants,
//           accounts,
//           payoutCount,
//           extended
//         );

//         await writeJsonAndCsv(dir, "payins", payins);
//         await writeJsonAndCsv(dir, "payouts", payouts);

//         if (stream) {
//           await produce("payin-events", payins.slice(0, 5000));
//           await produce("payout-events", payouts.slice(0, 5000));
//         }

//         console.log(`✅ ${date}: ${payinCount} payins, ${payoutCount} payouts`);
//       })
//     );
//   }

//   await Promise.all(tasks);
//   res.json({
//     message: `✅ History generated ${from} → ${to}`,
//     kafka: kafkaEnabled && stream,
//   });
// });

// /** --------------------------------------------------------
//  *  Graceful shutdown
//  * ------------------------------------------------------- */
// process.on("SIGINT", async () => {
//   console.log("\n🛑 Shutting down gracefully...");
//   if (kafkaProducer) await kafkaProducer.disconnect();
//   process.exit(0);
// });

// /** --------------------------------------------------------
//  *  Init and Start
//  * ------------------------------------------------------- */
// await initKafka();
// app.listen(PORT, () => {
//   console.log(`🚀 Mock Data API v4 running on http://localhost:${PORT}`);
// });

// import express from 'express';
// import cors from 'cors';
// import dayjs from 'dayjs';
// import fs from 'fs-extra';
// import dotenv from 'dotenv';
// import { Kafka } from 'kafkajs';
// import { generateMerchants } from './generators/merchant.js';
// import { generateAccounts } from './generators/account.js';
// import { generatePayins } from './generators/payin.js';
// import { generatePayouts } from './generators/payout.js';
// import { writeJsonAndCsv } from './utils/helpers.js';

// dotenv.config();

// const app = express();
// app.use(cors());
// app.use(express.json());

// const PORT = process.env.PORT || 4000;
// const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : [];
// const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'mock-generator-v3';

// let kafkaProducer = null;
// let kafkaEnabled = false;

// // init Kafka if brokers provided
// async function initKafka() {
//   if (!KAFKA_BROKERS.length) {
//     console.log('⚠️ KAFKA_BROKERS not set - running in file-only mode');
//     kafkaEnabled = false;
//     return;
//   }

//   try {
//     const kafka = new Kafka({ clientId: KAFKA_CLIENT_ID, brokers: KAFKA_BROKERS });
//     kafkaProducer = kafka.producer();
//     await kafkaProducer.connect();
//     kafkaEnabled = true;
//     console.log('✅ Connected to Kafka brokers:', KAFKA_BROKERS);
//   } catch (err) {
//     console.error('❌ Kafka init failed, continuing in file-only mode:', err.message);
//     kafkaEnabled = false;
//     kafkaProducer = null;
//   }
// }

// await initKafka(); // top-level await in ESM, Node 18+ required

// async function produce(topic, records) {
//   if (!kafkaEnabled || !kafkaProducer) return;
//   try {
//     const messages = records.map(r => ({ key: String(r.id || r.seqNum || ''), value: JSON.stringify(r) }));
//     await kafkaProducer.send({ topic, messages });
//   } catch (err) {
//     console.error('❌ Kafka produce error', err.message);
//   }
// }

// // in-memory seed arrays (server lifetime)
// let merchants = [];
// let accounts = [];

// function ensureSeed(date) {
//   if (!Array.isArray(merchants) || merchants.length === 0) merchants = generateMerchants([], date);
//   if (!Array.isArray(accounts) || accounts.length === 0) accounts = generateAccounts([], date);
// }

// // health
// app.get('/health', (req, res) => res.json({ status: 'ok', date: new Date().toISOString(), kafka: kafkaEnabled }));

// // generate payin
// app.get('/generate/payin', async (req, res) => {
//   const count = Math.max(1, parseInt(req.query.count) || 10);
//   const extended = req.query.extended === 'true' || req.query.extended === '1';
//   const stream = req.query.stream === 'true' || req.query.stream === '1';
//   const date = dayjs().format('YYYY-MM-DD');

//   ensureSeed(date);
//   const payins = generatePayins(date, merchants, accounts, count, extended);

//   // save to disk
//   const dir = `./data/${date}`;
//   await fs.ensureDir(dir);
//   await writeJsonAndCsv(dir, `payins_${Date.now()}`, payins); // write unique file per call

//   if (stream && kafkaEnabled) {
//     await produce('payin-events', payins);
//   }

//   res.json({ count: payins.length, kafka: kafkaEnabled && stream, data: payins });
// });

// // generate payout
// app.get('/generate/payout', async (req, res) => {
//   const count = Math.max(1, parseInt(req.query.count) || 10);
//   const extended = req.query.extended === 'true' || req.query.extended === '1';
//   const stream = req.query.stream === 'true' || req.query.stream === '1';
//   const date = dayjs().format('YYYY-MM-DD');

//   ensureSeed(date);
//   const payouts = generatePayouts(date, merchants, accounts, count, extended);

//   const dir = `./data/${date}`;
//   await fs.ensureDir(dir);
//   await writeJsonAndCsv(dir, `payouts_${Date.now()}`, payouts);

//   if (stream && kafkaEnabled) {
//     await produce('payout-events', payouts);
//   }

//   res.json({ count: payouts.length, kafka: kafkaEnabled && stream, data: payouts });
// });

// // generate merchants
// app.get('/generate/merchant', async (req, res) => {
//   const date = dayjs().format('YYYY-MM-DD');
//   merchants = generateMerchants(merchants, date);
//   res.json({ count: merchants.length, data: merchants });
// });

// // generate accounts
// app.get('/generate/account', async (req, res) => {
//   const date = dayjs().format('YYYY-MM-DD');
//   accounts = generateAccounts(accounts, date);
//   res.json({ count: accounts.length, data: accounts });
// });

// // generate all and save files for today
// app.get('/generate/all', async (req, res) => {
//   const stream = req.query.stream === 'true' || req.query.stream === '1';
//   const extended = req.query.extended === 'true' || req.query.extended === '1';
//   const date = dayjs().format('YYYY-MM-DD');
//   const dir = `./data/${date}`;
//   await fs.ensureDir(dir);

//   merchants = generateMerchants(merchants, date);
//   accounts = generateAccounts(accounts, date);

//   const payins = generatePayins(date, merchants, accounts, 100, extended);
//   const payouts = generatePayouts(date, merchants, accounts, 50, extended);

//   await writeJsonAndCsv(dir, 'merchants', merchants);
//   await writeJsonAndCsv(dir, 'accounts', accounts);
//   await writeJsonAndCsv(dir, 'payins', payins);
//   await writeJsonAndCsv(dir, 'payouts', payouts);
//   await fs.writeJson(`${dir}/meta.json`, { merchants: merchants.length, accounts: accounts.length, payins: payins.length, payouts: payouts.length }, { spaces: 2 });

//   if (stream && kafkaEnabled) {
//     await produce('payin-events', payins);
//     await produce('payout-events', payouts);
//   }

//   res.json({ message: `✅ Data generated for ${date}`, merchants: merchants.length, accounts: accounts.length, payins: payins.length, payouts: payouts.length, kafka: kafkaEnabled && stream });
// });

// // generate history (range)
// app.get('/generate/history', async (req, res) => {
//   const from = req.query.from || '2025-08-20';
//   const to = req.query.to || dayjs().format('YYYY-MM-DD');
//   const stream = req.query.stream === 'true' || req.query.stream === '1';
//   const extended = req.query.extended === 'true' || req.query.extended === '1';
//   const startDate = dayjs(from);
//   const endDate = dayjs(to);

//   merchants = [];
//   accounts = [];

//   for (let d = startDate; d.isBefore(endDate) || d.isSame(endDate); d = d.add(1, 'day')) {
//     const dateStr = d.format('YYYY-MM-DD');
//     const dir = `./data/${dateStr}`;
//     await fs.ensureDir(dir);

//     merchants = generateMerchants(merchants, dateStr);
//     accounts = generateAccounts(accounts, dateStr);

//     const daysPassed = d.diff(startDate, 'day');
//     const payinCount = 2000 + daysPassed * 100;
//     const payoutCount = 1000 + daysPassed * 50;

//     const payins = generatePayins(dateStr, merchants, accounts, payinCount, extended);
//     const payouts = generatePayouts(dateStr, merchants, accounts, payoutCount, extended);

//     await writeJsonAndCsv(dir, 'merchants', merchants);
//     await writeJsonAndCsv(dir, 'accounts', accounts);
//     await writeJsonAndCsv(dir, 'payins', payins);
//     await writeJsonAndCsv(dir, 'payouts', payouts);
//     await fs.writeJson(`${dir}/meta.json`, { merchants: merchants.length, accounts: accounts.length, payins: payinCount, payouts: payoutCount }, { spaces: 2 });

//     if (stream && kafkaEnabled) {
//       // stream in chunks to avoid huge single send
//       const chunkSize = 1000;
//       for (let i = 0; i < payins.length; i += chunkSize) {
//         const slice = payins.slice(i, i + chunkSize);
//         await produce('payin-events', slice);
//       }
//       for (let i = 0; i < payouts.length; i += chunkSize) {
//         const slice = payouts.slice(i, i + chunkSize);
//         await produce('payout-events', slice);
//       }
//     }

//     console.log(`Generated ${dateStr}`);
//   }

//   res.json({ message: `✅ Historical data generated from ${from} to ${to}`, kafka: kafkaEnabled && stream });
// });

// app.listen(PORT, () => console.log(`🚀 Mock Data API v3 listening on http://localhost:${PORT} (kafka: ${kafkaEnabled})`));

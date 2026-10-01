/**
 * Q-NIRVANA — Smart Digital Hospital Management System — backend (single file)
 * Node.js + Express · MySQL (mysql2) · Firebase Admin SDK · Socket.IO · Healthcare Digital Twin (T2D prototype)
 *
 * Project files:  index.html (frontend) · server.js (this backend) · package.json · vercel.json
 *
 * Environment variables (never commit real values):
 *   MYSQL_HOST, MYSQL_PORT (3306), MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE   — remote MySQL (required)
 *     or MYSQL_URL=mysql://user:pass@host:3306/dbname ; MYSQL_SSL=1 if the provider requires TLS (MYSQL_SSL_CA optional)
 *   FIREBASE_PROJECT_ID (default q-nirvana-d5620) · FIREBASE_SERVICE_ACCOUNT_JSON (optional, enables revocation checks)
 *   GOOGLE_MAPS_API_KEY (optional) · CORS_ORIGINS (default *) · REQUIRE_EMAIL_VERIFICATION (default on, "0" disables)
 *   HOSPITAL_ACCESS_CODE (optional) · PORT (default 3000, local server only)
 *
 * Run locally:   npm install && MYSQL_HOST=... MYSQL_USER=... MYSQL_PASSWORD=... MYSQL_DATABASE=... node server.js
 *                → open http://<host>:3000 (index.html is served by this server; Socket.IO realtime enabled)
 * Vercel:        vercel.json routes /api/* to this file (@vercel/node) and / to index.html. Set the MySQL variables in
 *                Project → Settings → Environment Variables. Serverless functions cannot keep WebSockets open, so on
 *                Vercel the frontend refreshes automatically instead (health reports realtime=false).
 *                In production a localhost/127.0.0.1 MySQL host is rejected — use a remotely accessible MySQL server.
 * Tables are created/upgraded automatically on the first database request. No sample or demo data is ever inserted.
 */
import crypto from 'crypto';
import fs from 'fs';
import http from 'http';
import path from 'path';
import { fileURLToPath, pathToFileURL } from 'url';
import express from 'express';
import cors from 'cors';
import mysql from 'mysql2/promise';
import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getAuth } from 'firebase-admin/auth';
import { Server as SocketServer } from 'socket.io';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ON_SERVERLESS = !!(process.env.VERCEL || process.env.AWS_LAMBDA_FUNCTION_NAME);
const IS_PROD = ON_SERVERLESS || process.env.NODE_ENV === 'production';

class HttpError extends Error { constructor(status, message, code) { super(message); this.status = status; this.code = code; } }

/* ================= Firebase Admin SDK (server-side only) ================= */
const FB_PROJECT = process.env.FIREBASE_PROJECT_ID || 'q-nirvana-d5620';
let FB_CHECK_REVOKED = false;
if (!getApps().length) {
  if (process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    initializeApp({ credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON)), projectId: FB_PROJECT });
    FB_CHECK_REVOKED = true;
  } else {
    initializeApp({ projectId: FB_PROJECT }); // ID-token verification only needs Google's public signing keys
  }
}
async function verifyFirebaseToken(tok) {
  if (!tok || String(tok).split('.').length !== 3) return null;
  try {
    const c = await getAuth().verifyIdToken(String(tok), FB_CHECK_REVOKED);
    if (c.aud !== FB_PROJECT || !c.uid) return null;
    return { uid: c.uid, email: String(c.email || '').toLowerCase(), email_verified: c.email_verified === true };
  } catch { return null; }
}

/* ================= MySQL (remote, environment-configured) ================= */
function mysqlConfig() {
  const c = {
    host: process.env.MYSQL_HOST || (IS_PROD ? '' : 'localhost'), port: Number(process.env.MYSQL_PORT || 3306),
    user: process.env.MYSQL_USER || '', password: process.env.MYSQL_PASSWORD || '', database: process.env.MYSQL_DATABASE || process.env.MYSQL_DB || 'qnirvana',
    ssl: /^(1|true|yes|required)$/i.test(process.env.MYSQL_SSL || ''),
  };
  const url = process.env.MYSQL_URL || process.env.DATABASE_URL || '';
  if (/^mysql:\/\//i.test(url)) {
    const u = new URL(url);
    Object.assign(c, { host: u.hostname, port: Number(u.port || 3306), user: decodeURIComponent(u.username), password: decodeURIComponent(u.password), database: decodeURIComponent(u.pathname.slice(1)) || c.database });
    if (/ssl/i.test(u.search)) c.ssl = true;
  }
  return c;
}
const MY = mysqlConfig();
const LOCAL_DB_HOST = /^(localhost|127(\.\d+){3}|::1|0\.0\.0\.0)$/i;
function dbConfigProblem() {
  if (!MY.host) return 'MySQL is not configured — set MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD and MYSQL_DATABASE (or MYSQL_URL) in the deployment environment';
  if (IS_PROD && LOCAL_DB_HOST.test(MY.host) && process.env.ALLOW_LOCAL_DB !== '1') return `MYSQL_HOST is "${MY.host}", which is not reachable from a deployed server — use the hostname of a remotely accessible MySQL database`;
  if (!MY.user) return 'MYSQL_USER is not set';
  if (!/^[A-Za-z0-9_$-]{1,64}$/.test(MY.database)) return 'MYSQL_DATABASE contains invalid characters';
  return null;
}
const sslOpt = () => (MY.ssl ? { rejectUnauthorized: process.env.MYSQL_SSL_REJECT_UNAUTHORIZED !== '0', ...(process.env.MYSQL_SSL_CA ? { ca: process.env.MYSQL_SSL_CA.replace(/\\n/g, '\n') } : {}) } : undefined);
const DB_DOWN_CODES = new Set(['ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', 'ECONNRESET', 'EHOSTUNREACH', 'PROTOCOL_CONNECTION_LOST', 'ER_ACCESS_DENIED_ERROR', 'ER_BAD_DB_ERROR', 'HANDSHAKE_SSL_ERROR', 'ER_CON_COUNT_ERROR', 'PROTOCOL_SEQUENCE_TIMEOUT', 'ER_DBACCESS_DENIED_ERROR']);
function dbError(e) {
  if (e instanceof HttpError) return e;
  if (DB_DOWN_CODES.has(e.code) || /connect|timed? ?out|handshake/i.test(e.message || '')) {
    pool = null; schemaReady = null; // retry on the next request
    return new HttpError(503, `Database unavailable: cannot connect to MySQL at ${MY.host}:${MY.port} (${e.code || e.message}). Check MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE.`, 'db_unavailable');
  }
  console.error('MySQL error:', e.code, e.message);
  return new HttpError(500, `Database error (${e.code || 'unknown'})`, 'db_error');
}
let pool = null, schemaReady = null;
function getPool() {
  const problem = dbConfigProblem();
  if (problem) throw new HttpError(503, 'Database unavailable: ' + problem, 'db_unavailable');
  if (!pool) {
    pool = mysql.createPool({ host: MY.host, port: MY.port, user: MY.user, password: MY.password, database: MY.database, ssl: sslOpt(), waitForConnections: true, connectionLimit: ON_SERVERLESS ? 3 : 10, connectTimeout: 8000, dateStrings: true, timezone: 'Z', charset: 'utf8mb4' });
    pool.pool.on('connection', (c) => c.query("SET time_zone = '+00:00'"));
  }
  return pool;
}
const SCHEMA = {
  users: { id: 'INT AUTO_INCREMENT PRIMARY KEY', firebase_uid: 'VARCHAR(128) NOT NULL', role: 'VARCHAR(20) NOT NULL', full_name: 'VARCHAR(120) NOT NULL', email: 'VARCHAR(160) NOT NULL', phone: 'VARCHAR(20) NULL', hospital_id: 'INT NULL', designation: 'VARCHAR(40) NULL', department: 'VARCHAR(80) NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  patients: { id: 'INT AUTO_INCREMENT PRIMARY KEY', user_id: 'INT NOT NULL', dob: 'DATE NULL', gender: 'VARCHAR(20) NULL', blood_group: 'VARCHAR(5) NULL', address: 'VARCHAR(255) NULL', emergency_contact: 'VARCHAR(20) NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  hospitals: { id: 'INT AUTO_INCREMENT PRIMARY KEY', name: 'VARCHAR(120) NOT NULL', name_key: 'VARCHAR(120) NOT NULL', address: 'VARCHAR(255) NULL', phone: 'VARCHAR(20) NULL', lat: 'DOUBLE NULL', lng: 'DOUBLE NULL', join_code: 'VARCHAR(12) NOT NULL', created_by: 'INT NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  departments: { id: 'INT AUTO_INCREMENT PRIMARY KEY', hospital_id: 'INT NOT NULL', name: 'VARCHAR(80) NOT NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  hospital_users: { id: 'INT AUTO_INCREMENT PRIMARY KEY', user_id: 'INT NOT NULL', hospital_id: 'INT NOT NULL', designation: 'VARCHAR(40) NOT NULL', department_id: 'INT NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  doctors: { id: 'INT AUTO_INCREMENT PRIMARY KEY', user_id: 'INT NOT NULL', hospital_id: 'INT NOT NULL', department_id: 'INT NULL', specialization: 'VARCHAR(120) NULL', qualification: 'VARCHAR(120) NULL', room: 'VARCHAR(30) NULL', token_prefix: 'VARCHAR(4) NOT NULL', avg_consult_minutes: 'INT DEFAULT 10', is_available: 'TINYINT(1) DEFAULT 1', active: 'TINYINT(1) NOT NULL DEFAULT 1', avail_start: 'VARCHAR(5) NULL', avail_end: 'VARCHAR(5) NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  ambulance_operators: { id: 'INT AUTO_INCREMENT PRIMARY KEY', user_id: 'INT NOT NULL', license_no: 'VARCHAR(40) NULL', ambulance_id: 'INT NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  ambulances: { id: 'INT AUTO_INCREMENT PRIMARY KEY', vehicle_no: 'VARCHAR(20) NOT NULL', ambulance_type: "VARCHAR(10) DEFAULT 'BLS'", status: "VARCHAR(20) DEFAULT 'available'", operator_id: 'INT NULL', hospital_id: 'INT NULL', current_lat: 'DOUBLE NULL', current_lng: 'DOUBLE NULL', updated_at: 'DATETIME NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  appointments: { id: 'INT AUTO_INCREMENT PRIMARY KEY', patient_id: 'INT NOT NULL', hospital_id: 'INT NOT NULL', doctor_id: 'INT NOT NULL', department_id: 'INT NULL', appointment_date: 'DATE NOT NULL', appointment_time: 'VARCHAR(5) NOT NULL', reason: 'VARCHAR(255) NULL', status: "VARCHAR(20) DEFAULT 'waiting'", created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  queue_tokens: { id: 'INT AUTO_INCREMENT PRIMARY KEY', appointment_id: 'INT NOT NULL', doctor_id: 'INT NOT NULL', hospital_id: 'INT NOT NULL', token_date: 'DATE NOT NULL', token_number: 'INT NOT NULL', token_code: 'VARCHAR(10) NOT NULL', status: "VARCHAR(20) DEFAULT 'waiting'", called_at: 'DATETIME NULL', started_at: 'DATETIME NULL', completed_at: 'DATETIME NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  consultations: { id: 'INT AUTO_INCREMENT PRIMARY KEY', appointment_id: 'INT NOT NULL', hospital_id: 'INT NOT NULL', doctor_id: 'INT NOT NULL', patient_id: 'INT NOT NULL', diagnosis: 'TEXT NULL', prescription: 'TEXT NULL', notes: 'TEXT NULL', follow_up_date: 'DATE NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  hospital_resources: { id: 'INT AUTO_INCREMENT PRIMARY KEY', hospital_id: 'INT NOT NULL', resource_type: 'VARCHAR(30) NOT NULL', name: 'VARCHAR(60) NOT NULL', ward: 'VARCHAR(60) NULL', status: "VARCHAR(20) DEFAULT 'available'", assigned_emergency_id: 'INT NULL', assigned_patient_name: 'VARCHAR(120) NULL', updated_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  emergency_cases: { id: 'INT AUTO_INCREMENT PRIMARY KEY', hospital_id: 'INT NOT NULL', patient_id: 'INT NULL', reporter_user_id: 'INT NULL', patient_name: 'VARCHAR(120) NULL', contact_phone: 'VARCHAR(20) NULL', emergency_type: 'VARCHAR(60) NULL', severity: 'VARCHAR(20) NULL', description: 'TEXT NULL', pickup_lat: 'DOUBLE NULL', pickup_lng: 'DOUBLE NULL', pickup_address: 'VARCHAR(255) NULL', status: "VARCHAR(20) DEFAULT 'requested'", ambulance_id: 'INT NULL', operator_id: 'INT NULL', eta_minutes: 'INT NULL', distance_km: 'DOUBLE NULL', triage_level: 'VARCHAR(10) NULL', resource_id: 'INT NULL', hospital_prepared: 'TINYINT(1) DEFAULT 0', hospital_notes: 'VARCHAR(255) NULL', accepted_at: 'DATETIME NULL', arrived_at: 'DATETIME NULL', trip_completed_at: 'DATETIME NULL', closed_at: 'DATETIME NULL', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP', updated_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  ambulance_locations: { id: 'INT AUTO_INCREMENT PRIMARY KEY', ambulance_id: 'INT NOT NULL', emergency_id: 'INT NULL', lat: 'DOUBLE NOT NULL', lng: 'DOUBLE NOT NULL', speed: 'DOUBLE NULL', heading: 'DOUBLE NULL', recorded_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
  notifications: { id: 'INT AUTO_INCREMENT PRIMARY KEY', user_id: 'INT NOT NULL', title: 'VARCHAR(160) NULL', message: 'VARCHAR(500) NULL', type: "VARCHAR(20) DEFAULT 'info'", is_read: 'TINYINT(1) DEFAULT 0', created_at: 'DATETIME DEFAULT CURRENT_TIMESTAMP' },
};
const SCHEMA_KEYS = {
  users: ['UNIQUE KEY uq_users_uid (firebase_uid)', 'UNIQUE KEY uq_users_email (email)'], patients: ['UNIQUE KEY uq_pat_user (user_id)'], hospitals: ['UNIQUE KEY uq_hosp_key (name_key)'],
  departments: ['KEY ix_dept_h (hospital_id)'], hospital_users: ['UNIQUE KEY uq_hu_user (user_id)', 'KEY ix_hu_h (hospital_id)'], doctors: ['UNIQUE KEY uq_doc_user (user_id)', 'KEY ix_doc_h (hospital_id)'],
  ambulance_operators: ['UNIQUE KEY uq_op_user (user_id)'], ambulances: ['UNIQUE KEY uq_amb_vehicle (vehicle_no)'], appointments: ['KEY ix_ap_doc (doctor_id, appointment_date)', 'KEY ix_ap_h (hospital_id, appointment_date)', 'KEY ix_ap_p (patient_id)'],
  queue_tokens: ['KEY ix_q_doc (doctor_id, token_date)', 'KEY ix_q_ap (appointment_id)'], consultations: ['KEY ix_c_ap (appointment_id)'], hospital_resources: ['KEY ix_r_h (hospital_id)'],
  emergency_cases: ['KEY ix_e_h (hospital_id, status)'], ambulance_locations: ['KEY ix_al_a (ambulance_id)'], notifications: ['KEY ix_n_u (user_id, is_read)'],
};
const qi = (c) => '`' + String(c).replace(/`/g, '') + '`';
async function ensureSchema() {
  if (schemaReady) return schemaReady;
  schemaReady = (async () => {
    getPool();
    try { // managed databases may forbid CREATE DATABASE; then the database must already exist
      const c = await mysql.createConnection({ host: MY.host, port: MY.port, user: MY.user, password: MY.password, ssl: sslOpt(), connectTimeout: 8000 });
      try { await c.query(`CREATE DATABASE IF NOT EXISTS ${qi(MY.database)} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`); } catch (e) { if (DB_DOWN_CODES.has(e.code) && e.code !== 'ER_DBACCESS_DENIED_ERROR' && e.code !== 'ER_ACCESS_DENIED_ERROR') throw e; } finally { await c.end().catch(() => {}); }
    } catch (e) { if (['ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'HANDSHAKE_SSL_ERROR'].includes(e.code)) throw e; }
    const p = getPool();
    for (const [table, cols] of Object.entries(SCHEMA)) {
      const defs = [...Object.entries(cols).map(([c, d]) => `${qi(c)} ${d}`), ...(SCHEMA_KEYS[table] || [])];
      await p.query(`CREATE TABLE IF NOT EXISTS ${qi(table)} (${defs.join(', ')}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
      const [have] = await p.query(`SHOW COLUMNS FROM ${qi(table)}`);
      const names = new Set(have.map((r) => r.Field));
      for (const [c, d] of Object.entries(cols)) if (!names.has(c)) await p.query(`ALTER TABLE ${qi(table)} ADD COLUMN ${qi(c)} ${d.replace(/NOT NULL/i, 'NULL').replace(/AUTO_INCREMENT PRIMARY KEY/i, '')}`);
      if (table === 'users' && names.has('password_hash')) await p.query('ALTER TABLE `users` DROP COLUMN `password_hash`'); // passwords are never stored (Firebase handles them)
    }
  })();
  try { await schemaReady; } catch (e) { schemaReady = null; throw dbError(e); }
  return schemaReady;
}
async function dbq(sql, params = []) {
  await ensureSchema();
  try { const [rows] = await getPool().query(sql, params); return rows; } catch (e) {
    if (e.code === 'ER_NO_SUCH_TABLE' || e.code === 'ER_BAD_DB_ERROR') { // database/tables were removed while running: recreate once, then retry
      const old = pool; pool = null; schemaReady = null; if (old) old.end().catch(() => {});
      await ensureSchema();
      try { const [rows] = await getPool().query(sql, params); return rows; } catch (e2) { throw dbError(e2); }
    }
    throw dbError(e);
  }
}

/* Minimal parameterised query builder (MySQL). All values are bound with "?" placeholders. */
const BOOL_COLS = new Set(['is_available', 'is_read', 'hospital_prepared', 'active']);
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;
const SQL_DT_RE = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$/;
function toDb(v) {
  if (v === undefined) return null;
  if (typeof v === 'boolean') return v ? 1 : 0;
  if (v instanceof Date) return v.toISOString().slice(0, 19).replace('T', ' ');
  if (typeof v === 'string' && ISO_RE.test(v)) return new Date(v).toISOString().slice(0, 19).replace('T', ' ');
  return v;
}
function fromDb(r) {
  for (const k of Object.keys(r)) {
    const v = r[k];
    if (BOOL_COLS.has(k) && v !== null && v !== undefined) r[k] = !!Number(v);
    else if (typeof v === 'string' && SQL_DT_RE.test(v)) r[k] = v.slice(0, 19).replace(' ', 'T') + 'Z';
  }
  return r;
}
class Query {
  constructor(table) { if (!SCHEMA[table]) throw new HttpError(500, 'Unknown table ' + table); this.t = table; this.op = 'select'; this.cols = '*'; this.w = []; this.p = []; this.ord = []; this.lim = null; this.cnt = false; this.head = false; this.ret = false; this.vals = null; }
  select(cols = '*', opts = {}) { if (this.op === 'select') { this.cols = cols; this.cnt = opts.count === 'exact'; this.head = !!opts.head; } else this.ret = true; return this; }
  cond(sql, ...vals) { this.w.push(sql); this.p.push(...vals.map(toDb)); return this; }
  eq(c, v) { return v === null ? this.cond(`${qi(c)} IS NULL`) : this.cond(`${qi(c)} = ?`, v); }
  neq(c, v) { return this.cond(`${qi(c)} <> ?`, v); }
  gte(c, v) { return this.cond(`${qi(c)} >= ?`, v); }
  lte(c, v) { return this.cond(`${qi(c)} <= ?`, v); }
  ilike(c, v) { return this.cond(`${qi(c)} LIKE ?`, v); }
  is(c, v) { return v === null ? this.cond(`${qi(c)} IS NULL`) : this.cond(`${qi(c)} = ?`, v); }
  in(c, arr) { const a = arr && arr.length ? arr : [null]; return this.cond(`${qi(c)} IN (${a.map(() => '?').join(',')})`, ...a); }
  or(expr) { // supports "col.eq.value,col.is.null"
    const parts = [], vals = [];
    for (const term of String(expr).split(',')) {
      const [c, op, ...rest] = term.split('.'); const v = rest.join('.');
      if (op === 'eq') { parts.push(`${qi(c)} = ?`); vals.push(v); } else if (op === 'is' && v === 'null') parts.push(`${qi(c)} IS NULL`); else throw new HttpError(500, 'Unsupported filter ' + term);
    }
    return this.cond(`(${parts.join(' OR ')})`, ...vals);
  }
  order(c, o = {}) { this.ord.push(`${qi(c)} ${o.ascending === false ? 'DESC' : 'ASC'}`); return this; }
  limit(n) { this.lim = Math.max(0, Number(n) | 0); return this; }
  insert(rows) { this.op = 'insert'; this.vals = Array.isArray(rows) ? rows : [rows]; return this; }
  update(fields) { this.op = 'update'; this.vals = fields; return this; }
  delete() { this.op = 'delete'; return this; }
  where() { return this.w.length ? ' WHERE ' + this.w.join(' AND ') : ''; }
  then(resolve, reject) { return this.run().then(resolve, reject); }
  async byIds(idList) { if (!idList.length) return []; return (await dbq(`SELECT * FROM ${qi(this.t)} WHERE id IN (${idList.map(() => '?').join(',')}) ORDER BY id`, idList)).map(fromDb); }
  async run() {
    const t = qi(this.t);
    if (this.op === 'select') {
      if (this.cnt && this.head) { const r = await dbq(`SELECT COUNT(*) AS c FROM ${t}${this.where()}`, this.p); return { data: null, error: null, count: Number(r[0].c) }; }
      const cols = this.cols === '*' ? '*' : String(this.cols).split(',').map((c) => qi(c.trim())).join(', ');
      const rows = await dbq(`SELECT ${cols} FROM ${t}${this.where()}${this.ord.length ? ' ORDER BY ' + this.ord.join(', ') : ''}${this.lim !== null ? ' LIMIT ' + this.lim : ''}`, this.p);
      return { data: rows.map(fromDb), error: null, count: this.cnt ? rows.length : null };
    }
    if (this.op === 'insert') {
      const idsOut = [];
      for (const row of this.vals) {
        const keys = Object.keys(row).filter((k) => row[k] !== undefined && SCHEMA[this.t][k]);
        const r = await dbq(`INSERT INTO ${t} (${keys.map(qi).join(', ')}) VALUES (${keys.map(() => '?').join(', ')})`, keys.map((k) => toDb(row[k])));
        idsOut.push(r.insertId);
      }
      return { data: this.ret ? await this.byIds(idsOut) : null, error: null };
    }
    if (!this.w.length) throw new HttpError(500, 'Refusing to ' + this.op + ' without a WHERE clause');
    if (this.op === 'update') {
      const keys = Object.keys(this.vals).filter((k) => this.vals[k] !== undefined && SCHEMA[this.t][k]);
      const target = this.ret ? (await dbq(`SELECT id FROM ${t}${this.where()}`, this.p)).map((r) => r.id) : null;
      if (this.ret && !target.length) return { data: [], error: null };
      const set = keys.map((k) => `${qi(k)} = ?`).join(', '), vals = keys.map((k) => toDb(this.vals[k]));
      if (this.ret) await dbq(`UPDATE ${t} SET ${set} WHERE id IN (${target.map(() => '?').join(',')})`, [...vals, ...target]);
      else await dbq(`UPDATE ${t} SET ${set}${this.where()}`, [...vals, ...this.p]);
      return { data: this.ret ? await this.byIds(target) : null, error: null };
    }
    await dbq(`DELETE FROM ${t}${this.where()}`, this.p);
    return { data: null, error: null };
  }
}
const db = { from: (table) => new Query(table) };
async function syncUserProfile(userId) { // mirror hospital_id / designation / department onto the users row
  await dbq(`UPDATE users u LEFT JOIN hospital_users hu ON hu.user_id = u.id LEFT JOIN departments d ON d.id = hu.department_id
             SET u.hospital_id = hu.hospital_id, u.designation = hu.designation, u.department = d.name WHERE u.id = ?`, [userId]);
}

/* ================= realtime (Socket.IO) ================= */
const rt = {
  io: null,
  notify(userIds, n) { if (this.io) for (const u of userIds) this.io.to('user_' + u).emit('notification', n); },
  emit(ev, data) { if (this.io) this.io.emit(ev, data || {}); },
};


/* ================= Healthcare Digital Twin (synthetic data only) ================= */
// Q-NIRVANA Healthcare Digital Twin — Type 2 diabetes: predicting a glucose spike ~2 hours ahead.
// PROTOTYPE / PREDICTION ONLY — not a medical device, not a diagnosis.
// Data policy: every EHR value and wearable sample used here is SYNTHETIC, produced by a seeded physiological simulator.
// No real or identifiable patient measurements are used, stored or derived. Real Q-NIRVANA records only provide the
// patient id that selects (seeds) a synthetic twin; twin data is never written to the patient's medical records.
const Twin = (() => {
  const VERSION = 'qn-dt-t2d-1.0';
  const TZ = Number((typeof process !== 'undefined' && process.env && process.env.TWIN_TZ_OFFSET_MIN) || 330); // IST daily rhythm
  const STEP = 5, DAY = 1440, PER_DAY = DAY / STEP, HORIZON = 120, HSTEPS = HORIZON / STEP;
  const SPIKE_LEVEL = 180, SPIKE_RISE = 30;
  const clamp = (x, a, b) => Math.min(b, Math.max(a, x));
  const rd = (x, d = 1) => Math.round(x * 10 ** d) / 10 ** d;

  /* ---------- deterministic pseudo-random numbers (seeded, reproducible) ---------- */
  function strHash(s) { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } h ^= h >>> 13; h = Math.imul(h, 0x5bd1e995); return (h ^ (h >>> 15)) >>> 0; }
  function mix(a, b, c) { let h = (a ^ Math.imul(b + 0x9e3779b9, 0x85ebca6b) ^ Math.imul(c + 0x27d4eb2f, 0xc2b2ae35)) >>> 0; h ^= h >>> 16; h = Math.imul(h, 0x7feb352d); h ^= h >>> 15; h = Math.imul(h, 0x846ca68b); h ^= h >>> 16; return h >>> 0; }
  const u3 = (a, b, c) => mix(a, b, c) / 4294967296;
  const n3 = (a, b, c) => { const u = Math.max(1e-9, u3(a, b, c)), v = u3(a, b + 7777, c); return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v); };

  /* ---------- synthetic static EHR profile ---------- */
  const profiles = new Map();
  function profile(seed) {
    const hit = profiles.get(seed); if (hit) return hit;
    const sn = strHash(String(seed)), n = (k) => n3(sn, k, 0), u = (k) => u3(sn, k, 0);
    const age = Math.round(clamp(55 + n(1) * 10, 28, 82));
    const bmi = rd(clamp(29 + n(3) * 4.5, 19.5, 46));
    const hba1c = rd(clamp(7.5 + n(4) * 1.1, 5.9, 11.8));
    const insulin = hba1c >= 8.5 && u(6) < 0.55, metformin = u(7) < 0.85;
    const sbp = Math.round(clamp(133 + n(8) * 14, 102, 182)), dbp = Math.round(clamp(83 + n(9) * 9, 60, 112));
    const ldl = Math.round(clamp(112 + n(10) * 30, 48, 220)), hdl = Math.round(clamp(44 + n(11) * 9, 25, 80)), tg = Math.round(clamp(165 + n(12) * 55, 60, 480));
    const egfr = Math.round(clamp(92 - (age - 50) * 0.7 + n(13) * 12, 22, 125));
    const fpg = Math.round(clamp(92 + (hba1c - 5.5) * 24 + n(14) * 9, 84, 290));
    const P = {
      seed, sn, age, sex: u(2) < 0.5 ? 'Female' : 'Male', bmi, hba1c, years: Math.round(clamp(7 + n(5) * 4.5, 0, 28)), insulin, metformin,
      sbp, dbp, ldl, hdl, tg, egfr, fpg, htn: sbp >= 135 || u(15) < 0.3, dysl: ldl >= 130 || tg >= 200, obese: bmi >= 30, ckd: egfr < 60,
      sens: clamp(1.05 + 0.26 * (hba1c - 7) + 0.025 * (bmi - 28) - (insulin ? 0.2 : 0) - (metformin ? 0.12 : 0) + 0.12 * n(16), 0.55, 2.8),
      tau: clamp(55 + 9 * (hba1c - 7) + 5 * n(17), 40, 90), base: clamp(fpg - 12 + 5 * n(18), 75, 260), dawn: clamp(6 + 5 * (hba1c - 6.5) + 4 * n(19), 0, 35),
      bt: 465 + 25 * n(20), lt: 795 + 30 * n(21), dnt: 1185 + 35 * n(22), bc: clamp(48 + 12 * n(23), 20, 90), lc: clamp(68 + 15 * n(24), 25, 120), dc: clamp(72 + 16 * n(25), 25, 130),
      snackP: clamp(0.45 + 0.2 * n(26), 0, 1), walkP: clamp(0.55 + 0.2 * n(27), 0.05, 0.95), walkT: u(28) < 0.5 ? 390 + 30 * n(29) : 1110 + 35 * n(29),
      bed: 1365 + 35 * n(30), activity: clamp(1 + 0.3 * n(31), 0.4, 1.8), rhr: clamp(70 + 7 * n(32) + (bmi - 28) * 0.35, 52, 98), hrv: clamp(42 - (age - 45) * 0.45 + 8 * n(33), 12, 85),
      ph1: u(34) * 6.283, ph2: u(35) * 6.283, days: new Map(),
    };
    if (profiles.size > 600) profiles.clear();
    profiles.set(seed, P); return P;
  }
  function plan(P, d) { // daily routine: meals (carbs), walk, sleep
    let p = P.days.get(d); if (p) return p;
    const n = (k) => n3(P.sn, k, d + 100000), u = (k) => u3(P.sn, k, d + 100000);
    const meals = [{ t: P.bt + 22 * n(1), c: clamp(P.bc * (1 + 0.22 * n(2)), 10, 140) }, { t: P.lt + 28 * n(3), c: clamp(P.lc * (1 + 0.22 * n(4)), 10, 160) }, { t: P.dnt + 32 * n(5), c: clamp(P.dc * (1 + 0.22 * n(6)), 10, 160) }];
    if (u(7) < P.snackP) meals.push({ t: 1000 + 55 * n(8), c: 15 + 25 * u(9) });
    const walk = u(10) < P.walkP ? { t: P.walkT + 35 * n(11), dur: 20 + 30 * u(12), inten: 0.6 + 0.5 * u(13) } : null;
    p = { meals, walk, wake: 385 + 28 * n(14), bed: P.bed + 38 * n(15), stress: clamp(1 + 0.08 * n(16), 0.85, 1.25) };
    if (P.days.size > 64) P.days.clear();
    P.days.set(d, p); return p;
  }
  const sleepH = (P, d) => clamp((plan(P, d).wake + DAY - plan(P, d - 1).bed) / 60, 3, 10.5);

  /* ---------- synthetic wearable sample at absolute minute T (UTC minutes since epoch) ---------- */
  function sample(P, T) {
    const L = T + TZ, d = Math.floor(L / DAY), m = L - d * DAY, slot = Math.floor(T / STEP);
    const pd = plan(P, d), pp = plan(P, d - 1), sl = sleepH(P, d);
    const asleep = (m < pd.wake && m >= pp.bed - DAY) || m >= pd.bed;
    const sleepF = 1 + 0.07 * Math.max(0, 7 - sl); // short sleep -> higher insulin resistance
    let g = P.base + P.dawn * Math.exp(-(((m - 400) / 75) ** 2)); // dawn phenomenon
    for (const [pl, off] of [[pp, -DAY], [pd, 0]]) {
      for (const ml of pl.meals) {
        const dt = m - (ml.t + off);
        if (dt >= 0 && dt <= 300) { let A = ml.c * P.sens * sleepF * pl.stress; if (pl.walk && pl.walk.t - ml.t >= 0 && pl.walk.t - ml.t <= 90) A *= 0.72; const x = dt / P.tau; g += A * x * Math.exp(1 - x); }
      }
      if (pl.walk) { const dt = m - (pl.walk.t + off); if (dt >= 0 && dt <= 160) g -= 22 * pl.walk.inten * Math.min(1.2, P.sens) * (dt <= pl.walk.dur ? dt / pl.walk.dur : Math.exp(-(dt - pl.walk.dur) / 45)); }
    }
    g = clamp(g + 5 * Math.sin(2 * Math.PI * T / 97 + P.ph1) + 3.5 * Math.sin(2 * Math.PI * T / 233 + P.ph2) + 2.5 * n3(P.sn, 901, slot), 55, 420);
    const walking = !!(pd.walk && m >= pd.walk.t && m < pd.walk.t + pd.walk.dur);
    const steps = asleep ? 0 : Math.round(walking ? 470 * pd.walk.inten * (0.9 + 0.2 * u3(P.sn, 902, slot)) : 140 * P.activity * (m > 420 && m < 1320 ? 1 : 0.35) * u3(P.sn, 903, slot) ** 1.5);
    const hr = clamp(P.rhr + (asleep ? -9 : 0) + (walking ? 36 * pd.walk.inten : steps * 0.025) + (g > 200 ? 2 : 0) + 2.5 * n3(P.sn, 904, slot), 40, 175);
    const hrv = clamp(P.hrv * (asleep ? 1.25 : 1) * (walking ? 0.55 : 1) * (1 - 0.06 * Math.max(0, 7 - sl)) * (g > 180 ? 0.88 : 1) + 3 * n3(P.sn, 905, slot), 6, 140);
    return { g, hr, hrv, steps, asleep };
  }
  function series(P, T0, count) {
    const S = { t0: T0, g: new Float64Array(count), hr: new Float64Array(count), hrv: new Float64Array(count), st: new Float64Array(count), sl: new Uint8Array(count) };
    for (let i = 0; i < count; i++) { const s = sample(P, T0 + i * STEP); S.g[i] = s.g; S.hr[i] = s.hr; S.hrv[i] = s.hrv; S.st[i] = s.steps; S.sl[i] = s.asleep ? 1 : 0; }
    return S;
  }

  /* ---------- preprocessing / feature extraction (uses only data at or before index i) ---------- */
  const FEATS = [
    ['g0', 'Current glucose', 'mg/dL'], ['d30', 'Glucose change, last 30 min', 'mg/dL'], ['d60', 'Glucose change, last 60 min', 'mg/dL'], ['acc', 'Glucose acceleration (15 min)', 'mg/dL'],
    ['sd2h', 'Glucose variability, last 2 h (SD)', 'mg/dL'], ['mean24', '24-h mean glucose', 'mg/dL'], ['tar24', 'Time above 180 mg/dL, last 24 h', 'fraction'],
    ['hr', 'Heart rate (15-min mean)', 'bpm'], ['hrv', 'HRV RMSSD (1-h mean)', 'ms'], ['st60', 'Steps, last 60 min', 'steps'], ['st180', 'Steps, last 3 h', 'steps'], ['sleep', 'Sleep last night', 'h'],
    ['tsin', 'Time of day (24-h sin)', ''], ['tcos', 'Time of day (24-h cos)', ''], ['t2sin', 'Time of day (12-h sin)', ''], ['t2cos', 'Time of day (12-h cos)', ''],
    ['age', 'Age (EHR)', 'years'], ['bmi', 'BMI (EHR)', 'kg/m²'], ['a1c', 'HbA1c (EHR)', '%'], ['yrs', 'Years since T2D diagnosis (EHR)', 'years'], ['ins', 'On insulin (EHR)', '0/1'], ['met', 'On metformin (EHR)', '0/1'], ['sbp', 'Systolic BP (EHR)', 'mmHg'],
  ];
  const TIME_IDX = [12, 13, 14, 15];
  function features(P, S, i) {
    const g = S.g, g0 = g[i];
    let s = 0, s2 = 0; for (let k = i - 24; k <= i; k++) { s += g[k]; s2 += g[k] * g[k]; }
    const n2 = 25, sd2h = Math.sqrt(Math.max(0, s2 / n2 - (s / n2) ** 2));
    let m24 = 0, a24 = 0; for (let k = i - PER_DAY + 1; k <= i; k++) { m24 += g[k]; if (g[k] > SPIKE_LEVEL) a24++; }
    let hr = 0; for (let k = i - 2; k <= i; k++) hr += S.hr[k];
    let hrv = 0, st60 = 0, st180 = 0; for (let k = i - 11; k <= i; k++) { hrv += S.hrv[k]; st60 += S.st[k]; } for (let k = i - 35; k <= i; k++) st180 += S.st[k];
    const L = S.t0 + i * STEP + TZ, d = Math.floor(L / DAY), ang = 2 * Math.PI * (L - d * DAY) / DAY;
    return [g0, g0 - g[i - 6], g0 - g[i - 12], (g0 - g[i - 3]) - (g[i - 3] - g[i - 6]), sd2h, m24 / PER_DAY, a24 / PER_DAY, hr / 3, hrv / 12, st60, st180, sleepH(P, d),
      Math.sin(ang), Math.cos(ang), Math.sin(2 * ang), Math.cos(2 * ang), P.age, P.bmi, P.hba1c, P.years, P.insulin ? 1 : 0, P.metformin ? 1 : 0, P.sbp];
  }
  function label(S, i) { let mx = -1; for (let k = i + 1; k <= i + HSTEPS; k++) mx = Math.max(mx, S.g[k]); return { spike: mx >= SPIKE_LEVEL && mx - S.g[i] >= SPIKE_RISE ? 1 : 0, g120: S.g[i + HSTEPS], max: mx }; }

  /* ---------- small linear-algebra helpers ---------- */
  function solve(A, b) { // Gaussian elimination with partial pivoting (A: n×n array of arrays)
    const n = b.length, M = A.map((r, i) => [...r, b[i]]);
    for (let c = 0; c < n; c++) {
      let p = c; for (let r = c + 1; r < n; r++) if (Math.abs(M[r][c]) > Math.abs(M[p][c])) p = r;
      [M[c], M[p]] = [M[p], M[c]]; const v = M[c][c] || 1e-12;
      for (let r = 0; r < n; r++) { if (r === c) continue; const f = M[r][c] / v; if (f) for (let k = c; k <= n; k++) M[r][k] -= f * M[c][k]; }
    }
    return M.map((r, i) => r[n] / (r[i] || 1e-12));
  }
  const sigmoid = (z) => 1 / (1 + Math.exp(-clamp(z, -30, 30)));
  function auc(scores, ys) {
    const idx = scores.map((s, i) => [s, ys[i]]).sort((a, b) => a[0] - b[0]);
    let rank = 0, sumPos = 0, pos = 0;
    for (let i = 0; i < idx.length;) { let j = i; while (j < idx.length && idx[j][0] === idx[i][0]) j++; const r = (i + j + 1) / 2; for (let k = i; k < j; k++) if (idx[k][1]) { sumPos += r; pos++; } rank = j; i = j; }
    const neg = idx.length - pos; return pos && neg ? (sumPos - pos * (pos + 1) / 2) / (pos * neg) : null;
  }

  /* ---------- training (synthetic cohort only, deterministic) ---------- */
  let MODEL = null;
  function train() {
    if (MODEL) return MODEL;
    const t0 = Date.now(), NPAT = 260, PER = 36, TRAIN_PAT = 208, F = FEATS.length;
    const T0 = Math.floor(Date.UTC(2026, 0, 5) / 60000);
    const X = [], Y = [], YG = [], PID = [];
    for (let p = 0; p < NPAT; p++) {
      const P = profile('synthetic-cohort-v1|' + p), S = series(P, T0, PER_DAY * 5 + HSTEPS + 1);
      for (let k = 0; k < PER; k++) { const i = PER_DAY + Math.floor(u3(P.sn, 950, k) * (PER_DAY * 4 - 1)); const lb = label(S, i); X.push(features(P, S, i)); Y.push(lb.spike); YG.push(lb.g120); PID.push(p); }
      profiles.delete(P.seed);
    }
    const tr = [], te = []; PID.forEach((p, i) => (p < TRAIN_PAT ? tr : te).push(i));
    const mu = new Array(F).fill(0), sd = new Array(F).fill(0);
    for (const i of tr) for (let j = 0; j < F; j++) mu[j] += X[i][j] / tr.length;
    for (const i of tr) for (let j = 0; j < F; j++) sd[j] += (X[i][j] - mu[j]) ** 2 / tr.length;
    for (let j = 0; j < F; j++) sd[j] = Math.sqrt(sd[j]) || 1;
    const Z = X.map((x) => [1, ...x.map((v, j) => (v - mu[j]) / sd[j])]);
    // L2-regularised logistic regression fitted with Newton-Raphson (IRLS)
    const D = F + 1, LAM = 1.0; let w = new Array(D).fill(0);
    const base = tr.reduce((a, i) => a + Y[i], 0) / tr.length; w[0] = Math.log(base / (1 - base));
    for (let it = 0; it < 12; it++) {
      const H = Array.from({ length: D }, () => new Array(D).fill(0)), gr = new Array(D).fill(0);
      for (const i of tr) { const z = Z[i]; let s = 0; for (let j = 0; j < D; j++) s += w[j] * z[j]; const pr = sigmoid(s), wt = pr * (1 - pr), e = Y[i] - pr; for (let a = 0; a < D; a++) { gr[a] += e * z[a]; const za = z[a] * wt; for (let b = a; b < D; b++) H[a][b] += za * z[b]; } }
      for (let a = 0; a < D; a++) { for (let b = 0; b < a; b++) H[a][b] = H[b][a]; if (a) { H[a][a] += LAM; gr[a] -= LAM * w[a]; } }
      const step = solve(H, gr); let mx = 0; w = w.map((v, j) => { mx = Math.max(mx, Math.abs(step[j])); return v + step[j]; }); if (mx < 1e-6) break;
    }
    // ridge regression for glucose 2 h ahead
    const A = Array.from({ length: D }, () => new Array(D).fill(0)), bv = new Array(D).fill(0);
    for (const i of tr) { const z = Z[i]; for (let a = 0; a < D; a++) { bv[a] += z[a] * YG[i]; for (let b = a; b < D; b++) A[a][b] += z[a] * z[b]; } }
    for (let a = 0; a < D; a++) { for (let b = 0; b < a; b++) A[a][b] = A[b][a]; if (a) A[a][a] += 1.0; }
    const beta = solve(A, bv);
    // evaluation on held-out synthetic individuals (patient-level split, no leakage)
    const pr = te.map((i) => sigmoid(Z[i].reduce((s, z, j) => s + z * w[j], 0))), yt = te.map((i) => Y[i]);
    let tp = 0, fp = 0, fn = 0, tn = 0, brier = 0, mae = 0, rmse = 0, maeP = 0;
    te.forEach((i, k) => { const yh = pr[k] >= 0.5 ? 1 : 0; if (yh && yt[k]) tp++; else if (yh) fp++; else if (yt[k]) fn++; else tn++; brier += (pr[k] - yt[k]) ** 2; const gh = Z[i].reduce((s, z, j) => s + z * beta[j], 0); mae += Math.abs(gh - YG[i]); rmse += (gh - YG[i]) ** 2; maeP += Math.abs(X[i][0] - YG[i]); });
    const nT = te.length, prec = tp / Math.max(1, tp + fp), rec = tp / Math.max(1, tp + fn);
    MODEL = {
      w, beta, mu, sd, trained_at: new Date().toISOString(), train_ms: Date.now() - t0,
      metrics: {
        test_auc: rd(auc(pr, yt), 3), accuracy: rd((tp + tn) / nT, 3), precision: rd(prec, 3), recall: rd(rec, 3), f1: rd(2 * prec * rec / Math.max(1e-9, prec + rec), 3), brier: rd(brier / nT, 3),
        spike_rate_train: rd(base, 3), spike_rate_test: rd(yt.reduce((a, b) => a + b, 0) / nT, 3), glucose_2h_mae: rd(mae / nT, 1), glucose_2h_rmse: rd(Math.sqrt(rmse / nT), 1), persistence_baseline_mae: rd(maeP / nT, 1),
        confusion_at_0_5: { tp, fp, fn, tn }, n_train_windows: tr.length, n_test_windows: nT, n_train_individuals: TRAIN_PAT, n_test_individuals: NPAT - TRAIN_PAT,
      },
    };
    return MODEL;
  }
  function predict(P, S, i) {
    const M = train(), x = features(P, S, i), z = [1, ...x.map((v, j) => (v - M.mu[j]) / M.sd[j])];
    const logit = z.reduce((s, v, j) => s + v * M.w[j], 0), prob = sigmoid(logit);
    const g120 = clamp(z.reduce((s, v, j) => s + v * M.beta[j], 0), 55, 420);
    let contrib = FEATS.map((f, j) => ({ key: f[0], label: f[1], unit: f[2], value: x[j], impact: M.w[j + 1] * z[j + 1] }));
    const tod = TIME_IDX.reduce((s, j) => s + contrib[j].impact, 0);
    contrib = contrib.filter((_, j) => !TIME_IDX.includes(j));
    contrib.push({ key: 'tod', label: 'Time of day (usual meal-timing pattern)', unit: '', value: null, impact: tod });
    contrib.sort((a, b) => Math.abs(b.impact) - Math.abs(a.impact));
    const fmt = (c) => ({ ...c, value: c.value === null ? null : rd(c.value, c.key === 'tar24' ? 2 : 1), impact: rd(c.impact, 3), direction: c.impact >= 0 ? 'raises risk' : 'lowers risk' });
    return { probability: rd(prob, 3), risk: prob >= 0.6 ? 'High' : prob >= 0.3 ? 'Moderate' : 'Low', predicted_glucose_2h: Math.round(g120), logit: rd(logit, 3), top_factors: contrib.slice(0, 6).map(fmt), features: Object.fromEntries(FEATS.map((f, j) => [f[0], rd(x[j], 3)])) };
  }

  /* ---------- per-patient digital twin ---------- */
  const isoOf = (T) => new Date(T * 60000).toISOString();
  const localDate = (d) => new Date(d * DAY * 60000).toISOString().slice(0, 10);
  const avg = (a) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : null);
  function twinState(key, nowMs = Date.now()) { // lightweight (list view)
    const P = profile('patient-twin|' + key), T = Math.floor(nowMs / 60000 / STEP) * STEP;
    const S = series(P, T - PER_DAY * STEP, PER_DAY + 1), pr = predict(P, S, PER_DAY);
    return { twin_id: twinId(P), current_glucose: Math.round(S.g[PER_DAY]), probability: pr.probability, risk: pr.risk, predicted_glucose_2h: pr.predicted_glucose_2h };
  }
  const twinId = (P) => 'DT-' + P.sn.toString(36).toUpperCase().padStart(7, '0');
  function patientTwin(key, nowMs = Date.now()) {
    const M = train(), P = profile('patient-twin|' + key);
    const T = Math.floor(nowMs / 60000 / STEP) * STEP, span = 7 * PER_DAY + HSTEPS, T0 = T - span * STEP, S = series(P, T0, span + 1), i = span;
    const pred = predict(P, S, i), back = predict(P, S, i - HSTEPS);
    let mx = -1; for (let k = i - HSTEPS + 1; k <= i; k++) mx = Math.max(mx, S.g[k]);
    const cur = [], prior = []; for (let k = i - PER_DAY + 1; k <= i; k++) cur.push(k); for (let k = i - 7 * PER_DAY + 1; k <= i - PER_DAY; k++) if (k >= 0) prior.push(k);
    const pick = (arr, ks) => ks.map((k) => arr[k]);
    const tir = (ks) => ks.filter((k) => S.g[k] >= 70 && S.g[k] <= 180).length / ks.length, tar = (ks) => ks.filter((k) => S.g[k] > 180).length / ks.length;
    const cv = (ks) => { const v = pick(S.g, ks), m = avg(v); return 100 * Math.sqrt(avg(v.map((x) => (x - m) ** 2))) / m; };
    const restHr = (ks) => avg(ks.filter((k) => S.sl[k]).map((k) => S.hr[k])) ?? avg(pick(S.hr, ks));
    const Lnow = T + TZ, dNow = Math.floor(Lnow / DAY);
    const nights = []; for (let d = dNow - 6; d <= dNow; d++) nights.push({ date: localDate(d), hours: rd(sleepH(P, d)) });
    const mean24 = avg(pick(S.g, cur)), gmi = 3.31 + 0.02392 * mean24;
    const trend = 0.25 * n3(P.sn, 960, 0);
    const labs = [12, 9, 6, 3].map((mo, k) => ({ date: new Date(nowMs - mo * 30.4 * 864e5).toISOString().slice(0, 10), hba1c: rd(clamp(P.hba1c - trend * mo / 12 + 0.12 * n3(P.sn, 961, k), 5.5, 13)), fasting_glucose: Math.round(clamp(P.fpg - trend * 24 * mo / 12 + 6 * n3(P.sn, 962, k), 80, 300)) }));
    const morning = cur.filter((k) => { const L = T0 + k * STEP + TZ, m = L - Math.floor(L / DAY) * DAY; return m >= 330 && m <= 390; });
    const pts = []; for (let k = i - PER_DAY + 3; k <= i; k += 3) pts.push(k);
    const out = {
      synthetic: true, version: VERSION, twin_id: twinId(P),
      disclaimer: 'Prototype digital twin. All EHR values and wearable data shown are SYNTHETIC (simulated) and are not measurements of this patient. Predictions are not a medical diagnosis.',
      generated_at: isoOf(T), prediction_for: isoOf(T + HORIZON), horizon_minutes: HORIZON, spike_definition: `glucose reaching ≥ ${SPIKE_LEVEL} mg/dL and rising ≥ ${SPIKE_RISE} mg/dL above the current value within the next ${HORIZON} minutes`,
      ehr: {
        age: P.age, sex: P.sex, bmi: P.bmi, hba1c: P.hba1c, years_since_diagnosis: P.years, blood_pressure: `${P.sbp}/${P.dbp}`, fasting_glucose: P.fpg, ldl: P.ldl, hdl: P.hdl, triglycerides: P.tg, egfr: P.egfr,
        diagnoses: ['Type 2 diabetes mellitus (E11)', P.htn && 'Essential hypertension (I10)', P.dysl && 'Dyslipidaemia (E78.5)', P.obese && 'Obesity (E66)', P.ckd && 'Chronic kidney disease, stage 3 (N18.3)'].filter(Boolean),
        medications: [P.metformin && 'Metformin', P.insulin && 'Insulin glargine (basal)', P.dysl && 'Atorvastatin', P.htn && 'Telmisartan'].filter(Boolean),
        lab_history: labs,
      },
      state: {
        glucose: Math.round(S.g[i]), change_30min: Math.round(S.g[i] - S.g[i - 6]), heart_rate: Math.round(S.hr[i]), hrv_rmssd: Math.round(avg(pick(S.hrv, [i - 11, i - 10, i - 9, i - 8, i - 7, i - 6, i - 5, i - 4, i - 3, i - 2, i - 1, i]))),
        steps_last_hour: Math.round(pred.features.st60), sleep_last_night: nights[nights.length - 1].hours, mean_24h: Math.round(mean24), time_in_range_24h: rd(tir(cur) * 100, 0), gmi: rd(gmi),
      },
      prediction: pred,
      backtest: { made_at: isoOf(T - HORIZON), predicted_glucose: back.predicted_glucose_2h, probability: back.probability, risk: back.risk, actual_glucose: Math.round(S.g[i]), actual_spike: mx >= SPIKE_LEVEL && mx - S.g[i - HSTEPS] >= SPIKE_RISE },
      series: {
        resolution_minutes: 15, t: pts.map((k) => isoOf(T0 + k * STEP)), glucose: pts.map((k) => Math.round(S.g[k])), heart_rate: pts.map((k) => Math.round((S.hr[k] + S.hr[k - 1] + S.hr[k - 2]) / 3)),
        hrv: pts.map((k) => Math.round((S.hrv[k] + S.hrv[k - 1] + S.hrv[k - 2]) / 3)), steps: pts.map((k) => Math.round(S.st[k] + S.st[k - 1] + S.st[k - 2])),
      },
      sleep: nights,
      comparison: [
        { metric: 'Mean glucose', unit: 'mg/dL', historical: Math.round(avg(pick(S.g, prior))), current: Math.round(mean24), basis: 'previous 6 days vs last 24 h' },
        { metric: 'Time in range 70–180', unit: '%', historical: rd(tir(prior) * 100, 0), current: rd(tir(cur) * 100, 0), basis: 'previous 6 days vs last 24 h' },
        { metric: 'Time above 180', unit: '%', historical: rd(tar(prior) * 100, 0), current: rd(tar(cur) * 100, 0), basis: 'previous 6 days vs last 24 h' },
        { metric: 'Glucose variability (CV)', unit: '%', historical: rd(cv(prior), 0), current: rd(cv(cur), 0), basis: 'previous 6 days vs last 24 h' },
        { metric: 'Resting heart rate', unit: 'bpm', historical: Math.round(restHr(prior)), current: Math.round(restHr(cur)), basis: 'during sleep' },
        { metric: 'HRV (RMSSD)', unit: 'ms', historical: Math.round(avg(pick(S.hrv, prior))), current: Math.round(avg(pick(S.hrv, cur))), basis: 'previous 6 days vs last 24 h' },
        { metric: 'Steps per day', unit: 'steps', historical: Math.round(prior.reduce((a, k) => a + S.st[k], 0) / Math.max(1, prior.length / PER_DAY)), current: Math.round(cur.reduce((a, k) => a + S.st[k], 0)), basis: 'previous 6 days vs last 24 h' },
        { metric: 'Sleep', unit: 'h', historical: rd(avg(nights.slice(0, -1).map((x) => x.hours))), current: nights[nights.length - 1].hours, basis: 'previous 6 nights vs last night' },
        { metric: 'HbA1c (EHR lab) vs GMI (from CGM)', unit: '%', historical: labs[labs.length - 1].hba1c, current: rd(gmi), basis: 'lab 3 months ago vs CGM-estimated' },
        { metric: 'Fasting glucose (EHR lab) vs early-morning CGM', unit: 'mg/dL', historical: labs[labs.length - 1].fasting_glucose, current: morning.length ? Math.round(avg(pick(S.g, morning))) : null, basis: 'lab vs 05:30–06:30 today' },
      ],
      model: { version: VERSION, test_auc: M.metrics.test_auc, brier: M.metrics.brier, glucose_2h_mae: M.metrics.glucose_2h_mae },
    };
    return out;
  }

  function modelCard() {
    const M = train();
    const weights = FEATS.map((f, j) => ({ feature: f[1], weight: rd(M.w[j + 1], 3) })).sort((a, b) => Math.abs(b.weight) - Math.abs(a.weight));
    return {
      version: VERSION, trained_at: M.trained_at, train_ms: M.train_ms, status: 'Prototype for research/demonstration — not clinically validated, not a medical device.',
      dataset: {
        name: 'Q-NIRVANA synthetic T2D cohort v1', kind: 'synthetic (simulated) — contains no real persons and is not derived from real patient records',
        generator: 'Seeded physiological simulator: carbohydrate meal-response kernels scaled by insulin resistance (HbA1c, BMI, medication), dawn phenomenon, post-meal walking effect, sleep-duration effect on insulin sensitivity, daily stress variation, sensor noise; heart rate, HRV, steps and sleep derived from the same daily routine.',
        individuals: 260, days_per_individual: 5, sampling: '5-minute CGM-style samples', windows_per_individual: 36, timezone_offset_minutes: TZ,
      },
      ehr_features: ['Age', 'Sex', 'BMI', 'HbA1c', 'Years since diagnosis', 'Previous diagnoses (ICD-10)', 'Medications (insulin, metformin)', 'Blood pressure', 'Lipids', 'eGFR', 'Fasting glucose / lab history'],
      wearable_features: ['Continuous glucose', 'Heart rate', 'Heart-rate variability (RMSSD)', 'Steps', 'Sleep duration', 'Activity (walking)'],
      model_features: FEATS.map((f) => ({ key: f[0], label: f[1], unit: f[2] })),
      preprocessing: [
        'Resample all streams to a common 5-minute grid; aggregate to 15-minute values for display.',
        'Look-back window of 24 h, prediction horizon of 2 h; features use only data at or before the prediction time.',
        'Feature extraction: current glucose, 30/60-min deltas, 15-min acceleration, 2-h SD, 24-h mean and time-above-range, heart-rate and HRV means, step sums, last-night sleep, time-of-day harmonics, static EHR values.',
        'Z-score standardisation fitted on the training split only, then applied to test and live data.',
        'Patient-level train/test split (80/20 of synthetic individuals) to avoid leakage between windows of the same individual.',
      ],
      label: `Spike = glucose reaching ≥ ${SPIKE_LEVEL} mg/dL and rising ≥ ${SPIKE_RISE} mg/dL above the current value within the next ${HORIZON} minutes.`,
      model: { classifier: 'L2-regularised logistic regression (λ = 1.0), fitted with Newton–Raphson / IRLS', regressor: 'Ridge regression (λ = 1.0) for glucose 2 h ahead', risk_bands: 'Low < 30 % ≤ Moderate < 60 % ≤ High', explanation: 'Per-feature contribution = coefficient × standardised value (log-odds), time-of-day harmonics combined' },
      training: 'The model is retrained deterministically from the synthetic cohort when the server starts (first request on serverless platforms). No real patient data is used for training.',
      prediction: 'For each registered patient, a synthetic twin stream (seeded by the patient id) is generated up to the current time; features are extracted at "now" and the model outputs spike probability, risk band and predicted glucose 2 h ahead. A back-test compares the prediction made 2 h ago with the simulated value now.',
      metrics: M.metrics, top_weights: weights.slice(0, 10),
      limitations: ['Metrics are computed on simulated data from the same generator and do not demonstrate clinical accuracy.', 'Meals are not observed directly; the model infers meal risk from time of day and glucose dynamics.', 'Validation on real, consented, de-identified CGM datasets would be required before any clinical use.'],
    };
  }
  function latestSample(key, nowMs = Date.now()) { // newest simulated wearable sample + updated prediction (synthetic)
    const P = profile('patient-twin|' + key), T = Math.floor(nowMs / 60000 / STEP) * STEP;
    const S = series(P, T - PER_DAY * STEP, PER_DAY + 1), i = PER_DAY, pr = predict(P, S, i);
    return { synthetic: true, twin_id: twinId(P), t: isoOf(T), glucose: Math.round(S.g[i]), heart_rate: Math.round(S.hr[i]), hrv: Math.round(S.hrv[i]), steps_5min: Math.round(S.st[i]), asleep: !!S.sl[i],
      probability: pr.probability, risk: pr.risk, predicted_glucose_2h: pr.predicted_glucose_2h, prediction_for: isoOf(T + HORIZON) };
  }
  return { VERSION, train, modelCard, patientTwin, twinState, latestSample };
})();

/* ================= Q-NIRVANA API (patients · hospitals · appointments · queue · emergency · ambulances · resources · notifications · settings · digital twin) ================= */

const bad = (m) => { throw new HttpError(400, m); };
const forbid = (m = 'You are not allowed to perform this action') => { throw new HttpError(403, m); };

const TB = {};
const from = (t) => db.from(TB[t] || t);

/* ---------------- security helpers ---------------- */
async function firebaseClaims(req) {
  const tok = String(req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  const c = tok ? await verifyFirebaseToken(tok) : null;
  if (!c) throw new HttpError(401, 'Please log in to continue');
  return c;
}
const joinCode = () => Array.from(crypto.randomBytes(6), (b) => 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'[b % 32]).join('');

/* ---------------- validation helpers ---------------- */
const str = (v, max = 255) => (v == null ? '' : String(v)).trim().slice(0, max);
function need(v, name, min = 1, max = 255) {
  const s = (v == null ? '' : String(v)).trim();
  if (s.length < min) bad(`${name} is required${min > 1 ? ` (min ${min} characters)` : ''}`);
  if (s.length > max) bad(`${name} is too long (max ${max})`);
  return s;
}
const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
const isDate = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s || '') && !isNaN(Date.parse(s));
const isTime = (s) => /^([01]\d|2[0-3]):[0-5]\d$/.test(s || '');
const toInt = (v) => { const n = parseInt(v, 10); return Number.isFinite(n) ? n : null; };
const clampInt = (v, lo, hi, d) => { const n = toInt(v); return n == null ? d : Math.min(hi, Math.max(lo, n)); };
const numOrNull = (v) => (v === '' || v == null ? null : Number.isFinite(Number(v)) ? Number(v) : NaN);
const today = () => new Date().toISOString().slice(0, 10);
const addDays = (d, n) => { const x = new Date(d + 'T00:00:00Z'); x.setUTCDate(x.getUTCDate() + n); return x.toISOString().slice(0, 10); };
const pickDate = (d) => (isDate(d) ? d : today());
const nowIso = () => new Date().toISOString();
const ids = (a) => { const u = [...new Set(a.filter((x) => x != null))]; return u.length ? u : [-1]; };
const mapBy = (rows, k) => Object.fromEntries((rows || []).map((r) => [r[k], r]));
const nameKey = (s) => String(s || '').trim().replace(/\s+/g, ' ').toLowerCase();

const ROLES = { patient: 'patients', hospital: 'hospital_users', operator: 'ambulance_operators' };
const ROLE_LABEL = { patient: 'Patient', hospital: 'Hospital', operator: 'Ambulance Operator' };
const DESIGNATIONS = ['Doctor', 'Hospital Administrator', 'Receptionist', 'Nurse', 'Emergency Staff', 'Other Authorized Staff'];
const PERMS = {
  'Doctor': ['consult', 'emergency_view', 'emergency_clinical'],
  'Hospital Administrator': ['dashboard', 'appointments', 'queues', 'resources', 'ambulances', 'emergency_view', 'emergency_manage', 'emergency_clinical', 'reports', 'settings', 'people'],
  'Receptionist': ['dashboard', 'appointments', 'queues', 'emergency_view'],
  'Nurse': ['dashboard', 'queues', 'resources', 'emergency_view', 'emergency_clinical'],
  'Emergency Staff': ['dashboard', 'resources', 'ambulances', 'emergency_view', 'emergency_manage', 'emergency_clinical'],
  'Other Authorized Staff': ['dashboard', 'appointments', 'queues', 'emergency_view'],
};
const ACTIVE = ['called', 'in_consultation'];
const RES_TYPES = ['general_bed', 'icu_bed', 'emergency_bed', 'private_room', 'ventilator', 'operation_theatre'];
const RES_LABEL = { general_bed: 'General Bed', icu_bed: 'ICU Bed', emergency_bed: 'Emergency Bed', private_room: 'Private Room', ventilator: 'Ventilator', operation_theatre: 'Operation Theatre' };
const SLOT_MINUTES = 15; // one appointment per slot
const toMin = (t) => +t.slice(0, 2) * 60 + +t.slice(3, 5);
const fromMin = (m) => `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;
const E_OPEN = ['requested', 'accepted', 'en_route', 'transporting', 'arrived', 'triage', 'admitted', 'treatment'];

/* ---------------- db helpers ---------------- */
async function q(p) { const { data, error } = await p; if (error) throw new HttpError(500, error.message); return data; }
async function one(p) { const d = await q(p.limit(1)); return d[0] || null; }
async function insertOne(table, row) { return (await q(from(table).insert(row).select('*')))[0]; }
async function updateRows(table, fields, col, val) { return q(from(table).update(fields).eq(col, val).select('*')); }
const pubUser = (u, verified) => ({ id: u.id, role: u.role, full_name: u.full_name, email: u.email, phone: u.phone, created_at: u.created_at, email_verified: verified ?? !!u.email_verified });
const pubHospital = (h, full) => { if (!h) return null; const { join_code, name_key, ...rest } = h; return full ? { ...rest, join_code } : rest; };
const getHospital = (id) => (id ? one(from('hospitals').select('*').eq('id', id)) : null);

const auth = (req, roles, ...perm) => authCore(req, false, roles, perm);
async function authCore(req, allowUnverified, roles, perm = []) {
  const claims = await firebaseClaims(req);
  const user = await one(from('users').select('*').eq('firebase_uid', claims.uid));
  if (!user || !ROLES[user.role]) throw new HttpError(404, 'No Q-NIRVANA profile for this account yet', 'no_profile');
  user.email_verified = claims.email_verified;
  if (process.env.REQUIRE_EMAIL_VERIFICATION !== '0' && !claims.email_verified && !allowUnverified) throw new HttpError(403, 'Please verify your email address to continue', 'email_unverified');
  if (roles && !roles.includes(user.role)) forbid();
  const profile = await one(from(ROLES[user.role]).select('*').eq('user_id', user.id));
  if (!profile) forbid(user.role === 'hospital' ? 'Your hospital access has been removed' : 'Profile not found');
  const ctx = { user, profile, perms: [], hid: null, doctor: null, hospital: null };
  if (user.role === 'hospital') {
    ctx.hid = profile.hospital_id;
    ctx.hospital = await getHospital(ctx.hid);
    if (!ctx.hospital) forbid('Your hospital is not registered');
    ctx.perms = PERMS[profile.designation] || PERMS['Other Authorized Staff'];
    if (profile.designation === 'Doctor') ctx.doctor = await one(from('doctors').select('*').eq('user_id', user.id));
  }
  if (perm.length) requirePerm(ctx, ...perm);
  return ctx;
}
const can = (ctx, ...p) => p.some((x) => ctx.perms.includes(x));
function requirePerm(ctx, ...p) { if (!can(ctx, ...p)) forbid('Your designation does not have access to this feature'); }
async function notify(userIds, title, message, type = 'info') {
  const u = [...new Set(userIds.filter(Boolean))];
  if (!u.length) return;
  await q(from('notifications').insert(u.map((user_id) => ({ user_id, title, message, type, is_read: false }))));
  rt.notify(u, { title, message, type });
}
async function hospitalUserIds(hid, ...perm) {
  const rows = await q(from('hospital_users').select('user_id,designation').eq('hospital_id', hid ?? -1));
  return rows.filter((r) => !perm.length || perm.some((p) => (PERMS[r.designation] || []).includes(p))).map((r) => r.user_id);
}

/* ---------------- hospitals & departments ---------------- */
async function findOrCreateDepartment(hid, name) {
  const all = await q(from('departments').select('*').eq('hospital_id', hid));
  return all.find((d) => nameKey(d.name) === nameKey(name)) || insertOne('departments', { hospital_id: hid, name: name.replace(/\s+/g, ' ') });
}
async function listHospitals(query) {
  let qb = from('hospitals').select('*').order('name').limit(200);
  const s = str(query.q, 80);
  if (s) qb = qb.ilike('name', `%${s.replace(/[%_,()]/g, '')}%`);
  const hs = await q(qb);
  const [deps, docs] = await Promise.all([
    q(from('departments').select('id,hospital_id').in('hospital_id', ids(hs.map((h) => h.id)))),
    q(from('doctors').select('id,hospital_id,is_available,active').in('hospital_id', ids(hs.map((h) => h.id)))),
  ]);
  return hs.map((h) => {
    const d = docs.filter((x) => x.hospital_id === h.id && x.active !== false);
    return { ...pubHospital(h), departments_count: deps.filter((x) => x.hospital_id === h.id).length, doctors_count: d.length, available_doctors: d.filter((x) => x.is_available).length };
  });
}
async function listDepartments(req, query) {
  let hid = toInt(query.hospital_id);
  if (!hid && query.hospital_name) { const h = await one(from('hospitals').select('id').eq('name_key', nameKey(query.hospital_name))); hid = h?.id; }
  if (!hid && req.headers.authorization) { try { const c = await auth(req, ['hospital']); hid = c.hid; } catch {} }
  if (!hid) return [];
  const [deps, docs] = await Promise.all([q(from('departments').select('*').eq('hospital_id', hid).order('name')), q(from('doctors').select('department_id,is_available,active').eq('hospital_id', hid))]);
  return deps.map((d) => { const ds = docs.filter((x) => x.department_id === d.id && x.active !== false); return { ...d, doctors: ds.length, available: ds.filter((x) => x.is_available).length }; });
}
async function departmentsWrite(req, method, b) {
  const ctx = await auth(req, ['hospital'], 'people');
  if (method === 'POST') {
    const name = need(b.name, 'Department name', 2, 80);
    const all = await q(from('departments').select('*').eq('hospital_id', ctx.hid));
    if (all.some((d) => nameKey(d.name) === nameKey(name))) bad('Department already exists');
    return insertOne('departments', { hospital_id: ctx.hid, name: name.replace(/\s+/g, ' ') });
  }
  const d = await one(from('departments').select('*').eq('id', toInt(b.id) || -1).eq('hospital_id', ctx.hid));
  if (!d) throw new HttpError(404, 'Department not found');
  const [st, dc] = await Promise.all([q(from('hospital_users').select('id').eq('department_id', d.id)), q(from('doctors').select('id').eq('department_id', d.id).neq('active', false))]);
  if (st.length || dc.length) bad('Reassign the doctors/staff in this department before deleting it');
  await q(from('departments').delete().eq('id', d.id));
  return { ok: true };
}

/* ---------------- auth routes ---------------- */
async function register(req, b) {
  const claims = await firebaseClaims(req);
  if (await one(from('users').select('id').eq('firebase_uid', claims.uid))) bad('Your Q-NIRVANA profile already exists — please log in');
  const role = str(b.role, 20);
  if (!ROLES[role]) bad('Please choose a valid role');
  const full_name = need(b.full_name, 'Full name', 2, 120);
  const email = claims.email; // from the verified Firebase token, never from the request body
  if (!EMAIL.test(email)) bad('Your Firebase account has no valid email address');
  const phone = need(b.phone, 'Phone', 7, 20);
  if (!/^\+?[\d\s-]{7,20}$/.test(phone)) bad('Please enter a valid phone number');
  const code = process.env.HOSPITAL_ACCESS_CODE;
  if (role !== 'patient' && code && str(b.access_code, 100) !== code) forbid('Invalid platform access code');
  if (await one(from('users').select('id').eq('email', email))) bad('A profile with this email already exists');

  const p = b.profile || {};
  let orphaned = false;
  let hospital = null, newHospital = false, designation = null, deptName = null, vehicle = null, opHospital = null;
  if (role === 'hospital') {
    const hname = need(p.hospital_name, 'Hospital name', 2, 120);
    designation = str(p.designation, 40);
    if (!DESIGNATIONS.includes(designation)) bad('Please choose a valid designation');
    deptName = need(p.department, 'Department', 2, 80);
    hospital = await one(from('hospitals').select('*').eq('name_key', nameKey(hname)));
    if (hospital) {
      const [mem, ap, em] = await Promise.all([
        q(from('hospital_users').select('id').eq('hospital_id', hospital.id).limit(1)),
        q(from('appointments').select('id').eq('hospital_id', hospital.id).limit(1)),
        q(from('emergency_cases').select('id').eq('hospital_id', hospital.id).limit(1)),
      ]);
      orphaned = !mem.length && !ap.length && !em.length; // no members and no patient data → can be reclaimed
    }
    if (orphaned) {
      if (designation !== 'Hospital Administrator') bad(`"${hospital.name}" has no members yet. The first account must be a Hospital Administrator.`);
    } else if (hospital) {
      if (str(p.join_code, 20).toUpperCase() !== hospital.join_code) forbid(`Invalid join code for ${hospital.name}. Ask your Hospital Administrator for the hospital join code.`);
    } else {
      if (designation !== 'Hospital Administrator') bad(`"${hname}" is not registered yet. The first account of a new hospital must be a Hospital Administrator.`);
      newHospital = true;
    }
  } else if (role === 'operator') {
    need(p.license_no, 'Driving licence number', 4, 40);
    const v = str(p.vehicle_no, 20).toUpperCase();
    if (toInt(p.hospital_id)) { opHospital = await getHospital(toInt(p.hospital_id)); if (!opHospital) bad('Selected hospital not found'); }
    if (v) {
      if (!/^[A-Z0-9 -]{4,20}$/.test(v)) bad('Invalid vehicle number');
      if (await one(from('ambulances').select('id').eq('vehicle_no', v))) bad('This ambulance vehicle is already registered');
      vehicle = { vehicle_no: v, ambulance_type: ['BLS', 'ALS', 'ICU'].includes(p.ambulance_type) ? p.ambulance_type : 'BLS', hospital_id: opHospital?.id || null };
    }
  }
  const user = await insertOne('users', { firebase_uid: claims.uid, role, full_name, email, phone });
  try {
    if (role === 'patient') {
      await insertOne('patients', { user_id: user.id, dob: isDate(p.dob) ? p.dob : null, gender: str(p.gender, 20) || null, blood_group: str(p.blood_group, 5) || null, address: str(p.address) || null, emergency_contact: str(p.emergency_contact, 20) || null });
    } else if (role === 'hospital') {
      if (newHospital) {
        const lat = numOrNull(p.hospital_lat), lng = numOrNull(p.hospital_lng);
        hospital = await insertOne('hospitals', { name: need(p.hospital_name, 'Hospital name', 2, 120).replace(/\s+/g, ' '), name_key: nameKey(p.hospital_name), address: str(p.hospital_address) || null, phone, lat: Number.isFinite(lat) ? lat : null, lng: Number.isFinite(lng) ? lng : null, join_code: joinCode(), created_by: user.id });
      }
      if (orphaned) { hospital = (await updateRows('hospitals', { created_by: user.id, join_code: joinCode(), phone: hospital.phone || phone }, 'id', hospital.id))[0]; newHospital = true; }
      const dept = await findOrCreateDepartment(hospital.id, deptName);
      await insertOne('hospital_users', { user_id: user.id, hospital_id: hospital.id, designation, department_id: dept.id });
      await syncUserProfile(user.id);
      if (designation === 'Doctor') {
        const { count } = await from('doctors').select('id', { count: 'exact', head: true }).eq('hospital_id', hospital.id);
        const n = count || 0;
        await insertOne('doctors', { user_id: user.id, hospital_id: hospital.id, department_id: dept.id, specialization: str(p.specialization, 120) || dept.name, qualification: str(p.qualification, 120) || null, room: str(p.room, 30) || null, token_prefix: String.fromCharCode(65 + (n % 26)) + (n >= 26 ? String(Math.floor(n / 26)) : ''), avg_consult_minutes: clampInt(p.avg_consult_minutes, 3, 60, 10), is_available: true, active: true, avail_start: isTime(p.avail_start) ? p.avail_start : '09:00', avail_end: isTime(p.avail_end) ? p.avail_end : '17:00' });
      }
    } else {
      const op = await insertOne('ambulance_operators', { user_id: user.id, license_no: str(p.license_no, 40) });
      if (vehicle) {
        const amb = await insertOne('ambulances', { ...vehicle, status: 'offline', operator_id: op.id });
        await q(from('ambulance_operators').update({ ambulance_id: amb.id }).eq('id', op.id));
      }
    }
  } catch (e) {
    await from('hospital_users').delete().eq('user_id', user.id); await from('doctors').delete().eq('user_id', user.id);
    await from('users').delete().eq('id', user.id); throw e;
  }
  await notify([user.id], 'Welcome to Q-NIRVANA', role === 'hospital' ? (newHospital ? `${hospital.name} is now registered. Share the hospital join code ${hospital.join_code} with your team (Settings).` : `You joined ${hospital.name} as ${designation}.`) : `Your ${ROLE_LABEL[role]} account is ready.`, 'success');
  if (role === 'hospital' && !newHospital) await notify((await hospitalUserIds(hospital.id, 'people')).filter((x) => x !== user.id), 'New team member', `${full_name} joined as ${designation} · ${deptName}.`, 'info');
  return { user: pubUser(user, claims.email_verified) };
}
async function me(req) {
  const ctx = await authCore(req, true);
  let ambulance = null, department = null, doctor = null;
  if (ctx.user.role === 'operator') ambulance = await one(from('ambulances').select('*').eq('operator_id', ctx.profile.id));
  if (ambulance?.hospital_id) ambulance.hospital_name = (await getHospital(ambulance.hospital_id))?.name || null;
  if (ctx.user.role === 'hospital') {
    department = await one(from('departments').select('*').eq('id', ctx.profile.department_id ?? -1));
    if (ctx.doctor) doctor = await doctorCard(ctx.doctor.id);
  }
  return { user: pubUser(ctx.user, ctx.user.email_verified), profile: ctx.profile, perms: ctx.perms, hospital: pubHospital(ctx.hospital, can(ctx, 'settings', 'people')), department, doctor, ambulance };
}

/* ---------------- doctors ---------------- */
async function doctorCard(id) {
  const d = await one(from('doctors').select('*').eq('id', id));
  if (!d) throw new HttpError(404, 'Doctor not found');
  const [u, dep, h] = await Promise.all([one(from('users').select('full_name,phone').eq('id', d.user_id)), one(from('departments').select('name').eq('id', d.department_id ?? -1)), getHospital(d.hospital_id)]);
  return { ...d, full_name: u?.full_name || '', department: dep?.name || '', hospital_name: h?.name || '' };
}
async function listDoctors({ hospital_id, department_id, date }) {
  let qb = from('doctors').select('*').eq('hospital_id', hospital_id).neq('active', false).order('id');
  if (department_id) qb = qb.eq('department_id', department_id);
  const docs = await q(qb);
  const [users, deps, toks, h] = await Promise.all([
    q(from('users').select('id,full_name,phone').in('id', ids(docs.map((d) => d.user_id)))).then((r) => mapBy(r, 'id')),
    q(from('departments').select('id,name').eq('hospital_id', hospital_id)).then((r) => mapBy(r, 'id')),
    q(from('queue_tokens').select('doctor_id,status,token_code,token_number').eq('token_date', pickDate(date)).in('doctor_id', ids(docs.map((d) => d.id)))),
    getHospital(hospital_id),
  ]);
  return docs.map((d) => {
    const t = toks.filter((x) => x.doctor_id === d.id).sort((a, b) => a.token_number - b.token_number);
    const waiting = t.filter((x) => x.status === 'waiting').length;
    const active = t.filter((x) => ACTIVE.includes(x.status));
    return { ...d, full_name: users[d.user_id]?.full_name || '', department: deps[d.department_id]?.name || '', hospital_name: h?.name || '', waiting, served: t.filter((x) => x.status === 'completed').length, total_today: t.length, current_token: active.length ? active[active.length - 1].token_code : null, est_wait: (waiting + active.length) * d.avg_consult_minutes };
  }).sort((a, b) => a.department.localeCompare(b.department) || a.id - b.id);
}
async function getDoctors(req, query) {
  const ctx = await auth(req, ['patient', 'hospital']);
  const hid = ctx.user.role === 'hospital' ? ctx.hid : toInt(query.hospital_id);
  if (!hid) bad('Please select a hospital');
  return listDoctors({ hospital_id: hid, department_id: toInt(query.department_id), date: query.date });
}
async function updateDoctor(req, b) {
  const ctx = await auth(req, ['hospital'], 'consult');
  if (!ctx.doctor) forbid('Doctor profile not found');
  const f = {};
  if (b.is_available !== undefined) f.is_available = !!b.is_available;
  if (b.avail_start !== undefined) { if (!isTime(b.avail_start)) bad('Invalid start time'); f.avail_start = b.avail_start; }
  if (b.avail_end !== undefined) { if (!isTime(b.avail_end)) bad('Invalid end time'); f.avail_end = b.avail_end; }
  if (b.room !== undefined) f.room = str(b.room, 30) || null;
  if (b.avg_consult_minutes !== undefined) f.avg_consult_minutes = clampInt(b.avg_consult_minutes, 3, 60, ctx.doctor.avg_consult_minutes);
  if (!Object.keys(f).length) bad('Nothing to update');
  await updateRows('doctors', f, 'id', ctx.doctor.id);
  return doctorCard(ctx.doctor.id);
}

/* ---------------- staff (people) ---------------- */
async function staff(req, method, b) {
  const ctx = await auth(req, ['hospital'], 'people');
  if (method === 'GET') {
    const rows = await q(from('hospital_users').select('*').eq('hospital_id', ctx.hid).order('id'));
    const [users, deps, docs] = await Promise.all([
      q(from('users').select('id,full_name,email,phone,created_at').in('id', ids(rows.map((r) => r.user_id)))).then((r) => mapBy(r, 'id')),
      q(from('departments').select('id,name').eq('hospital_id', ctx.hid)).then((r) => mapBy(r, 'id')),
      q(from('doctors').select('*').eq('hospital_id', ctx.hid)).then((r) => mapBy(r, 'user_id')),
    ]);
    return rows.map((r) => ({ ...r, ...(users[r.user_id] || {}), id: r.id, department: deps[r.department_id]?.name || '', doctor: docs[r.user_id] || null, is_me: r.user_id === ctx.user.id }));
  }
  const row = await one(from('hospital_users').select('*').eq('user_id', toInt(b.user_id) || -1).eq('hospital_id', ctx.hid));
  if (!row) throw new HttpError(404, 'Team member not found');
  if (method === 'PUT') {
    const dep = await one(from('departments').select('*').eq('id', toInt(b.department_id) || -1).eq('hospital_id', ctx.hid));
    if (!dep) bad('Select a department of your hospital');
    await updateRows('hospital_users', { department_id: dep.id }, 'id', row.id);
    await q(from('doctors').update({ department_id: dep.id }).eq('user_id', row.user_id));
    await syncUserProfile(row.user_id);
    return { ok: true };
  }
  if (row.user_id === ctx.user.id) bad('You cannot remove yourself');
  await q(from('hospital_users').delete().eq('id', row.id));
  await syncUserProfile(row.user_id);
  await q(from('doctors').update({ active: false, is_available: false }).eq('user_id', row.user_id));
  await notify([row.user_id], 'Hospital access removed', `Your access to ${ctx.hospital.name} was removed by an administrator.`, 'warning');
  return { ok: true };
}

/* ---------------- appointments ---------------- */
async function enrichAppointments(appts) {
  if (!appts.length) return [];
  const [docs, pats, toks, deps, hs] = await Promise.all([
    q(from('doctors').select('*').in('id', ids(appts.map((a) => a.doctor_id)))),
    q(from('patients').select('*').in('id', ids(appts.map((a) => a.patient_id)))),
    q(from('queue_tokens').select('*').in('appointment_id', ids(appts.map((a) => a.id)))),
    q(from('departments').select('id,name').in('id', ids(appts.map((a) => a.department_id)))),
    q(from('hospitals').select('id,name').in('id', ids(appts.map((a) => a.hospital_id)))),
  ]);
  const users = mapBy(await q(from('users').select('id,full_name,phone').in('id', ids([...docs.map((d) => d.user_id), ...pats.map((p) => p.user_id)]))), 'id');
  const dm = mapBy(docs, 'id'), pm = mapBy(pats, 'id'), tm = mapBy(toks, 'appointment_id'), dpm = mapBy(deps, 'id'), hm = mapBy(hs, 'id');
  return appts.map((a) => {
    const d = dm[a.doctor_id] || {}, p = pm[a.patient_id] || {}, t = tm[a.id] || {};
    return { ...a, doctor_name: users[d.user_id]?.full_name || '', department: dpm[a.department_id]?.name || '', hospital_name: hm[a.hospital_id]?.name || '', room: d.room || '', patient_name: users[p.user_id]?.full_name || '', patient_phone: users[p.user_id]?.phone || '', patient_user_id: p.user_id || null, gender: p.gender || '', dob: p.dob || null, blood_group: p.blood_group || '', token_id: t.id || null, token_code: t.token_code || null, token_status: t.status || null, token_number: t.token_number || null };
  });
}
async function listAppointments(req, query) {
  const ctx = await auth(req, ['patient', 'hospital']);
  let qb = from('appointments').select('*').order('appointment_date', { ascending: false }).order('id', { ascending: false }).limit(300);
  if (ctx.user.role === 'patient') qb = qb.eq('patient_id', ctx.profile.id);
  else if (ctx.doctor && !can(ctx, 'appointments')) qb = qb.eq('doctor_id', ctx.doctor.id);
  else { requirePerm(ctx, 'appointments'); qb = qb.eq('hospital_id', ctx.hid); if (toInt(query.doctor_id)) qb = qb.eq('doctor_id', toInt(query.doctor_id)); }
  if (isDate(query.date)) qb = qb.eq('appointment_date', query.date);
  if (query.status) qb = qb.eq('status', str(query.status, 20));
  return enrichAppointments(await q(qb));
}
function localNowMin(tz) { const off = Number.isFinite(Number(tz)) ? Math.max(-840, Math.min(840, Number(tz))) : 0; const d = new Date(Date.now() - off * 60000); return { date: d.toISOString().slice(0, 10), min: d.getUTCHours() * 60 + d.getUTCMinutes() }; }
async function doctorSlots(doctor, date, tz) {
  if (doctor.active === false || !doctor.is_available) return [];
  const start = toMin(doctor.avail_start || '09:00'), end = toMin(doctor.avail_end || '17:00');
  const booked = new Set((await q(from('appointments').select('appointment_time').eq('doctor_id', doctor.id).eq('appointment_date', date).neq('status', 'cancelled'))).map((r) => r.appointment_time));
  const ln = localNowMin(tz), nowMin = date === ln.date ? ln.min : date < ln.date ? 1e9 : -1;
  const out = [];
  for (let m = start; m + SLOT_MINUTES <= end; m += SLOT_MINUTES) { const t = fromMin(m); const reason = booked.has(t) ? 'booked' : m <= nowMin ? 'past' : null; out.push({ time: t, available: !reason, reason }); }
  return out;
}
async function getSlots(req, query) {
  const ctx = await auth(req, ['patient', 'hospital']);
  const doctor = await doctorCard(toInt(query.doctor_id) || -1);
  if (ctx.user.role === 'hospital' && doctor.hospital_id !== ctx.hid) forbid('Doctor does not belong to your hospital');
  const date = str(query.date, 10);
  if (!isDate(date)) bad('Please choose a valid date');
  if (date < addDays(today(), -1) || date > addDays(today(), 30)) return { date, slot_minutes: SLOT_MINUTES, slots: [] };
  return { date, slot_minutes: SLOT_MINUTES, doctor_available: !!doctor.is_available && doctor.active !== false, slots: await doctorSlots(doctor, date, query.tz_offset) };
}
async function createAppointment(req, b) {
  const ctx = await auth(req, ['patient']);
  const doctorId = toInt(b.doctor_id); if (!doctorId) bad('Please select a doctor');
  const doctor = await doctorCard(doctorId);
  if (doctor.active === false) bad('This doctor is no longer available');
  if (b.hospital_id && toInt(b.hospital_id) !== doctor.hospital_id) bad('Doctor does not belong to the selected hospital');
  if (b.department_id && toInt(b.department_id) !== doctor.department_id) bad('Doctor does not belong to the selected department');
  const date = str(b.appointment_date, 10), time = str(b.appointment_time, 5);
  if (!isDate(date)) bad('Please choose a valid date');
  if (date < addDays(today(), -1) || date > addDays(today(), 30)) bad('Appointments can be booked from today up to 30 days ahead');
  if (!doctor.is_available) bad(`${doctor.full_name} is not accepting appointments right now`);
  if (!isTime(time)) bad('Please select an appointment slot');
  const slot = (await doctorSlots(doctor, date, b.tz_offset)).find((x) => x.time === time);
  if (!slot) bad(`${time} is not a valid slot for ${doctor.full_name}`);
  if (!slot.available) bad(slot.reason === 'booked' ? 'This slot has already been booked — please pick another' : 'This slot has already passed');
  const dup = await q(from('appointments').select('id').eq('patient_id', ctx.profile.id).eq('doctor_id', doctorId).eq('appointment_date', date).in('status', ['booked', 'waiting', 'called', 'in_consultation']));
  if (dup.length) bad('You already have an active appointment with this doctor on that date');
  let appt = await insertOne('appointments', { patient_id: ctx.profile.id, hospital_id: doctor.hospital_id, doctor_id: doctorId, department_id: doctor.department_id, appointment_date: date, appointment_time: time, reason: str(b.reason, 255) || null, status: 'waiting' });
  const clash = await q(from('appointments').select('id').eq('doctor_id', doctorId).eq('appointment_date', date).eq('appointment_time', time).neq('status', 'cancelled').order('id'));
  if (clash.length > 1 && clash[0].id !== appt.id) { await from('appointments').delete().eq('id', appt.id); bad('This slot has just been booked by someone else — please pick another'); }
  const { count } = await from('queue_tokens').select('id', { count: 'exact', head: true }).eq('doctor_id', doctorId).eq('token_date', date);
  const n = (count || 0) + 1;
  const token = await insertOne('queue_tokens', { appointment_id: appt.id, doctor_id: doctorId, hospital_id: doctor.hospital_id, token_date: date, token_number: n, token_code: `${doctor.token_prefix}${String(n).padStart(3, '0')}`, status: 'waiting' });
  const qd = await buildQueue(doctorId, date);
  const ahead = aheadOf(qd, token);
  await notify([ctx.user.id], `Token ${token.token_code} confirmed`, `${doctor.hospital_name} · ${doctor.full_name} (${doctor.department}) on ${date} at ${time}. ${ahead} patient(s) ahead · est. wait ~${ahead * qd.avg} min.`, 'success');
  await notify([doctor.user_id], 'New appointment booked', `Token ${token.token_code} · ${ctx.user.full_name} · ${date} ${time}`, 'info');
  return { appointment: appt, token, ahead, est_wait: ahead * qd.avg, doctor };
}
async function updateAppointment(req, b) {
  const ctx = await auth(req, ['patient', 'hospital']);
  const appt = await one(from('appointments').select('*').eq('id', toInt(b.id) || -1));
  if (!appt) throw new HttpError(404, 'Appointment not found');
  if (ctx.user.role === 'patient' && appt.patient_id !== ctx.profile.id) forbid();
  if (ctx.user.role === 'hospital') {
    if (appt.hospital_id !== ctx.hid) forbid();
    if (!can(ctx, 'appointments') && !(ctx.doctor && appt.doctor_id === ctx.doctor.id)) forbid();
  }
  const action = str(b.action, 20);
  if (action === 'cancel') {
    if (!['booked', 'waiting'].includes(appt.status)) bad('Only waiting appointments can be cancelled');
    await updateRows('appointments', { status: 'cancelled' }, 'id', appt.id);
    await updateRows('queue_tokens', { status: 'cancelled' }, 'appointment_id', appt.id);
  } else if (action === 'no_show') {
    if (ctx.user.role === 'patient') forbid();
    if (!['booked', 'waiting', 'called'].includes(appt.status)) bad('Cannot mark this appointment as no-show');
    await updateRows('appointments', { status: 'no_show' }, 'id', appt.id);
    await updateRows('queue_tokens', { status: 'skipped' }, 'appointment_id', appt.id);
  } else bad('Unknown action');
  const [e] = await enrichAppointments([{ ...appt }]);
  if (ctx.user.role !== 'patient') await notify([e.patient_user_id], `Appointment ${action === 'cancel' ? 'cancelled' : 'marked no-show'}`, `Token ${e.token_code} with ${e.doctor_name} on ${appt.appointment_date}.`, 'warning');
  return { ok: true };
}

/* ---------------- queue ---------------- */
async function buildQueue(doctorId, date) {
  const [doc, tokens] = await Promise.all([
    one(from('doctors').select('avg_consult_minutes').eq('id', doctorId)),
    q(from('queue_tokens').select('*').eq('doctor_id', doctorId).eq('token_date', date).order('token_number')),
  ]);
  const active = tokens.filter((t) => ACTIVE.includes(t.status));
  const waiting = tokens.filter((t) => t.status === 'waiting');
  const current = active.length ? active[active.length - 1] : [...tokens].reverse().find((t) => t.status === 'completed') || null;
  return { tokens, active, waiting, current, avg: doc?.avg_consult_minutes || 10 };
}
function aheadOf(qd, t) {
  if (t.status !== 'waiting') return 0;
  return qd.waiting.filter((w) => w.token_number < t.token_number).length + qd.active.length;
}
async function enrichTokens(tokens) {
  const appts = await enrichAppointments(await q(from('appointments').select('*').in('id', ids(tokens.map((t) => t.appointment_id)))));
  const am = mapBy(appts, 'id');
  return tokens.map((t) => { const a = am[t.appointment_id] || {}; return { ...t, patient_name: a.patient_name, patient_phone: a.patient_phone, gender: a.gender, dob: a.dob, blood_group: a.blood_group, reason: a.reason, patient_id: a.patient_id, appointment_time: a.appointment_time }; });
}
async function patientUserForAppointment(apptId) {
  const a = await one(from('appointments').select('patient_id').eq('id', apptId));
  const p = a && await one(from('patients').select('user_id').eq('id', a.patient_id));
  return p?.user_id || null;
}
async function hospitalDoctor(ctx, doctorId) {
  const d = await doctorCard(doctorId || -1);
  if (d.hospital_id !== ctx.hid) forbid('Doctor does not belong to your hospital');
  return d;
}
async function getQueue(req, query) {
  const ctx = await auth(req, ['patient', 'hospital']);
  const date = pickDate(query.date);
  if (ctx.user.role === 'patient') {
    const appts = await q(from('appointments').select('*').eq('patient_id', ctx.profile.id).eq('appointment_date', date).in('status', ['waiting', 'called', 'in_consultation']));
    const mine = [];
    for (const a of appts) {
      const qd = await buildQueue(a.doctor_id, date);
      const t = qd.tokens.find((x) => x.appointment_id === a.id);
      if (!t) continue;
      const ahead = aheadOf(qd, t);
      mine.push({ appointment_id: a.id, appointment_time: a.appointment_time, reason: a.reason, token: t, doctor: await doctorCard(a.doctor_id), current_token: qd.current?.token_code || null, current_status: qd.current?.status || null, ahead, est_wait: ahead * qd.avg, waiting_count: qd.waiting.length, total: qd.tokens.length, served: qd.tokens.filter((x) => ['completed', 'skipped'].includes(x.status)).length });
    }
    return { date, mine };
  }
  let doctor = null;
  if (toInt(query.doctor_id)) { requirePerm(ctx, 'queues'); doctor = await hospitalDoctor(ctx, toInt(query.doctor_id)); }
  else if (ctx.doctor && !can(ctx, 'queues', 'dashboard')) doctor = await doctorCard(ctx.doctor.id);
  if (doctor) {
    const qd = await buildQueue(doctor.id, date);
    const tokens = await enrichTokens(qd.tokens);
    return { date, doctor, current: tokens.find((t) => t.id === qd.current?.id) || null, tokens, waiting_count: qd.waiting.length, completed_count: qd.tokens.filter((t) => t.status === 'completed').length, avg: qd.avg };
  }
  requirePerm(ctx, 'queues', 'dashboard');
  const docs = await listDoctors({ hospital_id: ctx.hid, date });
  const toks = await q(from('queue_tokens').select('*').eq('token_date', date).eq('hospital_id', ctx.hid).order('token_number'));
  return { date, doctors: docs.map((d) => ({ ...d, tokens: toks.filter((t) => t.doctor_id === d.id).map((t) => ({ id: t.id, token_code: t.token_code, status: t.status })) })) };
}
async function callNext(doctor, date) {
  const qd = await buildQueue(doctor.id, date);
  if (qd.active.length) bad(`Token ${qd.active[0].token_code} is still active — complete or skip it first`);
  const next = qd.waiting[0];
  if (!next) bad('No patients waiting in the queue');
  const [t] = await updateRows('queue_tokens', { status: 'called', called_at: nowIso() }, 'id', next.id);
  await updateRows('appointments', { status: 'called' }, 'id', next.appointment_id);
  await notify([await patientUserForAppointment(next.appointment_id)], `🔔 Token ${next.token_code} — it's your turn!`, `Please proceed to ${doctor.room ? 'Room ' + doctor.room : 'the consultation room'} · ${doctor.full_name}, ${doctor.hospital_name}.`, 'call');
  const upcoming = qd.waiting.slice(1, 3);
  for (let i = 0; i < upcoming.length; i++) {
    await notify([await patientUserForAppointment(upcoming[i].appointment_id)], `Token ${upcoming[i].token_code}: get ready`, `${i + 1} patient(s) ahead of you for ${doctor.full_name}. Est. wait ~${(i + 1) * qd.avg} min.`, 'info');
  }
  return t;
}
async function queueAction(req, b) {
  const ctx = await auth(req, ['hospital']);
  const date = pickDate(b.date);
  let doctor;
  if (ctx.doctor && (!b.doctor_id || toInt(b.doctor_id) === ctx.doctor.id)) doctor = await doctorCard(ctx.doctor.id);
  else { requirePerm(ctx, 'queues'); doctor = await hospitalDoctor(ctx, toInt(b.doctor_id)); }
  const action = str(b.action, 20);
  if (action === 'call_next') return { token: await callNext(doctor, date) };
  const qd = await buildQueue(doctor.id, date);
  const cur = b.token_id ? qd.tokens.find((t) => t.id === toInt(b.token_id)) : qd.active[qd.active.length - 1];
  if (!cur) bad('No active token');
  const puid = await patientUserForAppointment(cur.appointment_id);
  if (action === 'start') {
    if (!ctx.doctor || ctx.doctor.id !== doctor.id) forbid('Only the assigned doctor can start the consultation');
    if (cur.status !== 'called') bad('Call the patient before starting the consultation');
    const [t] = await updateRows('queue_tokens', { status: 'in_consultation', started_at: nowIso() }, 'id', cur.id);
    await updateRows('appointments', { status: 'in_consultation' }, 'id', cur.appointment_id);
    await notify([puid], 'Consultation started', `${doctor.full_name} is now consulting you (token ${cur.token_code}).`, 'info');
    return { token: t };
  }
  if (action === 'skip') {
    if (!ACTIVE.includes(cur.status)) bad('Only an active token can be skipped');
    const [t] = await updateRows('queue_tokens', { status: 'skipped', completed_at: nowIso() }, 'id', cur.id);
    await updateRows('appointments', { status: 'no_show' }, 'id', cur.appointment_id);
    await notify([puid], `Token ${cur.token_code} skipped`, `You were not present when called by ${doctor.full_name}. Please contact the front desk.`, 'warning');
    return { token: t };
  }
  if (action === 'recall') {
    if (cur.status !== 'called') bad('Only a called token can be recalled');
    await notify([puid], `🔔 Reminder: token ${cur.token_code}`, `${doctor.full_name} is waiting for you${doctor.room ? ' in Room ' + doctor.room : ''}.`, 'call');
    return { token: cur };
  }
  bad('Unknown queue action');
}

/* ---------------- consultations ---------------- */
async function listConsultations(req) {
  const ctx = await auth(req, ['patient', 'hospital']);
  let qb = from('consultations').select('*').order('id', { ascending: false }).limit(200);
  if (ctx.user.role === 'patient') qb = qb.eq('patient_id', ctx.profile.id);
  else if (ctx.doctor) qb = qb.eq('doctor_id', ctx.doctor.id);
  else { requirePerm(ctx, 'reports'); qb = qb.eq('hospital_id', ctx.hid); }
  const rows = await q(qb);
  const appts = mapBy(await enrichAppointments(await q(from('appointments').select('*').in('id', ids(rows.map((r) => r.appointment_id))))), 'id');
  return rows.map((r) => { const a = appts[r.appointment_id] || {}; return { ...r, appointment_date: a.appointment_date, appointment_time: a.appointment_time, doctor_name: a.doctor_name, department: a.department, hospital_name: a.hospital_name, patient_name: a.patient_name, token_code: a.token_code, reason: a.reason }; });
}
async function completeConsultation(req, b) {
  const ctx = await auth(req, ['hospital'], 'consult');
  if (!ctx.doctor) forbid('Doctor profile not found');
  const doctor = await doctorCard(ctx.doctor.id);
  const date = pickDate(b.date);
  const qd = await buildQueue(doctor.id, date);
  const t = b.token_id ? await one(from('queue_tokens').select('*').eq('id', toInt(b.token_id) || -1)) : qd.active[qd.active.length - 1];
  if (!t || t.doctor_id !== doctor.id) bad('No active consultation found');
  if (!ACTIVE.includes(t.status)) bad('This token is not in consultation');
  const diagnosis = need(b.diagnosis, 'Diagnosis', 2, 2000);
  const follow = isDate(b.follow_up_date) ? b.follow_up_date : null;
  const appt = await one(from('appointments').select('*').eq('id', t.appointment_id));
  const consultation = await insertOne('consultations', { appointment_id: appt.id, hospital_id: appt.hospital_id, doctor_id: doctor.id, patient_id: appt.patient_id, diagnosis, prescription: str(b.prescription, 3000) || null, notes: str(b.notes, 3000) || null, follow_up_date: follow });
  const now = new Date();
  const startedAt = t.started_at || t.called_at || nowIso();
  await updateRows('queue_tokens', { status: 'completed', completed_at: now.toISOString(), started_at: startedAt }, 'id', t.id);
  await updateRows('appointments', { status: 'completed' }, 'id', appt.id);
  const actual = (now - new Date(startedAt)) / 60000;
  if (actual >= 1 && actual <= 120) await updateRows('doctors', { avg_consult_minutes: Math.min(60, Math.max(3, Math.round(0.7 * doctor.avg_consult_minutes + 0.3 * actual))) }, 'id', doctor.id);
  await notify([await patientUserForAppointment(appt.id)], 'Consultation completed ✅', `${doctor.full_name}: ${diagnosis.slice(0, 120)}${follow ? ` · Follow-up on ${follow}` : ''}. View your prescription in History.`, 'success');
  let next = null;
  if (b.auto_next !== false) { try { next = await callNext(doctor, date); } catch { next = null; } }
  return { consultation, next };
}

/* ---------------- emergency ---------------- */
function haversine(a, b, c, d) {
  const R = 6371, r = Math.PI / 180;
  const x = Math.sin(((c - a) * r) / 2) ** 2 + Math.cos(a * r) * Math.cos(c * r) * Math.sin(((d - b) * r) / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(x));
}
const etaMin = (km, speed) => Math.max(1, Math.round(((km * 1.3) / (speed && speed > 15 ? speed : 32)) * 60));
function computeEta(ec, amb, hospital, speed) {
  if (!amb || amb.current_lat == null) return {};
  let tLat, tLng;
  if (['accepted', 'en_route'].includes(ec.status)) { tLat = ec.pickup_lat; tLng = ec.pickup_lng; }
  else if (ec.status === 'transporting') { tLat = hospital?.lat; tLng = hospital?.lng; }
  if (tLat == null || tLng == null) return {};
  const km = haversine(amb.current_lat, amb.current_lng, tLat, tLng);
  return { eta_minutes: etaMin(km, speed), distance_km: Math.round(km * 1.3 * 10) / 10 };
}
const ambOK = (amb, ec) => !amb.hospital_id || amb.hospital_id === ec.hospital_id;
async function enrichEmergencies(rows) {
  if (!rows.length) return [];
  const [ambs, ops, res, hs] = await Promise.all([
    q(from('ambulances').select('*').in('id', ids(rows.map((r) => r.ambulance_id)))),
    q(from('ambulance_operators').select('*').in('id', ids(rows.map((r) => r.operator_id)))),
    q(from('hospital_resources').select('*').in('id', ids(rows.map((r) => r.resource_id)))),
    q(from('hospitals').select('*').in('id', ids(rows.map((r) => r.hospital_id)))),
  ]);
  const users = mapBy(await q(from('users').select('id,full_name,phone').in('id', ids(ops.map((o) => o.user_id)))), 'id');
  const am = mapBy(ambs, 'id'), om = mapBy(ops, 'id'), rm = mapBy(res, 'id'), hm = mapBy(hs, 'id');
  return rows.map((e) => {
    const a = am[e.ambulance_id], o = om[e.operator_id], r = rm[e.resource_id];
    return { ...e, hospital: pubHospital(hm[e.hospital_id]), ambulance: a || null, operator_name: o ? users[o.user_id]?.full_name : null, operator_phone: o ? users[o.user_id]?.phone : null, resource_label: r ? `${r.name} · ${RES_LABEL[r.resource_type] || r.resource_type}` : null, resource_status: r?.status || null };
  });
}
async function listEmergencies(req, query) {
  const ctx = await auth(req);
  let rows;
  if (ctx.user.role === 'patient') rows = await q(from('emergency_cases').select('*').eq('reporter_user_id', ctx.user.id).order('id', { ascending: false }).limit(20));
  else if (ctx.user.role === 'operator') {
    const amb = await one(from('ambulances').select('*').eq('operator_id', ctx.profile.id));
    let oq = from('emergency_cases').select('*').eq('status', 'requested').order('id', { ascending: false }).limit(30);
    if (amb?.hospital_id) oq = oq.eq('hospital_id', amb.hospital_id);
    const [open, mine] = await Promise.all([amb ? q(oq) : [], q(from('emergency_cases').select('*').eq('operator_id', ctx.profile.id).order('id', { ascending: false }).limit(30))]);
    rows = [...open, ...mine.filter((m) => !open.some((o) => o.id === m.id))];
  } else {
    requirePerm(ctx, 'emergency_view');
    let qb = from('emergency_cases').select('*').eq('hospital_id', ctx.hid).order('id', { ascending: false }).limit(100);
    if (query.active === '1') qb = qb.in('status', E_OPEN);
    rows = await q(qb);
  }
  return enrichEmergencies(rows);
}
async function createEmergency(req, b) {
  const ctx = await auth(req, ['patient', 'hospital']);
  let hospital;
  if (ctx.user.role === 'hospital') { requirePerm(ctx, 'emergency_view'); hospital = ctx.hospital; }
  else { hospital = await getHospital(toInt(b.hospital_id)); if (!hospital) bad('Please select the hospital to send the emergency to'); }
  const patient_name = need(b.patient_name || (ctx.user.role === 'patient' ? ctx.user.full_name : ''), 'Patient name', 2, 120);
  const contact_phone = str(b.contact_phone, 20) || ctx.user.phone;
  const emergency_type = need(b.emergency_type, 'Emergency type', 2, 60);
  const severity = ['critical', 'high', 'moderate'].includes(b.severity) ? b.severity : 'high';
  const lat = numOrNull(b.pickup_lat), lng = numOrNull(b.pickup_lng);
  if (Number.isNaN(lat) || Number.isNaN(lng) || (lat != null && (lat < -90 || lat > 90)) || (lng != null && (lng < -180 || lng > 180))) bad('Invalid pickup coordinates');
  const pickup_address = str(b.pickup_address, 255) || null;
  if ((lat == null || lng == null) && !pickup_address) bad('Share your location or enter a pickup address');
  if (ctx.user.role === 'patient') {
    const open = await q(from('emergency_cases').select('id').eq('reporter_user_id', ctx.user.id).in('status', ['requested', 'accepted', 'en_route', 'transporting']));
    if (open.length) bad('You already have an active emergency request');
  }
  const ec = await insertOne('emergency_cases', { hospital_id: hospital.id, patient_id: ctx.user.role === 'patient' ? ctx.profile.id : null, reporter_user_id: ctx.user.id, patient_name, contact_phone, emergency_type, severity, description: str(b.description, 1000) || null, pickup_lat: lat, pickup_lng: lng, pickup_address, status: 'requested', hospital_prepared: false });
  const where = pickup_address || `${lat?.toFixed(4)}, ${lng?.toFixed(4)}`;
  await notify(await hospitalUserIds(hospital.id, 'emergency_view'), `🚨 Emergency #${ec.id} · ${severity.toUpperCase()}`, `${emergency_type} — ${patient_name} at ${where}.`, 'emergency');
  const freeAmbs = (await q(from('ambulances').select('operator_id,hospital_id').eq('status', 'available'))).filter((a) => ambOK(a, ec));
  const ops = await q(from('ambulance_operators').select('user_id').in('id', ids(freeAmbs.map((a) => a.operator_id))));
  await notify(ops.map((o) => o.user_id), `🚑 New emergency request #${ec.id}`, `${emergency_type} (${severity}) at ${where} → ${hospital.name}. Open Dispatch to accept.`, 'emergency');
  await notify([ctx.user.id], 'Emergency request sent', `${hospital.name} alerted. Finding the nearest available ambulance…`, 'emergency');
  return ec;
}
async function freeAmbulance(ambId) { if (ambId) await updateRows('ambulances', { status: 'available', updated_at: nowIso() }, 'id', ambId); }
async function releaseResource(resId) { if (resId) await updateRows('hospital_resources', { status: 'available', assigned_emergency_id: null, assigned_patient_name: null, updated_at: nowIso() }, 'id', resId); }
async function updateEmergency(req, b) {
  const ctx = await auth(req);
  const { user, profile } = ctx;
  const ec = await one(from('emergency_cases').select('*').eq('id', toInt(b.id) || -1));
  if (!ec) throw new HttpError(404, 'Emergency not found');
  const action = str(b.action, 20);
  const hospital = await getHospital(ec.hospital_id);
  const staffIds = await hospitalUserIds(ec.hospital_id, 'emergency_view');
  const set = async (f) => (await updateRows('emergency_cases', { ...f, updated_at: nowIso() }, 'id', ec.id))[0];
  const hospGuard = (...perm) => { if (user.role !== 'hospital' || ctx.hid !== ec.hospital_id) forbid('This emergency belongs to another hospital'); requirePerm(ctx, ...perm); };
  const opGuard = () => { if (user.role !== 'operator' || ec.operator_id !== profile.id) forbid('Only the assigned operator can do this'); };
  const hospitalResource = async (rid) => {
    const r = await one(from('hospital_resources').select('*').eq('id', rid || -1));
    if (!r || r.hospital_id !== ec.hospital_id) bad('Select a bed / ICU / room of this hospital');
    if (r.status !== 'available' && r.assigned_emergency_id !== ec.id) bad(`${r.name} is not available`);
    return r;
  };
  const assign = async (amb, byStaff) => {
    if (ec.status !== 'requested') bad('This emergency is no longer open');
    if (!amb || amb.status !== 'available') bad('Ambulance is not available');
    if (!amb.operator_id) bad('Ambulance has no operator');
    if (!ambOK(amb, ec)) forbid('This ambulance is attached to another hospital');
    const busy = await q(from('emergency_cases').select('id').eq('ambulance_id', amb.id).in('status', ['accepted', 'en_route', 'transporting']));
    if (busy.length) bad('Ambulance already on another trip');
    await updateRows('ambulances', { status: 'busy', updated_at: nowIso() }, 'id', amb.id);
    const eta = computeEta({ ...ec, status: 'accepted' }, amb, hospital);
    const out = await set({ status: 'accepted', ambulance_id: amb.id, operator_id: amb.operator_id, accepted_at: nowIso(), ...eta });
    const op = await one(from('ambulance_operators').select('user_id').eq('id', amb.operator_id));
    await notify([ec.reporter_user_id], '🚑 Ambulance assigned', `${amb.vehicle_no} (${amb.ambulance_type}) is on the way${eta.eta_minutes ? ` · ETA ~${eta.eta_minutes} min` : ''}.`, 'emergency');
    await notify(staffIds, `Emergency #${ec.id}: ambulance ${amb.vehicle_no} assigned`, `Prepare resources for ${ec.patient_name} (${ec.emergency_type}, ${ec.severity}).`, 'emergency');
    if (byStaff) await notify([op?.user_id], `🚨 You have been dispatched to #${ec.id}`, `${ec.emergency_type} — ${ec.pickup_address || 'see map'} → ${hospital?.name}. Start the trip now.`, 'emergency');
    return out;
  };
  switch (action) {
    case 'accept': {
      if (user.role !== 'operator') forbid();
      const amb = await one(from('ambulances').select('*').eq('operator_id', profile.id));
      if (!amb) bad('Register your ambulance first');
      if (amb.status === 'offline') bad('Set your ambulance to Available before accepting');
      return assign(amb, false);
    }
    case 'assign': hospGuard('emergency_manage'); return assign(await one(from('ambulances').select('*').eq('id', toInt(b.ambulance_id) || -1)), true);
    case 'start': {
      opGuard(); if (ec.status !== 'accepted') bad('Trip already started');
      const out = await set({ status: 'en_route' });
      await notify([ec.reporter_user_id], 'Ambulance en route', `Your ambulance is heading to the pickup location${ec.eta_minutes ? ` · ETA ~${ec.eta_minutes} min` : ''}. Track it live.`, 'emergency');
      return out;
    }
    case 'pickup': {
      opGuard(); if (!['accepted', 'en_route'].includes(ec.status)) bad('Invalid step');
      const amb = await one(from('ambulances').select('*').eq('id', ec.ambulance_id));
      const eta = computeEta({ ...ec, status: 'transporting' }, amb, hospital);
      const out = await set({ status: 'transporting', ...eta });
      await notify(staffIds, `Emergency #${ec.id}: patient onboard`, `${ec.patient_name} en route to hospital${eta.eta_minutes ? ` · ETA ~${eta.eta_minutes} min` : ''}. Keep ${ec.resource_id ? 'reserved bed' : 'a bed/ICU'} ready.`, 'emergency');
      await notify([ec.reporter_user_id], 'Patient onboard', `Heading to ${hospital?.name || 'the hospital'} now.`, 'emergency');
      return out;
    }
    case 'arrive': {
      opGuard(); if (!['en_route', 'transporting'].includes(ec.status)) bad('Invalid step');
      const out = await set({ status: 'arrived', arrived_at: nowIso(), eta_minutes: 0, distance_km: 0 });
      await notify(staffIds, `🏥 Emergency #${ec.id}: ambulance arrived`, `${ec.patient_name} has arrived — begin triage.`, 'emergency');
      await notify([ec.reporter_user_id], 'Arrived at hospital', 'The emergency team is receiving the patient for triage.', 'emergency');
      return out;
    }
    case 'complete_trip': {
      opGuard();
      if (!['arrived', 'triage', 'admitted', 'treatment', 'closed'].includes(ec.status)) bad('Mark arrival before completing the trip');
      if (ec.trip_completed_at) bad('Trip already completed');
      await freeAmbulance(ec.ambulance_id);
      return set({ trip_completed_at: nowIso() });
    }
    case 'prepare': {
      hospGuard('emergency_manage');
      if (!E_OPEN.includes(ec.status)) bad('Emergency is closed');
      const r = await hospitalResource(toInt(b.resource_id));
      if (ec.resource_id && ec.resource_id !== r.id) await releaseResource(ec.resource_id);
      await updateRows('hospital_resources', { status: 'reserved', assigned_emergency_id: ec.id, assigned_patient_name: ec.patient_name, updated_at: nowIso() }, 'id', r.id);
      const out = await set({ resource_id: r.id, hospital_prepared: true, hospital_notes: str(b.notes, 255) || ec.hospital_notes });
      await notify([ec.reporter_user_id], 'Hospital is ready', `${RES_LABEL[r.resource_type]} ${r.name} has been reserved.`, 'emergency');
      return out;
    }
    case 'triage': {
      hospGuard('emergency_clinical');
      if (!['arrived', 'triage'].includes(ec.status)) bad('Patient must arrive before triage');
      const lvl = str(b.triage_level, 10);
      if (!['red', 'orange', 'yellow', 'green'].includes(lvl)) bad('Choose a triage level');
      return set({ status: 'triage', triage_level: lvl, hospital_notes: str(b.notes, 255) || ec.hospital_notes });
    }
    case 'admit': {
      hospGuard('emergency_manage');
      if (!['arrived', 'triage'].includes(ec.status)) bad('Complete triage first');
      const r = await hospitalResource(toInt(b.resource_id) || ec.resource_id);
      if (ec.resource_id && ec.resource_id !== r.id) await releaseResource(ec.resource_id);
      await updateRows('hospital_resources', { status: 'occupied', assigned_emergency_id: ec.id, assigned_patient_name: ec.patient_name, updated_at: nowIso() }, 'id', r.id);
      const out = await set({ status: 'admitted', resource_id: r.id, hospital_prepared: true });
      await notify([ec.reporter_user_id], 'Admitted', `${ec.patient_name} allocated ${RES_LABEL[r.resource_type]} ${r.name}.`, 'emergency');
      return out;
    }
    case 'treat': {
      hospGuard('emergency_clinical');
      if (ec.status !== 'admitted') bad('Allocate a bed first');
      const out = await set({ status: 'treatment' });
      await notify([ec.reporter_user_id], 'Treatment started', `The medical team has started treating ${ec.patient_name}.`, 'emergency');
      return out;
    }
    case 'close': {
      hospGuard('emergency_manage');
      if (!E_OPEN.includes(ec.status)) bad('Already closed');
      await releaseResource(ec.resource_id);
      if (!ec.trip_completed_at && ec.ambulance_id) await freeAmbulance(ec.ambulance_id);
      return set({ status: 'closed', closed_at: nowIso(), trip_completed_at: ec.trip_completed_at || (ec.ambulance_id ? nowIso() : null) });
    }
    case 'cancel': {
      if (ec.reporter_user_id !== user.id) hospGuard('emergency_manage');
      if (!['requested', 'accepted', 'en_route'].includes(ec.status)) bad('This emergency can no longer be cancelled');
      await freeAmbulance(ec.ambulance_id);
      await releaseResource(ec.resource_id);
      const out = await set({ status: 'cancelled', closed_at: nowIso() });
      if (ec.operator_id) { const op = await one(from('ambulance_operators').select('user_id').eq('id', ec.operator_id)); await notify([op?.user_id], `Emergency #${ec.id} cancelled`, 'Trip cancelled. You are available again.', 'warning'); }
      await notify(staffIds, `Emergency #${ec.id} cancelled`, `${ec.patient_name} — request cancelled.`, 'warning');
      return out;
    }
    default: bad('Unknown emergency action');
  }
}

/* ---------------- ambulances ---------------- */
async function listAmbulances(req) {
  const ctx = await auth(req, ['hospital', 'operator']);
  let ambs;
  if (ctx.user.role === 'operator') ambs = await q(from('ambulances').select('*').eq('operator_id', ctx.profile.id));
  else { requirePerm(ctx, 'ambulances', 'emergency_manage', 'dashboard'); ambs = await q(from('ambulances').select('*').or(`hospital_id.eq.${ctx.hid},hospital_id.is.null`).order('id')); }
  const ops = await q(from('ambulance_operators').select('*').in('id', ids(ambs.map((a) => a.operator_id))));
  const users = mapBy(await q(from('users').select('id,full_name,phone').in('id', ids(ops.map((o) => o.user_id)))), 'id');
  const hs = mapBy(await q(from('hospitals').select('id,name').in('id', ids(ambs.map((a) => a.hospital_id)))), 'id');
  const om = mapBy(ops, 'id');
  const active = await q(from('emergency_cases').select('id,ambulance_id,status,hospital_id').in('status', ['accepted', 'en_route', 'transporting', 'arrived']).is('trip_completed_at', null));
  return ambs.map((a) => { const o = om[a.operator_id]; const c = active.find((x) => x.ambulance_id === a.id); const mineCase = c && (ctx.user.role === 'operator' || c.hospital_id === ctx.hid); return { ...a, hospital_name: hs[a.hospital_id]?.name || null, operator_name: o ? users[o.user_id]?.full_name : null, operator_phone: o ? users[o.user_id]?.phone : null, active_case_id: mineCase ? c.id : null, active_case_status: mineCase ? c.status : c ? 'other_hospital' : null }; });
}
async function registerAmbulance(req, b) {
  const ctx = await auth(req, ['operator']);
  if (await one(from('ambulances').select('id').eq('operator_id', ctx.profile.id))) bad('You already have an ambulance registered');
  const v = need(b.vehicle_no, 'Vehicle number', 4, 20).toUpperCase();
  if (!/^[A-Z0-9 -]{4,20}$/.test(v)) bad('Invalid vehicle number');
  if (await one(from('ambulances').select('id').eq('vehicle_no', v))) bad('Vehicle already registered');
  let hid = null;
  if (toInt(b.hospital_id)) { const h = await getHospital(toInt(b.hospital_id)); if (!h) bad('Selected hospital not found'); hid = h.id; }
  const amb = await insertOne('ambulances', { vehicle_no: v, ambulance_type: ['BLS', 'ALS', 'ICU'].includes(b.ambulance_type) ? b.ambulance_type : 'BLS', status: 'available', operator_id: ctx.profile.id, hospital_id: hid });
  await q(from('ambulance_operators').update({ ambulance_id: amb.id }).eq('id', ctx.profile.id));
  if (hid) await notify(await hospitalUserIds(hid, 'ambulances'), 'Ambulance added to fleet', `${amb.vehicle_no} (${amb.ambulance_type}) is now attached to your hospital.`, 'info');
  return amb;
}
async function updateAmbulance(req, b, forceLocation) {
  const ctx = await auth(req, ['operator']);
  const amb = await one(from('ambulances').select('*').eq('operator_id', ctx.profile.id));
  if (!amb) bad('Register your ambulance first');
  const action = forceLocation ? 'location' : str(b.action, 20);
  if (action === 'status') {
    const s = str(b.status, 20);
    if (!['available', 'offline'].includes(s)) bad('Invalid status');
    const active = await q(from('emergency_cases').select('id').eq('ambulance_id', amb.id).in('status', ['accepted', 'en_route', 'transporting', 'arrived']).is('trip_completed_at', null));
    if (active.length) bad('Complete your active trip first');
    return (await updateRows('ambulances', { status: s, updated_at: nowIso() }, 'id', amb.id))[0];
  }
  if (action === 'location') {
    const lat = Number(b.lat), lng = Number(b.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || lat < -90 || lat > 90 || lng < -180 || lng > 180) bad('Invalid coordinates');
    const speed = Number.isFinite(Number(b.speed)) && b.speed !== null ? Math.max(0, Math.min(250, Number(b.speed))) : null;
    const heading = Number.isFinite(Number(b.heading)) && b.heading !== null ? Number(b.heading) : null;
    const [upd] = await updateRows('ambulances', { current_lat: lat, current_lng: lng, updated_at: nowIso() }, 'id', amb.id);
    const ec = await one(from('emergency_cases').select('*').eq('ambulance_id', amb.id).in('status', ['accepted', 'en_route', 'transporting']).order('id', { ascending: false }));
    await q(from('ambulance_locations').insert({ ambulance_id: amb.id, emergency_id: ec?.id || null, lat, lng, speed, heading }));
    let eta = {};
    if (ec) { eta = computeEta(ec, upd, await getHospital(ec.hospital_id), speed); if (eta.eta_minutes) await updateRows('emergency_cases', { ...eta, updated_at: nowIso() }, 'id', ec.id); }
    return { ambulance: upd, emergency_id: ec?.id || null, ...eta };
  }
  bad('Unknown action');
}

/* ---------------- resources ---------------- */
async function resources(req, method, b) {
  const ctx = await auth(req, ['hospital']);
  if (method === 'GET') return q(from('hospital_resources').select('*').eq('hospital_id', ctx.hid).order('resource_type').order('id'));
  requirePerm(ctx, 'resources');
  if (method === 'POST') {
    const type = str(b.resource_type, 30); if (!RES_TYPES.includes(type)) bad('Invalid resource type');
    const name = need(b.name, 'Name / number', 1, 50);
    const count = clampInt(b.count, 1, 50, 1);
    const ward = str(b.ward, 60) || null;
    return q(from('hospital_resources').insert(Array.from({ length: count }, (_, i) => ({ hospital_id: ctx.hid, resource_type: type, name: count > 1 ? `${name}-${i + 1}` : name, ward, status: 'available' }))).select('*'));
  }
  const r = await one(from('hospital_resources').select('*').eq('id', toInt(b.id) || -1).eq('hospital_id', ctx.hid));
  if (!r) throw new HttpError(404, 'Resource not found');
  if (method === 'PUT') {
    const s = str(b.status, 20); if (!['available', 'occupied', 'reserved', 'maintenance'].includes(s)) bad('Invalid status');
    const f = { status: s, updated_at: nowIso() };
    if (s === 'available' || s === 'maintenance') { f.assigned_emergency_id = null; f.assigned_patient_name = null; }
    else if (b.assigned_patient_name !== undefined) f.assigned_patient_name = str(b.assigned_patient_name, 120) || null;
    return (await updateRows('hospital_resources', f, 'id', r.id))[0];
  }
  if (r.status === 'occupied' || r.status === 'reserved') bad('Release this resource before deleting');
  await q(from('hospital_resources').delete().eq('id', r.id));
  return { ok: true };
}

/* ---------------- notifications ---------------- */
async function notifications(req, method, b) {
  const { user } = await auth(req);
  if (method === 'GET') {
    const items = await q(from('notifications').select('*').eq('user_id', user.id).order('id', { ascending: false }).limit(50));
    const { count } = await from('notifications').select('id', { count: 'exact', head: true }).eq('user_id', user.id).eq('is_read', false);
    return { items, unread: count || 0 };
  }
  let qb = from('notifications').update({ is_read: true }).eq('user_id', user.id);
  if (!b.all) qb = qb.eq('id', toInt(b.id) || -1);
  await q(qb);
  return { ok: true };
}

/* ---------------- analytics & settings ---------------- */
async function analytics(req, query) {
  const ctx = await auth(req, ['hospital'], 'dashboard', 'reports');
  const hid = ctx.hid, date = pickDate(query.date), from7 = addDays(date, -6);
  const [week, docs, emerg, res, ambs, allAppts] = await Promise.all([
    q(from('appointments').select('id,doctor_id,appointment_date,status').eq('hospital_id', hid).gte('appointment_date', from7).lte('appointment_date', date)),
    listDoctors({ hospital_id: hid, date }),
    q(from('emergency_cases').select('*').eq('hospital_id', hid).order('id', { ascending: false }).limit(500)),
    q(from('hospital_resources').select('resource_type,status').eq('hospital_id', hid)),
    q(from('ambulances').select('status').or(`hospital_id.eq.${hid},hospital_id.is.null`)),
    q(from('appointments').select('patient_id').eq('hospital_id', hid).limit(5000)),
  ]);
  const dayAppts = week.filter((a) => a.appointment_date === date);
  const dm = mapBy(docs, 'id');
  const count = (arr, fn) => arr.reduce((m, x) => { const k = fn(x); m[k] = (m[k] || 0) + 1; return m; }, {});
  const resSummary = {};
  res.forEach((r) => { const s = (resSummary[r.resource_type] ||= { total: 0, available: 0, occupied: 0, reserved: 0, maintenance: 0 }); s.total++; s[r.status] = (s[r.status] || 0) + 1; });
  const responded = emerg.filter((e) => e.accepted_at);
  return {
    date,
    total_patients: new Set(allAppts.map((a) => a.patient_id)).size,
    appointments_today: dayAppts.length,
    in_queue: dayAppts.filter((a) => ['waiting', 'called'].includes(a.status)).length,
    in_consultation: dayAppts.filter((a) => a.status === 'in_consultation').length,
    completed_today: dayAppts.filter((a) => a.status === 'completed').length,
    status_breakdown: count(dayAppts, (a) => a.status),
    by_department: count(dayAppts.filter((a) => a.status !== 'cancelled'), (a) => dm[a.doctor_id]?.department || 'Unknown'),
    trend: Array.from({ length: 7 }, (_, i) => { const d = addDays(from7, i); return { date: d, count: week.filter((a) => a.appointment_date === d && a.status !== 'cancelled').length }; }),
    doctors: docs.map((d) => ({ id: d.id, full_name: d.full_name, department: d.department, is_available: d.is_available, waiting: d.waiting, served: d.served, total_today: d.total_today, avg_consult_minutes: d.avg_consult_minutes, current_token: d.current_token })),
    emergencies_active: emerg.filter((e) => E_OPEN.includes(e.status)).length,
    emergencies_today: emerg.filter((e) => String(e.created_at).slice(0, 10) === date).length,
    emergency_by_type: count(emerg, (e) => e.emergency_type),
    emergency_by_severity: count(emerg.filter((e) => E_OPEN.includes(e.status)), (e) => e.severity),
    avg_response_min: responded.length ? Math.round(responded.reduce((s, e) => s + (new Date(e.accepted_at) - new Date(e.created_at)) / 60000, 0) / responded.length * 10) / 10 : null,
    resources: resSummary,
    ambulances: count(ambs, (a) => a.status),
    ambulances_total: ambs.length,
  };
}
async function settings(req, method, b) {
  const ctx = await auth(req, ['hospital']);
  if (method === 'GET') return pubHospital(ctx.hospital, can(ctx, 'settings', 'people'));
  requirePerm(ctx, 'settings');
  const name = need(b.name, 'Hospital name', 2, 120).replace(/\s+/g, ' ');
  const other = await one(from('hospitals').select('id').eq('name_key', nameKey(name)).neq('id', ctx.hid));
  if (other) bad('Another hospital is already registered with this name');
  const lat = numOrNull(b.lat), lng = numOrNull(b.lng);
  if (lat == null || lng == null || Number.isNaN(lat) || Number.isNaN(lng) || lat < -90 || lat > 90 || lng < -180 || lng > 180) bad('Valid hospital latitude and longitude are required');
  const f = { name, name_key: nameKey(name), address: str(b.address) || null, phone: str(b.phone, 20) || null, lat, lng };
  if (b.regenerate_code) f.join_code = joinCode();
  return pubHospital((await updateRows('hospitals', f, 'id', ctx.hid))[0], true);
}

/* ---------------- digital twin (T2D glucose-spike prototype, synthetic data only) ---------------- */
async function twinDoctor(req) {
  const ctx = await auth(req, ['hospital'], 'consult');
  if (!ctx.doctor) forbid('Doctor profile not found');
  return ctx;
}
async function twinPatientsOf(doctorId) {
  const appts = await q(from('appointments').select('patient_id,appointment_date,status').eq('doctor_id', doctorId).order('appointment_date', { ascending: false }).limit(1000));
  const last = {}; for (const a of appts) if (!last[a.patient_id]) last[a.patient_id] = a;
  const pats = await q(from('patients').select('id,user_id').in('id', ids(Object.keys(last).map(Number))));
  const users = mapBy(await q(from('users').select('id,full_name').in('id', ids(pats.map((p) => p.user_id)))), 'id');
  return pats.map((p) => ({ patient_id: p.id, name: users[p.user_id]?.full_name || 'Patient', last_visit: last[p.id].appointment_date, last_status: last[p.id].status }));
}
async function twinPatients(req) {
  const ctx = await twinDoctor(req);
  const list = await twinPatientsOf(ctx.doctor.id);
  return list.map((p) => ({ ...p, ...Twin.twinState(p.patient_id) })).sort((a, b) => b.probability - a.probability);
}
async function twinPatient(req, query) {
  const ctx = await twinDoctor(req);
  const pid = toInt(query.patient_id);
  const p = (await twinPatientsOf(ctx.doctor.id)).find((x) => x.patient_id === pid);
  if (!p) forbid('This patient has no appointment with you');
  return { patient: p, twin: Twin.patientTwin(pid) };
}

/* ---------------- router ---------------- */
async function route(method, path, b, query, req) {
  const M = (...allowed) => { if (!allowed.includes(method)) throw new HttpError(405, 'Method not allowed'); };
  switch (path) {
    case 'health': return healthData();
    case 'register': M('POST'); return register(req, b);
    case 'me': M('GET'); return me(req);
    case 'hospitals': M('GET'); return listHospitals(query);
    case 'departments': M('GET', 'POST', 'DELETE'); return method === 'GET' ? listDepartments(req, query) : departmentsWrite(req, method, b);
    case 'staff': M('GET', 'PUT', 'DELETE'); return staff(req, method, b);
    case 'slots': M('GET'); return getSlots(req, query);
    case 'twin/model': M('GET'); await twinDoctor(req); return Twin.modelCard();
    case 'twin/patients': M('GET'); return twinPatients(req);
    case 'twin/patient': M('GET'); return twinPatient(req, query);
    case 'doctors': M('GET', 'PUT'); return method === 'GET' ? getDoctors(req, query) : updateDoctor(req, b);
    case 'appointments': M('GET', 'POST', 'PUT');
      if (method === 'GET') return listAppointments(req, query);
      if (method === 'POST') return createAppointment(req, b);
      return updateAppointment(req, b);
    case 'queue': M('GET', 'POST'); return method === 'GET' ? getQueue(req, query) : queueAction(req, b);
    case 'queue/next': M('POST'); return queueAction(req, { ...b, action: 'call_next' });
    case 'consultations': M('GET', 'POST'); return method === 'GET' ? listConsultations(req) : completeConsultation(req, b);
    case 'emergency': M('GET', 'POST', 'PUT');
      if (method === 'GET') return listEmergencies(req, query);
      if (method === 'POST') return createEmergency(req, b);
      return updateEmergency(req, b);
    case 'ambulances': M('GET', 'POST', 'PUT');
      if (method === 'GET') return listAmbulances(req);
      if (method === 'POST') return registerAmbulance(req, b);
      return updateAmbulance(req, b, false);
    case 'ambulances/location': M('POST', 'PUT'); return updateAmbulance(req, b, true);
    case 'resources': M('GET', 'POST', 'PUT', 'DELETE'); return resources(req, method, b);
    case 'notifications': M('GET', 'PUT'); return notifications(req, method, b);
    case 'analytics': M('GET'); return analytics(req, query);
    case 'settings': M('GET', 'PUT'); return settings(req, method, b);
    default: throw new HttpError(404, 'Endpoint not found');
  }
}

async function apiHandler(req, res) {
  res.setHeader('Cache-Control', 'no-store');
  const url = new URL(req.url, 'http://api.internal');
  const query = {};
  url.searchParams.forEach((v, k) => { query[k] = v; });
  Object.assign(query, req.query || {});
  let path = query.__p ?? url.pathname.replace(/^\/api\/?/, '');
  if (Array.isArray(path)) path = path.join('/');
  delete query.__p;
  path = String(path).replace(/^index(\/|$)/, '').replace(/^\/+|\/+$/g, '');
  let body = req.body || {};
  if (typeof body === 'string') { try { body = JSON.parse(body); } catch { body = {}; } }
  try {
    const out = await route(req.method, path, body, query, req);
    res.status(req.method === 'POST' && ['register', 'appointments', 'emergency'].includes(path) ? 201 : 200).json(out ?? null);
  } catch (e) {
    const s = e.status || 500;
    if (s === 500) console.error('API error:', e);
    res.status(s).json({ error: e.message || 'Server error', ...(e.code ? { code: e.code } : {}) });
  }
}


/* ================= health ================= */
async function healthData() {
  let db_error = null, db_ms = null;
  try { const t0 = Date.now(); await dbq('SELECT 1 AS ok'); db_ms = Date.now() - t0; } catch (e) { db_error = e.message; }
  return {
    status: 'ok', ok: true, backend: 'node-express-mysql', platform: process.env.VERCEL ? 'vercel' : ON_SERVERLESS ? 'serverless' : 'server',
    realtime: !ON_SERVERLESS, db: db_error ? 'unavailable' : 'connected', db_error, db_ms, firebase_project_id: FB_PROJECT,
    maps_key: process.env.GOOGLE_MAPS_API_KEY || null, access_code_required: !!process.env.HOSPITAL_ACCESS_CODE, designations: DESIGNATIONS, perms: PERMS,
    digital_twin: Twin.VERSION, time: new Date().toISOString(),
  };
}

/* ================= Express app ================= */
const app = express();
app.disable('x-powered-by');
const ORIGINS = (process.env.CORS_ORIGINS || '*').split(',').map((s) => s.trim()).filter(Boolean);
app.use(cors({ origin: ORIGINS.includes('*') ? true : ORIGINS, methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], allowedHeaders: ['Content-Type', 'Authorization'], maxAge: 86400 }));
app.use(express.json({ limit: '1mb' }));
app.get('/api/health', async (req, res) => { res.set('Cache-Control', 'no-store'); res.status(200).json(await healthData()); });
const CHANGE_EVENTS = [
  [/^(appointments|queue|consultations|doctors|staff|departments)/, ['queue_update']],
  [/^(emergency)/, ['emergency_update', 'resources_update']],
  [/^(ambulances)/, ['ambulance_location', 'emergency_update']],
  [/^(resources)/, ['resources_update']],
  [/^(settings|register)/, ['queue_update', 'emergency_update']],
];
app.all(/^\/api(\/.*)?$/, (req, res) => {
  if (req.method !== 'GET') res.on('finish', () => {
    if (res.statusCode >= 400) return;
    const p = String(req.query.__p || req.path.replace(/^\/api\/?/, '')).replace(/^index\/?/, '');
    for (const [re, evs] of CHANGE_EVENTS) if (re.test(p)) evs.forEach((ev) => rt.emit(ev));
  });
  return apiHandler(req, res);
});
const INDEX_FILE = path.join(__dirname, 'index.html');
app.get(['/', '/index.html'], (req, res) => (fs.existsSync(INDEX_FILE) ? res.sendFile(INDEX_FILE) : res.status(404).send('index.html not found next to server.js')));
app.use((err, req, res, next) => { if (err && err.type === 'entity.parse.failed') return res.status(400).json({ error: 'Invalid JSON body' }); console.error(err); res.status(500).json({ error: 'Server error' }); });

/* ================= local / long-running server with Socket.IO ================= */
const isMain = !!process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
if (isMain && !ON_SERVERLESS) {
  const server = http.createServer(app);
  const io = new SocketServer(server, { cors: { origin: ORIGINS.includes('*') ? true : ORIGINS } });
  io.use(async (socket, next) => {
    try {
      const claims = await verifyFirebaseToken(socket.handshake.auth && socket.handshake.auth.token);
      if (!claims || (process.env.REQUIRE_EMAIL_VERIFICATION !== '0' && !claims.email_verified)) return next(new Error('unauthorized'));
      const rows = await dbq('SELECT id, role FROM users WHERE firebase_uid = ? LIMIT 1', [claims.uid]);
      if (!rows.length) return next(new Error('no profile'));
      socket.join('user_' + rows[0].id); socket.join('role_' + rows[0].role);
      socket.data.userId = rows[0].id;
      if (rows[0].role === 'hospital') { // doctors get live synthetic wearable samples for their own patients
        const doc = await dbq('SELECT d.id FROM doctors d JOIN hospital_users hu ON hu.user_id = d.user_id WHERE d.user_id = ? AND d.active = 1 LIMIT 1', [rows[0].id]);
        if (doc.length) socket.data.doctorId = doc[0].id;
      }
      next();
    } catch { next(new Error('unavailable')); }
  });
  rt.io = io;
  // Real-time Digital Twin stream: the synthetic wearable streams advance every 5 minutes; each new sample (and the
  // re-computed 2-h prediction) is pushed only to the doctor whose patients they belong to.
  let lastSlot = 0;
  const pushTwinSamples = async (force) => {
    const slot = Math.floor(Date.now() / 300000);
    if (!force && slot === lastSlot) return;
    lastSlot = slot;
    for (const [, socket] of io.sockets.sockets) {
      if (!socket.data.doctorId) continue;
      try {
        const pats = await twinPatientsOf(socket.data.doctorId);
        if (pats.length) socket.emit('twin_sample', { at: new Date().toISOString(), samples: pats.map((p) => ({ patient_id: p.patient_id, ...Twin.latestSample(p.patient_id) })) });
      } catch (e) { /* database temporarily unavailable — next tick retries */ }
    }
  };
  setInterval(() => pushTwinSamples(false), 15000).unref();
  io.on('connection', (socket) => { if (socket.data.doctorId) setTimeout(() => pushTwinSamples(true), 500); });
  const PORT = Number(process.env.PORT || 3000);
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`[Q-NIRVANA] server.js listening on port ${PORT} (Socket.IO enabled)`);
    const problem = dbConfigProblem();
    if (problem) console.warn('[Q-NIRVANA] ' + problem);
    else ensureSchema().then(() => console.log(`[Q-NIRVANA] MySQL connected: ${MY.host}:${MY.port}/${MY.database}`)).catch((e) => console.warn('[Q-NIRVANA] ' + e.message));
    setTimeout(() => { try { Twin.train(); console.log('[Q-NIRVANA] digital-twin model trained on synthetic cohort'); } catch (e) { console.warn('twin training failed', e); } }, 100);
  });
}

export default app;

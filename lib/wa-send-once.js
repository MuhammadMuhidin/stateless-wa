import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason,
} from "@whiskeysockets/baileys";
import { Pool } from "pg";
import fs from "fs";
import os from "os";
import path from "path";
import { randomUUID } from "crypto";

const DATABASE_URL = process.env.NEONDB_URI || process.env.DATABASE_URL;
const DEFAULT_CONNECT_TIMEOUT_MS = Number(process.env.WA_CONNECT_TIMEOUT_MS || 180000);
const DEFAULT_AFTER_CONNECT_WAIT_MS = Number(process.env.WA_AFTER_CONNECT_WAIT_MS || 2500);
const PAIRING_CODE_DELAY_MS = Number(process.env.WA_PAIRING_CODE_DELAY_MS || 5000);

if (!DATABASE_URL) {
  throw new Error("NEONDB_URI atau DATABASE_URL wajib diisi.");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const silentLogger = {
  level: "silent",
  child() {
    return this;
  },
  info() {},
  error() {},
  warn() {},
  debug() {},
  trace() {},
  fatal() {},
};

function normalize(value) {
  return String(value || "").trim();
}

function getAuthDir(sessionId) {
  const safeSessionId = normalize(sessionId).replace(/[^a-zA-Z0-9_-]/g, "_") || "main";

  return path.join(os.tmpdir(), `wa-auth-${safeSessionId}-${randomUUID()}`);
}

function formatPairingCode(code) {
  return String(code || "").match(/.{1,4}/g)?.join("-") || code;
}

function removeDirSafe(dir) {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {}
}

async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS wa_session (
      id TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS wa_send_jobs (
      job_id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      chat_id TEXT NOT NULL,
      message TEXT NOT NULL,
      source TEXT,
      period TEXT,
      status TEXT NOT NULL,
      error TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function saveJob({ jobId, sessionId, chatId, text, source, period, status, error = "", metadata = {} }) {
  if (!jobId) return;

  await pool.query(
    `
    INSERT INTO wa_send_jobs (job_id, session_id, chat_id, message, source, period, status, error, metadata, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9::jsonb, NOW())
    ON CONFLICT (job_id)
    DO UPDATE SET
      status = EXCLUDED.status,
      error = EXCLUDED.error,
      metadata = wa_send_jobs.metadata || EXCLUDED.metadata,
      updated_at = NOW()
    `,
    [jobId, sessionId, chatId, text, source, period, status, error, JSON.stringify(metadata)],
  );
}

async function uploadAuth({ authDir, sessionId }) {
  if (!fs.existsSync(authDir)) return 0;

  const files = fs.readdirSync(authDir);
  const data = {};
  let uploaded = 0;

  for (const file of files) {
    const filePath = path.join(authDir, file);
    if (!fs.statSync(filePath).isFile()) continue;

    const content = fs.readFileSync(filePath);
    data[file] = content.toString("base64");
    uploaded += 1;
  }

  await pool.query(
    `
    INSERT INTO wa_session (id, data)
    VALUES ($1, $2)
    ON CONFLICT (id)
    DO UPDATE SET data = $2, updated_at = NOW()
    `,
    [sessionId, JSON.stringify(data)],
  );

  return uploaded;
}

export async function clearStoredSession(sessionId) {
  await pool.query("DELETE FROM wa_session WHERE id = $1", [sessionId]);
}

async function downloadAuth({ authDir, sessionId }) {
  fs.mkdirSync(authDir, { recursive: true });

  const result = await pool.query("SELECT data FROM wa_session WHERE id = $1", [sessionId]);

  if (result.rows.length === 0) return 0;

  const data = typeof result.rows[0].data === "string" ? JSON.parse(result.rows[0].data) : result.rows[0].data;
  let restored = 0;

  for (const file in data) {
    const buffer = Buffer.from(data[file], "base64");
    fs.writeFileSync(path.join(authDir, file), buffer);
    restored += 1;
  }

  return restored;
}

function createConnectionWaiter(timeoutMs) {
  let resolveConnection;
  let rejectConnection;
  let timeout;

  const promise = new Promise((resolve, reject) => {
    resolveConnection = resolve;
    rejectConnection = reject;
    timeout = setTimeout(() => {
      reject(new Error("Timeout menunggu koneksi WhatsApp. Jika pairing code sudah muncul, masukkan kode lalu coba ulangi bila pesan belum terkirim."));
    }, timeoutMs);
  });

  function clear() {
    if (timeout) clearTimeout(timeout);
    timeout = null;
  }

  return {
    promise,
    resolve() {
      clear();
      resolveConnection?.();
    },
    reject(err) {
      clear();
      rejectConnection?.(err);
    },
    clear,
  };
}

async function closeSocket(sock) {
  try {
    sock?.ws?.close?.();
  } catch {}

  try {
    sock?.end?.();
  } catch {}
}

function assertReadyToSend({ sock, isConnected, isLoggedOut, requiresRePair }) {
  if (!sock || !isConnected) {
    throw new Error("WhatsApp belum connected.");
  }

  if (requiresRePair || isLoggedOut) {
    throw new Error("Session WhatsApp perlu pairing ulang.");
  }
}

export async function sendWaOnce(payload, options = {}) {
  const emit = typeof options.emit === "function" ? options.emit : () => {};
  const phoneNumber = normalize(payload.phoneNumber || process.env.PHONE_NUMBER || process.env.WA_PHONE_NUMBER);
  const chatId = normalize(payload.chatId);
  const text = normalize(payload.text);
  const sessionId = normalize(payload.sessionId || "main");
  const source = normalize(payload.source || "vercel-function");
  const period = normalize(payload.period || "");
  const jobId = normalize(payload.jobId || randomUUID());
  const authDir = getAuthDir(sessionId);
  const connectTimeoutMs = Number(payload.connectTimeoutMs || DEFAULT_CONNECT_TIMEOUT_MS);
  const afterConnectWaitMs = Number(payload.afterConnectWaitMs || DEFAULT_AFTER_CONNECT_WAIT_MS);

  let sock = null;
  let isConnected = false;
  let isLoggedOut = false;
  let requiresRePair = false;
  let isPairingRequested = false;
  let lastConnectedAt = null;
  let lastDisconnectAt = null;
  const waiter = createConnectionWaiter(connectTimeoutMs);

  try {
    await ensureTables();
    await saveJob({
      jobId,
      sessionId,
      chatId,
      text,
      source,
      period,
      status: "running",
      metadata: { startedAt: new Date().toISOString(), pairType: "CODE", mode: "SEND" },
    });

    const restored = await downloadAuth({ authDir, sessionId });
    emit({ status: "AUTH_RESTORED", restored });

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
      version,
      auth: state,
      browser: ["Ubuntu", "Chrome", "120.0.0"],
      printQRInTerminal: false,
      logger: silentLogger,
    });

    sock.ev.on("creds.update", async () => {
      await saveCreds();
      await uploadAuth({ authDir, sessionId });
    });

    sock.ev.on("connection.update", ({ connection, lastDisconnect }) => {
      if (connection === "open") {
        isConnected = true;
        isLoggedOut = false;
        requiresRePair = false;
        lastConnectedAt = Date.now();
        emit({ status: "CONNECTED" });
        waiter.resolve();
      }

      if (connection === "close") {
        isConnected = false;
        lastDisconnectAt = Date.now();

        const statusCode = lastDisconnect?.error?.output?.statusCode;

        if (statusCode === DisconnectReason.loggedOut) {
          isLoggedOut = true;
          requiresRePair = true;
          waiter.reject(new Error("Device unpaired/logout. Pairing ulang diperlukan."));
          return;
        }
      }
    });

    if (!state.creds.registered) {
      if (!phoneNumber) {
        throw new Error("phoneNumber wajib diisi saat session belum login untuk pairing code.");
      }

      isPairingRequested = true;
      emit({ status: "PAIRING_REQUESTED" });

      setTimeout(async () => {
        try {
          const code = await sock.requestPairingCode(phoneNumber);
          emit({ status: "PAIRING_CODE", pairingCode: formatPairingCode(code) });
        } catch (err) {
          waiter.reject(new Error(err?.message || "Gagal membuat pairing code."));
        }
      }, PAIRING_CODE_DELAY_MS);
    }

    await waiter.promise;

    if (afterConnectWaitMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, afterConnectWaitMs));
    }

    assertReadyToSend({ sock, isConnected, isLoggedOut, requiresRePair });

    await sock.sendMessage(chatId, { text });
    const uploaded = await uploadAuth({ authDir, sessionId });

    await saveJob({
      jobId,
      sessionId,
      chatId,
      text,
      source,
      period,
      status: "sent",
      metadata: {
        sentAt: new Date().toISOString(),
        lastConnectedAt,
        lastDisconnectAt,
        uploadedAuthFiles: uploaded,
        pairingRequested: isPairingRequested,
      },
    });

    return {
      status: "SENT",
      success: true,
      jobId,
    };
  } catch (err) {
    const message = err?.message || "Gagal mengirim WhatsApp.";

    await saveJob({
      jobId,
      sessionId,
      chatId,
      text,
      source,
      period,
      status: "failed",
      error: message,
      metadata: { failedAt: new Date().toISOString(), pairType: "CODE", mode: "SEND" },
    }).catch(() => {});

    throw err;
  } finally {
    waiter.clear();

    try {
      await uploadAuth({ authDir, sessionId });
    } catch {}

    await closeSocket(sock);
    removeDirSafe(authDir);
  }
}

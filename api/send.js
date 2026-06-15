import { Pool } from "pg";
import { sendWaOnce } from "../lib/wa-send-once.js";

export const config = {
  maxDuration: 300,
};

const DATABASE_URL = process.env.NEONDB_URI || process.env.DATABASE_URL;
const resetPool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    })
  : null;

function writeEvent(res, payload) {
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function getBearerToken(req) {
  const authorization = req.headers.authorization || "";
  const match = authorization.match(/^Bearer\s+(.+)$/i);

  return match?.[1]?.trim() || "";
}

function isAuthorized(req) {
  const expectedToken = process.env.SENDWA_API_KEY || process.env.WA_API_KEY || "";

  if (!expectedToken) return true;

  const bearerToken = getBearerToken(req);
  const headerToken = String(req.headers["x-api-key"] || "").trim();

  return bearerToken === expectedToken || headerToken === expectedToken;
}

function readJsonBody(req) {
  if (req.body && typeof req.body === "object") return Promise.resolve(req.body);

  return new Promise((resolve, reject) => {
    let raw = "";

    req.on("data", (chunk) => {
      raw += chunk;
    });

    req.on("end", () => {
      if (!raw.trim()) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new Error("Body harus JSON valid."));
      }
    });

    req.on("error", reject);
  });
}

function normalizePayload(body = {}) {
  return {
    phoneNumber: String(body.phoneNumber || body.phone_number || body.waPhoneNumber || "").trim(),
    chatId: String(body.chatId || body.chat_id || "").trim(),
    text: String(body.text || body.message || body.messageText || "").trim(),
    sessionId: String(body.sessionId || body.session_id || process.env.WA_SESSION_ID || process.env.SESSION_ID || "main").trim(),
    source: String(body.source || "vercel-function").trim(),
    period: String(body.period || "").trim(),
    jobId: String(body.jobId || body.job_id || "").trim(),
  };
}

function validatePayload(payload) {
  if (!payload.chatId) return "chatId wajib diisi.";
  if (!payload.text) return "text/message wajib diisi.";
  if (!payload.sessionId) return "sessionId tidak valid.";

  return "";
}

function isLoggedOutSessionError(error) {
  const message = String(error?.message || "").toLowerCase();

  return message.includes("loggedout")
    || message.includes("logged out")
    || message.includes("logout")
    || message.includes("unpaired")
    || message.includes("pairing ulang")
    || message.includes("perlu pairing");
}

async function clearStoredSession(sessionId) {
  if (!resetPool) {
    throw new Error("NEONDB_URI atau DATABASE_URL wajib diisi untuk reset session WhatsApp.");
  }

  await resetPool.query("DELETE FROM wa_session WHERE id = $1", [sessionId]);
}

function runSend(payload, res) {
  return sendWaOnce(payload, {
    emit: (event) => writeEvent(res, event),
  });
}

export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    res.status(405).json({ error: "Method not allowed" });
    return;
  }

  if (!isAuthorized(req)) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }

  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  try {
    const body = await readJsonBody(req);
    const payload = normalizePayload(body);
    const validationError = validatePayload(payload);

    if (validationError) {
      writeEvent(res, { status: "FAILED", error: validationError });
      res.end();
      return;
    }

    writeEvent(res, { status: "STARTED", sessionId: payload.sessionId });

    let result;

    try {
      result = await runSend(payload, res);
    } catch (error) {
      const canRetryPairing = Boolean(payload.phoneNumber) && isLoggedOutSessionError(error);

      if (!canRetryPairing) throw error;

      await clearStoredSession(payload.sessionId);
      writeEvent(res, {
        status: "SESSION_RESET",
        sessionId: payload.sessionId,
      });

      result = await runSend(payload, res);
    }

    writeEvent(res, result);
    res.end();
  } catch (err) {
    writeEvent(res, {
      status: "FAILED",
      error: err?.message || "Gagal mengirim WhatsApp.",
    });
    res.end();
  }
}

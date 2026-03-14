// server/index.js — DIYOR MARKET (FULL + EDIT/DELETE + DEBT DETAILS + BOT ADMIN TOOLS)

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const path = require("path");
const fs = require("fs");
const sqlite3 = require("sqlite3").verbose();
const session = require("express-session");
const TelegramBot = require("node-telegram-bot-api");

const app = express();

// ===== CONFIG =====
const PORT = Number(process.env.PORT || 4000);
const APP_USER = String(process.env.APP_USER || "admin").trim();
const APP_PASS = String(process.env.APP_PASS || "12345").trim();
const SESSION_SECRET = String(process.env.SESSION_SECRET || "secret").trim();

const BOT_TOKEN = String(process.env.BOT_TOKEN || "").trim();
const OWNER_CHAT_ID = String(process.env.OWNER_CHAT_ID || "").trim();
const REMINDER_INTERVAL_MIN = Number(process.env.REMINDER_INTERVAL_MIN || 60);

// ===== PATHS =====
const PUBLIC_DIR = path.join(__dirname, "public");
const dbPath = process.env.DB_PATH || "/data/data.db";

// storage papka avtomatik yaratiladi
const storageDir = path.dirname(dbPath);
if (!fs.existsSync(storageDir)) {
  fs.mkdirSync(storageDir, { recursive: true });
}

// ===== GLOBAL ERROR HANDLERS =====
process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED REJECTION:", reason);
});

// ===== MIDDLEWARE =====
app.use(
  cors({
    origin: true,
    credentials: true,
  })
);

app.use(express.json({ limit: "2mb" }));

app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: { httpOnly: true },
  })
);

// ===== DB =====
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error("DB CONNECTION ERROR:", err.message);
  } else {
    console.log("✅ SQLite connected:", dbPath);
  }
});

function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function runCallback(err) {
      if (err) return reject(err);
      resolve({ id: this.lastID, changes: this.changes });
    });
  });
}

function get(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) return reject(err);
      resolve(row);
    });
  });
}

function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

// ===== HELPERS =====
function normalizePhone(phone = "") {
  return String(phone).replace(/[^\d]/g, "");
}

function phonesMatch(a = "", b = "") {
  const x = normalizePhone(a);
  const y = normalizePhone(b);
  if (!x || !y) return false;
  return x === y || x.endsWith(y) || y.endsWith(x);
}

function parseDateInput(input = "") {
  const v = String(input).trim();

  if (/^\d{4}-\d{2}-\d{2}$/.test(v)) return v;

  const m = v.match(/^(\d{2})[./-](\d{2})[./-](\d{4})$/);
  if (m) {
    const dd = m[1];
    const mm = m[2];
    const yyyy = m[3];
    return `${yyyy}-${mm}-${dd}`;
  }

  return null;
}

function isAdminChat(chatId) {
  return String(chatId) === String(OWNER_CHAT_ID);
}

function money(n) {
  return Number(n || 0).toFixed(0);
}

function adminCredsOk(username, password) {
  return String(username || "").trim() === APP_USER && String(password || "").trim() === APP_PASS;
}

function safeText(v) {
  return String(v ?? "").trim();
}

function formatDateTime(v) {
  if (!v) return "-";
  return String(v).replace("T", " ").slice(0, 19);
}

// ===== DB INIT =====
async function initDb() {
  await run(`
    CREATE TABLE IF NOT EXISTS customers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      phone TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      price REAL NOT NULL DEFAULT 0,
      stock REAL NOT NULL DEFAULT 0,
      unit_type TEXT NOT NULL DEFAULT 'piece',
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS debts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_id INTEGER NOT NULL,
      total REAL NOT NULL DEFAULT 0,
      paid REAL NOT NULL DEFAULT 0,
      note TEXT,
      due_date TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY(customer_id) REFERENCES customers(id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS payments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      debt_id INTEGER NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      note TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY(debt_id) REFERENCES debts(id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS customer_telegram_links (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_id INTEGER NOT NULL,
      telegram_chat_id TEXT NOT NULL UNIQUE,
      telegram_user_id TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY(customer_id) REFERENCES customers(id)
    )
  `);

  // Qarz tafsilotlari uchun har bir yozilgan qarz/to‘lov eventlari
  await run(`
    CREATE TABLE IF NOT EXISTS debt_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      debt_id INTEGER NOT NULL,
      customer_id INTEGER NOT NULL,
      event_type TEXT NOT NULL DEFAULT 'debt_add', -- debt_add | payment
      amount REAL NOT NULL DEFAULT 0,
      note TEXT,
      product_id INTEGER,
      qty REAL,
      due_date TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY(debt_id) REFERENCES debts(id),
      FOREIGN KEY(customer_id) REFERENCES customers(id),
      FOREIGN KEY(product_id) REFERENCES products(id)
    )
  `);

  try {
    await run(`ALTER TABLE debts ADD COLUMN due_date TEXT`);
  } catch (_) {}

  try {
    await run(`ALTER TABLE products ADD COLUMN unit_type TEXT NOT NULL DEFAULT 'piece'`);
  } catch (_) {}

  await run(`CREATE INDEX IF NOT EXISTS idx_debts_customer ON debts(customer_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_payments_debt ON payments(debt_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_products_name ON products(name)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_links_customer_id ON customer_telegram_links(customer_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_debt_events_debt ON debt_events(debt_id)`);
  await run(`CREATE INDEX IF NOT EXISTS idx_debt_events_customer ON debt_events(customer_id)`);
}

// ===== AUTH =====
function requireAuth(req, res, next) {
  if (req.session?.user === "admin") return next();
  return res.status(401).json({ ok: false, error: "Unauthorized" });
}

// protect pages BEFORE static
const protectedPages = [
  "/index.html",
  "/customers.html",
  "/products.html",
  "/debts.html",
  "/today-debts.html",
  "/today-payments.html",
  "/settings.html",
];

app.use((req, res, next) => {
  if (req.path === "/" || req.path === "/login" || req.path === "/login.html") return next();
  if (req.path === "/style.css" || req.path === "/app.js") return next();

  if (protectedPages.includes(req.path)) {
    if (req.session?.user === "admin") return next();
    return res.redirect("/login.html");
  }

  return next();
});

app.use(express.static(PUBLIC_DIR));

app.get("/", (req, res) => res.redirect("/login.html"));
app.get("/login", (req, res) => res.redirect("/login.html"));

app.post("/login", (req, res) => {
  const { username, password } = req.body || {};

  if (adminCredsOk(username, password)) {
    req.session.user = "admin";
    return res.json({ ok: true });
  }

  return res.status(401).json({ ok: false, error: "Login yoki parol xato" });
});

app.post("/logout", (req, res) => {
  req.session.destroy(() => res.json({ ok: true }));
});

// ===== BOT STATE =====
let bot = null;
let reminderInterval = null;
const userStates = new Map();

function setUserState(chatId, state) {
  userStates.set(String(chatId), state);
}

function getUserState(chatId) {
  return userStates.get(String(chatId)) || null;
}

function clearUserState(chatId) {
  userStates.delete(String(chatId));
}

// ===== COMMON DATA HELPERS =====
async function addDebtEvent({
  debt_id,
  customer_id,
  event_type = "debt_add",
  amount = 0,
  note = null,
  product_id = null,
  qty = null,
  due_date = null,
}) {
  await run(
    `
    INSERT INTO debt_events(
      debt_id, customer_id, event_type, amount, note, product_id, qty, due_date
    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
    `,
    [
      Number(debt_id),
      Number(customer_id),
      String(event_type),
      Number(amount || 0),
      note || null,
      product_id ? Number(product_id) : null,
      qty != null ? Number(qty) : null,
      due_date || null,
    ]
  );
}

async function getDebtHeader(debtId) {
  return await get(
    `
    SELECT
      d.*,
      c.name AS customer_name,
      c.phone AS customer_phone,
      (d.total - d.paid) AS remaining,
      CASE
        WHEN d.due_date IS NOT NULL
         AND date(d.due_date) < date('now','localtime')
         AND (d.total - d.paid) > 0
        THEN 1 ELSE 0
      END AS overdue
    FROM debts d
    JOIN customers c ON c.id = d.customer_id
    WHERE d.id = ?
    LIMIT 1
    `,
    [Number(debtId)]
  );
}

async function getDebtTimeline(debtId) {
  return await all(
    `
    SELECT
      e.*,
      p.name AS product_name
    FROM debt_events e
    LEFT JOIN products p ON p.id = e.product_id
    WHERE e.debt_id = ?
    ORDER BY datetime(e.created_at) ASC, e.id ASC
    `,
    [Number(debtId)]
  );
}

async function getCustomerSummary(customerId) {
  const customer = await get(`SELECT * FROM customers WHERE id = ?`, [Number(customerId)]);
  if (!customer) return null;

  const totalRemainingRow = await get(
    `SELECT COALESCE(SUM(total - paid), 0) AS remaining FROM debts WHERE customer_id = ? AND (total - paid) > 0`,
    [Number(customerId)]
  );

  const debtsCountRow = await get(`SELECT COUNT(*) AS c FROM debts WHERE customer_id = ?`, [Number(customerId)]);

  return {
    customer,
    remaining: Number(totalRemainingRow?.remaining || 0),
    debts_count: Number(debtsCountRow?.c || 0),
  };
}

async function buildDebtDetailsText(debtId) {
  const debt = await getDebtHeader(debtId);
  if (!debt) return "Qarz topilmadi.";

  const timeline = await getDebtTimeline(debtId);

  const head =
    `🧾 <b>Qarz tafsiloti #${debt.id}</b>\n` +
    `👤 ${debt.customer_name}\n` +
    `📞 ${debt.customer_phone || "-"}\n` +
    `💸 Jami: <b>${money(debt.total)}</b>\n` +
    `✅ To‘langan: <b>${money(debt.paid)}</b>\n` +
    `💰 Qolgan: <b>${money(debt.remaining)}</b>\n` +
    `📅 Due: ${debt.due_date || "-"}\n` +
    `📝 Asosiy izoh: ${debt.note || "-"}\n\n`;

  if (!timeline.length) {
    return head + "Tafsilot eventlari yo‘q.";
  }

  const lines = timeline
    .map((x, i) => {
      const when = formatDateTime(x.created_at);
      if (x.event_type === "payment") {
        return `${i + 1}) ${when}\n   ✅ TO‘LOV: <b>${money(x.amount)}</b>\n   📝 ${x.note || "-"}`;
      }

      const itemName = x.product_name || (x.product_id ? `Mahsulot #${x.product_id}` : "Qo‘lda/Eski qarz");
      const qtyText = x.qty != null ? `\n   🔢 Miqdor: ${x.qty}` : "";
      const dueText = x.due_date ? `\n   📅 Due: ${x.due_date}` : "";
      return (
        `${i + 1}) ${when}\n` +
        `   ➕ QARZ: <b>${money(x.amount)}</b>\n` +
        `   📦 ${itemName}${qtyText}${dueText}\n` +
        `   📝 ${x.note || "-"}`
      );
    })
    .join("\n\n");

  return head + lines;
}

async function buildCustomerManageText(customerId) {
  const summary = await getCustomerSummary(customerId);
  if (!summary) return "Mijoz topilmadi.";

  return (
    `👤 <b>Mijoz</b>\n` +
    `ID: <b>${summary.customer.id}</b>\n` +
    `Ism: <b>${summary.customer.name}</b>\n` +
    `Telefon: <b>${summary.customer.phone || "-"}</b>\n` +
    `Qarzlar soni: <b>${summary.debts_count}</b>\n` +
    `Umumiy qolgan qarz: <b>${money(summary.remaining)}</b>`
  );
}

async function deleteDebtDeep(debtId) {
  const debt = await get(`SELECT * FROM debts WHERE id = ?`, [Number(debtId)]);
  if (!debt) {
    return { ok: false, error: "Qarz topilmadi" };
  }

  // Mahsulot stockni qaytarish
  const restoreRows = await all(
    `
    SELECT product_id, COALESCE(SUM(qty),0) AS qty_sum
    FROM debt_events
    WHERE debt_id = ?
      AND event_type = 'debt_add'
      AND product_id IS NOT NULL
      AND qty IS NOT NULL
    GROUP BY product_id
    `,
    [Number(debtId)]
  );

  for (const row of restoreRows) {
    if (row.product_id && Number(row.qty_sum || 0) > 0) {
      await run(`UPDATE products SET stock = stock + ? WHERE id = ?`, [Number(row.qty_sum), Number(row.product_id)]);
    }
  }

  await run(`DELETE FROM payments WHERE debt_id = ?`, [Number(debtId)]);
  await run(`DELETE FROM debt_events WHERE debt_id = ?`, [Number(debtId)]);
  await run(`DELETE FROM debts WHERE id = ?`, [Number(debtId)]);

  return { ok: true, customer_id: debt.customer_id };
}

async function deleteCustomerDeep(customerId) {
  const customer = await get(`SELECT * FROM customers WHERE id = ?`, [Number(customerId)]);
  if (!customer) {
    return { ok: false, error: "Mijoz topilmadi" };
  }

  const debts = await all(`SELECT id FROM debts WHERE customer_id = ? ORDER BY id ASC`, [Number(customerId)]);
  for (const d of debts) {
    const r = await deleteDebtDeep(d.id);
    if (!r.ok) return r;
  }

  await run(`DELETE FROM customer_telegram_links WHERE customer_id = ?`, [Number(customerId)]);
  await run(`DELETE FROM customers WHERE id = ?`, [Number(customerId)]);

  return { ok: true, customer };
}

// ===== BOT HELPERS =====
function botEnabled() {
  return Boolean(BOT_TOKEN && OWNER_CHAT_ID);
}

async function notifyAdmin(text) {
  if (!botEnabled() || !bot) return;
  try {
    await bot.sendMessage(OWNER_CHAT_ID, text, { parse_mode: "HTML" });
  } catch (e) {
    console.error("Telegram notify error:", e.message);
  }
}

function getAdminMenuKeyboard() {
  return {
    keyboard: [
      [{ text: "📊 Statistika" }, { text: "👥 Mijozlar" }],
      [{ text: "📦 Mahsulotlar" }, { text: "🧾 Bugungi qarzlar" }],
      [{ text: "✅ Bugungi to‘lovlar" }, { text: "🏆 Top qarzdorlar" }],
      [{ text: "➕ Qarz yozish" }, { text: "👤 Mijoz qo‘shish" }],
      [{ text: "✏️ Mijoz tahrirlash" }, { text: "📄 Qarz tafsiloti" }],
      [{ text: "🗑️ Qarz o‘chirish" }],
    ],
    resize_keyboard: true,
  };
}

function getCustomerMenuKeyboard() {
  return {
    keyboard: [[{ text: "💳 Mening qarzim" }], [{ text: "📱 Telefon raqamni yuborish", request_contact: true }]],
    resize_keyboard: true,
  };
}

function getCustomerManageInline(customerId) {
  return {
    inline_keyboard: [
      [{ text: "✏️ Ismni tahrirlash", callback_data: `customer_edit_name_${customerId}` }],
      [{ text: "📱 Telefonni tahrirlash", callback_data: `customer_edit_phone_${customerId}` }],
      [{ text: "📄 Qarz tafsilotlari", callback_data: `customer_debt_list_${customerId}` }],
      [{ text: "🗑️ Mijozni o‘chirish", callback_data: `customer_delete_${customerId}` }],
      [{ text: "❌ Bekor qilish", callback_data: "flow_cancel" }],
    ],
  };
}

async function sendAdminHome(chatId) {
  await bot.sendMessage(chatId, "Admin menu tayyor ✅", {
    reply_markup: getAdminMenuKeyboard(),
  });
}

async function sendCustomerHome(chatId) {
  const linked = await getLinkedCustomerByChat(chatId);

  if (linked) {
    await bot.sendMessage(chatId, `Assalomu alaykum, ${linked.name}.\nPastdagi menyudan foydalaning.`, {
      reply_markup: getCustomerMenuKeyboard(),
    });
  } else {
    await bot.sendMessage(
      chatId,
      "Telefon raqamingizni yuboring. Agar u bizning bazadagi raqam bilan mos kelsa, qarzingiz ko‘rsatiladi.",
      { reply_markup: getCustomerMenuKeyboard() }
    );
  }
}

async function buildStatsText() {
  const totalCustomers = (await get(`SELECT COUNT(*) AS c FROM customers`))?.c || 0;

  const totalDebtRow = await get(`
    SELECT COALESCE(SUM(total - paid),0) AS remaining
    FROM debts
    WHERE (total - paid) > 0
  `);

  const todayDebtsRow = await get(`
    SELECT COALESCE(SUM(total),0) AS s
    FROM debts
    WHERE date(created_at) = date('now','localtime')
  `);

  const todayPaysRow = await get(`
    SELECT COALESCE(SUM(amount),0) AS s
    FROM payments
    WHERE date(created_at) = date('now','localtime')
  `);

  const top = await all(`
    SELECT c.name, c.phone, SUM(d.total - d.paid) AS remaining
    FROM debts d
    JOIN customers c ON c.id = d.customer_id
    WHERE (d.total - d.paid) > 0
    GROUP BY d.customer_id
    ORDER BY remaining DESC
    LIMIT 5
  `);

  const topText = top.length
    ? top.map((x, i) => `${i + 1}) ${x.name} — ${money(x.remaining)}`).join("\n")
    : "Top qarzdorlar yo‘q.";

  return (
    `📊 <b>Diyor Market Stats</b>\n` +
    `👥 Mijozlar: <b>${totalCustomers}</b>\n` +
    `💸 Umumiy qolgan qarz: <b>${money(totalDebtRow?.remaining || 0)}</b>\n` +
    `🧾 Bugungi qarzlar: <b>${money(todayDebtsRow?.s || 0)}</b>\n` +
    `✅ Bugungi to‘lovlar: <b>${money(todayPaysRow?.s || 0)}</b>\n\n` +
    `🏆 <b>Top 5 qarzdor:</b>\n${topText}`
  );
}

async function sendCustomersList(chatId) {
  const rows = await all(`
    SELECT c.id, c.name, c.phone, COALESCE(SUM(d.total - d.paid),0) AS remaining
    FROM customers c
    LEFT JOIN debts d ON d.customer_id = c.id
    GROUP BY c.id
    ORDER BY c.id DESC
    LIMIT 20
  `);

  const text = rows.length
    ? "👥 <b>Oxirgi mijozlar</b>\n\n" +
      rows.map((r) => `#${r.id} ${r.name} | ${r.phone || "-"} | qolgan: <b>${money(r.remaining)}</b>`).join("\n")
    : "Mijozlar yo‘q.";

  await bot.sendMessage(chatId, text, { parse_mode: "HTML" });
}

async function sendProductsList(chatId) {
  const rows = await all(`
    SELECT *
    FROM products
    ORDER BY id DESC
    LIMIT 20
  `);

  const text = rows.length
    ? "📦 <b>Mahsulotlar</b>\n\n" +
      rows.map((r) => `#${r.id} ${r.name} | narx: <b>${money(r.price)}</b> | stock: <b>${r.stock}</b> | ${r.unit_type}`).join("\n")
    : "Mahsulotlar yo‘q.";

  await bot.sendMessage(chatId, text, { parse_mode: "HTML" });
}

async function sendTodayDebts(chatId) {
  const rows = await all(`
    SELECT d.id, c.name, d.total, d.paid, (d.total - d.paid) AS remaining, d.note
    FROM debts d
    JOIN customers c ON c.id = d.customer_id
    WHERE date(d.created_at) = date('now','localtime')
    ORDER BY d.id DESC
    LIMIT 30
  `);

  const text = rows.length
    ? "🧾 <b>Bugungi qarzlar</b>\n\n" +
      rows.map((r) => `#${r.id} ${r.name}: <b>${money(r.total)}</b> | qolgan: <b>${money(r.remaining)}</b> | ${r.note || ""}`).join("\n")
    : "Bugun qarz yozilmagan.";

  await bot.sendMessage(chatId, text, { parse_mode: "HTML" });
}

async function sendTodayPayments(chatId) {
  const rows = await all(`
    SELECT p.id, c.name, p.amount, p.note
    FROM payments p
    JOIN debts d ON d.id = p.debt_id
    JOIN customers c ON c.id = d.customer_id
    WHERE date(p.created_at) = date('now','localtime')
    ORDER BY p.id DESC
    LIMIT 30
  `);

  const text = rows.length
    ? "✅ <b>Bugungi to‘lovlar</b>\n\n" +
      rows.map((r) => `#${r.id} ${r.name}: <b>${money(r.amount)}</b> | ${r.note || ""}`).join("\n")
    : "Bugun to‘lov bo‘lmagan.";

  await bot.sendMessage(chatId, text, { parse_mode: "HTML" });
}

async function sendTopDebtors(chatId) {
  const rows = await all(`
    SELECT c.id, c.name, SUM(d.total - d.paid) AS remaining
    FROM debts d
    JOIN customers c ON c.id = d.customer_id
    WHERE (d.total - d.paid) > 0
    GROUP BY d.customer_id
    ORDER BY remaining DESC
    LIMIT 20
  `);

  const text = rows.length
    ? "🏆 <b>Top qarzdorlar</b>\n\n" +
      rows.map((r, i) => `${i + 1}) ${r.name} — <b>${money(r.remaining)}</b>`).join("\n")
    : "Top qarzdorlar yo‘q.";

  await bot.sendMessage(chatId, text, { parse_mode: "HTML" });
}

async function findCustomerByPhone(phone) {
  const rows = await all(`SELECT * FROM customers WHERE phone IS NOT NULL AND TRIM(phone) <> ''`);
  return rows.find((r) => phonesMatch(r.phone, phone)) || null;
}

async function linkCustomerToChat(customerId, chatId, userId) {
  const existing = await get(`SELECT * FROM customer_telegram_links WHERE telegram_chat_id = ?`, [String(chatId)]);

  if (existing) {
    await run(
      `
      UPDATE customer_telegram_links
      SET customer_id = ?, telegram_user_id = ?
      WHERE telegram_chat_id = ?
      `,
      [Number(customerId), String(userId || ""), String(chatId)]
    );
    return;
  }

  await run(
    `
    INSERT INTO customer_telegram_links(customer_id, telegram_chat_id, telegram_user_id)
    VALUES(?, ?, ?)
    `,
    [Number(customerId), String(chatId), String(userId || "")]
  );
}

async function getLinkedCustomerByChat(chatId) {
  return await get(
    `
    SELECT c.*
    FROM customer_telegram_links l
    JOIN customers c ON c.id = l.customer_id
    WHERE l.telegram_chat_id = ?
    LIMIT 1
    `,
    [String(chatId)]
  );
}

async function buildCustomerDebtText(customerId) {
  const customer = await get(`SELECT * FROM customers WHERE id = ?`, [Number(customerId)]);
  if (!customer) return "Mijoz topilmadi.";

  const debts = await all(
    `
    SELECT id, total, paid, note, due_date, created_at, (total - paid) AS remaining
    FROM debts
    WHERE customer_id = ?
    ORDER BY id DESC
    LIMIT 20
    `,
    [Number(customerId)]
  );

  const totalRemainingRow = await get(
    `
    SELECT COALESCE(SUM(total - paid), 0) AS remaining
    FROM debts
    WHERE customer_id = ?
      AND (total - paid) > 0
    `,
    [Number(customerId)]
  );

  const totalRemaining = Number(totalRemainingRow?.remaining || 0);

  if (!debts.length) {
    return `👤 <b>${customer.name}</b>\n📞 ${customer.phone || "-"}\n✅ Sizda qarz yo‘q.`;
  }

  const lines = debts
    .map((d, i) => {
      return (
        `${i + 1}) #${d.id}\n` +
        `   Jami: <b>${money(d.total)}</b>\n` +
        `   To‘langan: <b>${money(d.paid)}</b>\n` +
        `   Qolgan: <b>${money(d.remaining)}</b>\n` +
        `   Izoh: ${d.note || "-"}\n` +
        `   Qaytarish sanasi: ${d.due_date || "-"}`
      );
    })
    .join("\n\n");

  return (
    `👤 <b>${customer.name}</b>\n` +
    `📞 ${customer.phone || "-"}\n` +
    `💸 Umumiy qolgan qarz: <b>${money(totalRemaining)}</b>\n\n` +
    `${lines}`
  );
}

async function createOrMergeDebt({ customer_id, total, note, due_date, items = [] }) {
  const open = await get(
    `
    SELECT * FROM debts
    WHERE customer_id = ?
      AND (total - paid) > 0
    ORDER BY id DESC
    LIMIT 1
    `,
    [Number(customer_id)]
  );

  let debtId;

  if (open) {
    const mergedNote = (open.note ? open.note + " | " : "") + (note || `Qarz qo‘shildi (+${total})`);

    await run(
      `
      UPDATE debts
      SET total = total + ?,
          note = ?,
          due_date = COALESCE(?, due_date)
      WHERE id = ?
      `,
      [Number(total), mergedNote, due_date || null, Number(open.id)]
    );

    debtId = open.id;
  } else {
    const r = await run(
      `
      INSERT INTO debts(customer_id, total, paid, note, due_date)
      VALUES(?, ?, 0, ?, ?)
      `,
      [Number(customer_id), Number(total), note || null, due_date || null]
    );

    debtId = r.id;
  }

  let firstProductId = null;
  let totalQty = null;

  for (const it of items) {
    const pid = Number(it.product_id || 0);
    const qty = Number(it.qty || 0);

    if (pid && qty > 0) {
      await run(`UPDATE products SET stock = stock - ? WHERE id = ?`, [qty, pid]);

      if (!firstProductId) firstProductId = pid;
      totalQty = (Number(totalQty || 0) + qty);
    }
  }

  await addDebtEvent({
    debt_id: debtId,
    customer_id: customer_id,
    event_type: "debt_add",
    amount: total,
    note: note || null,
    product_id: firstProductId,
    qty: totalQty,
    due_date: due_date || null,
  });

  return { debtId, merged: Boolean(open) };
}

async function askDebtModeAfterCustomer(chatId) {
  await bot.sendMessage(chatId, "Qanday qarz yozamiz?", {
    reply_markup: {
      inline_keyboard: [
        [{ text: "💵 Qo‘lda summa yozish", callback_data: "debt_type_manual" }],
        [{ text: "📦 Mahsulot tanlash", callback_data: "debt_type_product" }],
        [{ text: "❌ Bekor qilish", callback_data: "flow_cancel" }],
      ],
    },
  });
}

async function showDebtCustomerPickMenu(chatId) {
  await bot.sendMessage(chatId, "Qarz yozish uchun variant tanlang:", {
    reply_markup: {
      inline_keyboard: [
        [{ text: "👥 Mijoz tanlash", callback_data: "debt_customer_existing" }],
        [{ text: "👤 Mijoz qo‘shish", callback_data: "debt_customer_new" }],
        [{ text: "❌ Bekor qilish", callback_data: "flow_cancel" }],
      ],
    },
  });
}

async function showCustomerSearchResults(chatId, query, mode = "debt") {
  const rows = await all(
    `SELECT * FROM customers WHERE name LIKE ? OR phone LIKE ? ORDER BY id DESC LIMIT 20`,
    [`%${query}%`, `%${query}%`]
  );

  if (!rows.length) {
    await bot.sendMessage(chatId, "Mijoz topilmadi. Yana boshqa ism yoki raqam yuboring.");
    return;
  }

  const buttons = rows.map((r) => {
    const callback =
      mode === "manage"
        ? `manage_pick_customer_${r.id}`
        : `debt_pick_customer_${r.id}`;

    return [{ text: `${r.name} (${r.phone || "-"})`, callback_data: callback }];
  });

  if (mode === "debt") {
    buttons.push([{ text: "👤 Yangi mijoz qo‘shish", callback_data: "debt_customer_new" }]);
  }

  buttons.push([{ text: "❌ Bekor qilish", callback_data: "flow_cancel" }]);

  await bot.sendMessage(chatId, "Mijozni tanlang:", {
    reply_markup: { inline_keyboard: buttons },
  });
}

async function showProductPickMenu(chatId) {
  const rows = await all(`SELECT * FROM products ORDER BY id DESC LIMIT 20`);

  if (!rows.length) {
    await bot.sendMessage(chatId, "Mahsulot yo‘q. Qo‘lda summa yozishdan foydalaning.");
    setUserState(chatId, { step: "debt_wait_manual_amount", data: getUserState(chatId)?.data || {} });
    await bot.sendMessage(chatId, "Qarz summasini yuboring:");
    return;
  }

  const buttons = rows.map((r) => [
    { text: `${r.name} • ${money(r.price)} • stock:${r.stock}`, callback_data: `debt_pick_product_${r.id}` },
  ]);

  buttons.push([{ text: "💵 Qo‘lda summa yozish", callback_data: "debt_type_manual" }]);
  buttons.push([{ text: "❌ Bekor qilish", callback_data: "flow_cancel" }]);

  await bot.sendMessage(chatId, "Mahsulotni tanlang:", {
    reply_markup: { inline_keyboard: buttons },
  });
}

async function askDebtConfirm(chatId, data) {
  const customer = await get(`SELECT * FROM customers WHERE id = ?`, [Number(data.customer_id)]);
  const product = data.product_id ? await get(`SELECT * FROM products WHERE id = ?`, [Number(data.product_id)]) : null;

  const summary =
    `Tasdiqlaysizmi?\n\n` +
    `👤 Mijoz: ${customer?.name || "-"}\n` +
    `📦 Mahsulot: ${product ? product.name : "qo‘lda yozilgan"}\n` +
    `🔢 Miqdor: ${data.qty || "-"}\n` +
    `💸 Summa: ${money(data.total)}\n` +
    `📅 Due date: ${data.due_date || "-"}\n` +
    `📝 Izoh: ${data.note || "-"}`;

  await bot.sendMessage(chatId, summary, {
    reply_markup: {
      inline_keyboard: [
        [{ text: "✅ Tasdiqlash", callback_data: "debt_confirm_yes" }],
        [{ text: "❌ Bekor qilish", callback_data: "flow_cancel" }],
      ],
    },
  });
}

// ===== TELEGRAM BOT =====
function startBot() {
  if (!botEnabled()) {
    console.log("🤖 Bot disabled: BOT_TOKEN or OWNER_CHAT_ID missing");
    return;
  }

  bot = new TelegramBot(BOT_TOKEN, { polling: true });

  bot.on("polling_error", (e) => {
    const msg = String(e?.message || "");
    console.error("Bot polling error:", msg);

    if (msg.includes("401") || msg.toLowerCase().includes("unauthorized")) {
      try {
        bot.stopPolling();
      } catch (_) {}
    }
  });

  bot.onText(/\/start/, async (msg) => {
    const chatId = msg.chat.id;

    clearUserState(chatId);

    if (isAdminChat(chatId)) {
      await sendAdminHome(chatId);
      return;
    }

    await sendCustomerHome(chatId);
  });

  bot.on("contact", async (msg) => {
    const chatId = msg.chat.id;
    const phone = msg.contact?.phone_number || "";

    const customer = await findCustomerByPhone(phone);

    if (!customer) {
      await bot.sendMessage(chatId, "Bu raqam bazada topilmadi. Admin bilan bog‘laning.", {
        reply_markup: getCustomerMenuKeyboard(),
      });
      return;
    }

    await linkCustomerToChat(customer.id, chatId, msg.from?.id);
    const text = await buildCustomerDebtText(customer.id);

    await bot.sendMessage(chatId, `✅ Raqam tasdiqlandi.\n\n${text}`, {
      parse_mode: "HTML",
      reply_markup: getCustomerMenuKeyboard(),
    });
  });

  bot.on("callback_query", async (q) => {
    const chatId = q.message?.chat?.id;
    if (!chatId) return;

    const data = q.data || "";

    try {
      if (isAdminChat(chatId)) {
        if (data === "flow_cancel") {
          clearUserState(chatId);
          await bot.sendMessage(chatId, "Bekor qilindi.");
          await sendAdminHome(chatId);
          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data === "debt_customer_existing") {
          setUserState(chatId, { step: "debt_wait_customer_search", data: {} });
          await bot.sendMessage(chatId, "Mijoz ismini yoki telefonini yuboring:");
          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data === "debt_customer_new") {
          setUserState(chatId, {
            step: "add_customer_wait_name",
            data: { continueDebt: true },
          });
          await bot.sendMessage(chatId, "Yangi mijoz ismini yuboring:");
          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data.startsWith("debt_pick_customer_")) {
          const customerId = Number(data.replace("debt_pick_customer_", ""));
          const st = getUserState(chatId) || { step: "", data: {} };

          st.step = "debt_after_customer_selected";
          st.data = { ...(st.data || {}), customer_id: customerId };
          setUserState(chatId, st);

          const c = await get(`SELECT * FROM customers WHERE id = ?`, [customerId]);
          await bot.sendMessage(chatId, `Mijoz tanlandi: ${c?.name || customerId}`);
          await askDebtModeAfterCustomer(chatId);
          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data === "debt_type_manual") {
          const st = getUserState(chatId) || { data: {} };
          st.step = "debt_wait_manual_amount";
          setUserState(chatId, st);
          await bot.sendMessage(chatId, "Qarz summasini yuboring:");
          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data === "debt_type_product") {
          const st = getUserState(chatId) || { data: {} };
          st.step = "debt_wait_product_pick";
          setUserState(chatId, st);
          await showProductPickMenu(chatId);
          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data.startsWith("debt_pick_product_")) {
          const productId = Number(data.replace("debt_pick_product_", ""));
          const product = await get(`SELECT * FROM products WHERE id = ?`, [productId]);

          if (!product) {
            await bot.sendMessage(chatId, "Mahsulot topilmadi.");
          } else {
            const st = getUserState(chatId) || { data: {} };
            st.step = "debt_wait_qty";
            st.data = {
              ...(st.data || {}),
              product_id: productId,
              product_name: product.name,
              product_price: Number(product.price || 0),
            };
            setUserState(chatId, st);

            await bot.sendMessage(chatId, `Mahsulot: ${product.name}\nNarx: ${money(product.price)}\nMiqdor yuboring:`);
          }

          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data === "debt_confirm_yes") {
          const st = getUserState(chatId);

          if (!st?.data?.customer_id || !st?.data?.total) {
            await bot.sendMessage(chatId, "Ma'lumot topilmadi. Qaytadan boshlang.");
            await bot.answerCallbackQuery(q.id);
            return;
          }

          const items = st.data.product_id ? [{ product_id: st.data.product_id, qty: Number(st.data.qty || 0) }] : [];

          const result = await createOrMergeDebt({
            customer_id: Number(st.data.customer_id),
            total: Number(st.data.total),
            note: st.data.note || null,
            due_date: st.data.due_date || null,
            items,
          });

          const c = await get(`SELECT * FROM customers WHERE id = ?`, [Number(st.data.customer_id)]);

          await notifyAdmin(
            `🧾 <b>Yangi qarz (BOT)</b>\n` +
              `👤 ${c?.name || "Mijoz"}\n` +
              `💸 Summa: <b>${money(st.data.total)}</b>\n` +
              `📅 Muddat: <b>${st.data.due_date || "-"}</b>\n` +
              `📝 ${st.data.note || ""}`
          );

          clearUserState(chatId);
          await bot.sendMessage(chatId, result.merged ? "✅ Qarz mavjud ochiq qarzga qo‘shildi." : "✅ Yangi qarz saqlandi.");
          await sendAdminHome(chatId);
          await bot.answerCallbackQuery(q.id);
          return;
        }

        // ===== CUSTOMER MANAGE =====
        if (data.startsWith("manage_pick_customer_")) {
          const customerId = Number(data.replace("manage_pick_customer_", ""));
          const text = await buildCustomerManageText(customerId);

          await bot.sendMessage(chatId, text, {
            parse_mode: "HTML",
            reply_markup: getCustomerManageInline(customerId),
          });

          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data.startsWith("customer_edit_name_")) {
          const customerId = Number(data.replace("customer_edit_name_", ""));
          setUserState(chatId, { step: "customer_edit_name_wait", data: { customer_id: customerId } });
          await bot.sendMessage(chatId, "Yangi ismni yuboring:");
          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data.startsWith("customer_edit_phone_")) {
          const customerId = Number(data.replace("customer_edit_phone_", ""));
          setUserState(chatId, { step: "customer_edit_phone_wait", data: { customer_id: customerId } });
          await bot.sendMessage(chatId, "Yangi telefon raqamni yuboring:");
          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data.startsWith("customer_debt_list_")) {
          const customerId = Number(data.replace("customer_debt_list_", ""));
          const debts = await all(
            `
            SELECT id, total, paid, (total - paid) AS remaining, due_date
            FROM debts
            WHERE customer_id = ?
            ORDER BY id DESC
            LIMIT 20
            `,
            [customerId]
          );

          if (!debts.length) {
            await bot.sendMessage(chatId, "Bu mijozda qarz yo‘q.");
          } else {
            const buttons = debts.map((d) => [
              {
                text: `#${d.id} • ${money(d.remaining)} qolgan`,
                callback_data: `show_debt_details_${d.id}`,
              },
            ]);
            buttons.push([{ text: "❌ Bekor qilish", callback_data: "flow_cancel" }]);

            await bot.sendMessage(chatId, "Qaysi qarz tafsilotini ko‘rasiz?", {
              reply_markup: { inline_keyboard: buttons },
            });
          }

          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data.startsWith("customer_delete_")) {
          const customerId = Number(data.replace("customer_delete_", ""));
          setUserState(chatId, { step: "customer_delete_wait_username", data: { customer_id: customerId } });
          await bot.sendMessage(chatId, "Mijozni o‘chirish uchun admin loginni yuboring:");
          await bot.answerCallbackQuery(q.id);
          return;
        }

        // ===== DEBT DETAILS =====
        if (data.startsWith("show_debt_details_")) {
          const debtId = Number(data.replace("show_debt_details_", ""));
          const detailsText = await buildDebtDetailsText(debtId);
          await bot.sendMessage(chatId, detailsText, { parse_mode: "HTML" });
          await bot.answerCallbackQuery(q.id);
          return;
        }

        // ===== DEBT DELETE =====
        if (data.startsWith("debt_delete_yes_")) {
          const debtId = Number(data.replace("debt_delete_yes_", ""));
          const result = await deleteDebtDeep(debtId);

          if (!result.ok) {
            await bot.sendMessage(chatId, `❌ ${result.error}`);
          } else {
            await bot.sendMessage(chatId, `✅ Qarz #${debtId} o‘chirildi.`);
          }

          clearUserState(chatId);
          await sendAdminHome(chatId);
          await bot.answerCallbackQuery(q.id);
          return;
        }

        if (data.startsWith("debt_delete_no_")) {
          clearUserState(chatId);
          await bot.sendMessage(chatId, "Bekor qilindi.");
          await sendAdminHome(chatId);
          await bot.answerCallbackQuery(q.id);
          return;
        }
      }

      await bot.answerCallbackQuery(q.id);
    } catch (e) {
      console.error("Bot callback error:", e.message);
      try {
        await bot.answerCallbackQuery(q.id);
      } catch (_) {}
    }
  });

  bot.on("message", async (msg) => {
    if (!msg.text) return;
    if (msg.text.startsWith("/start")) return;

    const chatId = msg.chat.id;
    const text = msg.text.trim();
    const st = getUserState(chatId);

    try {
      if (isAdminChat(chatId)) {
        if (!st) {
          if (text === "📊 Statistika") {
            const t = await buildStatsText();
            await bot.sendMessage(chatId, t, { parse_mode: "HTML" });
            return;
          }

          if (text === "👥 Mijozlar") {
            await sendCustomersList(chatId);
            return;
          }

          if (text === "📦 Mahsulotlar") {
            await sendProductsList(chatId);
            return;
          }

          if (text === "🧾 Bugungi qarzlar") {
            await sendTodayDebts(chatId);
            return;
          }

          if (text === "✅ Bugungi to‘lovlar") {
            await sendTodayPayments(chatId);
            return;
          }

          if (text === "🏆 Top qarzdorlar") {
            await sendTopDebtors(chatId);
            return;
          }

          if (text === "➕ Qarz yozish") {
            clearUserState(chatId);
            await showDebtCustomerPickMenu(chatId);
            return;
          }

          if (text === "👤 Mijoz qo‘shish") {
            setUserState(chatId, { step: "add_customer_wait_name", data: { continueDebt: false } });
            await bot.sendMessage(chatId, "Yangi mijoz ismini yuboring:");
            return;
          }

          if (text === "✏️ Mijoz tahrirlash") {
            setUserState(chatId, { step: "customer_manage_search", data: {} });
            await bot.sendMessage(chatId, "Tahrirlash uchun mijoz ismi yoki telefonini yuboring:");
            return;
          }

          if (text === "📄 Qarz tafsiloti") {
            setUserState(chatId, { step: "debt_detail_wait_id", data: {} });
            await bot.sendMessage(chatId, "Qarz ID yuboring:");
            return;
          }

          if (text === "🗑️ Qarz o‘chirish") {
            setUserState(chatId, { step: "debt_delete_wait_id", data: {} });
            await bot.sendMessage(chatId, "O‘chiriladigan qarz ID ni yuboring:");
            return;
          }
        }

        if (st?.step === "add_customer_wait_name") {
          st.data = { ...(st.data || {}), customer_name: text };
          st.step = "add_customer_wait_phone";
          setUserState(chatId, st);
          await bot.sendMessage(chatId, "Telefon raqamini yuboring:");
          return;
        }

        if (st?.step === "add_customer_wait_phone") {
          const phone = text;
          const name = st.data?.customer_name || "";

          if (!name) {
            clearUserState(chatId);
            await bot.sendMessage(chatId, "Xatolik. Qaytadan boshlang.");
            return;
          }

          const r = await run(`INSERT INTO customers(name, phone) VALUES(?, ?)`, [name, phone || null]);

          if (st.data?.continueDebt) {
            setUserState(chatId, {
              step: "debt_after_customer_selected",
              data: { customer_id: r.id },
            });

            await bot.sendMessage(chatId, `✅ Mijoz saqlandi: ${name}`);
            await askDebtModeAfterCustomer(chatId);
          } else {
            clearUserState(chatId);
            await bot.sendMessage(chatId, `✅ Mijoz saqlandi: ${name}`);
            await sendAdminHome(chatId);
          }
          return;
        }

        if (st?.step === "debt_wait_customer_search") {
          await showCustomerSearchResults(chatId, text, "debt");
          return;
        }

        if (st?.step === "debt_wait_manual_amount") {
          const total = Number(text.replace(",", "."));
          if (!total || total <= 0) {
            await bot.sendMessage(chatId, "To‘g‘ri summa yuboring.");
            return;
          }

          st.data = {
            ...(st.data || {}),
            total,
            note: "Bot orqali qo‘lda yozilgan qarz",
          };
          st.step = "debt_wait_due_date";
          setUserState(chatId, st);

          await bot.sendMessage(chatId, "Qaytarish sanasini yuboring.\nFormat: YYYY-MM-DD yoki DD/MM/YYYY");
          return;
        }

        if (st?.step === "debt_wait_qty") {
          const qty = Number(text.replace(",", "."));
          if (!qty || qty <= 0) {
            await bot.sendMessage(chatId, "To‘g‘ri miqdor yuboring.");
            return;
          }

          const price = Number(st.data?.product_price || 0);
          const total = qty * price;

          st.data = {
            ...(st.data || {}),
            qty,
            total,
            note: `${st.data?.product_name || "Mahsulot"} x ${qty}`,
          };
          st.step = "debt_wait_due_date";
          setUserState(chatId, st);

          await bot.sendMessage(chatId, `Jami summa: ${money(total)}\nQaytarish sanasini yuboring.\nFormat: YYYY-MM-DD yoki DD/MM/YYYY`);
          return;
        }

        if (st?.step === "debt_wait_due_date") {
          const due = parseDateInput(text);
          if (!due) {
            await bot.sendMessage(chatId, "Sana noto‘g‘ri. Masalan: 2026-03-15 yoki 15/03/2026");
            return;
          }

          st.data = {
            ...(st.data || {}),
            due_date: due,
          };
          st.step = "debt_wait_confirm";
          setUserState(chatId, st);

          await askDebtConfirm(chatId, st.data);
          return;
        }

        // ===== CUSTOMER MANAGE MESSAGE FLOWS =====
        if (st?.step === "customer_manage_search") {
          await showCustomerSearchResults(chatId, text, "manage");
          return;
        }

        if (st?.step === "customer_edit_name_wait") {
          const customerId = Number(st.data?.customer_id);
          const newName = safeText(text);

          if (!customerId || !newName) {
            await bot.sendMessage(chatId, "Ism noto‘g‘ri.");
            return;
          }

          await run(`UPDATE customers SET name = ? WHERE id = ?`, [newName, customerId]);
          clearUserState(chatId);

          const info = await buildCustomerManageText(customerId);
          await bot.sendMessage(chatId, `✅ Ism yangilandi.\n\n${info}`, {
            parse_mode: "HTML",
            reply_markup: getCustomerManageInline(customerId),
          });
          return;
        }

        if (st?.step === "customer_edit_phone_wait") {
          const customerId = Number(st.data?.customer_id);
          const newPhone = safeText(text);

          if (!customerId) {
            await bot.sendMessage(chatId, "Xatolik.");
            return;
          }

          await run(`UPDATE customers SET phone = ? WHERE id = ?`, [newPhone || null, customerId]);
          clearUserState(chatId);

          const info = await buildCustomerManageText(customerId);
          await bot.sendMessage(chatId, `✅ Telefon yangilandi.\n\n${info}`, {
            parse_mode: "HTML",
            reply_markup: getCustomerManageInline(customerId),
          });
          return;
        }

        if (st?.step === "customer_delete_wait_username") {
          st.data = { ...(st.data || {}), delete_username: text };
          st.step = "customer_delete_wait_password";
          setUserState(chatId, st);
          await bot.sendMessage(chatId, "Endi admin parolni yuboring:");
          return;
        }

        if (st?.step === "customer_delete_wait_password") {
          const username = st.data?.delete_username;
          const password = text;
          const customerId = Number(st.data?.customer_id);

          if (!adminCredsOk(username, password)) {
            clearUserState(chatId);
            await bot.sendMessage(chatId, "❌ Login yoki parol noto‘g‘ri. O‘chirish bekor qilindi.");
            await sendAdminHome(chatId);
            return;
          }

          const result = await deleteCustomerDeep(customerId);
          clearUserState(chatId);

          if (!result.ok) {
            await bot.sendMessage(chatId, `❌ ${result.error}`);
          } else {
            await bot.sendMessage(chatId, `✅ Mijoz o‘chirildi: ${result.customer?.name || customerId}`);
          }

          await sendAdminHome(chatId);
          return;
        }

        // ===== DEBT DETAILS =====
        if (st?.step === "debt_detail_wait_id") {
          const debtId = Number(text);
          if (!debtId) {
            await bot.sendMessage(chatId, "To‘g‘ri debt ID yuboring.");
            return;
          }

          const detailsText = await buildDebtDetailsText(debtId);
          clearUserState(chatId);
          await bot.sendMessage(chatId, detailsText, { parse_mode: "HTML" });
          await sendAdminHome(chatId);
          return;
        }

        // ===== DEBT DELETE =====
        if (st?.step === "debt_delete_wait_id") {
          const debtId = Number(text);
          if (!debtId) {
            await bot.sendMessage(chatId, "To‘g‘ri debt ID yuboring.");
            return;
          }

          const debt = await getDebtHeader(debtId);
          if (!debt) {
            clearUserState(chatId);
            await bot.sendMessage(chatId, "Qarz topilmadi.");
            await sendAdminHome(chatId);
            return;
          }

          clearUserState(chatId);

          const preview =
            `🗑️ <b>Qarzni o‘chirishni tasdiqlaysizmi?</b>\n\n` +
            `ID: <b>#${debt.id}</b>\n` +
            `Mijoz: <b>${debt.customer_name}</b>\n` +
            `Jami: <b>${money(debt.total)}</b>\n` +
            `To‘langan: <b>${money(debt.paid)}</b>\n` +
            `Qolgan: <b>${money(debt.remaining)}</b>\n` +
            `Due: ${debt.due_date || "-"}`;

          await bot.sendMessage(chatId, preview, {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "✅ Ha, o‘chir", callback_data: `debt_delete_yes_${debtId}` }],
                [{ text: "❌ Yo‘q", callback_data: `debt_delete_no_${debtId}` }],
              ],
            },
          });
          return;
        }

        return;
      }

      // ===== CUSTOMER SIDE =====
      if (text === "💳 Mening qarzim") {
        const linked = await getLinkedCustomerByChat(chatId);
        if (!linked) {
          await bot.sendMessage(chatId, "Avval telefon raqamingizni yuboring.", {
            reply_markup: getCustomerMenuKeyboard(),
          });
          return;
        }

        const debtText = await buildCustomerDebtText(linked.id);
        await bot.sendMessage(chatId, debtText, {
          parse_mode: "HTML",
          reply_markup: getCustomerMenuKeyboard(),
        });
        return;
      }

      if (text === "📱 Telefon raqamni yuborish") {
        await bot.sendMessage(chatId, "Pastdagi tugma orqali telefon raqamingizni yuboring.", {
          reply_markup: getCustomerMenuKeyboard(),
        });
        return;
      }

      const linked = await getLinkedCustomerByChat(chatId);
      if (!linked) {
        await bot.sendMessage(chatId, "Telefon raqamingizni yuboring. Agar u bazadagi raqam bilan mos kelsa, qarzingiz ko‘rsatiladi.", {
          reply_markup: getCustomerMenuKeyboard(),
        });
      }
    } catch (e) {
      console.error("Bot message error:", e.message);
    }
  });

  reminderInterval = setInterval(async () => {
    try {
      const overdue = await get(`
        SELECT COUNT(*) AS c
        FROM debts
        WHERE (total - paid) > 0
          AND due_date IS NOT NULL
          AND date(due_date) < date('now','localtime')
      `);

      const count = overdue?.c || 0;
      if (count > 0) {
        await notifyAdmin(`⏰ <b>Eslatma:</b> ${count} ta qarz muddati o‘tgan.`);
      }
    } catch (e) {
      console.error("Reminder error:", e.message);
    }
  }, Math.max(10, REMINDER_INTERVAL_MIN) * 60 * 1000);
}

// ===== HEALTH =====
app.get("/health", async (req, res) => {
  try {
    const dbCheck = await get(`SELECT 1 AS ok`);
    res.json({
      ok: true,
      auth: req.session?.user === "admin",
      bot: botEnabled() ? "ON" : "OFF",
      db: dbCheck?.ok === 1 ? "ON" : "OFF",
      dbPath,
      uptime_sec: Math.floor(process.uptime()),
      time: new Date().toISOString(),
    });
  } catch (e) {
    res.status(500).json({
      ok: false,
      auth: req.session?.user === "admin",
      bot: botEnabled() ? "ON" : "OFF",
      db: "OFF",
      error: e.message,
      dbPath,
      time: new Date().toISOString(),
    });
  }
});

// ===== CUSTOMERS API =====
app.get("/api/customers", requireAuth, async (req, res) => {
  try {
    const q = safeText(req.query.q || "");

    const rows = q
      ? await all(`SELECT * FROM customers WHERE name LIKE ? OR phone LIKE ? ORDER BY id DESC`, [`%${q}%`, `%${q}%`])
      : await all(`SELECT * FROM customers ORDER BY id DESC`);

    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/customers/:id", requireAuth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const summary = await getCustomerSummary(id);

    if (!summary) {
      return res.status(404).json({ ok: false, error: "Mijoz topilmadi" });
    }

    res.json({
      ok: true,
      customer: summary.customer,
      remaining: summary.remaining,
      debts_count: summary.debts_count,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/customers", requireAuth, async (req, res) => {
  try {
    const name = safeText(req.body?.name);
    const phone = safeText(req.body?.phone);

    if (!name) {
      return res.status(400).json({ ok: false, error: "Mijoz nomi kiritilmagan" });
    }

    const r = await run(`INSERT INTO customers(name, phone) VALUES(?, ?)`, [name, phone || null]);
    res.json({ ok: true, id: r.id });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.patch("/api/customers/:id", requireAuth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const existing = await get(`SELECT * FROM customers WHERE id = ?`, [id]);

    if (!existing) {
      return res.status(404).json({ ok: false, error: "Mijoz topilmadi" });
    }

    const name = req.body?.name != null ? safeText(req.body.name) : null;
    const phone = req.body?.phone != null ? safeText(req.body.phone) : null;

    await run(
      `
      UPDATE customers
      SET name = COALESCE(?, name),
          phone = COALESCE(?, phone)
      WHERE id = ?
      `,
      [name || null, phone || null, id]
    );

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// customer delete => admin login/parol required
app.delete("/api/customers/:id", requireAuth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const username = safeText(req.body?.username);
    const password = safeText(req.body?.password);

    if (!adminCredsOk(username, password)) {
      return res.status(403).json({ ok: false, error: "Admin login yoki parol noto‘g‘ri" });
    }

    const result = await deleteCustomerDeep(id);
    if (!result.ok) {
      return res.status(404).json({ ok: false, error: result.error });
    }

    res.json({ ok: true, deleted_customer_id: id });
  } catch (e) {
    console.error("DELETE CUSTOMER ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===== PRODUCTS API =====
app.get("/api/products", requireAuth, async (req, res) => {
  try {
    const q = safeText(req.query.q || "");

    const rows = q
      ? await all(`SELECT * FROM products WHERE name LIKE ? ORDER BY id DESC`, [`%${q}%`])
      : await all(`SELECT * FROM products ORDER BY id DESC`);

    res.json({ ok: true, rows });
  } catch (e) {
    console.error("GET PRODUCTS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/products", requireAuth, async (req, res) => {
  try {
    const name = safeText(req.body?.name);
    const price = Number(req.body?.price || 0);
    const stock = Number(req.body?.stock || 0);
    const unit_type = req.body?.unit_type === "kilogram" ? "kilogram" : "piece";

    if (!name) {
      return res.status(400).json({ ok: false, error: "Mahsulot nomi kiritilmagan" });
    }

    const existing = await get(`SELECT * FROM products WHERE lower(name) = lower(?)`, [name]);

    if (existing) {
      await run(
        `
        UPDATE products
        SET stock = stock + ?,
            price = ?,
            unit_type = ?
        WHERE id = ?
        `,
        [stock, price, unit_type, existing.id]
      );

      return res.json({
        ok: true,
        updated: true,
        id: existing.id,
        message: "Mavjud mahsulot yangilandi",
      });
    }

    const r = await run(
      `INSERT INTO products(name, price, stock, unit_type) VALUES(?, ?, ?, ?)`,
      [name, price, stock, unit_type]
    );

    return res.json({
      ok: true,
      created: true,
      id: r.id,
      message: "Yangi mahsulot qo‘shildi",
    });
  } catch (e) {
    console.error("POST PRODUCTS ERROR:", e);
    return res.status(500).json({
      ok: false,
      error: "Server xatosi: " + e.message,
    });
  }
});

app.patch("/api/products/:id", requireAuth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const name = safeText(req.body?.name || "");
    const price = req.body?.price != null ? Number(req.body.price) : null;
    const stock = req.body?.stock != null ? Number(req.body.stock) : null;
    const unit_type = req.body?.unit_type ? (req.body.unit_type === "kilogram" ? "kilogram" : "piece") : null;

    await run(
      `
      UPDATE products
      SET name = COALESCE(?, name),
          price = COALESCE(?, price),
          stock = COALESCE(?, stock),
          unit_type = COALESCE(?, unit_type)
      WHERE id = ?
      `,
      [name || null, price, stock, unit_type, id]
    );

    res.json({ ok: true });
  } catch (e) {
    console.error("PATCH PRODUCTS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.delete("/api/products/:id", requireAuth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    await run(`DELETE FROM products WHERE id = ?`, [id]);
    res.json({ ok: true });
  } catch (e) {
    console.error("DELETE PRODUCTS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===== DEBTS API =====
app.post("/api/debts", requireAuth, async (req, res) => {
  try {
    const customer_id = Number(req.body?.customer_id);
    const total = Number(req.body?.total || 0);
    const note = safeText(req.body?.note || "");
    const due_date = req.body?.due_date ? String(req.body.due_date) : null;
    const items = Array.isArray(req.body?.items) ? req.body.items : [];

    if (!customer_id || total <= 0) {
      return res.status(400).json({ ok: false, error: "Ma'lumot noto‘g‘ri" });
    }

    const result = await createOrMergeDebt({
      customer_id,
      total,
      note,
      due_date,
      items,
    });

    const c = await get(`SELECT * FROM customers WHERE id = ?`, [customer_id]);

    await notifyAdmin(
      `🧾 <b>Yangi qarz</b>\n` +
        `👤 ${c?.name || "Mijoz"}\n` +
        `💸 Summa: <b>${money(total)}</b>\n` +
        `📅 Muddat: <b>${due_date || "-"}</b>\n` +
        `📝 ${note || ""}`
    );

    res.json({ ok: true, debt_id: result.debtId, merged: result.merged });
  } catch (e) {
    console.error("POST DEBTS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/debts", requireAuth, async (req, res) => {
  try {
    const customer_id = req.query.customer_id ? Number(req.query.customer_id) : null;
    const unpaid = req.query.unpaid === "1";
    const today = req.query.today === "1";
    const q = safeText(req.query.q || "");

    let where = `1=1`;
    const params = [];

    if (customer_id) {
      where += ` AND d.customer_id = ?`;
      params.push(customer_id);
    }

    if (unpaid) {
      where += ` AND (d.total - d.paid) > 0`;
    }

    if (today) {
      where += ` AND date(d.created_at) = date('now','localtime')`;
    }

    if (q) {
      where += ` AND c.name LIKE ?`;
      params.push(`%${q}%`);
    }

    const rows = await all(
      `
      SELECT
        d.*,
        c.name AS customer_name,
        c.phone AS customer_phone,
        (d.total - d.paid) AS remaining,
        CASE
          WHEN d.due_date IS NOT NULL
           AND date(d.due_date) < date('now','localtime')
           AND (d.total - d.paid) > 0
          THEN 1 ELSE 0
        END AS overdue
      FROM debts d
      JOIN customers c ON c.id = d.customer_id
      WHERE ${where}
      ORDER BY d.id DESC
      LIMIT 500
      `,
      params
    );

    res.json({ ok: true, rows });
  } catch (e) {
    console.error("GET DEBTS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// qarz tafsilotlari
app.get("/api/debts/:id/details", requireAuth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const debt = await getDebtHeader(id);

    if (!debt) {
      return res.status(404).json({ ok: false, error: "Qarz topilmadi" });
    }

    const timeline = await getDebtTimeline(id);

    res.json({
      ok: true,
      debt,
      timeline,
    });
  } catch (e) {
    console.error("GET DEBT DETAILS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// qarz o‘chirish
app.delete("/api/debts/:id", requireAuth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const result = await deleteDebtDeep(id);

    if (!result.ok) {
      return res.status(404).json({ ok: false, error: result.error });
    }

    res.json({ ok: true, deleted_debt_id: id });
  } catch (e) {
    console.error("DELETE DEBT ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===== PAYMENTS API =====
app.post("/api/payments", requireAuth, async (req, res) => {
  try {
    const debt_id = Number(req.body?.debt_id);
    const amount = Number(req.body?.amount || 0);
    const note = safeText(req.body?.note || "");

    if (!debt_id || amount <= 0) {
      return res.status(400).json({ ok: false, error: "Ma'lumot noto‘g‘ri" });
    }

    const d = await get(`SELECT * FROM debts WHERE id = ?`, [debt_id]);
    if (!d) {
      return res.status(404).json({ ok: false, error: "Qarz topilmadi" });
    }

    const remaining = Number(d.total - d.paid);
    const pay = Math.min(amount, remaining);

    await run(`INSERT INTO payments(debt_id, amount, note) VALUES(?, ?, ?)`, [debt_id, pay, note || null]);
    await run(`UPDATE debts SET paid = paid + ? WHERE id = ?`, [pay, debt_id]);

    await addDebtEvent({
      debt_id,
      customer_id: d.customer_id,
      event_type: "payment",
      amount: pay,
      note: note || "To‘lov",
    });

    const c = await get(`SELECT * FROM customers WHERE id = ?`, [d.customer_id]);

    await notifyAdmin(
      `✅ <b>To‘lov</b>\n` +
        `👤 ${c?.name || "Mijoz"}\n` +
        `💰 To‘landi: <b>${money(pay)}</b>\n` +
        `📌 Qarz #${debt_id}\n` +
        `📝 ${note || ""}`
    );

    res.json({ ok: true, paid: pay });
  } catch (e) {
    console.error("POST PAYMENTS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/payments", requireAuth, async (req, res) => {
  try {
    const debt_id = req.query.debt_id ? Number(req.query.debt_id) : null;

    const rows = debt_id
      ? await all(
          `
          SELECT p.*, c.name AS customer_name
          FROM payments p
          JOIN debts d ON d.id = p.debt_id
          JOIN customers c ON c.id = d.customer_id
          WHERE p.debt_id = ?
          ORDER BY p.id DESC
          LIMIT 500
          `,
          [debt_id]
        )
      : await all(
          `
          SELECT p.*, c.name AS customer_name, p.debt_id
          FROM payments p
          JOIN debts d ON d.id = p.debt_id
          JOIN customers c ON c.id = d.customer_id
          ORDER BY p.id DESC
          LIMIT 200
          `
        );

    res.json({ ok: true, rows });
  } catch (e) {
    console.error("GET PAYMENTS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===== STATS API =====
app.get("/api/stats", requireAuth, async (req, res) => {
  try {
    const totalCustomers = (await get(`SELECT COUNT(*) AS c FROM customers`))?.c || 0;
    const totalDebt = (await get(`SELECT COALESCE(SUM(total - paid),0) AS s FROM debts WHERE (total - paid) > 0`))?.s || 0;
    const todayDebts = (await get(`SELECT COALESCE(SUM(total),0) AS s FROM debts WHERE date(created_at) = date('now','localtime')`))?.s || 0;
    const todayPays = (await get(`SELECT COALESCE(SUM(amount),0) AS s FROM payments WHERE date(created_at) = date('now','localtime')`))?.s || 0;

    const topDebtors = await all(`
      SELECT c.id, c.name, SUM(d.total - d.paid) AS remaining
      FROM debts d
      JOIN customers c ON c.id = d.customer_id
      WHERE (d.total - d.paid) > 0
      GROUP BY d.customer_id
      ORDER BY remaining DESC
      LIMIT 10
    `);

    res.json({
      ok: true,
      totalCustomers,
      totalDebt,
      todayDebts,
      todayPays,
      topDebtors,
    });
  } catch (e) {
    console.error("GET STATS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===== START =====
(async () => {
  try {
    await initDb();

    if (botEnabled()) {
      startBot();
    }

    app.listen(PORT, () => {
      console.log(`✅ Server running: http://localhost:${PORT}`);
      console.log(`🤖 Bot: ${botEnabled() ? "ON" : "OFF"}`);
      console.log(`🗄️ DB: ${dbPath}`);
    });
  } catch (e) {
    console.error("SERVER START ERROR:", e);
    process.exit(1);
  }
})();
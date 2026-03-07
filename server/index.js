// server/index.js — DIYOR MARKET (FULL + TELEGRAM BOT MENU + CUSTOMER LINK)

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
const APP_USER = (process.env.APP_USER || "admin").trim();
const APP_PASS = (process.env.APP_PASS || "12345").trim();
const SESSION_SECRET = (process.env.SESSION_SECRET || "secret").trim();

const BOT_TOKEN = (process.env.BOT_TOKEN || "").trim();
const OWNER_CHAT_ID = (process.env.OWNER_CHAT_ID || "").trim();
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
    db.run(sql, params, function (err) {
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

  if ((username || "").trim() === APP_USER && (password || "").trim() === APP_PASS) {
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
  const totalCustomers = (await get(`SELECT COUNT(*) as c FROM customers`))?.c || 0;

  const totalDebtRow = await get(`
    SELECT COALESCE(SUM(total - paid),0) as remaining
    FROM debts
    WHERE (total - paid) > 0
  `);

  const todayDebtsRow = await get(`
    SELECT COALESCE(SUM(total),0) as s
    FROM debts
    WHERE date(created_at) = date('now','localtime')
  `);

  const todayPaysRow = await get(`
    SELECT COALESCE(SUM(amount),0) as s
    FROM payments
    WHERE date(created_at) = date('now','localtime')
  `);

  const top = await all(`
    SELECT c.name, c.phone, SUM(d.total - d.paid) as remaining
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
    SELECT c.id, c.name, c.phone, COALESCE(SUM(d.total - d.paid),0) as remaining
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
      rows
        .map((r) => `#${r.id} ${r.name} | narx: <b>${money(r.price)}</b> | stock: <b>${r.stock}</b> | ${r.unit_type}`)
        .join("\n")
    : "Mahsulotlar yo‘q.";

  await bot.sendMessage(chatId, text, { parse_mode: "HTML" });
}

async function sendTodayDebts(chatId) {
  const rows = await all(`
    SELECT d.id, c.name, d.total, d.paid, (d.total - d.paid) as remaining, d.note
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
    SELECT c.id, c.name, SUM(d.total - d.paid) as remaining
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
      `UPDATE customer_telegram_links
       SET customer_id = ?, telegram_user_id = ?
       WHERE telegram_chat_id = ?`,
      [customerId, String(userId || ""), String(chatId)]
    );
    return;
  }

  await run(
    `INSERT INTO customer_telegram_links(customer_id, telegram_chat_id, telegram_user_id)
     VALUES(?, ?, ?)`,
    [customerId, String(chatId), String(userId || "")]
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
  const customer = await get(`SELECT * FROM customers WHERE id = ?`, [customerId]);
  if (!customer) return "Mijoz topilmadi.";

  const debts = await all(
    `
    SELECT id, total, paid, note, due_date, created_at, (total - paid) as remaining
    FROM debts
    WHERE customer_id = ?
    ORDER BY id DESC
    LIMIT 20
  `,
    [customerId]
  );

  const totalRemainingRow = await get(
    `
    SELECT COALESCE(SUM(total - paid), 0) as remaining
    FROM debts
    WHERE customer_id = ?
      AND (total - paid) > 0
  `,
    [customerId]
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
    `SELECT * FROM debts
     WHERE customer_id = ?
       AND (total - paid) > 0
     ORDER BY id DESC
     LIMIT 1`,
    [customer_id]
  );

  let debtId;

  if (open) {
    const mergedNote = (open.note ? open.note + " | " : "") + (note || `Qarz qo‘shildi (+${total})`);

    await run(
      `UPDATE debts
       SET total = total + ?,
           note = ?,
           due_date = COALESCE(?, due_date)
       WHERE id = ?`,
      [total, mergedNote, due_date, open.id]
    );

    debtId = open.id;
  } else {
    const r = await run(
      `INSERT INTO debts(customer_id, total, paid, note, due_date)
       VALUES(?, ?, 0, ?, ?)`,
      [customer_id, total, note || null, due_date]
    );

    debtId = r.id;
  }

  for (const it of items) {
    const pid = Number(it.product_id);
    const qty = Number(it.qty || 0);
    if (pid && qty > 0) {
      await run(`UPDATE products SET stock = stock - ? WHERE id = ?`, [qty, pid]);
    }
  }

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

async function showCustomerSearchResults(chatId, query) {
  const rows = await all(`SELECT * FROM customers WHERE name LIKE ? OR phone LIKE ? ORDER BY id DESC LIMIT 20`, [`%${query}%`, `%${query}%`]);

  if (!rows.length) {
    await bot.sendMessage(chatId, "Mijoz topilmadi. Yana boshqa ism yoki raqam yuboring.");
    return;
  }

  const buttons = rows.map((r) => [{ text: `${r.name} (${r.phone || "-"})`, callback_data: `debt_pick_customer_${r.id}` }]);
  buttons.push([{ text: "👤 Yangi mijoz qo‘shish", callback_data: "debt_customer_new" }]);
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

  const buttons = rows.map((r) => [{ text: `${r.name} • ${money(r.price)} • stock:${r.stock}`, callback_data: `debt_pick_product_${r.id}` }]);

  buttons.push([{ text: "💵 Qo‘lda summa yozish", callback_data: "debt_type_manual" }]);
  buttons.push([{ text: "❌ Bekor qilish", callback_data: "flow_cancel" }]);

  await bot.sendMessage(chatId, "Mahsulotni tanlang:", {
    reply_markup: { inline_keyboard: buttons },
  });
}

async function askDebtConfirm(chatId, data) {
  const customer = await get(`SELECT * FROM customers WHERE id = ?`, [data.customer_id]);
  const product = data.product_id ? await get(`SELECT * FROM products WHERE id = ?`, [data.product_id]) : null;

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
          await showCustomerSearchResults(chatId, text);
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

        return;
      }

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
        SELECT COUNT(*) as c
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
    const dbCheck = await get(`SELECT 1 as ok`);
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
    const q = (req.query.q || "").trim();

    const rows = q
      ? await all(`SELECT * FROM customers WHERE name LIKE ? OR phone LIKE ? ORDER BY id DESC`, [`%${q}%`, `%${q}%`])
      : await all(`SELECT * FROM customers ORDER BY id DESC`);

    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/customers", requireAuth, async (req, res) => {
  try {
    const name = String(req.body?.name || "").trim();
    const phone = String(req.body?.phone || "").trim();

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
    const name = String(req.body?.name || "").trim();
    const phone = String(req.body?.phone || "").trim();

    await run(
      `UPDATE customers
       SET name = COALESCE(?, name),
           phone = COALESCE(?, phone)
       WHERE id = ?`,
      [name || null, phone || null, id]
    );

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===== PRODUCTS API =====
app.get("/api/products", requireAuth, async (req, res) => {
  try {
    const q = (req.query.q || "").trim();

    const rows = q ? await all(`SELECT * FROM products WHERE name LIKE ? ORDER BY id DESC`, [`%${q}%`]) : await all(`SELECT * FROM products ORDER BY id DESC`);

    res.json({ ok: true, rows });
  } catch (e) {
    console.error("GET PRODUCTS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/products", requireAuth, async (req, res) => {
  try {
    const name = String(req.body?.name || "").trim();
    const price = Number(req.body?.price || 0);
    const stock = Number(req.body?.stock || 0);
    const unit_type = req.body?.unit_type === "kilogram" ? "kilogram" : "piece";

    if (!name) {
      return res.status(400).json({ ok: false, error: "Mahsulot nomi kiritilmagan" });
    }

    const existing = await get(`SELECT * FROM products WHERE lower(name) = lower(?)`, [name]);

    if (existing) {
      await run(
        `UPDATE products
         SET stock = stock + ?,
             price = ?,
             unit_type = ?
         WHERE id = ?`,
        [stock, price, unit_type, existing.id]
      );

      return res.json({
        ok: true,
        updated: true,
        id: existing.id,
        message: "Mavjud mahsulot yangilandi",
      });
    }

    const r = await run(`INSERT INTO products(name, price, stock, unit_type) VALUES(?, ?, ?, ?)`, [name, price, stock, unit_type]);

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
    const name = String(req.body?.name || "").trim();
    const price = req.body?.price != null ? Number(req.body.price) : null;
    const stock = req.body?.stock != null ? Number(req.body.stock) : null;
    const unit_type = req.body?.unit_type ? (req.body.unit_type === "kilogram" ? "kilogram" : "piece") : null;

    await run(
      `UPDATE products
       SET name = COALESCE(?, name),
           price = COALESCE(?, price),
           stock = COALESCE(?, stock),
           unit_type = COALESCE(?, unit_type)
       WHERE id = ?`,
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
    const note = String(req.body?.note || "").trim();
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
    const q = (req.query.q || "").trim();

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
        c.name as customer_name,
        c.phone as customer_phone,
        (d.total - d.paid) as remaining,
        CASE
          WHEN d.due_date IS NOT NULL
           AND date(d.due_date) < date('now','localtime')
           AND (d.total - d.paid) > 0
          THEN 1 ELSE 0
        END as overdue
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

// ===== PAYMENTS API =====
app.post("/api/payments", requireAuth, async (req, res) => {
  try {
    const debt_id = Number(req.body?.debt_id);
    const amount = Number(req.body?.amount || 0);
    const note = String(req.body?.note || "").trim();

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
          SELECT p.*, c.name as customer_name
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
          SELECT p.*, c.name as customer_name, p.debt_id
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
    const totalCustomers = (await get(`SELECT COUNT(*) as c FROM customers`))?.c || 0;
    const totalDebt = (await get(`SELECT COALESCE(SUM(total - paid),0) as s FROM debts WHERE (total - paid) > 0`))?.s || 0;
    const todayDebts = (await get(`SELECT COALESCE(SUM(total),0) as s FROM debts WHERE date(created_at) = date('now','localtime')`))?.s || 0;
    const todayPays = (await get(`SELECT COALESCE(SUM(amount),0) as s FROM payments WHERE date(created_at) = date('now','localtime')`))?.s || 0;

    const topDebtors = await all(`
      SELECT c.id, c.name, SUM(d.total - d.paid) as remaining
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
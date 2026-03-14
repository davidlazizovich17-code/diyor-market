async function api(url, opts = {}) {
  const res = await fetch(url, {
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    ...opts
  });

  if (res.status === 401) {
    location.href = "/login.html";
    throw new Error("Unauthorized");
  }

  const data = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(data.error || "API error");
  }

  return data;
}

function q(sel) {
  return document.querySelector(sel);
}

function qa(sel) {
  return [...document.querySelectorAll(sel)];
}

function money(x) {
  const n = Number(x || 0);
  return n.toFixed(0);
}

function num(x, fallback = 0) {
  const n = Number(x);
  return Number.isFinite(n) ? n : fallback;
}

function esc(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatDateTime(v) {
  if (!v) return "-";
  return String(v).replace("T", " ").slice(0, 19);
}

function showMsg(selector, text, type = "") {
  const el = q(selector);
  if (!el) return;
  el.textContent = text || "";
  el.className = "msg" + (type ? ` ${type}` : "");
}

function openModal(selector) {
  const el = q(selector);
  if (el) el.style.display = "block";
}

function closeModal(selector) {
  const el = q(selector);
  if (el) el.style.display = "none";
}

function setActiveNav() {
  const p = location.pathname.replace("/", "");
  qa(".nav a").forEach((a) => {
    if (a.getAttribute("href") === `/${p}`) a.classList.add("active");
  });
}

async function refreshHealth() {
  try {
    const h = await fetch("/health", { credentials: "include" }).then((r) => r.json());
    const dot = q("#dot");
    const txt = q("#healthText");
    if (!dot || !txt) return;
    dot.classList.toggle("ok", !!h.auth);
    txt.textContent = h.auth ? `Online • Bot: ${h.bot}` : "Offline";
  } catch (_) {}
}

async function doLogout() {
  await fetch("/logout", { method: "POST", credentials: "include" });
  location.href = "/login.html";
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    qa('[id$="Modal"]').forEach((m) => {
      m.style.display = "none";
    });
  }
});

document.addEventListener("DOMContentLoaded", () => {
  setActiveNav();
  refreshHealth();
});
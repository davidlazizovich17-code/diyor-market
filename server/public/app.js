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
  if (!res.ok) throw new Error(data.error || "API error");
  return data;
}

function q(sel) { return document.querySelector(sel); }
function qa(sel) { return [...document.querySelectorAll(sel)]; }

function money(x){
  const n = Number(x || 0);
  return n.toFixed(0);
}

function setActiveNav(){
  const p = location.pathname.replace("/", "");
  qa(".nav a").forEach(a=>{
    if (a.getAttribute("href") === `/${p}`) a.classList.add("active");
  });
}

async function refreshHealth(){
  try{
    const h = await fetch("/health", { credentials:"include" }).then(r=>r.json());
    const dot = q("#dot");
    const txt = q("#healthText");
    if (!dot || !txt) return;
    dot.classList.toggle("ok", !!h.auth);
    txt.textContent = h.auth ? `Online • Bot: ${h.bot}` : "Offline";
  }catch(_){}
}

async function doLogout(){
  await fetch("/logout", { method:"POST", credentials:"include" });
  location.href="/login.html";
}

document.addEventListener("DOMContentLoaded", ()=>{
  setActiveNav();
  refreshHealth();
});
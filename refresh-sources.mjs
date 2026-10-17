#!/usr/bin/env node
/**
 * motv-index 月度自动刷新引擎（GitHub Actions 版，零依赖）
 * ------------------------------------------------------------------
 * 运行环境：GitHub Actions ubuntu-latest + Node 20（原生 fetch，无需 npm install）
 * 触发方式：每月 1 日 10:00（北京时间）定时 + 仓库 Actions 页手动 Run workflow
 *
 * 流程：
 *   1) 读仓库根 motv-index.json（APK 订阅的文件）
 *   2) 逐条复验现有源：结构 → 列表/详情 → 播放流探测（慢更准，type=3 只做结构级）
 *   3) 剔除失效源（protected 保护名单除外，保护源失效仅警告不剔除）
 *   4) 可用源少于 N 时，从 tools/seeds.json 候选池择优补足到 N
 *   5) 写回 motv-index.json + finder-state.json（git push 由 workflow 负责）
 *
 * 规则：
 *   - 只写 motv-index.json 和 finder-state.json，不碰仓库其他文件
 *   - url 原样保留（尤其带尾部斜杠的），不做 URL 规范化
 *   - 日期口径：UTC+8（北京时间）
 *   - 退出码恒为 0（探活失败不等于任务失败，避免误报警）
 */
import { readFileSync, writeFileSync, existsSync, appendFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = process.cwd(); // checkout 后即仓库根
const TOOLS_DIR = dirname(fileURLToPath(import.meta.url));
const INDEX_PATH = join(ROOT, "motv-index.json");
const STATE_PATH = join(ROOT, "finder-state.json");
const SEEDS_PATH = join(TOOLS_DIR, "seeds.json");
const SUMMARY_PATH = process.env.GITHUB_STEP_SUMMARY || "";

const TARGET_COUNT = clampInt(process.env.TARGET_COUNT, 8, 1, 50);
const UA =
  "Mozilla/5.0 (Linux; Android 12; SM-S9110) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36";
const CONCURRENCY = 4;
const FETCH_TIMEOUT_MS = 15000;
const PROBE_TIMEOUT_MS = 15000;
const RETRIES = 1;
const MAX_CANDIDATES = 80; // 候选池单轮验证上限，防索引聚合出几百个跑不完

// ---------------- 基础工具 ----------------

function clampInt(v, dflt, min, max) {
  const n = parseInt(v, 10);
  if (!Number.isFinite(n)) return dflt;
  return Math.min(max, Math.max(min, n));
}

function today() {
  // 北京时间 UTC+8
  return new Date(Date.now() + 8 * 3600 * 1000).toISOString().slice(0, 10);
}

function log(...args) {
  console.log(...args);
}

function loadJson(path, dflt) {
  if (!existsSync(path)) return dflt;
  try {
    return JSON.parse(readFileSync(path, "utf-8"));
  } catch {
    return dflt;
  }
}

// 并发受限的 map（慢更准：并发 4，避免把源站打挂也避免被限流）
async function mapLimit(items, limit, fn) {
  const ret = new Array(items.length);
  let idx = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (idx < items.length) {
      const i = idx++;
      try {
        ret[i] = await fn(items[i], i);
      } catch (e) {
        ret[i] = { url: items[i].url || String(items[i]), ok: false, stage: 0, error: String(e && e.message || e) };
      }
    }
  });
  await Promise.all(workers);
  return ret;
}

// 带超时 + 重试的 GET
async function fetchTimeout(url, opts = {}) {
  const { timeoutMs = FETCH_TIMEOUT_MS, headers = {}, retries = RETRIES } = opts;
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      return await fetch(url, {
        method: "GET",
        headers: { "User-Agent": UA, ...headers },
        signal: ctrl.signal,
        redirect: "follow",
      });
    } catch (e) {
      lastErr = e;
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastErr;
}

async function fetchText(url, opts = {}) {
  const resp = await fetchTimeout(url, opts);
  if (!resp.ok) throw new Error("HTTP " + resp.status + " @ " + url);
  return await resp.text();
}

async function fetchJson(url, opts = {}) {
  return JSON.parse(await fetchText(url, opts));
}

// GET + Range 探测播放流（HEAD 常被拒，Range GET 兼容性最好）
async function probeStream(url, opts = {}) {
  const { timeoutMs = PROBE_TIMEOUT_MS, headers = {} } = opts;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, {
      method: "GET",
      headers: { "User-Agent": UA, ...headers, Range: "bytes=0-1023" },
      signal: ctrl.signal,
      redirect: "follow",
    });
  } finally {
    clearTimeout(timer);
  }
}

// ---------------- 三级验证 ----------------

function extractName(url) {
  try {
    const u = new URL(url);
    const seg = u.pathname.split("/").filter(Boolean).pop() || u.hostname;
    return decodeURIComponent(seg).replace(/\.json$/i, "");
  } catch {
    return url;
  }
}

function pickArray(obj) {
  if (!obj || typeof obj !== "object") return null;
  for (const k of ["list", "data", "items", "vod_list", "video_list"]) {
    if (Array.isArray(obj[k])) return obj[k];
  }
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    if (Array.isArray(v) && v.length && typeof v[0] === "object") return v;
  }
  return null;
}

function extractFirstPlayUrl(raw) {
  if (!raw) return null;
  const first = String(raw).split("#")[0];
  const parts = first.split("$");
  return (parts[parts.length - 1] || "").trim();
}

function originOf(url) {
  try {
    return new URL(url).origin;
  } catch {
    return "";
  }
}

// type=1：分类 → 片单 → 详情 → 播放流，全跑
async function validateApiFull(a) {
  const r = { api: a.api, type: a.type, stage: 1, ok: false, samplePlayUrl: null, needsHeaders: null, classes: [], error: null };
  const t0 = Date.now();
  const baseHeaders = {};

  // 1) 分类
  let classes = [];
  try {
    const cj = await fetchJson(a.api, { headers: baseHeaders });
    classes = Array.isArray(cj.class) ? cj.class : Array.isArray(cj.videoClass) ? cj.videoClass : [];
  } catch {}
  r.classes = classes.map((c) => c.type ?? c.tid ?? c.id ?? c).filter(Boolean);

  // 2) 片单（api 自带 query 时用 & 拼接）
  const firstClass = r.classes[0] ?? "";
  const sep = String(a.api).includes("?") ? "&" : "?";
  const listCandidates = [
    `${a.api}${sep}ac=list&t=${encodeURIComponent(firstClass)}&pg=1`,
    `${a.api}${sep}ac=list&pg=1`,
    `${a.api}${sep}extend=&ac=list&t=${encodeURIComponent(firstClass)}&pg=1`,
  ];
  let listJson = null;
  let usedHeaders = null;
  for (const lu of listCandidates) {
    try {
      const resp = await fetchTimeout(lu, { headers: baseHeaders });
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      listJson = JSON.parse(await resp.text());
      break;
    } catch {
      try {
        const h2 = { Referer: originOf(a.api) };
        const resp2 = await fetchTimeout(lu, { headers: h2 });
        if (!resp2.ok) throw new Error("HTTP " + resp2.status);
        listJson = JSON.parse(await resp2.text());
        usedHeaders = { referer: originOf(a.api) };
        break;
      } catch (e2) {
        if (lu === listCandidates[listCandidates.length - 1]) r.error = "list 拉取失败: " + e2.message;
      }
    }
  }
  if (!listJson) {
    r.latencyMs = Date.now() - t0;
    return r;
  }
  r.stage = 2;
  if (usedHeaders) r.needsHeaders = usedHeaders;

  const items = pickArray(listJson);
  if (!items || items.length === 0) {
    r.error = "list 为空";
    r.latencyMs = Date.now() - t0;
    return r;
  }
  const firstId = items[0].vod_id ?? items[0].id ?? items[0].ids;
  if (!firstId) {
    r.error = "list 无 vod_id";
    r.latencyMs = Date.now() - t0;
    return r;
  }

  // 3) 详情
  const dsep = String(a.api).includes("?") ? "&" : "?";
  const detailCandidates = [
    `${a.api}${dsep}ac=detail&w=${encodeURIComponent(firstId)}`,
    `${a.api}${dsep}ac=detail&ids=${encodeURIComponent(firstId)}`,
    `${a.api}${dsep}ac=videolist&ids=${encodeURIComponent(firstId)}`,
  ];
  let detailJson = null;
  for (const du of detailCandidates) {
    try {
      const resp = await fetchTimeout(du, { headers: usedHeaders ? { Referer: usedHeaders.referer } : {} });
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      detailJson = JSON.parse(await resp.text());
      break;
    } catch (e) {
      if (du === detailCandidates[detailCandidates.length - 1]) r.error = "detail 拉取失败: " + e.message;
    }
  }
  if (!detailJson) {
    r.latencyMs = Date.now() - t0;
    return r;
  }
  r.stage = 3;
  const dItems = pickArray(detailJson);
  if (!dItems || dItems.length === 0) {
    r.error = "detail 为空";
    r.latencyMs = Date.now() - t0;
    return r;
  }
  const playUrl = extractFirstPlayUrl(dItems[0].vod_play_url ?? dItems[0].playUrl ?? dItems[0].url);
  if (!playUrl) {
    r.error = "detail 无播放地址";
    r.latencyMs = Date.now() - t0;
    return r;
  }
  r.samplePlayUrl = playUrl;

  // 4) 播放流探测
  try {
    const resp = await probeStream(playUrl, {
      headers: usedHeaders ? { Referer: usedHeaders.referer } : {},
    });
    const ok = resp.status === 200 || resp.status === 206;
    const ct = (resp.headers.get("content-type") || "").toLowerCase();
    const videoLike = /video|mpegurl|m3u8|mp4|flv|octet-stream|ts|f4m|mkv|dash/i.test(ct);
    if (ok && (videoLike || resp.headers.get("content-length") != null)) {
      r.stage = 4;
      r.ok = true;
    } else {
      r.error = `播放流探测失败(状态 ${resp.status}, ct=${ct || "?"})`;
    }
  } catch (e) {
    r.error = "播放流探测异常: " + e.message;
  }
  r.latencyMs = Date.now() - t0;
  return r;
}

// type=2/3 且 api 是 URL：可达即算可用（jar/csp 在 APK 端运行，云端探不到流）
async function validateApiLight(a) {
  const r = { api: a.api, type: a.type, stage: 1, ok: false, samplePlayUrl: null, needsHeaders: null, classes: [], error: null };
  try {
    const resp = await fetchTimeout(a.api);
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    r.ok = true;
  } catch (e) {
    r.error = "可达性失败: " + e.message;
  }
  return r;
}

// 验证一个配置 URL（motv-index 的 sources[].url）
async function validateConfig(url) {
  const v = {
    url,
    name: "",
    ok: false,
    stage: 0,
    type: null,
    siteCount: 0,
    samplePlayUrl: null,
    needsHeaders: null,
    classes: [],
    latencyMs: 0,
    error: null,
    note: null,
  };
  const t0 = Date.now();
  let text;
  try {
    text = await fetchText(url);
  } catch (e) {
    v.error = "结构校验失败: " + e.message;
    v.latencyMs = Date.now() - t0;
    return v;
  }
  v.stage = 1;
  let cfgJson;
  try {
    cfgJson = JSON.parse(text);
  } catch {
    v.error = "非 JSON 配置";
    v.latencyMs = Date.now() - t0;
    return v;
  }

  const sites = Array.isArray(cfgJson.sites) ? cfgJson.sites : [];
  v.siteCount = sites.length;
  v.name = cfgJson.name || (sites[0] && sites[0].name) || extractName(url);

  const apis = [];
  if (cfgJson.api) apis.push({ api: cfgJson.api, type: cfgJson.type ?? 1 });
  for (const s of sites) {
    if (s && s.api) apis.push({ api: s.api, type: s.type ?? 1, name: s.name });
  }
  if (apis.length === 0) {
    v.error = "配置无可用 api";
    v.latencyMs = Date.now() - t0;
    return v;
  }

  // type=1 优先完整验证；其余按 api 形态区分
  const ordered = [...apis].sort((a, b) => (a.type === 1 ? -1 : 1) - (b.type === 1 ? -1 : 1));
  let best = null;
  for (const a of ordered) {
    let res;
    if (a.type === 1) {
      res = await validateApiFull(a);
    } else if (/^https?:\/\//i.test(a.api)) {
      res = await validateApiLight(a);
    } else {
      // type=2/3 且 api 为 spider 名（如 csp_xxx）：配置合法且含 sites 即结构可用
      res = {
        api: a.api,
        type: a.type,
        stage: 1,
        ok: true,
        note: `type=${a.type} 结构可用(APK 端运行 jar/csp，未做流探测)`,
        classes: [],
        needsHeaders: null,
        samplePlayUrl: null,
        error: null,
      };
    }
    if (!best || res.stage > best.stage || (res.stage === best.stage && res.ok && !best.ok)) best = res;
    if (res.ok) {
      best = res;
      break;
    }
  }
  v.type = best.type ?? null;
  v.stage = best.stage;
  v.ok = best.ok;
  v.samplePlayUrl = best.samplePlayUrl;
  v.needsHeaders = best.needsHeaders;
  v.classes = best.classes;
  v.note = best.note || null;
  v.latencyMs = Date.now() - t0;
  if (!v.ok && best) v.error = best.error || `仅达到 stage ${best.stage}`;
  return v;
}

function buildDesc(r) {
  const parts = [`type=${r.type ?? "?"}`];
  parts.push(`${today()} 验证通过(达 stage ${r.stage})`);
  if (r.latencyMs) parts.push(`延迟 ${r.latencyMs}ms`);
  parts.push(`${r.siteCount ?? "?"} 站点`);
  if (r.needsHeaders && r.needsHeaders.referer) parts.push("需 Referer");
  return parts.join("；");
}

// ---------------- 候选池展开 ----------------

async function expandCandidates() {
  const seedsRaw = loadJson(SEEDS_PATH, { seeds: [] });
  const map = new Map();
  const add = (url, name, from) => {
    if (!url || !/^https?:\/\//i.test(url)) return;
    const key = String(url).trim();
    if (map.has(key)) return;
    map.set(key, { url: key, name: name || "", from: from || "seed" });
  };

  for (const s of seedsRaw.seeds || []) {
    if (s.type === "index") await addFromIndex(s, add);
    else if (s.type === "raw") add(s.url, s.name, "seed:raw");
    else if (s.type === "links") await addFromLinks(s, add);
  }
  return [...map.values()];
}

async function addFromIndex(seed, add) {
  try {
    const json = await fetchJson(seed.url);
    // 格式A: { sources: [ {url} | {urls:[...]} ] }（如 Lightconer config/sources.json）
    if (Array.isArray(json.sources)) {
      for (const s of json.sources) {
        if (!s) continue;
        if (s.url) add(s.url, s.name || seed.name, "index:" + seed.name);
        if (Array.isArray(s.urls)) for (const u of s.urls) add(u, s.name || seed.name, "index:" + seed.name);
      }
    }
    // 格式B: 顶层数组 [ {url, name} ]（如 hkuc url.json）
    if (Array.isArray(json)) {
      for (const s of json) if (s && s.url) add(s.url, s.name || "", "index:" + seed.name);
    }
    // 格式C: 主配置含 sites[]，仅抽取 api 为 http(s) URL 的单线路源
    if (Array.isArray(json.sites)) {
      for (const s of json.sites) {
        if (s && s.api && /^https?:\/\//i.test(s.api)) add(s.api, s.name, "index:sites");
      }
    }
  } catch (e) {
    log(`[warn] 索引种子失败: ${seed.url} ${e.message}`);
  }
}

async function addFromLinks(seed, add) {
  try {
    const text = await fetchText(seed.url);
    const re = /https?:\/\/[^\s"'<>()]+/gi;
    let m;
    while ((m = re.exec(text))) add(m[0], "", "links:" + seed.name);
  } catch (e) {
    log(`[warn] links 种子失败: ${seed.url} ${e.message}`);
  }
}

// ---------------- 汇报 ----------------

function appendSummary(md) {
  if (!SUMMARY_PATH) return;
  try {
    appendFileSync(SUMMARY_PATH, md + "\n", "utf-8");
  } catch {}
}

function writeSummary({ total, target, added, removed, protectedDead, date }) {
  const lines = [];
  lines.push(`## motv 源刷新报告（${date}）`);
  lines.push("");
  lines.push(`| 指标 | 值 |`);
  lines.push(`| --- | --- |`);
  lines.push(`| 最终可用源 | **${total}** / 目标 ${target} |`);
  lines.push(`| 新增 | ${added.length} |`);
  lines.push(`| 剔除（失效） | ${removed.length} |`);
  lines.push(`| 保护源失效警告 | ${protectedDead.length} |`);
  lines.push("");
  if (added.length) {
    lines.push(`### 新增`);
    for (const a of added) lines.push(`- ${a.name} — \`${a.url}\``);
    lines.push("");
  }
  if (removed.length) {
    lines.push(`### 剔除（失效源）`);
    for (const r of removed) lines.push(`- ${r.name} — ${r.reason}`);
    lines.push("");
  }
  if (protectedDead.length) {
    lines.push(`### ⚠️ 保护源失效（未剔除，请人工确认）`);
    for (const p of protectedDead) lines.push(`- ${p.name} — ${p.reason}`);
    lines.push("");
  }
  appendSummary(lines.join("\n"));
}

// ---------------- 主流程 ----------------

async function main() {
  log(`[motv-refresh] ${today()} 目标 N=${TARGET_COUNT} 并发=${CONCURRENCY} 超时=${FETCH_TIMEOUT_MS}ms`);

  const index = loadJson(INDEX_PATH, { index_version: 2, updated: today(), sources: [] });
  const state = loadJson(STATE_PATH, {});
  const protectedNames = new Set(state.protected && state.protected.length ? state.protected : ["欧歌"]);
  const existing = Array.isArray(index.sources) ? index.sources : [];
  log(`[motv-refresh] 现有源 ${existing.length} 条，保护名单: ${[...protectedNames].join(", ") || "(无)"}`);

  // 1) 复验现有源
  log(`[motv-refresh] 复验现有源...`);
  const results = await mapLimit(existing, CONCURRENCY, (s) => validateConfig(s.url));
  const resByUrl = new Map(results.map((r) => [r.url, r]));

  const kept = [];
  const removed = [];
  const protectedDead = [];
  for (const s of existing) {
    const r = resByUrl.get(s.url);
    const ok = !!(r && r.ok);
    if (ok) {
      kept.push({ name: s.name, url: s.url, desc: s.desc || "" });
    } else if (protectedNames.has(s.name)) {
      kept.push({ name: s.name, url: s.url, desc: `${s.desc || ""} [${today()} 失效警告]`.trim() });
      protectedDead.push({ name: s.name, url: s.url, reason: r ? r.error || `仅达 stage ${r.stage}` : "未验证" });
    } else {
      removed.push({ name: s.name || s.url, url: s.url, reason: r ? r.error || `仅达 stage ${r.stage}` : "未验证" });
    }
  }
  log(`[motv-refresh] 复验完成：保留 ${kept.length}，剔除 ${removed.length}，保护失效 ${protectedDead.length}`);

  // 2) 补足到 N
  const added = [];
  if (kept.length < TARGET_COUNT) {
    const need = TARGET_COUNT - kept.length;
    log(`[motv-refresh] 可用源不足，需补 ${need} 条，展开候选池...`);
    const candidates = await expandCandidates();
    const existingUrls = new Set(existing.map((s) => s.url));
    const fresh = candidates.filter((c) => !existingUrls.has(c.url)).slice(0, MAX_CANDIDATES);
    log(`[motv-refresh] 候选 ${fresh.length} 个，验证中...`);
    const vres = await mapLimit(fresh, CONCURRENCY, (c) => validateConfig(c.url));
    const good = vres
      .filter((r) => r.ok)
      .sort((a, b) => b.stage - a.stage || (a.latencyMs || 9e9) - (b.latencyMs || 9e9));
    for (const r of good.slice(0, need)) {
      kept.push({ name: r.name || r.url, url: r.url, desc: buildDesc(r) });
      added.push({ name: r.name || r.url, url: r.url });
    }
    log(`[motv-refresh] 补足完成：新增 ${added.length} 条`);
  } else {
    log(`[motv-refresh] 可用源已达 ${kept.length} >= ${TARGET_COUNT}，无需补足`);
  }

  // 3) 写回 index + state（git 是否推送由 workflow 依据 diff 决定）
  const newIndex = {
    index_version: index.index_version || 2,
    updated: today(),
    sources: kept,
  };
  writeFileSync(INDEX_PATH, JSON.stringify(newIndex, null, 2) + "\n", "utf-8");

  const newState = {
    protected: [...protectedNames],
    managedUrls: kept.filter((s) => !protectedNames.has(s.name)).map((s) => s.url),
    lastRun: today(),
    lastCounts: { kept: kept.length, added: added.length, removed: removed.length, targetCount: TARGET_COUNT },
    note:
      "protected=手动维护、失效也不自动剔除(按 name 匹配)；其余源失效自动剔除，少于 N 从 tools/seeds.json 候选池补足。可在对话里说「保护 X / 取消保护 X」维护。",
  };
  writeFileSync(STATE_PATH, JSON.stringify(newState, null, 2) + "\n", "utf-8");

  // 4) 汇报
  writeSummary({ total: kept.length, target: TARGET_COUNT, added, removed, protectedDead, date: today() });
  log(`[motv-refresh] 完成：最终 ${kept.length} 条（新增 ${added.length} / 剔除 ${removed.length}）`);
}

main().catch((e) => {
  console.error("[motv-refresh] 引擎异常:", e);
  // 不 exit 1：探活失败/网络抖动不应让 workflow 变红；异常已在日志中体现
  process.exit(0);
});

const BASE_URL = "https://www.margonem.pl";
const STATE_KEY = "monitor_state_v1";
const DISCORD_EMBED_MAX_FIELDS = 25;
const DISCORD_EMBED_MAX_CHARS = 6000;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    if (url.pathname === "/health") {
      return jsonResponse({ ok: true, ts: new Date().toISOString() });
    }

    if (url.pathname === "/online") {
      try {
        const cfg = loadConfig(env);
        const snapshot = await computeSnapshot(cfg);
        return jsonResponse({ ok: true, ...snapshot });
      } catch (error) {
        return jsonResponse(
          {
            ok: false,
            error: "Snapshot error",
            details: String(error?.message || error),
          },
          500,
        );
      }
    }

    if (url.pathname === "/run" && request.method === "POST") {
      try {
        const result = await runMonitorCycle(env);
        return jsonResponse({ ok: true, ...result });
      } catch (error) {
        return jsonResponse(
          {
            ok: false,
            error: "Run failed",
            details: String(error?.message || error),
          },
          500,
        );
      }
    }

    if (url.pathname === "/run" && request.method === "GET") {
      ctx.waitUntil(runMonitorCycle(env));
      return jsonResponse({ ok: true, queued: true });
    }

    return jsonResponse(
      {
        ok: false,
        error: "Not found",
        available: ["/health", "/online", "POST /run"],
      },
      404,
    );
  },

  async scheduled(_event, env, ctx) {
    ctx.waitUntil(runMonitorCycle(env));
  },
};

async function runMonitorCycle(env) {
  const cfg = loadConfig(env);
  const state = await loadState(env);
  const nowTs = Math.floor(Date.now() / 1000);

  const snapshot = await computeSnapshot(cfg);
  let cycleData = buildCycleData(snapshot, state.onlineNorm || []);
  const history = enrichWith10mDelta(cycleData, state.history || [], nowTs);
  cycleData = history.cycleData;

  const profileChanged =
    (state.webhookUsername || "") !== (cfg.webhookUsername || "") ||
    (state.webhookAvatarUrl || "") !== (cfg.webhookAvatarUrl || "");

  if (profileChanged && cfg.webhookUrl) {
    for (const msgId of [state.webhookStatsMessageId, state.webhookNicksMessageId]) {
      if (!msgId) continue;
      try {
        await deleteWebhookMessage(cfg, String(msgId));
      } catch (_error) {
        // Ignore cleanup failures and recreate tracked messages.
      }
    }
  }

  let webhookStatsMessageId = profileChanged ? null : (state.webhookStatsMessageId || null);
  let webhookNicksMessageId = profileChanged ? null : (state.webhookNicksMessageId || null);
  let sentWebhook = false;

  if (cfg.outputMode === "discord" || cfg.outputMode === "both") {
    if (!cfg.webhookUrl) {
      throw new Error("Missing WEBHOOK_URL for discord output mode");
    }

    const statsPayload = buildDiscordStatsPayload(cfg.world, cycleData, cfg.webhookAvatarUrl);
    const nicksPayload = buildDiscordNicksPayload(cfg.world, cycleData, cfg.webhookAvatarUrl);

    webhookStatsMessageId = await upsertWebhookMessage(
      cfg,
      statsPayload,
      webhookStatsMessageId,
    );
    webhookNicksMessageId = await upsertWebhookMessage(
      cfg,
      nicksPayload,
      webhookNicksMessageId,
    );
    sentWebhook = true;
  }

  await saveState(env, {
    updatedAt: utcNow(),
    onlineNorm: cycleData.currentOnlineNorm,
    webhookStatsMessageId,
    webhookNicksMessageId,
    webhookUsername: cfg.webhookUsername,
    webhookAvatarUrl: cfg.webhookAvatarUrl,
    history: history.history,
  });

  return {
    checkedAt: new Date().toISOString(),
    world: cfg.world,
    sentWebhook,
    trackedMembersCount: cycleData.trackedMembersCount,
    onlineCount: cycleData.onlineCount,
    wentOnlineCount: cycleData.wentOnline.length,
    wentOfflineCount: cycleData.wentOffline.length,
    webhookStatsMessageId,
    webhookNicksMessageId,
  };
}

async function computeSnapshot(cfg) {
  if (cfg.guildIds.length === 0) {
    throw new Error("GUILD_IDS is empty");
  }

  const headers = {
    "user-agent": cfg.userAgent,
    "accept-language": "pl-PL,pl;q=0.9,en;q=0.8",
  };

  const [statsHtml, guildData] = await Promise.all([
    fetchText(`${BASE_URL}/stats`, headers, cfg.requestTimeout),
    fetchAllGuildMembers(cfg.world, cfg.guildIds, headers, cfg.requestTimeout),
  ]);

  const onlineNames = parseOnlineNamesForWorld(statsHtml, cfg.world);
  const onlineMap = toNormMap(onlineNames);

  const trackedMembers = new Set();
  for (const guild of guildData) {
    for (const member of guild.members) {
      trackedMembers.add(member);
    }
  }

  const trackedMap = toNormMap(trackedMembers);
  const currentOnlineNorm = Object.keys(trackedMap).filter((norm) => onlineMap[norm]);
  const trackedOnlineNames = currentOnlineNorm
    .map((norm) => onlineMap[norm])
    .sort((a, b) => a.localeCompare(b, "pl"));

  const guildBreakdown = guildData
    .slice()
    .sort((a, b) => a.guildId - b.guildId)
    .map((guild) => {
      const guildNorm = guild.members.map(normalizeName);
      const guildOnlineNorm = guildNorm.filter((norm) => onlineMap[norm]);
      const guildOnlineNames = guildOnlineNorm
        .map((norm) => onlineMap[norm])
        .sort((a, b) => a.localeCompare(b, "pl"));

      return {
        id: guild.guildId,
        name: guild.guildName,
        membersCount: guild.members.length,
        onlineCount: guildOnlineNames.length,
        onlineNames: guildOnlineNames,
      };
    });

  return {
    world: cfg.world,
    checkedAt: new Date().toISOString(),
    trackedMembersCount: trackedMembers.size,
    trackedNames: [...trackedMembers].sort((a, b) => a.localeCompare(b, "pl")),
    onlineCount: trackedOnlineNames.length,
    onlineNames: trackedOnlineNames,
    currentOnlineNorm,
    guildBreakdown,
  };
}

function buildCycleData(snapshot, previousOnlineNormList) {
  const previousOnlineNorm = new Set((previousOnlineNormList || []).map(normalizeName));
  const currentOnlineNorm = new Set((snapshot.currentOnlineNorm || []).map(normalizeName));

  const onlineMap = toNormMap(snapshot.onlineNames || []);
  const trackedMap = toNormMap(snapshot.trackedNames || []);

  const wentOnlineNorm = [...currentOnlineNorm].filter((norm) => !previousOnlineNorm.has(norm)).sort();
  const wentOfflineNorm = [...previousOnlineNorm].filter((norm) => !currentOnlineNorm.has(norm)).sort();

  return {
    trackedMembersCount: snapshot.trackedMembersCount,
    guildBreakdown: snapshot.guildBreakdown,
    onlineCount: snapshot.onlineCount,
    onlineNames: snapshot.onlineNames,
    wentOnline: wentOnlineNorm.map((norm) => onlineMap[norm]).filter(Boolean),
    wentOffline: wentOfflineNorm.map((norm) => trackedMap[norm]).filter(Boolean),
    currentOnlineNorm: [...currentOnlineNorm].sort(),
    delta10m: 0,
  };
}

function enrichWith10mDelta(cycleData, rawHistory, nowTs) {
  const history = pruneHistory(Array.isArray(rawHistory) ? rawHistory : [], nowTs, 600);
  const baseline = history.length > 0 ? history[0] : null;

  const baselineTotal = baseline ? toInt(baseline.onlineCount, cycleData.onlineCount) : cycleData.onlineCount;
  cycleData.delta10m = cycleData.onlineCount - baselineTotal;

  const baselineGuildCounts = baseline && baseline.guildCounts && typeof baseline.guildCounts === "object"
    ? baseline.guildCounts
    : {};

  for (const guild of cycleData.guildBreakdown) {
    const baselineCount = toInt(baselineGuildCounts[String(guild.id)], guild.onlineCount);
    guild.delta10m = guild.onlineCount - baselineCount;
  }

  history.push({
    ts: nowTs,
    onlineCount: cycleData.onlineCount,
    guildCounts: getGuildOnlineMap(cycleData),
  });

  return {
    cycleData,
    history: pruneHistory(history, nowTs, 600),
  };
}

function trendStyle(delta10m) {
  if (delta10m >= 20) return { color: 0xe74c3c, label: "Zagrozenie", marker: "RED" };
  if (delta10m >= 3) return { color: 0xf39c12, label: "Wzrost", marker: "ORANGE" };
  if (delta10m > 0) return { color: 0xf1c40f, label: "Lekki wzrost", marker: "YELLOW" };
  if (delta10m < 0) return { color: 0x2ecc71, label: "Spadek", marker: "GREEN" };
  return { color: 0x3498db, label: "Stabilnie", marker: "BLUE" };
}

function guildDeltaMarker(delta10m) {
  if (delta10m >= 6) return "[RED]";
  if (delta10m > 0) return "[ORANGE]";
  if (delta10m < 0) return "[GREEN]";
  return "[BLUE]";
}

function buildDiscordStatsPayload(world, cycleData, avatarUrl) {
  const nowIso = new Date().toISOString();
  const title = `Margonem monitor | ${world} | statystyki`;
  const delta10m = toInt(cycleData.delta10m, 0);
  const trend = trendStyle(delta10m);
  const deltaSign = delta10m > 0 ? "+" : "";

  const embed = {
    title: truncateText(title, 256),
    description: truncateText(`Zaktualizowano: ${utcNow()}`, 4096),
    color: trend.color,
    timestamp: nowIso,
    fields: [],
  };

  if (avatarUrl) {
    embed.thumbnail = { url: avatarUrl };
  }

  const fields = [
    { name: "Sledzonych", value: String(cycleData.trackedMembersCount), inline: true },
    { name: "Online teraz", value: String(cycleData.onlineCount), inline: true },
    {
      name: "Zmiana 10m",
      value: `${trend.marker} ${deltaSign}${delta10m} (${trend.label})`,
      inline: true,
    },
  ];

  if (!cycleData.guildBreakdown || cycleData.guildBreakdown.length === 0) {
    fields.push({
      name: "Brak danych klanow",
      value: "Sprawdz konfiguracje guild_ids.",
      inline: false,
    });
  } else {
    const guildInlineFields = [];
    for (const guild of cycleData.guildBreakdown) {
      const guildDelta = toInt(guild.delta10m, 0);
      const guildSign = guildDelta > 0 ? "+" : "";
      guildInlineFields.push({
        name: truncateText(`${guildDeltaMarker(guildDelta)} ${guild.name}`, 256),
        value: truncateText(
          `Online: **${guild.onlineCount}/${guild.membersCount}**\nZmiana 10m: **${guildSign}${guildDelta}**`,
          1024,
        ),
        inline: true,
      });
    }

    while (guildInlineFields.length % 3 !== 0) {
      guildInlineFields.push({ name: "\u200b", value: "\u200b", inline: true });
    }

    fields.push(...guildInlineFields);
  }

  embed.fields = applyDiscordEmbedLimits(embed.title, embed.description, fields);

  return {
    content: "",
    embeds: [embed],
    allowed_mentions: { parse: [] },
  };
}

function buildDiscordNicksPayload(world, cycleData, avatarUrl) {
  const nowIso = new Date().toISOString();
  const title = `Margonem monitor | ${world} | nicki online`;

  const embed = {
    title: truncateText(title, 256),
    description: truncateText(`Zaktualizowano: ${utcNow()}`, 4096),
    color: 0x5865f2,
    timestamp: nowIso,
    fields: [],
  };

  if (avatarUrl) {
    embed.thumbnail = { url: avatarUrl };
  }

  const fields = [];
  const guildRows = cycleData.guildBreakdown || [];

  if (guildRows.length === 0) {
    fields.push({
      name: "Brak danych klanow",
      value: "Sprawdz konfiguracje guild_ids.",
      inline: false,
    });
  } else {
    for (const guild of guildRows) {
      const chunks = splitNamesForDiscordSpoiler(guild.onlineNames || []);
      const totalChunks = chunks.length;

      for (let i = 0; i < chunks.length; i += 1) {
        const suffix = totalChunks === 1 ? "" : ` (${i + 1}/${totalChunks})`;
        fields.push({
          name: truncateText(`Nicki - ${guild.name}${suffix}`, 256),
          value: truncateText(`||${chunks[i]}||`, 1024),
          inline: false,
        });
      }
    }
  }

  embed.fields = applyDiscordEmbedLimits(embed.title, embed.description, fields);

  return {
    content: "",
    embeds: [embed],
    allowed_mentions: { parse: [] },
  };
}

async function upsertWebhookMessage(cfg, payload, messageId) {
  const patchPayload = {
    content: String(payload.content || "").slice(0, 1900),
    embeds: payload.embeds || [],
    allowed_mentions: payload.allowed_mentions || { parse: [] },
  };

  if (messageId) {
    const patchUrl = `${cfg.webhookUrl.replace(/\/+$/, "")}/messages/${messageId}`;
    const patchRes = await discordRequest("PATCH", patchUrl, patchPayload, cfg.requestTimeout);
    if (patchRes.status !== 404) {
      if (!patchRes.ok) {
        const body = await patchRes.text();
        throw new Error(`Discord PATCH failed (${patchRes.status}): ${body}`);
      }
      return messageId;
    }
  }

  const waitUrl = cfg.webhookUrl.includes("?")
    ? `${cfg.webhookUrl}&wait=true`
    : `${cfg.webhookUrl}?wait=true`;

  const postPayload = { ...patchPayload };
  if (cfg.webhookUsername) postPayload.username = cfg.webhookUsername;
  if (cfg.webhookAvatarUrl) postPayload.avatar_url = cfg.webhookAvatarUrl;

  const postRes = await discordRequest("POST", waitUrl, postPayload, cfg.requestTimeout);
  if (!postRes.ok) {
    const body = await postRes.text();
    throw new Error(`Discord POST failed (${postRes.status}): ${body}`);
  }

  const data = await postRes.json();
  return data && data.id ? String(data.id) : null;
}

async function deleteWebhookMessage(cfg, messageId) {
  const deleteUrl = `${cfg.webhookUrl.replace(/\/+$/, "")}/messages/${messageId}`;
  const deleteRes = await discordRequest("DELETE", deleteUrl, null, cfg.requestTimeout);
  if (deleteRes.status === 204 || deleteRes.status === 404) {
    return;
  }
  if (!deleteRes.ok) {
    const body = await deleteRes.text();
    throw new Error(`Discord DELETE failed (${deleteRes.status}): ${body}`);
  }
}

async function discordRequest(method, url, payload, timeoutSeconds, maxAttempts = 4) {
  let lastRes = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const res = await fetchJson(method, url, payload, timeoutSeconds);
    lastRes = res;
    if (res.status !== 429 || attempt >= maxAttempts) {
      return res;
    }

    const retryAfter = parseRetryAfterSeconds(res, Math.min(2 ** attempt, 30));
    await sleep(Math.max(retryAfter, 0.5) * 1000);
  }

  return lastRes;
}

async function fetchJson(method, url, payload, timeoutSeconds) {
  const controller = new AbortController();
  const timeoutMs = Math.max(1, timeoutSeconds) * 1000;
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      method,
      headers: {
        "content-type": "application/json",
      },
      body: payload == null ? null : JSON.stringify(payload),
      signal: controller.signal,
    });
  } catch (error) {
    if (error && typeof error === "object" && error.name === "AbortError") {
      throw new Error(`Request timeout after ${timeoutSeconds}s`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

function parseRetryAfterSeconds(response, fallbackSeconds) {
  const retryAfterHeader = response.headers.get("retry-after");
  if (retryAfterHeader) {
    const parsed = Number.parseFloat(retryAfterHeader);
    if (Number.isFinite(parsed)) {
      return Math.max(parsed, 0.5);
    }
  }
  return Math.max(fallbackSeconds, 0.5);
}

function splitNamesForDiscordSpoiler(names, maxChunkLen = 980) {
  if (!names || names.length === 0) {
    return ["brak"];
  }

  const chunks = [];
  let current = [];
  let currentLen = 0;

  for (const name of names) {
    const partLen = current.length === 0 ? name.length : name.length + 2;

    if (partLen > maxChunkLen) {
      if (current.length > 0) {
        chunks.push(current.join(", "));
        current = [];
        currentLen = 0;
      }
      chunks.push(name.slice(0, maxChunkLen));
      continue;
    }

    if (currentLen + partLen > maxChunkLen) {
      chunks.push(current.join(", "));
      current = [name];
      currentLen = name.length;
      continue;
    }

    current.push(name);
    currentLen += partLen;
  }

  if (current.length > 0) {
    chunks.push(current.join(", "));
  }

  return chunks;
}

function applyDiscordEmbedLimits(title, description, fields) {
  const kept = [];
  let omitted = 0;

  for (const field of fields) {
    if (kept.length >= DISCORD_EMBED_MAX_FIELDS) {
      omitted += 1;
      continue;
    }

    const trial = [...kept, field];
    if (embedCharCount(title, description, trial) > DISCORD_EMBED_MAX_CHARS) {
      omitted += 1;
      continue;
    }

    kept.push(field);
  }

  if (omitted > 0) {
    const noteField = {
      name: "Dodatkowe dane",
      value: `Pominieto ${omitted} pol przez limity Discord (25 pol / 6000 znakow).`,
      inline: false,
    };

    while (kept.length >= DISCORD_EMBED_MAX_FIELDS) {
      kept.pop();
    }

    while (kept.length > 0 && embedCharCount(title, description, [...kept, noteField]) > DISCORD_EMBED_MAX_CHARS) {
      kept.pop();
    }

    if (embedCharCount(title, description, [...kept, noteField]) <= DISCORD_EMBED_MAX_CHARS) {
      kept.push(noteField);
    }
  }

  return kept;
}

function embedCharCount(title, description, fields) {
  let total = String(title || "").length + String(description || "").length;
  for (const field of fields) {
    total += String(field.name || "").length;
    total += String(field.value || "").length;
  }
  return total;
}

function getGuildOnlineMap(cycleData) {
  const out = {};
  for (const guild of cycleData.guildBreakdown || []) {
    out[String(guild.id)] = toInt(guild.onlineCount, 0);
  }
  return out;
}

function pruneHistory(history, nowTs, windowSeconds) {
  const cutoff = nowTs - windowSeconds;
  const out = [];
  for (const item of history) {
    if (!item || typeof item !== "object") continue;
    const ts = toInt(item.ts, -1);
    if (ts >= cutoff) out.push(item);
  }
  return out;
}

async function loadState(env) {
  if (!env.MONITOR_STATE) {
    return defaultState();
  }

  const raw = await env.MONITOR_STATE.get(STATE_KEY, "json");
  if (!raw || typeof raw !== "object") {
    return defaultState();
  }

  return {
    onlineNorm: Array.isArray(raw.onlineNorm) ? raw.onlineNorm : [],
    webhookStatsMessageId: toNullableString(raw.webhookStatsMessageId),
    webhookNicksMessageId: toNullableString(raw.webhookNicksMessageId),
    webhookUsername: toNullableString(raw.webhookUsername),
    webhookAvatarUrl: toNullableString(raw.webhookAvatarUrl),
    history: Array.isArray(raw.history) ? raw.history : [],
  };
}

async function saveState(env, state) {
  if (!env.MONITOR_STATE) {
    return;
  }

  await env.MONITOR_STATE.put(STATE_KEY, JSON.stringify(state));
}

function defaultState() {
  return {
    onlineNorm: [],
    webhookStatsMessageId: null,
    webhookNicksMessageId: null,
    webhookUsername: null,
    webhookAvatarUrl: null,
    history: [],
  };
}

function parseGuildIds(raw) {
  return String(raw || "")
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean)
    .map((v) => Number(v))
    .filter((n) => Number.isInteger(n) && n > 0);
}

function loadConfig(env) {
  const world = parseString(env.WORLD, "gordion").toLowerCase();
  const guildIds = parseGuildIds(parseString(env.GUILD_IDS, ""));

  return {
    world,
    guildIds,
    outputMode: parseString(env.OUTPUT_MODE, "terminal").toLowerCase(),
    webhookUrl: parseString(env.WEBHOOK_URL, ""),
    webhookUsername: parseString(env.WEBHOOK_USERNAME, "Cwel monitor"),
    webhookAvatarUrl: parseString(env.WEBHOOK_AVATAR_URL, ""),
    pollSeconds: parsePositiveInt(env.POLL_SECONDS, 10),
    guildRefreshSeconds: parsePositiveInt(env.GUILD_REFRESH_SECONDS, 3600),
    requestTimeout: parsePositiveInt(env.REQUEST_TIMEOUT, 20),
    stateFile: parseString(env.STATE_FILE, "state.json"),
    notifyOnStartup: parseBool(env.NOTIFY_ON_STARTUP, true),
    userAgent: parseString(
      env.USER_AGENT,
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MargonemGuildMonitor/1.0",
    ),
  };
}

function parseString(value, fallback) {
  const parsed = typeof value === "string" ? value.trim() : "";
  return parsed || fallback;
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (Number.isInteger(parsed) && parsed > 0) {
    return parsed;
  }
  return fallback;
}

function parseBool(value, fallback) {
  if (typeof value === "boolean") return value;
  if (typeof value !== "string") return fallback;

  const norm = value.trim().toLowerCase();
  if (norm === "true" || norm === "1" || norm === "yes") return true;
  if (norm === "false" || norm === "0" || norm === "no") return false;
  return fallback;
}

function toInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toNullableString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function utcNow() {
  return new Date().toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC");
}

function truncateText(value, limit) {
  const text = String(value || "");
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 3))}...`;
}

async function fetchAllGuildMembers(world, guildIds, headers, timeoutSeconds) {
  const jobs = guildIds.map(async (guildId) => {
    const guildUrl = `${BASE_URL}/guilds/view,${encodeURIComponent(world)},${guildId}`;
    const guildHtml = await fetchText(guildUrl, headers, timeoutSeconds);

    return {
      guildId,
      guildName: parseGuildName(guildHtml, guildId),
      members: parseGuildMembers(guildHtml),
    };
  });

  return Promise.all(jobs);
}

async function fetchText(url, headers, timeoutSeconds) {
  const maxAttempts = 4;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const controller = new AbortController();
    const timeoutMs = Math.max(1, timeoutSeconds) * 1000;
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        method: "GET",
        headers,
        signal: controller.signal,
        cf: {
          cacheTtl: 15,
          cacheEverything: false,
        },
      });

      if (res.status === 429 && attempt < maxAttempts) {
        const retryAfter = parseRetryAfterSeconds(res, Math.min(2 ** attempt, 30));
        await sleep(retryAfter * 1000);
        continue;
      }

      if (!res.ok) {
        if (res.status >= 500 && attempt < maxAttempts) {
          await sleep(Math.min(2 ** attempt, 30) * 1000);
          continue;
        }
        throw new Error(`HTTP ${res.status} while fetching ${url}`);
      }

      return await res.text();
    } catch (error) {
      lastError = error;
      const isAbort = error && typeof error === "object" && error.name === "AbortError";
      if ((isAbort || attempt < maxAttempts) && attempt < maxAttempts) {
        await sleep(Math.min(2 ** attempt, 30) * 1000);
        continue;
      }

      if (isAbort) {
        throw new Error(`Request timeout after ${timeoutSeconds}s while fetching ${url}`);
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  if (lastError) {
    throw lastError;
  }
  throw new Error(`Unknown fetch error for ${url}`);
}

function parseGuildName(html, guildId) {
  const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  if (!titleMatch) return `Klan ${guildId}`;

  const titleText = decodeEntities(stripHtml(titleMatch[1])).trim();
  if (!titleText) return `Klan ${guildId}`;

  const [name] = titleText.split(" - ");
  return (name || `Klan ${guildId}`).trim();
}

function parseGuildMembers(html) {
  const rows = html.match(/<tr[\s\S]*?<\/tr>/gi) || [];
  const members = new Set();

  for (const row of rows) {
    const cells = row.match(/<td[\s\S]*?<\/td>/gi) || [];
    if (cells.length < 2) continue;

    const rawName = decodeEntities(stripHtml(cells[1])).trim();
    if (rawName) members.add(collapseWhitespace(rawName));
  }

  return [...members].sort((a, b) => a.localeCompare(b, "pl"));
}

function parseOnlineNamesForWorld(html, world) {
  const worldClass = `${world.toLowerCase()}-popup`;
  const popupPos = html.toLowerCase().indexOf(worldClass);
  if (popupPos < 0) {
    throw new Error(`Nie znaleziono popupa swiata: ${world}`);
  }

  const sectionHtml = html.slice(popupPos);
  const bodyStartMatch = sectionHtml.match(/<div[^>]*class=["'][^"']*news-body[^"']*["'][^>]*>/i);
  if (!bodyStartMatch || bodyStartMatch.index == null) {
    throw new Error(`Nie znaleziono sekcji news-body dla swiata: ${world}`);
  }

  const bodyStart = bodyStartMatch.index + bodyStartMatch[0].length;
  const afterBody = sectionHtml.slice(bodyStart);
  const footerPos = afterBody.search(/<div[^>]*class=["'][^"']*news-footer[^"']*["'][^>]*>/i);
  const searchArea = footerPos >= 0 ? afterBody.slice(0, footerPos) : afterBody;

  const anchors = searchArea.match(/<a[\s\S]*?<\/a>/gi) || [];

  const names = new Set();
  for (const anchor of anchors) {
    const text = decodeEntities(stripHtml(anchor)).trim();
    if (text) names.add(collapseWhitespace(text));
  }

  return [...names];
}

function toNormMap(names) {
  const out = {};
  for (const name of names || []) {
    out[normalizeName(name)] = name;
  }
  return out;
}

function stripHtml(input) {
  return String(input || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ");
}

function decodeEntities(text) {
  return String(text || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
}

function collapseWhitespace(text) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

function normalizeName(name) {
  return collapseWhitespace(name).toLocaleLowerCase("pl");
}

function escapeRegex(input) {
  return String(input || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,authorization",
  };
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      ...corsHeaders(),
    },
  });
}

import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import mqtt from 'mqtt';
import crypto from 'crypto';
import { fromBinary } from '@bufbuild/protobuf';

import {
  Mqtt,
  Mesh,
  Portnums,
  Telemetry
} from '@meshtastic/protobufs';

// ======================================================
// VIRTUALMESH
// READ ONLY MQTT DIAGNOSTIC RECEIVER
// ======================================================

const PORT = process.env.PORT || 8080;

const MQTT_URL =
  process.env.MQTT_URL ||
  'mqtts://mqtt.meshtastic.org:8883';

const MQTT_USER = process.env.MQTT_USER || '';
const MQTT_PASS = process.env.MQTT_PASS || '';

// Spain community MQTT source (official community infrastructure).
// TLS is disabled by the community configuration, so the default uses mqtt://:1883.
const SPAIN_MQTT_URL = process.env.SPAIN_MQTT_URL || 'mqtt://mqtt.meshtastic.es:1883';
const SPAIN_MQTT_USER = process.env.SPAIN_MQTT_USER || 'meshdev';
const SPAIN_MQTT_PASS = process.env.SPAIN_MQTT_PASS || 'large4cats';
const SPAIN_MQTT_TOPIC = process.env.SPAIN_MQTT_TOPIC || 'msh/EU_868/#';

// O Zulo community MQTT source (Galicia / Spain), documented by Mesh Galicia.
// Separate diagnostic source; packets are normalized into the SPAIN region.
const OZULO_MQTT_URL = process.env.OZULO_MQTT_URL || 'mqtt://mqtt.mesh.comunidadeozulo.org:1883';
const OZULO_MQTT_USER = process.env.OZULO_MQTT_USER || 'meshzulo';
const OZULO_MQTT_PASS = process.env.OZULO_MQTT_PASS || 'zulo4ever';
const OZULO_MQTT_TOPIC = process.env.OZULO_MQTT_TOPIC || 'msh/EU_868/#';


// Latin America community sources - READ ONLY
const CHILE_MQTT_URL = process.env.CHILE_MQTT_URL || 'mqtt://mqtt.meshchile.cl:1883';
const CHILE_MQTT_USER = process.env.CHILE_MQTT_USER || 'mshcl2025';
const CHILE_MQTT_PASS = process.env.CHILE_MQTT_PASS || 'meshtastic.cl';
const CHILE_MQTT_TOPIC = process.env.CHILE_MQTT_TOPIC || 'msh/CL/#';

const COLOMBIA_MQTT_URL = process.env.COLOMBIA_MQTT_URL || 'mqtt://mqtt.meshcolombia.co:1883';
const COLOMBIA_MQTT_USER = process.env.COLOMBIA_MQTT_USER || 'meshcousers';
const COLOMBIA_MQTT_PASS = process.env.COLOMBIA_MQTT_PASS || 'meshcousers';
const COLOMBIA_MQTT_TOPIC = process.env.COLOMBIA_MQTT_TOPIC || 'msh/CO/#';

const ozuloMqttDiag = {
  state: 'initializing',
  url: OZULO_MQTT_URL,
  topic: OZULO_MQTT_TOPIC,
  connectedAt: null,
  lastPacketAt: null,
  packets: 0,
  reconnects: 0,
  closes: 0,
  lastError: null,
  lastSuback: null
};

const spainMqttDiag = {
  state: 'initializing',
  url: SPAIN_MQTT_URL,
  topic: SPAIN_MQTT_TOPIC,
  connectedAt: null,
  lastPacketAt: null,
  packets: 0,
  reconnects: 0,
  closes: 0,
  lastError: null,
  lastSuback: null
};

// ======================================================
// REGIONS
// ======================================================

const REGIONS = [
  // One US root. Geographic state/territory is derived from decoded coordinates.
  { id: 'PR', name: 'Puerto Rico priority root', topic: 'msh/US/PR/#' },
  { id: 'US', name: 'United States / territories', topic: 'msh/US/2/#' },
  // EU_868 is retained only as the transport source. Operational EU nodes are
  // accepted only when their decoded coordinates can be classified as Spain.
  { id: 'EU868', name: 'Europe via global EU_868 public root', topic: 'msh/EU_868/2/#' },
  // ANZ is the regional root used by several Latin-American communities.
  { id: 'ANZ', name: 'ANZ regional public root (Latin America discovery)', topic: 'msh/ANZ/#' },
  { id: 'SPAIN', name: 'Spain community MQTT', topic: SPAIN_MQTT_TOPIC, broker: 'mqtt.meshtastic.es' },
  { id: 'CHILE', name: 'MeshChile community MQTT', topic: CHILE_MQTT_TOPIC, broker: 'mqtt.meshchile.cl' },
  { id: 'COLOMBIA', name: 'Meshtastic Colombia community MQTT', topic: COLOMBIA_MQTT_TOPIC, broker: 'mqtt.meshcolombia.co' }
];

const DEDICATED_REGION_IDS = new Set(['SPAIN','CHILE','COLOMBIA']);
const GLOBAL_REGIONS = REGIONS.filter(region => !DEDICATED_REGION_IDS.has(region.id));
const TOPICS = GLOBAL_REGIONS.map(region => region.topic);
const SPAIN_REGION = REGIONS.find(region => region.id === 'SPAIN');
const CHILE_REGION = REGIONS.find(region => region.id === 'CHILE');
const COLOMBIA_REGION = REGIONS.find(region => region.id === 'COLOMBIA');

// ======================================================
// LIVE CORE - TEMPORAL CLASSIFICATION
// ======================================================

const LIVE_MAX_AGE_SECONDS = 15 * 60;
const RECENT_MAX_AGE_SECONDS = 24 * 60 * 60;

const temporalStats = {
  live: 0,
  recent: 0,
  stale: 0,
  unknown: 0,
  staleNoPayloadDiscarded: 0,
  retainedStaleNoPayloadDiscarded: 0
};

function classifyPacketAge(packet) {
  const rx = Number(packet?.rxTime || 0);
  if (!Number.isFinite(rx) || rx <= 0) {
    temporalStats.unknown++;
    return { ageClass: 'UNKNOWN', ageSeconds: null, rxTime: null };
  }

  const now = Math.floor(Date.now() / 1000);
  const ageSeconds = Math.max(0, now - rx);
  let ageClass;

  if (ageSeconds <= LIVE_MAX_AGE_SECONDS) {
    ageClass = 'LIVE';
    temporalStats.live++;
  } else if (ageSeconds <= RECENT_MAX_AGE_SECONDS) {
    ageClass = 'RECENT';
    temporalStats.recent++;
  } else {
    ageClass = 'STALE';
    temporalStats.stale++;
  }

  return { ageClass, ageSeconds, rxTime: rx };
}

// ======================================================
// MQTT CLIENT ID
// ======================================================

const CLIENT_ID =
  'virtualmesh-' +
  crypto.randomBytes(6).toString('hex');

// ======================================================
// DEFAULT PUBLIC LONGFAST KEY
// ======================================================

const LONGFAST_KEY = Buffer.from([
  0xd4, 0xf1, 0xbb, 0x3a,
  0x20, 0x29, 0x07, 0x59,
  0xf0, 0xbc, 0xff, 0xab,
  0xcf, 0x4e, 0x69, 0x01
]);

// Public community channel keys discovered from community documentation.
// AQ== maps to Meshtastic's standard public/default channel key (LONGFAST_KEY).
const PUBLIC_CHANNEL_KEYS = new Map([
  ['LONGFAST', LONGFAST_KEY],
  ['SFNARROW', LONGFAST_KEY],
  ['BAIRESMESH', Buffer.from('aB3K7ZIciBKq49nxn5gVmPQEtbTUVZOHKxuCaCKaHtA=', 'base64')],
  ['ROSARIOMESH', Buffer.from('kss+4MMhc9unauU8i6bix0Lt/pkjWMv1PIFr0fH8g58=', 'base64')],
  ['NQNMESH', Buffer.from('B7jYDJLWy9TSnajWI/yAJETLBcjN2RNXUU4jS4eRyJo=', 'base64')],
  ['CORDOBAMESH', Buffer.from('CoRd0B4lHaBoN6OWT0u2EvNX9Jci7gsIiIJtD30BCCw=', 'base64')],
  ['ERMESH', Buffer.from('w9nTAUTYp2eFo7KyCfo5a42YSM4ewfrV/PSoxcjrAPI=', 'base64')],
  ['MENDOZAMESH', Buffer.from('yVyN1359YQb0S1LW2cslgMrXHbTkHnR1TSHYDa7VCCs=', 'base64')]
]);

function candidateChannelKeys(channelId) {
  const name = String(channelId || '').trim().toUpperCase();
  const out = [];
  if (PUBLIC_CHANNEL_KEYS.has(name)) out.push({ name, key: PUBLIC_CHANNEL_KEYS.get(name) });
  if (!out.some(x => x.key.equals(LONGFAST_KEY))) out.push({ name: 'PUBLIC_DEFAULT', key: LONGFAST_KEY });
  return out;
}


// ======================================================
// EXPRESS / WEBSOCKET
// ======================================================

const app = express();

app.use(express.static('public'));

const server = http.createServer(app);

const wss = new WebSocketServer({
  server,
  path: '/mesh'
});

const clients = new Set();

let mqttState = 'disconnected';
let lastError = '';

// ======================================================
// OPERATIONAL CORE v0.4
// Additive observability layer inspired by mature mesh dashboards.
// It does not transmit anything to MQTT.
// ======================================================
const PROCESS_STARTED_AT = Date.now();
const PLAYBACK_MAX = Number(process.env.PLAYBACK_MAX || 5000);
const operationalEvents = [];
const healthHistory = [];
const HEALTH_HISTORY_MAX = 1440; // ~24h at 1 sample/minute
const operationalStats = {
  geoBackfills: 0,
  invalidTextCandidates: 0,
  eventsRecorded: 0,
  websocketBroadcasts: 0
};
const invalidTextReasons = {};
let lastHealthPacketTotal = 0;
let lastHealthAt = Date.now();
let lastPacketAt = null;

function recordOperationalEvent(type, data = {}) {
  const event = { ts: new Date().toISOString(), type, ...data };
  operationalEvents.push(event);
  operationalStats.eventsRecorded++;
  if (operationalEvents.length > PLAYBACK_MAX) operationalEvents.splice(0, operationalEvents.length - PLAYBACK_MAX);
  return event;
}

function rememberInvalidText(reason) {
  operationalStats.invalidTextCandidates++;
  invalidTextReasons[reason || 'UNKNOWN'] = (invalidTextReasons[reason || 'UNKNOWN'] || 0) + 1;
}


// ======================================================
// OBSERVED NODES
// ======================================================

const observedNodes = new Map();

// Global node identity (deduplicated across MQTT roots/regions).
// Keyed only by Meshtastic node id, e.g. !16c508c0.
const uniqueNodes = new Map();

// Normalized message inbox. One Meshtastic message can be observed by
// several gateways and through both protobuf and JSON MQTT transports.
const messageInbox = new Map();
const MESSAGE_INBOX_MAX = 500;

const messageDedupStats = {
  observations: 0,
  unique: 0,
  duplicates: 0,
  protobuf: 0,
  json: 0,
  pkiJson: 0,
  directed: 0,
  broadcast: 0
};

// ======================================================
// STATISTICS
// ======================================================

const portStats = new Map();
const regionStats = new Map();

// ======================================================
// CHANNEL DISCOVERY CORE - READ ONLY
// Evidence is based on the MQTT source/root + channelId
// actually observed on the wire. Geography is not used
// to promote or corroborate a community channel.
// ======================================================

const channelDiscovery = new Map();


// ======================================================
// CHANNEL CORROBORATION REGISTRY
// The monitor must never infer a community from coordinates.
// Classification uses only the MQTT source/root + channelId.
// ======================================================

const VERIFIED_CHANNEL_RULES = [
  { source: 'PR', channels: ['LONGFAST'], community: 'Puerto Rico', countryCode: 'PR', status: 'VERIFIED' },
  { source: 'CHILE', channels: ['LONGFAST'], community: 'Chile', countryCode: 'CL', status: 'VERIFIED' },
  { source: 'COLOMBIA', channels: ['LONGFAST'], community: 'Colombia', countryCode: 'CO', status: 'VERIFIED' },
  // Argentina: published regional community channels. They are promoted only
  // when the channel name itself is observed; ANZ alone is never Argentina.
  { source: 'ANZ', channels: ['BAIRESMESH','ROSARIOMESH','NQNMESH','CORDOBAMESH','ERMESH','MENDOZAMESH'], community: 'Argentina', countryCode: 'AR', status: 'VERIFIED' },
  // Spain: documented community channel names. O Zulo is a Spanish source,
  // but an unknown channel on that broker remains OBSERVED until corroborated.
  { source: 'SPAIN_OZULO', channels: ['SFNARROW','MADRID','BARCELONA','VALENCIA','CADIZ','LARIOJA','TEST','BOTS','IBERIA','GALICIA','ACORUÑA','ACORUNA','LUGO','OURENSE','PONTEVEDRA'], community: 'España', countryCode: 'ES', status: 'VERIFIED' },
  { source: 'SPAIN', channels: ['SFNARROW','MADRID','BARCELONA','VALENCIA','CADIZ','LARIOJA','TEST','BOTS','IBERIA','GALICIA','ACORUÑA','ACORUNA','LUGO','OURENSE','PONTEVEDRA'], community: 'España', countryCode: 'ES', status: 'VERIFIED' }
];

function classifyObservedChannel(source, regionId, channelId) {
  const src = String(source || regionId || 'UNKNOWN').trim().toUpperCase();
  const ch = String(channelId || '(none)').trim();
  const chUpper = ch.toUpperCase();
  const rule = VERIFIED_CHANNEL_RULES.find(r => r.source === src && r.channels.includes(chUpper));
  if (rule) return { status: rule.status, community: rule.community, countryCode: rule.countryCode, evidence: 'MQTT_SOURCE_PLUS_CHANNEL_ID' };

  // US root policy (strict): the root itself is useful evidence, but it is not
  // sufficient to attribute arbitrary observed channel names to a US community.
  // Only LongFast is admitted to the corroborated monitor as a verified source/root.
  // Every other channel observed on msh/US/2/# remains DISCOVERY until separately
  // corroborated. Geography is intentionally not consulted here.
  if (src === 'US' && chUpper === 'LONGFAST') {
    return { status: 'VERIFIED_SOURCE', community: 'US public root', countryCode: 'US', evidence: 'MQTT_SOURCE_ROOT_PLUS_LONGFAST' };
  }

  if (src === 'SPAIN_OZULO' || src === 'SPAIN') {
    return { status: 'OBSERVED', community: 'España (fuente observada)', countryCode: 'ES', evidence: 'MQTT_SOURCE_ONLY' };
  }

  return { status: 'DISCOVERY', community: null, countryCode: null, evidence: 'OBSERVED_ONLY' };
}


function classifyMessageChannel(message) {
  const sources = Array.isArray(message?.sourcesSeen) && message.sourcesSeen.length
    ? message.sourcesSeen
    : [message?.source || message?.region || 'UNKNOWN'];
  const rank = { VERIFIED: 4, VERIFIED_SOURCE: 3, OBSERVED: 2, DISCOVERY: 1 };
  let best = null;
  for (const src of sources) {
    const c = classifyObservedChannel(src, message?.region, message?.channelId);
    const candidate = { ...c, source: src };
    if (!best || (rank[c.status] || 0) > (rank[best.status] || 0)) best = candidate;
  }
  return best || { status: 'DISCOVERY', community: null, countryCode: null, evidence: 'OBSERVED_ONLY', source: 'UNKNOWN' };
}

function observeChannelDiscovery({ source, regionId, channelId, from, isText = false }) {
  const src = String(source || regionId || 'UNKNOWN').trim() || 'UNKNOWN';
  const channel = String(channelId || '(none)').trim() || '(none)';
  const key = `${src}::${channel}`;
  const now = new Date().toISOString();
  let row = channelDiscovery.get(key);
  if (!row) {
    row = {
      source: src,
      regionId: regionId || 'UNKNOWN',
      channelId: channel,
      packets: 0,
      textMessages: 0,
      nodes: new Set(),
      firstSeen: now,
      lastSeen: now
    };
    channelDiscovery.set(key, row);
  }
  row.lastSeen = now;
  if (isText) row.textMessages++;
  else row.packets++;
  const node = normalizeNodeNumber(from);
  if (node !== null && node !== undefined && node !== 0) row.nodes.add(nodeIdToHex(node));
}

function getChannelDiscoveryObject() {
  return [...channelDiscovery.values()]
    .map(row => ({
      source: row.source,
      regionId: row.regionId,
      channelId: row.channelId,
      packets: row.packets,
      textMessages: row.textMessages,
      uniqueNodes: row.nodes.size,
      firstSeen: row.firstSeen,
      lastSeen: row.lastSeen,
      classification: classifyObservedChannel(row.source, row.regionId, row.channelId)
    }))
    .sort((a,b) => b.packets - a.packets || a.source.localeCompare(b.source) || a.channelId.localeCompare(b.channelId));
}

// ======================================================
// CANDIDATE ANALYZER CORE - READ ONLY
// Ranks OBSERVED/DISCOVERY channels for manual research only.
// A score NEVER promotes a channel to VERIFIED. Geography is not used.
// ======================================================

function analyzeChannelCandidate(row) {
  const status = row?.classification?.status || 'DISCOVERY';
  const packets = Math.max(0, Number(row?.packets || 0));
  const textMessages = Math.max(0, Number(row?.textMessages || 0));
  const uniqueNodes = Math.max(0, Number(row?.uniqueNodes || 0));
  const first = Date.parse(row?.firstSeen || '');
  const last = Date.parse(row?.lastSeen || '');
  const spanMinutes = Number.isFinite(first) && Number.isFinite(last)
    ? Math.max(0, Math.round((last - first) / 60000))
    : 0;

  let score = 0;
  const reasons = [];

  // Strongest signal: actual decoded text activity.
  if (textMessages >= 5) { score += 35; reasons.push('5+ decoded text messages'); }
  else if (textMessages >= 2) { score += 28; reasons.push('2+ decoded text messages'); }
  else if (textMessages === 1) { score += 20; reasons.push('decoded text message observed'); }

  // Community-like activity should involve more than one node.
  if (uniqueNodes >= 20) { score += 25; reasons.push('20+ unique nodes'); }
  else if (uniqueNodes >= 5) { score += 20; reasons.push('5+ unique nodes'); }
  else if (uniqueNodes >= 2) { score += 12; reasons.push('multiple unique nodes'); }
  else if (uniqueNodes === 1) { score += 3; reasons.push('single node only'); }

  // Sustained packet activity. Logarithmic-ish buckets prevent flood volume dominating.
  if (packets >= 100) { score += 20; reasons.push('100+ packets observed'); }
  else if (packets >= 25) { score += 16; reasons.push('25+ packets observed'); }
  else if (packets >= 10) { score += 12; reasons.push('10+ packets observed'); }
  else if (packets >= 3) { score += 7; reasons.push('3+ packets observed'); }
  else if (packets >= 1) { score += 2; reasons.push('packet observed'); }

  // Recurrence during the current server lifetime.
  if (spanMinutes >= 360) { score += 15; reasons.push('active across 6+ hours'); }
  else if (spanMinutes >= 60) { score += 12; reasons.push('active across 1+ hour'); }
  else if (spanMinutes >= 15) { score += 8; reasons.push('active across 15+ minutes'); }
  else if (spanMinutes >= 3) { score += 4; reasons.push('repeated across 3+ minutes'); }

  // Dedicated Spanish community source is useful evidence, but still not verification.
  if (status === 'OBSERVED') { score += 5; reasons.push('dedicated community MQTT source observed'); }

  score = Math.min(100, score);
  let priority = 'LOW';
  if (score >= 70) priority = 'HIGH';
  else if (score >= 45) priority = 'MEDIUM';

  return {
    score,
    priority,
    spanMinutes,
    reasons,
    recommendation: priority === 'HIGH'
      ? 'RESEARCH_NOW'
      : priority === 'MEDIUM'
        ? 'RESEARCH_WHEN_PRACTICAL'
        : 'KEEP_OBSERVING'
  };
}

function getChannelCandidates() {
  return getChannelDiscoveryObject()
    .filter(row => ['DISCOVERY','OBSERVED'].includes(row.classification?.status || 'DISCOVERY'))
    .map(row => ({ ...row, candidate: analyzeChannelCandidate(row) }))
    .sort((a,b) =>
      b.candidate.score - a.candidate.score ||
      b.textMessages - a.textMessages ||
      b.uniqueNodes - a.uniqueNodes ||
      b.packets - a.packets
    );
}

const jsonStats = {
  packets: 0,
  valid: 0,
  invalid: 0,
  possibleText: 0
};

const decryptStats = {
  attempts: 0,
  success: 0,
  failed: 0,
  byChannel: {}
};

const transportStats = {
  protobuf: 0,
  json: 0,
  map: 0,
  other: 0,
  serviceEnvelopeAttempts: 0,
  serviceEnvelopeSuccess: 0,
  serviceEnvelopeFailed: 0
};

const publicKeyStats = {
  discovered: 0,
  changed: 0,
  invalidLength: 0,
  bySource: {
    NODEINFO_APP: 0,
    MESH_PACKET: 0
  }
};

// ======================================================
// QUIET LOGGING
// Render drops log lines when output is too fast.
// Per-packet logs are buffered and only printed when the
// packet is "interesting" (or VERBOSE=1). A summary is
// printed every 60 seconds instead.
// ======================================================

const VERBOSE = process.env.VERBOSE === '1';

let pktLog = null;
let pktInteresting = false;
let suppressedPackets = 0;
let totalPackets = 0;
const seenUnhandledPorts = new Set();
const seenPlainTexts = new Set();
let lastSummaryTotal = 0;
let pktRetained = false;

// No-payload breakdown (who sends them, from where, how old)
const noPayloadByRegion = {};
const noPayloadByGateway = new Map();
const noPayloadByChannel = new Map();
const noPayloadSamplesByRegion = {};
let noPayloadOld = 0;
let noPayloadRetained = 0;

// Traffic analyzer: bounded in-memory diagnostics for unusually noisy gateways.
// Diagnostic only: it does NOT block or drop packets.
const TRAFFIC_ANALYZER = process.env.TRAFFIC_ANALYZER !== '0';
const SUSPECT_GATEWAY_THRESHOLD = Number(process.env.SUSPECT_GATEWAY_THRESHOLD || 1000);
const HASH_CACHE_MAX = Number(process.env.HASH_CACHE_MAX || 20000);
const trafficGateways = new Map();
const recentPayloadHashes = new Map();

function getTrafficGateway(gatewayId) {
  const key = gatewayId || '(none)';
  let g = trafficGateways.get(key);
  if (!g) {
    g = {
      gatewayId: key,
      total: 0,
      decoded: 0,
      encrypted: 0,
      noPayload: 0,
      noPacket: 0,
      retained: 0,
      stale24h: 0,
      live5m: 0,
      duplicatePayloads: 0,
      uniquePayloads: 0,
      firstSeen: null,
      lastSeen: null,
      oldestRxTime: null,
      newestRxTime: null,
      topics: new Map(),
      channels: new Map(),
      senders: new Map(),
      packetIds: new Map()
    };
    trafficGateways.set(key, g);
  }
  return g;
}

function analyzeTraffic({ gatewayId, topic, channelId, packet, payload, retained, variantCase }) {
  if (!TRAFFIC_ANALYZER) return;
  const g = getTrafficGateway(gatewayId);
  const now = Date.now();
  const nowIso = new Date(now).toISOString();
  g.total++;
  g.firstSeen ||= nowIso;
  g.lastSeen = nowIso;
  bump(g.topics, topic || '(none)');
  bump(g.channels, channelId || '(none)');
  if (retained) g.retained++;

  if (!packet) {
    g.noPacket++;
  } else {
    const sender = nodeIdToHex(packet.from || 0);
    bump(g.senders, sender);
    bump(g.packetIds, String(packet.id ?? 0));
    if (variantCase === 'decoded') g.decoded++;
    else if (variantCase === 'encrypted') g.encrypted++;
    else g.noPayload++;

    const rx = Number(packet.rxTime || 0);
    if (rx > 0) {
      const rxMs = rx * 1000;
      const age = now - rxMs;
      if (age > 86400000) g.stale24h++;
      if (age >= 0 && age <= 300000) g.live5m++;
      if (!g.oldestRxTime || rx < g.oldestRxTime) g.oldestRxTime = rx;
      if (!g.newestRxTime || rx > g.newestRxTime) g.newestRxTime = rx;
    }
  }

  const hash = crypto.createHash('sha256').update(Buffer.from(payload)).digest('hex').slice(0, 24);
  if (recentPayloadHashes.has(hash)) {
    g.duplicatePayloads++;
    recentPayloadHashes.delete(hash);
    recentPayloadHashes.set(hash, now);
  } else {
    g.uniquePayloads++;
    recentPayloadHashes.set(hash, now);
    if (recentPayloadHashes.size > HASH_CACHE_MAX) {
      const oldest = recentPayloadHashes.keys().next().value;
      recentPayloadHashes.delete(oldest);
    }
  }
}

function trafficGatewaySummary(g) {
  return {
    gatewayId: g.gatewayId,
    total: g.total,
    decoded: g.decoded,
    encrypted: g.encrypted,
    noPayload: g.noPayload,
    noPacket: g.noPacket,
    retained: g.retained,
    stale24h: g.stale24h,
    live5m: g.live5m,
    duplicatePayloads: g.duplicatePayloads,
    uniquePayloads: g.uniquePayloads,
    uniqueSenders: g.senders.size,
    uniquePacketIds: g.packetIds.size,
    firstSeen: g.firstSeen,
    lastSeen: g.lastSeen,
    oldestRxTime: g.oldestRxTime ? new Date(g.oldestRxTime * 1000).toISOString() : null,
    newestRxTime: g.newestRxTime ? new Date(g.newestRxTime * 1000).toISOString() : null,
    topTopics: topN(g.topics, 5),
    topChannels: topN(g.channels, 5),
    topSenders: topN(g.senders, 5),
    topPacketIds: topN(g.packetIds, 5)
  };
}

function getTrafficAnalyzerSummary() {
  return Array.from(trafficGateways.values())
    .sort((a, b) => b.total - a.total)
    .slice(0, 20)
    .map(trafficGatewaySummary);
}

function getSuspectGateways() {
  return Array.from(trafficGateways.values())
    .filter(g => g.total >= SUSPECT_GATEWAY_THRESHOLD)
    .sort((a, b) => b.total - a.total)
    .map(trafficGatewaySummary);
}


function bump(map, key) {
  map.set(key, (map.get(key) || 0) + 1);
}

function topN(map, n = 5) {
  return Array.from(map.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([k, v]) => `${k}=${v}`);
}

// Where do the packets go? (decoded / encrypted / no payload / no packet)
const variantStats = {
  decoded: 0,
  encrypted: 0,
  noPayload: 0,
  noPacket: 0,
  retained: 0,
  portnum0Rejected: 0,
  plainTextOnProtobufTopic: 0
};

function plog(...args) {
  if (pktLog) {
    pktLog.push(args);
  } else {
    console.log(...args);
  }
}

for (const region of REGIONS) {
  regionStats.set(region.id, {
    packets: 0,
    bytes: 0,
    messages: 0
  });
}

// ======================================================
// WEBSOCKET BROADCAST
// ======================================================

function broadcast(data) {
  operationalStats.websocketBroadcasts++;

  const message = JSON.stringify(data);

  for (const ws of clients) {
    if (ws.readyState === 1) {
      ws.send(message);
    }
  }
}

// ======================================================
// REGION FROM MQTT TOPIC
// ======================================================

function getRegionFromTopic(topic) {

  for (const region of REGIONS) {

    const prefix =
      region.topic.replace('/#', '/');

    if (topic.startsWith(prefix)) {
      return region;
    }
  }

  return {
    id: 'UNKNOWN',
    name: 'Unknown',
    topic: null
  };
}

// ======================================================
// MQTT TOPIC TYPE
// ======================================================

function getMqttTopicType(topic) {

  if (topic.includes('/2/e/')) {
    return 'protobuf';
  }

  if (topic.includes('/2/json/')) {
    return 'json';
  }

  if (topic.includes('/2/map/')) {
    return 'map';
  }

  return 'other';
}

// ======================================================
// NODE ID
// ======================================================

function nodeIdToHex(nodeId) {

  return (
    '!' +
    Number(nodeId)
      .toString(16)
      .padStart(8, '0')
  );
}

// ======================================================
// PORTNUM NAME
// ======================================================

function getPortNumName(portnum) {

  try {

    for (
      const [name, value]
      of Object.entries(Portnums.PortNum)
    ) {
      if (value === portnum) {
        return name;
      }
    }

  } catch {
    // fallback
  }

  return `PORT_${portnum}`;
}

// ======================================================
// PORT STATISTICS
// ======================================================

function addPortStat(portnum, portName, regionId) {

  const key = String(portnum);

  const existing =
    portStats.get(key) || {
      portnum,
      portName,
      total: 0,
      byRegion: {
        PR: 0,
        FL: 0,
        TX: 0,
        US: 0,
        UNKNOWN: 0
      }
    };

  existing.total++;

  if (existing.byRegion[regionId] === undefined) {
    existing.byRegion[regionId] = 0;
  }

  existing.byRegion[regionId]++;
  existing.portName = portName;

  portStats.set(key, existing);
}

function getPortStatsObject() {

  return Array.from(portStats.values())
    .sort((a, b) => b.total - a.total);
}

// ======================================================
// REGION STATISTICS
// ======================================================

function addRegionTraffic(regionId, bytes) {

  const stats = regionStats.get(regionId);

  if (!stats) {
    return;
  }

  stats.packets++;
  stats.bytes += bytes;
}

function addRegionMessage(regionId) {

  const stats = regionStats.get(regionId);

  if (!stats) {
    return;
  }

  stats.messages++;
}

function getRegionStatsObject() {

  const result = {};

  for (
    const [regionId, stats]
    of regionStats.entries()
  ) {
    result[regionId] = { ...stats };
  }

  return result;
}

// ======================================================
// DECRYPT STATISTICS
// ======================================================

function addDecryptAttempt(channelId, success) {

  const channel = channelId || '(none)';

  decryptStats.attempts++;

  if (!decryptStats.byChannel[channel]) {
    decryptStats.byChannel[channel] = {
      attempts: 0,
      success: 0,
      failed: 0
    };
  }

  const stats = decryptStats.byChannel[channel];

  stats.attempts++;

  if (success) {
    decryptStats.success++;
    stats.success++;
  } else {
    decryptStats.failed++;
    stats.failed++;
  }
}

// ======================================================
// NORMALIZED MESSAGE INBOX / DEDUPLICATION
// ======================================================

function normalizeNodeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? (n >>> 0) : 0;
}

function messageIdentity(from, packetId, text, to) {
  const f = normalizeNodeNumber(from);
  const id = normalizeNodeNumber(packetId);
  if (f && id) return `${f}:${id}`;
  // Fallback for transports that do not expose a packet id.
  return crypto.createHash('sha256')
    .update(`${f}|${normalizeNodeNumber(to)}|${String(text || '')}`)
    .digest('hex');
}

function validateTextMessage(text) {
  if (typeof text !== 'string') return { valid: false, reason: 'NOT_STRING' };
  if (!text.length) return { valid: false, reason: 'EMPTY' };
  if (text.length > 4096) return { valid: false, reason: 'TOO_LONG' };

  // U+FFFD means the original byte sequence was not valid UTF-8.
  if (text.includes('\uFFFD')) return { valid: false, reason: 'INVALID_UTF8' };

  let controls = 0;
  let printable = 0;
  let meaningful = 0;
  for (const ch of text) {
    const cp = ch.codePointAt(0);
    const allowedWhitespace = ch === '\n' || ch === '\r' || ch === '\t';
    const isControl = (cp < 0x20 && !allowedWhitespace) || (cp >= 0x7f && cp <= 0x9f);
    if (isControl) controls++;
    else printable++;
    if (!isControl && !/\s/u.test(ch)) meaningful++;
  }

  if (controls > 0) return { valid: false, reason: 'CONTROL_CHARACTERS' };
  if (meaningful === 0) return { valid: false, reason: 'NO_VISIBLE_TEXT' };
  if (printable / Math.max(1, [...text].length) < 0.90) {
    return { valid: false, reason: 'LOW_PRINTABLE_RATIO' };
  }
  return { valid: true, reason: 'OK' };
}

function nodeGeographyForMessage(from) {
  const nodeHex = nodeIdToHex(from);
  const node = uniqueNodes.get(nodeHex);
  if (!node) return null;
  return {
    countryCode: node.countryCode ?? null,
    country: node.country ?? null,
    subdivisionCode: node.subdivisionCode ?? null,
    subdivision: node.subdivision ?? null,
    geoStatus: node.geoStatus ?? null,
    latitude: node.latitude ?? node.position?.latitude ?? node.mapReport?.latitude ?? null,
    longitude: node.longitude ?? node.position?.longitude ?? node.mapReport?.longitude ?? null
  };
}

function recordMessageObservation({
  regionId, source, transport, topic, channelId, from, to, packetId,
  text, gatewayId, directed, pki = false, receivedAt = new Date().toISOString()
}) {
  const textValidation = validateTextMessage(text);
  if (!textValidation.valid) {
    rememberInvalidText(textValidation.reason);
    recordOperationalEvent('invalid_text', { regionId, channelId, from: nodeIdToHex(from), reason: textValidation.reason });
    return { isNew: false, rejected: true, reason: textValidation.reason, message: null };
  }

  messageDedupStats.observations++;
  if (transport === 'protobuf') messageDedupStats.protobuf++;
  if (transport === 'json') messageDedupStats.json++;
  if (pki && transport === 'json') messageDedupStats.pkiJson++;

  const key = messageIdentity(from, packetId, text, to);
  const existing = messageInbox.get(key);
  const gateway = gatewayId || null;

  if (existing) {
    messageDedupStats.duplicates++;
    existing.lastSeen = receivedAt;
    existing.mqttCopies++;
    if (gateway && !existing.gateways.includes(gateway)) existing.gateways.push(gateway);
    if (regionId && !existing.regionsSeen.includes(regionId)) existing.regionsSeen.push(regionId);
    if (source && !existing.sourcesSeen.includes(source)) existing.sourcesSeen.push(source);
    existing.source ||= source || regionId || null;
    if (transport && !existing.transports.includes(transport)) existing.transports.push(transport);
    if (topic && !existing.topics.includes(topic) && existing.topics.length < 12) existing.topics.push(topic);
    existing.pki = existing.pki || pki;
    // Preserve the MQTT transport region on the normalized message.
    // This lets the private monitor separate US vs EU868 even when a sender
    // has not published coordinates yet. Geographic PR classification still
    // requires known coordinates / node geography.
    existing.region ||= regionId || null;
    return { isNew: false, message: existing };
  }

  const isDirected = directed ?? (normalizeNodeNumber(to) !== 0xffffffff);
  const item = {
    key,
    packetId: normalizeNodeNumber(packetId),
    from: normalizeNodeNumber(from),
    fromHex: nodeIdToHex(from),
    to: normalizeNodeNumber(to),
    toHex: normalizeNodeNumber(to) === 0xffffffff ? 'BROADCAST' : nodeIdToHex(to),
    directed: !!isDirected,
    broadcast: !isDirected,
    pki: !!pki,
    region: regionId || null,
    source: source || regionId || null,
    channelId: channelId || null,
    text,
    ...(nodeGeographyForMessage(from) || {}),
    firstSeen: receivedAt,
    lastSeen: receivedAt,
    mqttCopies: 1,
    gateways: gateway ? [gateway] : [],
    regionsSeen: regionId ? [regionId] : [],
    sourcesSeen: (source || regionId) ? [source || regionId] : [],
    transports: transport ? [transport] : [],
    topics: topic ? [topic] : []
  };

  messageInbox.set(key, item);
  messageDedupStats.unique++;
  if (item.directed) messageDedupStats.directed++;
  else messageDedupStats.broadcast++;

  while (messageInbox.size > MESSAGE_INBOX_MAX) {
    const oldest = messageInbox.keys().next().value;
    messageInbox.delete(oldest);
  }

  recordOperationalEvent('message', {
    key: item.key, fromHex: item.fromHex, toHex: item.toHex,
    channelId: item.channelId, directed: item.directed, pki: item.pki,
    countryCode: item.countryCode || null, subdivisionCode: item.subdivisionCode || null
  });
  broadcast({ type: 'message', message: item, messageDedupStats: { ...messageDedupStats } });
  return { isNew: true, message: item };
}

function getMessagesNewestFirst() {
  return Array.from(messageInbox.values()).sort((a, b) =>
    String(b.lastSeen).localeCompare(String(a.lastSeen))
  );
}

// ======================================================
// GEOGRAPHIC CORE v0.4 - Operational Core
// ======================================================

// Lightweight offline geographic classification. This intentionally avoids
// network reverse-geocoding. State/territory labels are best-effort bounding
// boxes; exact border decisions remain UNKNOWN rather than being guessed when
// coordinates are absent. Spain includes mainland, Balearic and Canary Islands.
const US_GEO_BOUNDS = [
  ['PR','Puerto Rico',17.80,18.60,-67.35,-65.20],
  ['VI','U.S. Virgin Islands',17.60,18.50,-65.20,-64.45],
  ['GU','Guam',13.15,13.75,144.55,145.05],
  ['MP','Northern Mariana Islands',14.00,20.70,144.70,146.30],
  ['AS','American Samoa',-14.60,-10.90,-171.20,-168.00],
  ['HI','Hawaii',18.80,22.30,-160.30,-154.70],
  ['AK','Alaska',51.00,72.00,-180.00,-129.90],
  ['FL','Florida',24.35,31.10,-87.70,-79.80],
  ['TX','Texas',25.80,36.60,-106.70,-93.45],
  ['CA','California',32.45,42.10,-124.55,-114.00],
  ['OR','Oregon',41.90,46.35,-124.70,-116.35],
  ['WA','Washington',45.50,49.10,-124.90,-116.80],
  ['AZ','Arizona',31.25,37.10,-114.90,-109.00],
  ['NM','New Mexico',31.25,37.10,-109.10,-103.00],
  ['NV','Nevada',35.00,42.10,-120.10,-114.00],
  ['UT','Utah',36.90,42.10,-114.10,-109.00],
  ['CO','Colorado',36.90,41.10,-109.10,-102.00],
  ['WY','Wyoming',40.90,45.10,-111.10,-104.00],
  ['MT','Montana',44.30,49.10,-116.10,-104.00],
  ['ID','Idaho',41.90,49.10,-117.30,-111.00],
  ['ND','North Dakota',45.90,49.10,-104.10,-96.50],
  ['SD','South Dakota',42.45,46.05,-104.10,-96.40],
  ['NE','Nebraska',39.90,43.10,-104.10,-95.20],
  ['KS','Kansas',36.90,40.10,-102.10,-94.55],
  ['OK','Oklahoma',33.55,37.10,-103.10,-94.40],
  ['MN','Minnesota',43.40,49.50,-97.30,-89.45],
  ['IA','Iowa',40.30,43.60,-96.70,-90.10],
  ['MO','Missouri',35.90,40.70,-95.80,-89.00],
  ['AR','Arkansas',32.90,36.60,-94.70,-89.60],
  ['LA','Louisiana',28.80,33.10,-94.10,-88.75],
  ['WI','Wisconsin',42.45,47.35,-92.90,-86.70],
  ['IL','Illinois',36.90,42.60,-91.60,-87.45],
  ['MS','Mississippi',30.10,35.10,-91.70,-88.05],
  ['MI','Michigan',41.65,48.35,-90.45,-82.10],
  ['IN','Indiana',37.70,41.80,-88.10,-84.75],
  ['KY','Kentucky',36.45,39.20,-89.60,-81.90],
  ['TN','Tennessee',34.90,36.75,-90.35,-81.60],
  ['AL','Alabama',30.10,35.10,-88.55,-84.85],
  ['OH','Ohio',38.35,42.10,-84.85,-80.50],
  ['WV','West Virginia',37.15,40.70,-82.70,-77.70],
  ['VA','Virginia',36.45,39.55,-83.75,-75.15],
  ['NC','North Carolina',33.75,36.70,-84.40,-75.35],
  ['SC','South Carolina',32.00,35.25,-83.40,-78.45],
  ['GA','Georgia',30.30,35.10,-85.70,-80.75],
  ['PA','Pennsylvania',39.70,42.55,-80.60,-74.65],
  ['NY','New York',40.45,45.10,-79.80,-71.75],
  ['VT','Vermont',42.70,45.10,-73.50,-71.45],
  ['NH','New Hampshire',42.65,45.35,-72.60,-70.60],
  ['ME','Maine',42.95,47.50,-71.10,-66.85],
  ['MA','Massachusetts',41.15,42.95,-73.55,-69.85],
  ['RI','Rhode Island',41.10,42.05,-71.95,-71.10],
  ['CT','Connecticut',40.95,42.10,-73.75,-71.75],
  ['NJ','New Jersey',38.85,41.40,-75.60,-73.85],
  ['DE','Delaware',38.40,39.90,-75.80,-74.95],
  ['MD','Maryland',37.85,39.80,-79.50,-74.95],
  ['DC','District of Columbia',38.78,39.00,-77.13,-76.90]
];

function inBox(lat, lon, minLat, maxLat, minLon, maxLon) {
  return Number.isFinite(lat) && Number.isFinite(lon) &&
    lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
}

function isSpainCoordinate(lat, lon) {
  // Mainland
  if (inBox(lat, lon, 35.70, 43.90, -9.55, 3.35)) return true;
  // Balearic Islands
  if (inBox(lat, lon, 38.55, 40.15, 1.00, 4.60)) return true;
  // Canary Islands
  if (inBox(lat, lon, 27.45, 29.55, -18.30, -13.20)) return true;
  // Ceuta / Melilla
  if (inBox(lat, lon, 35.15, 35.95, -6.10, -2.80)) return true;
  return false;
}

function classifyLatinAmericaCoordinate(lat, lon) {
  // Country-level bounding boxes are intentionally conservative diagnostics,
  // not political-border GIS. Dedicated country brokers override these.
  if (inBox(lat, lon, 14.3, 32.8, -118.5, -86.5)) return {countryCode:'MX',country:'Mexico',geoStatus:'MEXICO'};
  if (inBox(lat, lon, -55.2, -21.7, -73.7, -53.5)) return {countryCode:'AR',country:'Argentina',geoStatus:'ARGENTINA'};
  if (inBox(lat, lon, -56.0, -17.3, -75.8, -66.0)) return {countryCode:'CL',country:'Chile',geoStatus:'CHILE'};
  if (inBox(lat, lon, -4.5, 13.6, -79.2, -66.7)) return {countryCode:'CO',country:'Colombia',geoStatus:'COLOMBIA'};
  if (inBox(lat, lon, 0.5, 12.8, -73.5, -59.5)) return {countryCode:'VE',country:'Venezuela',geoStatus:'VENEZUELA'};
  if (inBox(lat, lon, -18.5, 0.0, -81.5, -68.5)) return {countryCode:'PE',country:'Peru',geoStatus:'PERU'};
  if (inBox(lat, lon, -5.2, 1.8, -81.2, -75.0)) return {countryCode:'EC',country:'Ecuador',geoStatus:'ECUADOR'};
  if (inBox(lat, lon, -23.0, -9.5, -69.7, -57.3)) return {countryCode:'BO',country:'Bolivia',geoStatus:'BOLIVIA'};
  if (inBox(lat, lon, -27.7, -19.0, -62.8, -54.0)) return {countryCode:'PY',country:'Paraguay',geoStatus:'PARAGUAY'};
  if (inBox(lat, lon, -35.2, -30.0, -58.7, -53.0)) return {countryCode:'UY',country:'Uruguay',geoStatus:'URUGUAY'};
  if (inBox(lat, lon, 7.0, 10.0, -83.1, -77.0)) return {countryCode:'PA',country:'Panama',geoStatus:'PANAMA'};
  if (inBox(lat, lon, 8.0, 11.3, -86.0, -82.4)) return {countryCode:'CR',country:'Costa Rica',geoStatus:'COSTA_RICA'};
  if (inBox(lat, lon, 13.6, 17.9, -92.3, -88.0)) return {countryCode:'GT',country:'Guatemala',geoStatus:'GUATEMALA'};
  if (inBox(lat, lon, 12.8, 16.6, -89.4, -83.0)) return {countryCode:'HN',country:'Honduras',geoStatus:'HONDURAS'};
  if (inBox(lat, lon, 13.0, 14.5, -90.2, -87.6)) return {countryCode:'SV',country:'El Salvador',geoStatus:'EL_SALVADOR'};
  if (inBox(lat, lon, 10.7, 15.1, -87.8, -82.5)) return {countryCode:'NI',country:'Nicaragua',geoStatus:'NICARAGUA'};
  if (inBox(lat, lon, 17.4, 20.1, -72.1, -68.1)) return {countryCode:'DO',country:'Dominican Republic',geoStatus:'DOMINICAN_REPUBLIC'};
  if (inBox(lat, lon, 19.5, 23.4, -85.0, -74.0)) return {countryCode:'CU',country:'Cuba',geoStatus:'CUBA'};
  if (inBox(lat, lon, 0.8, 2.4, 9.2, 11.5)) return {countryCode:'GQ',country:'Equatorial Guinea',geoStatus:'EQUATORIAL_GUINEA'};
  return null;
}

function classifyGeography(regionId, latitude, longitude) {
  const lat = Number(latitude);
  const lon = Number(longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    const fixed = {PR:['PR','Puerto Rico','PUERTO_RICO'],CHILE:['CL','Chile','CHILE'],COLOMBIA:['CO','Colombia','COLOMBIA'],SPAIN:['ES','Spain','SPAIN']};
    if (fixed[regionId]) { const [countryCode,country,geoStatus]=fixed[regionId]; return {countryCode,country,subdivisionCode:null,subdivision:null,geoStatus}; }
    return regionId === 'EU868'
      ? { countryCode: null, country: null, subdivisionCode: null, subdivision: null, geoStatus: 'EU868_UNKNOWN' }
      : { countryCode: null, country: null, subdivisionCode: null, subdivision: null, geoStatus: `${regionId}_UNKNOWN` };
  }

  if (regionId === 'PR') return { countryCode:'PR', country:'Puerto Rico', subdivisionCode:'PR', subdivision:'Puerto Rico', geoStatus:'PUERTO_RICO' };
  if (regionId === 'CHILE') return { countryCode:'CL', country:'Chile', subdivisionCode:null, subdivision:null, geoStatus:'CHILE' };
  if (regionId === 'COLOMBIA') return { countryCode:'CO', country:'Colombia', subdivisionCode:null, subdivision:null, geoStatus:'COLOMBIA' };
  if (regionId === 'SPAIN') return { countryCode:'ES', country:'Spain', subdivisionCode:null, subdivision:null, geoStatus:'SPAIN' };

  if (regionId === 'EU868') {
    if (isSpainCoordinate(lat, lon)) {
      return { countryCode: 'ES', country: 'Spain', subdivisionCode: null, subdivision: null, geoStatus: 'SPAIN' };
    }
    return { countryCode: null, country: null, subdivisionCode: null, subdivision: null, geoStatus: 'NON_SPAIN_EU868' };
  }

  if (regionId === 'US') {
    const latin = classifyLatinAmericaCoordinate(lat, lon);
    if (latin?.countryCode === 'MX') return { ...latin, subdivisionCode:null, subdivision:null };
    for (const [code, name, minLat, maxLat, minLon, maxLon] of US_GEO_BOUNDS) {
      if (inBox(lat, lon, minLat, maxLat, minLon, maxLon)) {
        const territory = ['PR','VI','GU','MP','AS'].includes(code);
        return {
          countryCode: code === 'PR' ? 'PR' : 'US',
          country: code === 'PR' ? 'Puerto Rico' : 'United States',
          subdivisionCode: code,
          subdivision: name,
          geoStatus: territory ? 'US_TERRITORY' : 'US_STATE'
        };
      }
    }
    return { countryCode: 'US', country: 'United States', subdivisionCode: null, subdivision: null, geoStatus: 'US_UNKNOWN' };
  }

  if (regionId === 'ANZ') {
    const latin = classifyLatinAmericaCoordinate(lat, lon);
    if (latin) return { ...latin, subdivisionCode:null, subdivision:null };
    return { countryCode:null, country:null, subdivisionCode:null, subdivision:null, geoStatus:'ANZ_OUTSIDE_TARGETS' };
  }

  return { countryCode: null, country: null, subdivisionCode: null, subdivision: null, geoStatus: 'UNKNOWN' };
}

function extractCoordinates(existing, changes) {
  const candidates = [
    changes,
    changes?.position,
    changes?.mapReport,
    existing,
    existing?.position,
    existing?.mapReport
  ];
  for (const c of candidates) {
    if (!c) continue;
    const lat = Number(c.latitude);
    const lon = Number(c.longitude);
    if (Number.isFinite(lat) && Number.isFinite(lon) && !(lat === 0 && lon === 0)) {
      return { latitude: lat, longitude: lon };
    }
  }
  return { latitude: null, longitude: null };
}

function geographyFromChanges(regionId, existing, changes) {
  const { latitude, longitude } = extractCoordinates(existing, changes);
  return {
    ...classifyGeography(regionId, latitude, longitude),
    latitude,
    longitude
  };
}

function isOperationalGeography(node) {
  if (!node) return false;
  if (node.lastRegion === 'EU868' || node.region === 'EU868') return node.countryCode === 'ES';
  return true;
}

// ======================================================
// OBSERVED NODE
// ======================================================

function backfillMessageGeography(nodeId) {
  const geo = nodeGeographyForMessage(nodeId);
  if (!geo) return 0;
  let changed = 0;
  for (const msg of messageInbox.values()) {
    if (normalizeNodeNumber(msg.from) !== normalizeNodeNumber(nodeId)) continue;
    const before = `${msg.countryCode || ''}:${msg.subdivisionCode || ''}:${msg.latitude || ''}:${msg.longitude || ''}`;
    Object.assign(msg, geo);
    const after = `${msg.countryCode || ''}:${msg.subdivisionCode || ''}:${msg.latitude || ''}:${msg.longitude || ''}`;
    if (before !== after) changed++;
  }
  if (changed) {
    operationalStats.geoBackfills += changed;
    recordOperationalEvent('geo_backfill', { nodeHex: nodeIdToHex(nodeId), messagesUpdated: changed, ...geo });
  }
  return changed;
}

function updateObservedNode(regionId, nodeId, changes) {

  const nodeHex = nodeIdToHex(nodeId);

  const key = `${regionId}:${nodeHex}`;

  const existing =
    observedNodes.get(key) || {
      region: regionId,
      nodeId: Number(nodeId),
      nodeHex,
      firstSeen: new Date().toISOString()
    };

  const geography = geographyFromChanges(regionId, existing, changes);

  const updated = {
    ...existing,
    ...changes,
    ...geography,
    region: regionId,
    nodeHex,
    operational: regionId !== 'EU868' || geography.countryCode === 'ES',
    lastSeen: new Date().toISOString()
  };

  observedNodes.set(key, updated);

  // Maintain a second, global identity table so the same node observed
  // under US, FL, TX, PR, etc. is not counted as several nodes.
  const globalExisting = uniqueNodes.get(nodeHex) || {
    nodeId: Number(nodeId),
    nodeHex,
    firstSeen: updated.firstSeen,
    regionsSeen: []
  };
  const regionsSeen = Array.isArray(globalExisting.regionsSeen)
    ? [...globalExisting.regionsSeen]
    : [];
  if (!regionsSeen.includes(regionId)) regionsSeen.push(regionId);
  const globalGeo = geographyFromChanges(regionId, globalExisting, changes);
  uniqueNodes.set(nodeHex, {
    ...globalExisting,
    ...changes,
    ...globalGeo,
    nodeId: Number(nodeId),
    nodeHex,
    regionsSeen,
    lastRegion: regionId,
    operational: regionId !== 'EU868' || globalGeo.countryCode === 'ES',
    lastSeen: updated.lastSeen
  });

  backfillMessageGeography(nodeId);
  if (changes.position || changes.mapReport || changes.user || changes.publicKey) {
    recordOperationalEvent('node_update', {
      nodeHex, regionId,
      countryCode: globalGeo.countryCode || null,
      subdivisionCode: globalGeo.subdivisionCode || null,
      hasPosition: !!(globalGeo.latitude ?? changes?.position?.latitude ?? changes?.mapReport?.latitude)
    });
  }
  return updated;
}

function getObservedNode(regionId, nodeId) {

  const nodeHex = nodeIdToHex(nodeId);

  return (
    observedNodes.get(`${regionId}:${nodeHex}`) ||
    null
  );
}

function getNodeCountsByRegion() {

  const counts = {};

  for (const region of REGIONS) {
    counts[region.id] = 0;
  }

  for (const node of observedNodes.values()) {
    if (counts[node.region] !== undefined) {
      counts[node.region]++;
    }
  }

  return counts;
}

// ======================================================
// PUBLIC KEYS (read-only capture)
// ======================================================

function recordPublicKey(regionId, nodeId, key, source) {

  if (!key || key.length === 0) {
    return;
  }

  const buf =
    typeof key === 'string'
      ? Buffer.from(key, 'base64')
      : Buffer.from(key);

  if (buf.length === 0) {
    return;
  }

  if (buf.length !== 32) {
    publicKeyStats.invalidLength++;
  }

  const b64 = buf.toString('base64');

  const existing = getObservedNode(regionId, nodeId);

  const isNew = !existing?.publicKey;

  const isChanged =
    !isNew && existing.publicKey !== b64;

  updateObservedNode(regionId, nodeId, {
    publicKey: b64,
    publicKeyBytes: buf.length,
    publicKeySource: source
  });

  if (!isNew && !isChanged) {
    return;
  }

  if (isNew) {
    publicKeyStats.discovered++;
  } else {
    publicKeyStats.changed++;
  }

  publicKeyStats.bySource[source] =
    (publicKeyStats.bySource[source] || 0) + 1;

  console.log('===================================');
  console.log(
    isChanged
      ? 'PUBLIC KEY CHANGED'
      : 'PUBLIC KEY DISCOVERED'
  );
  console.log('===================================');
  console.log('Region:', regionId);
  console.log('Node:', nodeIdToHex(nodeId));
  console.log('Source:', source);
  console.log('Bytes:', buf.length);
  console.log('Public Key:', b64);
  console.log('===================================');
}

function getPublicKeyNodeCounts() {

  const counts = {};

  for (const region of REGIONS) {
    counts[region.id] = 0;
  }

  for (const node of observedNodes.values()) {
    if (
      node.publicKey &&
      counts[node.region] !== undefined
    ) {
      counts[node.region]++;
    }
  }

  return counts;
}

// ======================================================
// AES-128-CTR
// Public LongFast key used as diagnostic key.
// Successful AES transform alone DOES NOT mean
// successful decryption. Mesh.Data must also parse.
// ======================================================

function decryptWithChannelKey(encrypted, packetId, fromNode, key) {
  const nonce = Buffer.alloc(16);
  nonce.writeBigUInt64LE(BigInt(packetId), 0);
  nonce.writeUInt32LE(Number(fromNode) >>> 0, 8);
  const algorithm = key.length === 32 ? 'aes-256-ctr' : 'aes-128-ctr';
  const decipher = crypto.createDecipheriv(algorithm, key, nonce);
  decipher.setAutoPadding(false);
  return Buffer.concat([decipher.update(Buffer.from(encrypted)), decipher.final()]);
}

// ======================================================
// POSITION DECODER
// ======================================================

function decodePosition(payload) {

  const position =
    fromBinary(Mesh.PositionSchema, payload);

  let latitude = null;
  let longitude = null;

  if (
    position.latitudeI !== undefined &&
    position.latitudeI !== null
  ) {
    latitude = position.latitudeI * 1e-7;
  }

  if (
    position.longitudeI !== undefined &&
    position.longitudeI !== null
  ) {
    longitude = position.longitudeI * 1e-7;
  }

  return {
    latitude,
    longitude,
    altitude: position.altitude ?? null,
    time: position.time || null,
    satsInView: position.satsInView || null,
    precisionBits: position.precisionBits || null,
    groundSpeed: position.groundSpeed || null,
    groundTrack: position.groundTrack || null
  };
}

// ======================================================
// NODEINFO DECODER
// ======================================================

function decodeNodeInfo(payload) {

  const user =
    fromBinary(Mesh.UserSchema, payload);

  return {
    id: user.id || null,
    longName: user.longName || null,
    shortName: user.shortName || null,
    hwModel: user.hwModel ?? null,
    isLicensed: user.isLicensed ?? false,
    role: user.role ?? null,

    // Base64 so the WebSocket JSON stays serializable
    publicKey:
      user.publicKey?.length
        ? Buffer.from(user.publicKey)
            .toString('base64')
        : null,

    publicKeyBytes:
      user.publicKey?.length || 0
  };
}

// ======================================================
// MAP REPORT DECODER
// ======================================================

function decodeMapReport(payload) {

  const report =
    fromBinary(Mqtt.MapReportSchema, payload);

  return {
    longName: report.longName || null,
    shortName: report.shortName || null,
    role: report.role ?? null,
    hwModel: report.hwModel ?? null,
    firmwareVersion: report.firmwareVersion || null,
    region: report.region ?? null,
    modemPreset: report.modemPreset ?? null,
    hasDefaultChannel: report.hasDefaultChannel ?? false,

    latitude:
      report.latitudeI
        ? report.latitudeI * 1e-7
        : null,

    longitude:
      report.longitudeI
        ? report.longitudeI * 1e-7
        : null,

    altitude: report.altitude ?? null,
    positionPrecision: report.positionPrecision ?? null,
    numOnlineLocalNodes: report.numOnlineLocalNodes ?? null,
    hasOptedReportLocation: report.hasOptedReportLocation ?? false
  };
}

// ======================================================
// TELEMETRY DECODER
// ======================================================

function decodeTelemetry(payload) {

  const telemetry =
    fromBinary(Telemetry.TelemetrySchema, payload);

  const variant = telemetry.variant;

  const result = {
    type: 'telemetry',
    time: telemetry.time || null,
    telemetryType: variant?.case || 'unknown',
    metrics: null
  };

  if (!variant?.case) {
    return result;
  }

  const value = variant.value;

  if (variant.case === 'deviceMetrics') {

    result.metrics = {
      batteryLevel: value.batteryLevel ?? null,
      voltage: value.voltage ?? null,
      channelUtilization: value.channelUtilization ?? null,
      airUtilTx: value.airUtilTx ?? null,
      uptimeSeconds: value.uptimeSeconds ?? null
    };
  }

  else if (variant.case === 'environmentMetrics') {

    result.metrics = {
      temperature: value.temperature ?? null,
      relativeHumidity: value.relativeHumidity ?? null,
      barometricPressure: value.barometricPressure ?? null,
      gasResistance: value.gasResistance ?? null,
      voltage: value.voltage ?? null,
      current: value.current ?? null
    };
  }

  else if (variant.case === 'powerMetrics') {

    result.metrics = {
      ch1Voltage: value.ch1Voltage ?? null,
      ch1Current: value.ch1Current ?? null,
      ch2Voltage: value.ch2Voltage ?? null,
      ch2Current: value.ch2Current ?? null,
      ch3Voltage: value.ch3Voltage ?? null,
      ch3Current: value.ch3Current ?? null
    };
  }

  else {
    result.metrics = { detected: true };
  }

  return result;
}

// ======================================================
// APPLICATION DECODER
// ======================================================

function decodeApplicationPayload(portName, payload) {

  if (!payload) {
    return null;
  }

  // NORMAL TEXT
  if (portName === 'TEXT_MESSAGE_APP') {

    return {
      type: 'text',
      text: Buffer.from(payload).toString('utf8')
    };
  }

  // COMPRESSED TEXT
  // Detection only - no fake UTF-8 decoding.
  if (portName === 'TEXT_MESSAGE_COMPRESSED_APP') {

    return {
      type: 'compressed_text',
      compression: 'Unishox2',
      bytes: payload.length,
      hex: Buffer.from(payload).toString('hex'),
      base64: Buffer.from(payload).toString('base64')
    };
  }

  // POSITION
  if (portName === 'POSITION_APP') {

    return {
      type: 'position',
      ...decodePosition(payload)
    };
  }

  // NODEINFO
  if (portName === 'NODEINFO_APP') {

    return {
      type: 'nodeinfo',
      ...decodeNodeInfo(payload)
    };
  }

  // TELEMETRY
  if (portName === 'TELEMETRY_APP') {

    return decodeTelemetry(payload);
  }

  // MAP REPORT
  if (portName === 'MAP_REPORT_APP') {

    return {
      type: 'mapreport',
      ...decodeMapReport(payload)
    };
  }

  // EVERYTHING ELSE
  // Keep it visible instead of discarding it.
  return {
    type: 'unhandled',
    portName,
    bytes: payload.length,
    hex: Buffer.from(payload).toString('hex'),
    base64: Buffer.from(payload).toString('base64')
  };
}

// ======================================================
// JSON INSPECTION
// JSON is intentionally parsed as JSON instead of
// pretending that its text bytes are protobuf.
// ======================================================

function inspectJsonPacket(region, topic, payload) {

  jsonStats.packets++;

  const raw = Buffer.from(payload).toString('utf8');

  plog('MQTT transport: JSON');
  plog('JSON bytes:', payload.length);

  try {

    const obj = JSON.parse(raw);

    jsonStats.valid++;

    const type = String(obj.type || '').toLowerCase();

    const portnum =
      Number(
        obj.portnum ??
        obj.portNum ??
        obj.port ??
        -1
      );

    const possibleText =
      type === 'text' ||
      type === 'message' ||
      type.includes('text_message') ||
      portnum === 1 ||
      portnum === 7;

    if (possibleText) {

      jsonStats.possibleText++;
      pktInteresting = true;

      const jsonText =
        typeof obj?.payload?.text === 'string' ? obj.payload.text :
        typeof obj?.text === 'string' ? obj.text : null;
      const topicParts = String(topic).split('/');
      const jsonChannel = topicParts.length >= 5 ? topicParts[4] : null;
      const jsonGateway = obj.sender || (topicParts.length >= 6 ? topicParts[5] : null);
      const isPkiJson = String(jsonChannel || '').toUpperCase() === 'PKI';
      const jsonDirected = normalizeNodeNumber(obj.to) !== 0xffffffff;

      if (jsonText) {
        const recorded = recordMessageObservation({
          regionId: region.id,
          source: region.id,
          transport: 'json',
          topic,
          channelId: jsonChannel,
          from: obj.from,
          to: obj.to,
          packetId: obj.id,
          text: jsonText,
          gatewayId: jsonGateway,
          directed: jsonDirected,
          pki: isPkiJson
        });
        plog('Normalized message:', recorded?.rejected ? `REJECTED (${recorded.reason})` : (recorded?.isNew ? 'NEW' : 'DUPLICATE'));
        plog('Message class:', isPkiJson ? 'PKI JSON MESSAGE' : (jsonDirected ? 'DIRECTED JSON MESSAGE' : 'PUBLIC JSON MESSAGE'));
      }

      plog('===================================');
      plog('POSSIBLE JSON TEXT MESSAGE');
      plog('Region:', region.id);
      plog('Topic:', topic);
      plog('Type:', obj.type);
      plog('From:', obj.from);
      plog('To:', obj.to);
      plog('Payload:', JSON.stringify(obj.payload));
      plog('JSON:', JSON.stringify(obj));
      plog('===================================');

    } else {

      plog('JSON packet: OK');
      plog('JSON type:', obj.type ?? '(none)');
      plog(
        'JSON keys:',
        Object.keys(obj).join(', ') || '(none)'
      );
    }

    return {
      valid: true,
      possibleText,
      data: obj
    };

  } catch (err) {

    jsonStats.invalid++;

    plog('JSON parse: FAILED');
    plog('Reason:', err.message);

    return {
      valid: false,
      possibleText: false,
      error: err.message,
      rawHex: Buffer.from(payload).toString('hex')
    };
  }
}

// ======================================================
// MQTT OPTIONS
// ======================================================

const opts = {
  protocolVersion: 4,
  clientId: CLIENT_ID,
  reconnectPeriod: 5000,
  connectTimeout: 15000,
  keepalive: 60,
  clean: true
};

if (MQTT_USER) {
  opts.username = MQTT_USER;
}

if (MQTT_PASS) {
  opts.password = MQTT_PASS;
}

const ozuloOpts = {
  protocolVersion: 4,
  clientId: `${CLIENT_ID}-oz`.slice(0, 60),
  reconnectPeriod: 5000,
  connectTimeout: 15000,
  keepalive: 60,
  clean: true,
  username: OZULO_MQTT_USER,
  password: OZULO_MQTT_PASS
};

const spainOpts = {
  protocolVersion: 4,
  clientId: `${CLIENT_ID}-es`.slice(0, 60),
  reconnectPeriod: 5000,
  connectTimeout: 15000,
  keepalive: 60,
  clean: true,
  username: SPAIN_MQTT_USER,
  password: SPAIN_MQTT_PASS
};

// ======================================================
// STARTUP
// ======================================================

console.log('===================================');
console.log('VirtualMesh');
console.log('Diagnostic MQTT Receiver');
console.log('===================================');
console.log('Broker:', MQTT_URL);
console.log('Client ID:', CLIENT_ID);
console.log('Regions:');

for (const region of REGIONS) {
  console.log(
    ` - ${region.id} | ${region.name} | ${region.topic}`
  );
}

console.log('Mode: READ ONLY');
console.log('Puerto Rico priority root msh/US/PR/#: ENABLED');
console.log('US global root: ENABLED');
console.log('EU868 global root: ENABLED');
console.log('Spain direct broker: ENABLED (READ ONLY)');
console.log('Spain O Zulo broker: ENABLED (READ ONLY)');
console.log('Binary MQTT traffic: TRY SERVICE ENVELOPE');
console.log('/2/map/ ServiceEnvelope attempt: ENABLED');
console.log('Other binary ServiceEnvelope attempt: ENABLED');
console.log('JSON inspection: ENABLED');
console.log('Encrypted packet decode attempts: ENABLED');
console.log('LongFast public key diagnostic test: ENABLED');
console.log('PKI/non-LongFast automatic skip: DISABLED');
console.log('Directed node-to-node automatic skip: DISABLED');
console.log('TEXT_MESSAGE_APP decoder: ENABLED');
console.log('TEXT_MESSAGE_COMPRESSED_APP detector: ENABLED');
console.log('POSITION_APP decoder: ENABLED');
console.log('NODEINFO_APP decoder: ENABLED');
console.log('TELEMETRY_APP decoder: ENABLED');
console.log('MAP_REPORT_APP decoder: ENABLED');
console.log('Public key capture: ENABLED');
console.log('Traffic analyzer:', TRAFFIC_ANALYZER ? 'ENABLED (diagnostic, no blocking)' : 'DISABLED');
console.log('Per-packet log:', VERBOSE ? 'VERBOSE' : 'QUIET (interesting only + 60s summary)');
console.log('MQTT publish: DISABLED');
console.log('===================================');

// ======================================================
// MQTT CLIENT
// ======================================================

const mc = mqtt.connect(MQTT_URL, opts);

console.log('');
console.log('========== SPAIN MQTT DIAGNOSTIC ==========');
console.log('[SPAIN] Connecting:', SPAIN_MQTT_URL);
console.log('[SPAIN] Protocol: MQTT TCP / TLS disabled');
console.log('[SPAIN] Client ID:', spainOpts.clientId);
console.log('[SPAIN] Username:', SPAIN_MQTT_USER);
console.log('[SPAIN] Topic:', SPAIN_MQTT_TOPIC);
console.log('[SPAIN] Mode: READ ONLY - publish DISABLED');
console.log('===========================================');
spainMqttDiag.state = 'connecting';
const mcSpain = mqtt.connect(SPAIN_MQTT_URL, spainOpts);

console.log('');
console.log('========== O ZULO MQTT DIAGNOSTIC ==========');
console.log('[OZULO] Connecting:', OZULO_MQTT_URL);
console.log('[OZULO] Protocol: MQTT TCP / TLS disabled');
console.log('[OZULO] Client ID:', ozuloOpts.clientId);
console.log('[OZULO] Username:', OZULO_MQTT_USER);
console.log('[OZULO] Topic:', OZULO_MQTT_TOPIC);
console.log('[OZULO] Mode: READ ONLY - publish DISABLED');
console.log('=============================================');
ozuloMqttDiag.state = 'connecting';
const mcOzulo = mqtt.connect(OZULO_MQTT_URL, ozuloOpts);

// ======================================================
// MQTT CONNECT
// ======================================================

mc.on('connect', connack => {

  console.log('');
  console.log('MQTT CONNECTED');
  console.log('CONNACK:', JSON.stringify(connack));

  mqttState = 'connected';
  lastError = '';

  console.log('SUBSCRIBING TO:');

  for (const topic of TOPICS) {
    console.log(' -', topic);
  }

  mc.subscribe(
    TOPICS,
    { qos: 0 },
    (err, granted) => {

      if (err) {
        console.error('SUBSCRIBE ERROR:', err.message);
        return;
      }

      console.log('SUBACK:', JSON.stringify(granted));
    }
  );

  broadcast({
    type: 'status',
    mqttState,
    lastError,
    topics: TOPICS,
    regions: REGIONS
  });
});

// ======================================================
// MQTT MESSAGE
// ======================================================

function handleMessage(topic, payload, regionOverride = null, sourceOverride = null) {

  const region = regionOverride || getRegionFromTopic(topic);
  const discoverySource = sourceOverride || region.id;

  const mqttTopicType = getMqttTopicType(topic);

  if (transportStats[mqttTopicType] !== undefined) {
    transportStats[mqttTopicType]++;
  }

  addRegionTraffic(region.id, payload.length);

  plog('');
  plog('-----------------------------------');
  plog(`REGION: ${region.id} (${region.name})`);
  plog('MQTT PACKET:', topic, `${payload.length} bytes`);
  plog('MQTT transport:', mqttTopicType.toUpperCase());

  // ==================================================
  // JSON
  //
  // JSON is already a decoded transport representation.
  // Inspect it directly.
  // ==================================================

  if (mqttTopicType === 'json') {

    const jsonResult =
      inspectJsonPacket(region, topic, payload);

    broadcast({
      type: 'packet',
      region: region.id,
      regionName: region.name,
      topic,
      topicType: mqttTopicType,
      receivedAt: new Date().toISOString(),
      bytes: payload.length,
      decoded: jsonResult
    });

    return;
  }

  // ==================================================
  // ALL NON-JSON TRAFFIC
  //
  // IMPORTANT:
  // No preventive skip here.
  //
  // /2/e/   -> attempt ServiceEnvelope
  // /2/map/ -> attempt ServiceEnvelope
  // other   -> attempt ServiceEnvelope
  // ==================================================

  plog('ServiceEnvelope decode: WILL ATTEMPT');

  transportStats.serviceEnvelopeAttempts++;

  let result = null;

  try {

    // ================================================
    // SERVICE ENVELOPE
    // ================================================

    const envelope =
      fromBinary(Mqtt.ServiceEnvelopeSchema, payload);

    transportStats.serviceEnvelopeSuccess++;

    plog('SERVICE ENVELOPE: OK');
    plog('Gateway ID:', envelope.gatewayId || '(none)');
    plog('Channel ID:', envelope.channelId || '(none)');

    const packet = envelope.packet;

    observeChannelDiscovery({
      source: discoverySource,
      regionId: region.id,
      channelId: envelope.channelId || '(none)',
      from: packet?.from,
      isText: false
    });

    analyzeTraffic({
      gatewayId: envelope.gatewayId,
      topic,
      channelId: envelope.channelId,
      packet,
      payload,
      retained: pktRetained,
      variantCase: packet?.payloadVariant?.case || null
    });

    if (!packet) {

      plog('MESH PACKET: MISSING');

      variantStats.noPacket++;

      if (variantStats.noPacket <= 5) {
        pktInteresting = true;
        plog('SAMPLE NO-PACKET RAW HEX:', Buffer.from(payload).toString('hex'));
      }

      result = {
        serviceEnvelope: true,
        meshPacket: false,
        topicType: mqttTopicType,
        region: region.id,
        regionName: region.name,
        gatewayId: envelope.gatewayId || null,
        channelId: envelope.channelId || null
      };

    } else {

      const temporal = classifyPacketAge(packet);
      const earlyVariantCase = packet?.payloadVariant?.case || null;

      // Keep historical no-payload traffic in analyzer/statistics only.
      // It must not enter the operational node DB, inbox or WebSocket feed.
      if (temporal.ageClass === 'STALE' && !earlyVariantCase) {
        temporalStats.staleNoPayloadDiscarded++;
        if (pktRetained) temporalStats.retainedStaleNoPayloadDiscarded++;

        variantStats.noPayload++;
        noPayloadOld++;
        if (pktRetained) noPayloadRetained++;
        noPayloadByRegion[region.id] = (noPayloadByRegion[region.id] || 0) + 1;
        bump(noPayloadByGateway, envelope.gatewayId || '(none)');
        bump(noPayloadByChannel, envelope.channelId || '(none)');

        return;
      }

      plog('MESH PACKET: OK');
      plog('Age class:', temporal.ageClass, 'age_s=' + temporal.ageSeconds);

      const fromHex = nodeIdToHex(packet.from);

      const isBroadcast =
        Number(packet.to) === 0xffffffff;

      const toHex =
        isBroadcast
          ? null
          : nodeIdToHex(packet.to);

      plog('From:', packet.from, `(${fromHex})`);
      plog(
        'To:',
        packet.to,
        isBroadcast ? '(BROADCAST)' : `(${toHex})`
      );
      plog('Packet ID:', packet.id);
      plog('Channel:', packet.channel);
      plog('Hop Limit:', packet.hopLimit);
      plog('Hop Start:', packet.hopStart);

      updateObservedNode(region.id, packet.from, {
        lastGateway: envelope.gatewayId || null,
        channelId: envelope.channelId || null
      });

      // ==============================================
      // MESH PACKET PUBLIC KEY
      // ==============================================

      if (packet.publicKey?.length) {

        recordPublicKey(
          region.id,
          packet.from,
          packet.publicKey,
          'MESH_PACKET'
        );
      }

      const variant = packet.payloadVariant;

      let payloadType = 'UNKNOWN';

      let portnum = null;
      let portName = null;

      let encryptedBytes = 0;
      let decodedBytes = 0;

      let decryptionSuccess = false;
      let decryptionAttempted = false;

      let application = null;

      // ==============================================
      // ALREADY DECODED
      // ==============================================

      if (variant?.case === 'decoded') {

        payloadType = 'DECODED';

        variantStats.decoded++;

        const data = variant.value;

        portnum = data.portnum;
        portName = getPortNumName(portnum);

        decodedBytes = data.payload?.length || 0;

        plog('Payload: DECODED');
        plog('PortNum:', portnum, `(${portName})`);
        plog('Application payload bytes:', decodedBytes);
        plog('Want response:', data.wantResponse);
        plog('Request ID:', data.requestId);
        plog('Reply ID:', data.replyId);

        addPortStat(portnum, portName, region.id);

        plog(
          'PORT STATS:',
          JSON.stringify(getPortStatsObject())
        );

        try {

          application =
            decodeApplicationPayload(
              portName,
              data.payload
            );

        } catch (appError) {

          plog(
            'APPLICATION DECODE FAILED:',
            appError.message
          );
        }
      }

      // ==============================================
      // ENCRYPTED
      //
      // Diagnostic mode:
      // attempt every encrypted payload using the
      // public LongFast key.
      //
      // AES transform success is NOT enough.
      // Mesh.Data must parse successfully.
      // ==============================================

      else if (variant?.case === 'encrypted') {

        payloadType = 'ENCRYPTED';

        variantStats.encrypted++;

        encryptedBytes = variant.value?.length || 0;

        decryptionAttempted = true;

        plog('Payload: ENCRYPTED');
        plog('Encrypted bytes:', encryptedBytes);
        plog(
          'Decrypt attempt channel:',
          envelope.channelId || '(none)'
        );
        const keyCandidates = candidateChannelKeys(envelope.channelId);
        plog('Trying public key candidates:', keyCandidates.map(x => x.name).join(', '));

        try {
          let plaintext = null;
          let data = null;
          let keyUsed = null;
          let lastKeyError = null;
          for (const candidate of keyCandidates) {
            try {
              const candidatePlaintext = decryptWithChannelKey(variant.value, packet.id, packet.from, candidate.key);
              const candidateData = fromBinary(Mesh.DataSchema, candidatePlaintext);
              if (candidateData.portnum === 0) throw new Error('portnum 0');
              plaintext = candidatePlaintext;
              data = candidateData;
              keyUsed = candidate.name;
              break;
            } catch (e) { lastKeyError = e; }
          }
          if (!data) {
            variantStats.portnum0Rejected++;
            throw new Error(`no public channel key produced valid Mesh.Data${lastKeyError ? ': '+lastKeyError.message : ''}`);
          }

          plog('AES-CTR decrypt candidate: OK');
          plog('Public key used:', keyUsed);
          plog('Plaintext candidate bytes:', plaintext.length);

          decryptionSuccess = true;

          addDecryptAttempt(envelope.channelId, true);

          payloadType = 'DECRYPTED';

          portnum = data.portnum;
          portName = getPortNumName(portnum);

          decodedBytes = data.payload?.length || 0;

          plog('DATA PROTOBUF: OK');
          plog(
            'Channel ID:',
            envelope.channelId || '(none)'
          );
          plog('PortNum:', portnum, `(${portName})`);
          plog('Application payload bytes:', decodedBytes);
          plog('Want response:', data.wantResponse);
          plog('Request ID:', data.requestId);
          plog('Reply ID:', data.replyId);

          addPortStat(portnum, portName, region.id);

          plog(
            'PORT STATS:',
            JSON.stringify(getPortStatsObject())
          );

          try {

            application =
              decodeApplicationPayload(
                portName,
                data.payload
              );

          } catch (appError) {

            plog(
              'APPLICATION DECODE FAILED:',
              appError.message
            );
          }

        } catch (decryptError) {

          addDecryptAttempt(envelope.channelId, false);

          plog('DECRYPT / DATA PARSE FAILED');
          plog(
            'Channel ID:',
            envelope.channelId || '(none)'
          );
          plog('Reason:', decryptError.message);
          plog(
            'Packet retained as observed encrypted traffic.'
          );
        }
      }

      else {

        plog('Payload: NONE / UNKNOWN');

        variantStats.noPayload++;

        const ageSeconds =
          packet.rxTime
            ? Math.round(Date.now() / 1000 - Number(packet.rxTime))
            : null;

        noPayloadByRegion[region.id] =
          (noPayloadByRegion[region.id] || 0) + 1;

        bump(noPayloadByGateway, envelope.gatewayId || '(none)');
        bump(noPayloadByChannel, envelope.channelId || '(none)');

        if (pktRetained) {
          noPayloadRetained++;
        }

        // rx_time older than 1 day (or retained) = stale/replayed
        if (ageSeconds !== null && ageSeconds > 86400) {
          noPayloadOld++;
        }

        noPayloadSamplesByRegion[region.id] =
          (noPayloadSamplesByRegion[region.id] || 0) + 1;

        if (noPayloadSamplesByRegion[region.id] <= 3) {
          pktInteresting = true;
          plog('SAMPLE NO-PAYLOAD [' + region.id + ']',
            'retained=' + pktRetained,
            'age_s=' + ageSeconds);
          plog('SAMPLE NO-PAYLOAD RAW HEX:', Buffer.from(payload).toString('hex'));
          plog('SAMPLE NO-PAYLOAD TOPIC:', topic);
          plog(
            'SAMPLE NO-PAYLOAD PACKET FIELDS:',
            JSON.stringify(packet, (k, v) =>
              typeof v === 'bigint'
                ? v.toString()
                : v instanceof Uint8Array
                  ? Buffer.from(v).toString('hex')
                  : v
            )
          );
        }
        plog(
          'payloadVariant case:',
          variant?.case || '(none)'
        );
      }

      // ==============================================
      // TEXT MESSAGE
      // ==============================================

      if (application?.type === 'text') {

        pktInteresting = true;

        addRegionMessage(region.id);

        observeChannelDiscovery({ source: discoverySource, regionId: region.id, channelId: envelope.channelId || '(none)', from: packet.from, isText: true });

        const normalizedMessage = recordMessageObservation({
          regionId: region.id,
          source: discoverySource,
          transport: 'protobuf',
          topic,
          channelId: envelope.channelId || null,
          from: packet.from,
          to: packet.to,
          packetId: packet.id,
          text: application.text,
          gatewayId: envelope.gatewayId || null,
          directed: !isBroadcast,
          pki: String(envelope.channelId || '').toUpperCase() === 'PKI'
        });

        plog('');
        plog('===================================');
        plog(normalizedMessage?.rejected ? 'INVALID TEXT CANDIDATE REJECTED' : (normalizedMessage?.isNew ? 'TEXT MESSAGE RECEIVED' : 'DUPLICATE TEXT MESSAGE'));
        plog('===================================');
        plog('Region:', region.id);
        plog('Channel ID:', envelope.channelId || '(none)');
        plog('From:', fromHex);
        plog('To:', isBroadcast ? 'BROADCAST' : toHex);
        plog('PortNum:', portnum, `(${portName})`);
        if (normalizedMessage?.rejected) plog('Reject reason:', normalizedMessage.reason);
        else plog('Message:', application.text);
        plog('Gateway:', envelope.gatewayId || '(none)');
        plog('===================================');
        plog('');
      }

      // ==============================================
      // COMPRESSED TEXT
      // ==============================================

      if (application?.type === 'compressed_text') {

        pktInteresting = true;

        addRegionMessage(region.id);

        plog('');
        plog('===================================');
        plog('COMPRESSED TEXT MESSAGE DETECTED');
        plog('===================================');
        plog('Region:', region.id);
        plog('Channel ID:', envelope.channelId || '(none)');
        plog('From:', fromHex);
        plog('To:', isBroadcast ? 'BROADCAST' : toHex);
        plog('Compression:', application.compression);
        plog('Compressed bytes:', application.bytes);
        plog('Payload HEX:', application.hex);
        plog('Payload Base64:', application.base64);
        plog('===================================');
        plog('');
      }

      // ==============================================
      // POSITION
      // ==============================================

      if (application?.type === 'position') {

        plog('POSITION APP: OK');
        plog('Latitude:', application.latitude);
        plog('Longitude:', application.longitude);
        plog('Altitude:', application.altitude);
        plog('Satellites:', application.satsInView);
        plog('Precision bits:', application.precisionBits);

        updateObservedNode(region.id, packet.from, {
          position: application
        });
      }

      // ==============================================
      // NODEINFO
      // ==============================================

      if (application?.type === 'nodeinfo') {

        plog('NODEINFO APP: OK');
        plog('Node ID:', application.id);
        plog('Long Name:', application.longName);
        plog('Short Name:', application.shortName);
        plog('Hardware Model:', application.hwModel);
        plog('Role:', application.role);

        updateObservedNode(region.id, packet.from, {
          user: application
        });

        if (application.publicKey) {

          recordPublicKey(
            region.id,
            packet.from,
            application.publicKey,
            'NODEINFO_APP'
          );
        }
      }

      // ==============================================
      // TELEMETRY
      // ==============================================

      if (application?.type === 'telemetry') {

        plog('TELEMETRY APP: OK');
        plog('Telemetry type:', application.telemetryType);
        plog('Telemetry time:', application.time);

        if (application.telemetryType === 'deviceMetrics') {

          plog('Battery:', application.metrics?.batteryLevel);
          plog('Voltage:', application.metrics?.voltage);
          plog(
            'Channel utilization:',
            application.metrics?.channelUtilization
          );
          plog(
            'Air utilization TX:',
            application.metrics?.airUtilTx
          );
          plog(
            'Uptime seconds:',
            application.metrics?.uptimeSeconds
          );
        }

        else if (application.telemetryType === 'environmentMetrics') {

          plog('Temperature:', application.metrics?.temperature);
          plog('Humidity:', application.metrics?.relativeHumidity);
          plog('Pressure:', application.metrics?.barometricPressure);
        }

        updateObservedNode(region.id, packet.from, {
          telemetry: application
        });
      }

      // ==============================================
      // MAP REPORT
      // ==============================================

      if (application?.type === 'mapreport') {

        pktInteresting = true;

        plog('');
        plog('===================================');
        plog('MAP REPORT: OK');
        plog('===================================');
        plog('Region:', region.id);
        plog('Node:', fromHex);
        plog('Long Name:', application.longName);
        plog('Short Name:', application.shortName);
        plog('Hardware Model:', application.hwModel);
        plog('Role:', application.role);
        plog('Firmware:', application.firmwareVersion);
        plog('LoRa Region:', application.region);
        plog('Modem Preset:', application.modemPreset);
        plog('Default Channel:', application.hasDefaultChannel);
        plog('Latitude:', application.latitude);
        plog('Longitude:', application.longitude);
        plog('Altitude:', application.altitude);
        plog('Position Precision:', application.positionPrecision);
        plog('Online Local Nodes:', application.numOnlineLocalNodes);
        plog('Location Opt-In:', application.hasOptedReportLocation);
        plog('===================================');
        plog('');

        updateObservedNode(region.id, packet.from, {
          mapReport: application
        });
      }

      // ==============================================
      // UNHANDLED PORT
      // ==============================================

      if (application?.type === 'unhandled') {

        if (!seenUnhandledPorts.has(application.portName)) {
          seenUnhandledPorts.add(application.portName);
          pktInteresting = true;
        }

        plog('UNHANDLED APPLICATION PORT');
        plog('Port:', application.portName);
        plog('Bytes:', application.bytes);
        plog('HEX:', application.hex);
      }

      // ==============================================
      // NODE COUNTS
      // ==============================================

      const nodeCounts = getNodeCountsByRegion();

      plog('Observed nodes:', observedNodes.size);
      plog('By region:', JSON.stringify(nodeCounts));

      // ==============================================
      // RESULT
      // ==============================================

      result = {
        serviceEnvelope: true,
        meshPacket: true,
        topicType: mqttTopicType,
        region: region.id,
        regionName: region.name,
        gatewayId: envelope.gatewayId || null,
        channelId: envelope.channelId || null,
        from: packet.from,
        fromHex,
        to: packet.to,
        toHex,
        broadcast: isBroadcast,
        directed: !isBroadcast,
        id: packet.id,
        channel: packet.channel,
        hopLimit: packet.hopLimit,
        hopStart: packet.hopStart,
        payloadType,
        encryptedBytes,
        decodedBytes,
        decryptionAttempted,
        decryptionSuccess,
        portnum,
        portName,
        application,
        observedNode: getObservedNode(region.id, packet.from),
        observedNodeCount: observedNodes.size,
        nodeCounts,
        regionStats: getRegionStatsObject(),
        portStats: getPortStatsObject(),
        decryptStats,
        transportStats,
        publicKeyStats,
        publicKeyNodeCounts: getPublicKeyNodeCounts(),
        jsonStats: { ...jsonStats }
      };
    }

  } catch (err) {

    transportStats.serviceEnvelopeFailed++;

    {
      const preview = Buffer.from(payload).toString('utf8');

      if (/^[\x20-\x7e\r\n\t]+$/.test(preview)) {
        variantStats.plainTextOnProtobufTopic++;

        // Log each DISTINCT plain text once (a bot repeating the same
        // test string every few seconds would otherwise flood the log).
        const textKey = preview.slice(0, 200);

        if (!seenPlainTexts.has(textKey) && seenPlainTexts.size < 30) {
          seenPlainTexts.add(textKey);
          pktInteresting = true;
          plog(
            'PLAIN TEXT ON PROTOBUF TOPIC (not a Meshtastic packet):',
            JSON.stringify(preview.slice(0, 200)),
            'topic:',
            topic
          );
        }
      }
    }

    plog('SERVICE ENVELOPE DECODE FAILED');
    plog('Transport:', mqttTopicType);
    plog('Topic:', topic);
    plog('Bytes:', payload.length);
    plog('Reason:', err.message);
    plog('Raw HEX:', Buffer.from(payload).toString('hex'));

    result = {
      serviceEnvelope: false,
      topicType: mqttTopicType,
      region: region.id,
      regionName: region.name,
      error: err.message,
      bytes: payload.length,
      rawHex: Buffer.from(payload).toString('hex')
    };
  }

  // ==================================================
  // SEND TO BROWSER
  // ==================================================

  broadcast({
    type: 'packet',
    region: region.id,
    regionName: region.name,
    topic,
    topicType: mqttTopicType,
    receivedAt: new Date().toISOString(),
    bytes: payload.length,
    decoded: result
  });
}

mc.on('message', (topic, payload, mqttPacket) => {

  pktRetained = !!mqttPacket?.retain;

  if (pktRetained) {
    variantStats.retained++;
  }

  pktLog = [];
  pktInteresting = false;
  totalPackets++;
  lastPacketAt = Date.now();

  try {
    handleMessage(topic, payload);
  } catch (err) {
    console.error('HANDLER ERROR:', err.message);
  } finally {

    const buffered = pktLog;
    pktLog = null;

    if (VERBOSE || pktInteresting) {
      for (const args of buffered) {
        console.log(...args);
      }
    } else {
      suppressedPackets++;
    }
  }
});

// ======================================================
// LATIN AMERICA COMMUNITY MQTT CLIENTS - READ ONLY
// ======================================================

function createReadOnlyCommunityClient(label, url, username, password, topic, region) {
  const diag = { state:'connecting', url, topic, connectedAt:null, lastPacketAt:null, packets:0, reconnects:0, closes:0, lastError:null, lastSuback:null };
  const options = { protocolVersion:4, clientId:`${CLIENT_ID}-${label.toLowerCase()}`.slice(0,60), reconnectPeriod:5000, connectTimeout:15000, keepalive:60, clean:true, username, password };
  console.log(`[${label}] Connecting:`, url);
  console.log(`[${label}] Topic:`, topic);
  console.log(`[${label}] Mode: READ ONLY - publish DISABLED`);
  const client = mqtt.connect(url, options);
  client.on('connect', connack => { diag.state='connected'; diag.connectedAt=new Date().toISOString(); diag.lastError=null; console.log(`[${label}] MQTT CONNECTED`); client.subscribe(topic,{qos:0},(err,granted)=>{ if(err){diag.lastError=`SUBSCRIBE: ${err.message}`; console.error(`[${label}] SUBSCRIBE ERROR:`,err.message);return;} diag.lastSuback=granted; console.log(`[${label}] SUBACK:`,JSON.stringify(granted)); }); });
  client.on('message',(mqttTopic,payload,mqttPacket)=>{ diag.packets++; diag.lastPacketAt=new Date().toISOString(); if(diag.packets<=5||diag.packets%100===0) console.log(`[${label}] PACKET #${diag.packets}: ${mqttTopic} (${payload.length} bytes)`); pktRetained=!!mqttPacket?.retain; if(pktRetained)variantStats.retained++; pktLog=[]; pktInteresting=false; totalPackets++; lastPacketAt=Date.now(); try{handleMessage(mqttTopic,payload,region,label);}catch(err){console.error(`${label} HANDLER ERROR:`,err.message);}finally{const buffered=pktLog;pktLog=null;if(VERBOSE||pktInteresting){for(const args of buffered)console.log(...args);}else suppressedPackets++;} });
  client.on('reconnect',()=>{diag.state='reconnecting';diag.reconnects++;}); client.on('offline',()=>{diag.state='offline';}); client.on('close',()=>{diag.state='closed';diag.closes++;}); client.on('error',err=>{diag.state='error';diag.lastError=err?.message||String(err);console.error(`[${label}] MQTT ERROR:`,diag.lastError);});
  return {client,diag};
}

const chileSource = createReadOnlyCommunityClient('CHILE', CHILE_MQTT_URL, CHILE_MQTT_USER, CHILE_MQTT_PASS, CHILE_MQTT_TOPIC, CHILE_REGION);
const colombiaSource = createReadOnlyCommunityClient('COLOMBIA', COLOMBIA_MQTT_URL, COLOMBIA_MQTT_USER, COLOMBIA_MQTT_PASS, COLOMBIA_MQTT_TOPIC, COLOMBIA_REGION);

app.get('/api/hispanic-status', (req,res)=>res.json({service:'VirtualMesh Hispanic + US',mode:'READ_ONLY',version:'v0.6.0-hispanic-us',priority:{country:'Puerto Rico',topic:'msh/US/PR/#',verifiedPublicChannels:['LongFast'],discovery:'Any additional channel observed under PR root is recorded diagnostically, not pre-declared public.'},countries:['PR','US','ES','MX','AR','CL','CO','VE','PE','EC','BO','PY','UY','PA','CR','GT','HN','SV','NI','DO','CU','GQ'],spain:{direct:SPAIN_MQTT_TOPIC,ozulo:OZULO_MQTT_TOPIC},sources:{globalUS:'msh/US/2/#',puertoRico:'msh/US/PR/#',euSpain:'msh/EU_868/2/#',anzDiscovery:'msh/ANZ/#',chile:CHILE_MQTT_TOPIC,colombia:COLOMBIA_MQTT_TOPIC}}));

app.get('/api/latin-america-status', (req,res)=>res.json({service:'VirtualMesh',mode:'READ_ONLY',version:'v0.5.5-latam',sources:{chile:chileSource.diag,colombia:colombiaSource.diag,mexico:{mode:'US_ROOT_GEO',topic:'msh/US/2/#'},argentina:{mode:'ANZ_ROOT_GEO',topic:'msh/ANZ/#',publicChannels:['BairesMesh','RosarioMesh','NQNmesh','CordobaMesh','ERMesh','MendozaMesh']},venezuela:{mode:'REGIONAL_ROOT_GEO',dedicatedPublicBroker:null}}}));

// ======================================================
// O ZULO COMMUNITY MQTT CLIENT - READ ONLY
// ======================================================

mcOzulo.on('connect', connack => {
  ozuloMqttDiag.state = 'connected';
  ozuloMqttDiag.connectedAt = new Date().toISOString();
  ozuloMqttDiag.lastError = null;
  console.log('');
  console.log('[OZULO] MQTT CONNECTED');
  console.log('[OZULO] CONNACK:', JSON.stringify(connack));
  console.log('[OZULO] SUBSCRIBING TO:', OZULO_MQTT_TOPIC);
  mcOzulo.subscribe(OZULO_MQTT_TOPIC, { qos: 0 }, (err, granted) => {
    if (err) {
      ozuloMqttDiag.lastError = `SUBSCRIBE: ${err.message}`;
      console.error('[OZULO] SUBSCRIBE ERROR:', err.message);
      return;
    }
    ozuloMqttDiag.lastSuback = granted;
    console.log('[OZULO] SUBACK:', JSON.stringify(granted));
  });
});

mcOzulo.on('message', (topic, payload, mqttPacket) => {
  ozuloMqttDiag.packets++;
  ozuloMqttDiag.lastPacketAt = new Date().toISOString();
  if (ozuloMqttDiag.packets <= 5 || ozuloMqttDiag.packets % 100 === 0) {
    console.log(`[OZULO] PACKET #${ozuloMqttDiag.packets}: ${topic} (${payload.length} bytes)`);
  }
  pktRetained = !!mqttPacket?.retain;
  if (pktRetained) variantStats.retained++;
  pktLog = [];
  pktInteresting = false;
  totalPackets++;
  lastPacketAt = Date.now();
  try {
    // O Zulo is a Spanish community source; normalize into SPAIN while
    // keeping independent source diagnostics above.
    handleMessage(topic, payload, SPAIN_REGION, 'SPAIN_OZULO');
  } catch (err) {
    console.error('OZULO HANDLER ERROR:', err.message);
  } finally {
    const buffered = pktLog;
    pktLog = null;
    if (VERBOSE || pktInteresting) {
      for (const args of buffered) console.log(...args);
    } else {
      suppressedPackets++;
    }
  }
});

mcOzulo.on('reconnect', () => { ozuloMqttDiag.state = 'reconnecting'; ozuloMqttDiag.reconnects++; console.log('[OZULO] MQTT RECONNECTING'); });
mcOzulo.on('offline', () => { ozuloMqttDiag.state = 'offline'; console.log('[OZULO] MQTT OFFLINE'); });
mcOzulo.on('close', () => { ozuloMqttDiag.state = 'closed'; ozuloMqttDiag.closes++; console.log('[OZULO] MQTT CONNECTION CLOSED'); });
mcOzulo.on('error', err => {
  ozuloMqttDiag.state = 'error';
  ozuloMqttDiag.lastError = err?.message || String(err);
  console.error('[OZULO] MQTT ERROR:', ozuloMqttDiag.lastError);
  if (err?.code) console.error('[OZULO] ERROR CODE:', err.code);
});

setInterval(() => {
  console.log('[OZULO] STATUS:', JSON.stringify({
    state: ozuloMqttDiag.state,
    packets: ozuloMqttDiag.packets,
    lastPacketAt: ozuloMqttDiag.lastPacketAt,
    reconnects: ozuloMqttDiag.reconnects,
    closes: ozuloMqttDiag.closes,
    lastError: ozuloMqttDiag.lastError
  }));
}, 60000);

app.get('/api/spain-ozulo-status', (req, res) => {
  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    source: 'SPAIN_OZULO',
    normalizedRegion: 'SPAIN',
    ...ozuloMqttDiag
  });
});

// ======================================================
// SPAIN COMMUNITY MQTT CLIENT - READ ONLY
// ======================================================

mcSpain.on('connect', connack => {
  spainMqttDiag.state = 'connected';
  spainMqttDiag.connectedAt = new Date().toISOString();
  spainMqttDiag.lastError = null;
  console.log('');
  console.log('[SPAIN] MQTT CONNECTED');
  console.log('[SPAIN] CONNACK:', JSON.stringify(connack));
  console.log('[SPAIN] SUBSCRIBING TO:', SPAIN_MQTT_TOPIC);

  mcSpain.subscribe(SPAIN_MQTT_TOPIC, { qos: 0 }, (err, granted) => {
    if (err) {
      spainMqttDiag.lastError = `SUBSCRIBE: ${err.message}`;
      console.error('[SPAIN] SUBSCRIBE ERROR:', err.message);
      return;
    }
    spainMqttDiag.lastSuback = granted;
    console.log('[SPAIN] SUBACK:', JSON.stringify(granted));
  });
});

mcSpain.on('message', (topic, payload, mqttPacket) => {
  spainMqttDiag.packets++;
  spainMqttDiag.lastPacketAt = new Date().toISOString();
  if (spainMqttDiag.packets <= 5 || spainMqttDiag.packets % 100 === 0) {
    console.log(`[SPAIN] PACKET #${spainMqttDiag.packets}: ${topic} (${payload.length} bytes)`);
  }
  pktRetained = !!mqttPacket?.retain;
  if (pktRetained) variantStats.retained++;

  pktLog = [];
  pktInteresting = false;
  totalPackets++;
  lastPacketAt = Date.now();

  try {
    handleMessage(topic, payload, SPAIN_REGION, 'SPAIN_DIRECT');
  } catch (err) {
    console.error('SPAIN HANDLER ERROR:', err.message);
  } finally {
    const buffered = pktLog;
    pktLog = null;
    if (VERBOSE || pktInteresting) {
      for (const args of buffered) console.log(...args);
    } else {
      suppressedPackets++;
    }
  }
});

mcSpain.on('reconnect', () => { spainMqttDiag.state = 'reconnecting'; spainMqttDiag.reconnects++; console.log('[SPAIN] MQTT RECONNECTING'); });
mcSpain.on('offline', () => { spainMqttDiag.state = 'offline'; console.log('[SPAIN] MQTT OFFLINE'); });
mcSpain.on('close', () => { spainMqttDiag.state = 'closed'; spainMqttDiag.closes++; console.log('[SPAIN] MQTT CONNECTION CLOSED'); });
mcSpain.on('error', err => {
  spainMqttDiag.state = 'error';
  spainMqttDiag.lastError = err?.message || String(err);
  console.error('[SPAIN] MQTT ERROR:', spainMqttDiag.lastError);
  if (err?.code) console.error('[SPAIN] ERROR CODE:', err.code);
});

setInterval(() => {
  console.log('[SPAIN] STATUS:', JSON.stringify({
    state: spainMqttDiag.state,
    packets: spainMqttDiag.packets,
    lastPacketAt: spainMqttDiag.lastPacketAt,
    reconnects: spainMqttDiag.reconnects,
    closes: spainMqttDiag.closes,
    lastError: spainMqttDiag.lastError
  }));
}, 60000);

app.get('/api/spain-status', (req, res) => {
  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    source: 'SPAIN',
    ...spainMqttDiag
  });
});

setInterval(() => {

  console.log('');
  console.log('========== SUMMARY (60s) ==========');
  console.log('Total packets:', totalPackets);
  console.log('New in last 60s:', totalPackets - lastSummaryTotal);
  lastSummaryTotal = totalPackets;
  console.log('Variants:', JSON.stringify(variantStats));
  console.log('NoPayload by region:', JSON.stringify(noPayloadByRegion));
  console.log('NoPayload top gateways:', JSON.stringify(topN(noPayloadByGateway)));
  console.log('NoPayload top channels:', JSON.stringify(topN(noPayloadByChannel)));
  console.log('NoPayload retained:', noPayloadRetained, '| rx_time older than 1 day:', noPayloadOld);
  console.log('Suppressed from log:', suppressedPackets);
  console.log('Observed nodes:', observedNodes.size);
  console.log('Nodes by region:', JSON.stringify(getNodeCountsByRegion()));
  console.log('Region traffic:', JSON.stringify(getRegionStatsObject()));
  console.log('Channel discovery:', JSON.stringify(getChannelDiscoveryObject().slice(0, 50)));
  console.log('Ports:', JSON.stringify(
    getPortStatsObject().map(p => `${p.portName}=${p.total}`)
  ));
  console.log('Decrypt:', JSON.stringify({
    attempts: decryptStats.attempts,
    success: decryptStats.success,
    failed: decryptStats.failed
  }));
  console.log('Envelope:', JSON.stringify({
    ok: transportStats.serviceEnvelopeSuccess,
    failed: transportStats.serviceEnvelopeFailed
  }));
  console.log('Public keys:', JSON.stringify(publicKeyStats));
  if (TRAFFIC_ANALYZER) {
    const suspects = getSuspectGateways().slice(0, 5);
    console.log('Traffic analyzer suspects:', JSON.stringify(suspects));
  }
  console.log('Memory MB:', Math.round(process.memoryUsage().rss / 1048576));
  const health = operationalSnapshot();
  healthHistory.push(health);
  if (healthHistory.length > HEALTH_HISTORY_MAX) healthHistory.shift();
  lastHealthPacketTotal = totalPackets;
  lastHealthAt = Date.now();
  console.log('Operational health:', health.status, '| packets/min:', health.traffic.packetsPerMinute, '| live nodes:', health.nodes.liveOperational);
  console.log('===================================');

}, 60000);

// ======================================================
// MQTT CONNECTION EVENTS
// ======================================================

mc.on('reconnect', () => {
  console.log('MQTT RECONNECTING');
  mqttState = 'reconnecting';
});

mc.on('offline', () => {
  console.log('MQTT OFFLINE');
  mqttState = 'offline';
});

mc.on('close', () => {
  console.log('MQTT CONNECTION CLOSED');
});

mc.on('disconnect', packet => {
  console.log('MQTT DISCONNECT:', JSON.stringify(packet));
});

mc.on('error', err => {
  lastError = err.message;
  console.error('MQTT ERROR:', err.message);
});

// ======================================================
// WEBSOCKET
// ======================================================

wss.on('connection', ws => {

  clients.add(ws);

  ws.send(
    JSON.stringify({
      type: 'status',
      mqttState,
      lastError,
      topics: TOPICS,
      regions: REGIONS,
      mode: 'READ_ONLY',
      trafficMode: 'DIAGNOSTIC_TRY_ALL',
      observedNodeCount: observedNodes.size,
      uniqueNodeCount: uniqueNodes.size,
      messageCount: messageInbox.size,
      messageDedupStats: { ...messageDedupStats },
      nodeCounts: getNodeCountsByRegion(),
      regionStats: getRegionStatsObject(),
      portStats: getPortStatsObject(),
      decryptStats,
      transportStats,
      publicKeyStats,
      publicKeyNodeCounts: getPublicKeyNodeCounts(),
      jsonStats: { ...jsonStats }
    })
  );

  ws.send(
    JSON.stringify({
      type: 'nodes',
      nodes: Array.from(observedNodes.values()),
      uniqueNodes: Array.from(uniqueNodes.values()),
      uniqueNodeCount: uniqueNodes.size,
      nodeCounts: getNodeCountsByRegion()
    })
  );

  ws.on('close', () => {
    clients.delete(ws);
  });
});

// ======================================================
// OPERATIONAL HEALTH / TOPOLOGY / PLAYBACK
// ======================================================
function operationalSnapshot() {
  const now = Date.now();
  const elapsedMin = Math.max((now - lastHealthAt) / 60000, 1/60);
  const packetDelta = totalPackets - lastHealthPacketTotal;
  const packetsPerMinute = Math.round(packetDelta / elapsedMin);
  const liveCutoff = now - LIVE_MAX_AGE_SECONDS * 1000;
  let liveNodes = 0, spainNodes = 0, prNodes = 0;
  for (const node of uniqueNodes.values()) {
    const t = Date.parse(node.lastSeen || '');
    if (Number.isFinite(t) && t >= liveCutoff && isOperationalGeography(node)) liveNodes++;
    if (node.countryCode === 'ES') spainNodes++;
    if (node.subdivisionCode === 'PR') prNodes++;
  }
  const rss = Math.round(process.memoryUsage().rss / 1048576);
  const mqttFresh = lastPacketAt ? (now - lastPacketAt) < 120000 : false;
  let status = 'HEALTHY';
  const warnings = [];
  if (mqttState !== 'connected') { status = 'DEGRADED'; warnings.push('MQTT_NOT_CONNECTED'); }
  if (!mqttFresh && mqttState === 'connected') { status = 'DEGRADED'; warnings.push('MQTT_TRAFFIC_AGING'); }
  if (rss > 400) { status = 'DEGRADED'; warnings.push('HIGH_MEMORY'); }
  const decryptRate = decryptStats.attempts ? decryptStats.success / decryptStats.attempts : null;
  return {
    ts: new Date(now).toISOString(), status, warnings,
    uptimeSeconds: Math.floor((now - PROCESS_STARTED_AT)/1000),
    mqtt: { state: mqttState, lastPacketAt: lastPacketAt ? new Date(lastPacketAt).toISOString() : null, fresh: mqttFresh },
    traffic: { totalPackets, packetsPerMinute, suppressedPackets, staleNoPayload: noPayloadOld, retainedNoPayload: noPayloadRetained },
    nodes: { observed: observedNodes.size, unique: uniqueNodes.size, liveOperational: liveNodes, spain: spainNodes, puertoRico: prNodes },
    messages: { stored: messageInbox.size, ...messageDedupStats, invalidTextCandidates: operationalStats.invalidTextCandidates, invalidTextReasons },
    crypto: { decryptAttempts: decryptStats.attempts, decryptSuccess: decryptStats.success, decryptFailed: decryptStats.failed, decryptSuccessRate: decryptRate },
    publicKeys: { ...publicKeyStats },
    websocketClients: clients.size,
    memoryMB: rss,
    operationalStats: { ...operationalStats }
  };
}

function topologySummary() {
  const gateways = [];
  for (const g of trafficGateways.values()) {
    gateways.push({
      gatewayId: g.gatewayId, total: g.total, decoded: g.decoded, encrypted: g.encrypted,
      noPayload: g.noPayload, retained: g.retained, stale24h: g.stale24h, live5m: g.live5m,
      uniqueSenders: g.senders?.size ?? 0, uniquePacketIds: g.packetIds?.size ?? 0,
      firstSeen: g.firstSeen, lastSeen: g.lastSeen
    });
  }
  gateways.sort((a,b) => b.total-a.total);
  return gateways;
}

app.get('/api/health', (req, res) => res.json({ service:'VirtualMesh', mode:'READ_ONLY', operationalCore:'v0.5.5-latam', ...operationalSnapshot() }));

app.get('/api/topology', (req, res) => {
  const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 1000);
  res.json({ service:'VirtualMesh', mode:'READ_ONLY', count: Math.min(limit, trafficGateways.size), gateways: topologySummary().slice(0, limit) });
});

app.get('/api/playback', (req, res) => {
  const type = String(req.query.type || '').trim();
  const since = req.query.since ? Date.parse(String(req.query.since)) : NaN;
  const limit = Math.min(Math.max(Number(req.query.limit || 250), 1), 2000);
  let events = operationalEvents;
  if (type) events = events.filter(e => e.type === type);
  if (Number.isFinite(since)) events = events.filter(e => Date.parse(e.ts) >= since);
  events = events.slice(-limit);
  res.json({ service:'VirtualMesh', mode:'READ_ONLY', stored: operationalEvents.length, count: events.length, events });
});

app.get('/api/alerts', (req, res) => {
  const snap = operationalSnapshot();
  const alerts = snap.warnings.map(code => ({ severity:'warning', code, ts:snap.ts }));
  if (getSuspectGateways().length) alerts.push({ severity:'info', code:'STALE_TRAFFIC_SOURCES_PRESENT', count:getSuspectGateways().length, ts:snap.ts });
  res.json({ service:'VirtualMesh', mode:'READ_ONLY', count:alerts.length, alerts });
});

// API - NODES
// ======================================================

app.get('/api/nodes', (req, res) => {

  const requestedRegion =
    String(req.query.region || '').toUpperCase();

  let nodes = Array.from(observedNodes.values());

  if (requestedRegion) {
    nodes = nodes.filter(
      node => node.region === requestedRegion
    );
  }

  let globalNodes = Array.from(uniqueNodes.values());
  if (requestedRegion) {
    globalNodes = globalNodes.filter(node =>
      Array.isArray(node.regionsSeen) && node.regionsSeen.includes(requestedRegion)
    );
  }

  res.json({
    mode: 'READ_ONLY',
    trafficMode: 'DIAGNOSTIC_TRY_ALL',
    count: nodes.length,
    totalCount: observedNodes.size,
    uniqueCount: globalNodes.length,
    totalUniqueCount: uniqueNodes.size,
    uniqueNodes: globalNodes,
    nodeCounts: getNodeCountsByRegion(),
    publicKeyNodeCounts: getPublicKeyNodeCounts(),
    regionStats: getRegionStatsObject(),
    nodes
  });
});

// ======================================================
// API - NORMALIZED MESSAGES / UNIQUE NODES
// ======================================================

app.get('/api/messages', (req, res) => {
  const directed = String(req.query.directed || '').toLowerCase();
  const pki = String(req.query.pki || '').toLowerCase();
  const country = String(req.query.country || '').trim().toUpperCase();
  const state = String(req.query.state || '').trim().toUpperCase();
  const region = String(req.query.region || '').trim().toUpperCase();
  const limit = Math.min(Math.max(Number(req.query.limit || MESSAGE_INBOX_MAX), 1), MESSAGE_INBOX_MAX);
  let messages = getMessagesNewestFirst().map(m => ({
    ...m,
    ...(nodeGeographyForMessage(m.from) || {}),
    channelClassification: classifyMessageChannel(m)
  }));
  if (directed === 'true') messages = messages.filter(m => m.directed);
  if (directed === 'false') messages = messages.filter(m => !m.directed);
  if (pki === 'true') messages = messages.filter(m => m.pki);
  if (pki === 'false') messages = messages.filter(m => !m.pki);
  if (country) messages = messages.filter(m => String(m.countryCode || '').toUpperCase() === country);
  if (state) messages = messages.filter(m => String(m.subdivisionCode || '').toUpperCase() === state);
  if (region) messages = messages.filter(m =>
    String(m.region || '').toUpperCase() === region ||
    (Array.isArray(m.regionsSeen) && m.regionsSeen.some(r => String(r).toUpperCase() === region))
  );
  messages = messages.slice(0, limit);
  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    count: messages.length,
    maxStored: MESSAGE_INBOX_MAX,
    stats: { ...messageDedupStats },
    messages
  });
});

app.get('/api/unique-nodes', (req, res) => {
  const requestedRegion = String(req.query.region || '').toUpperCase();
  let nodes = Array.from(uniqueNodes.values());
  if (requestedRegion) {
    nodes = nodes.filter(node =>
      Array.isArray(node.regionsSeen) && node.regionsSeen.includes(requestedRegion)
    );
  }
  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    count: nodes.length,
    totalUniqueCount: uniqueNodes.size,
    nodes
  });
});

app.get('/api/live-nodes', (req, res) => {
  const now = Date.now();
  const liveWindowMs = LIVE_MAX_AGE_SECONDS * 1000;
  let nodes = Array.from(uniqueNodes.values()).filter(node => {
    const t = Date.parse(node.lastSeen || node.updatedAt || node.lastHeard || '');
    return Number.isFinite(t) && (now - t) <= liveWindowMs && isOperationalGeography(node);
  });
  const country = String(req.query.country || '').trim().toUpperCase();
  const state = String(req.query.state || '').trim().toUpperCase();
  if (country) nodes = nodes.filter(node => String(node.countryCode || '').toUpperCase() === country);
  if (state) nodes = nodes.filter(node => String(node.subdivisionCode || '').toUpperCase() === state);

  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    windowMinutes: LIVE_MAX_AGE_SECONDS / 60,
    count: nodes.length,
    temporalStats: { ...temporalStats },
    nodes
  });
});

app.get('/api/geography', (req, res) => {
  const nodes = Array.from(uniqueNodes.values());
  const bySubdivision = {};
  let spain = 0;
  let eu868UnknownOrOutsideSpain = 0;
  let usUnknown = 0;
  for (const node of nodes) {
    if (node.countryCode === 'ES') spain++;
    if (node.lastRegion === 'EU868' && node.countryCode !== 'ES') eu868UnknownOrOutsideSpain++;
    if (node.lastRegion === 'US' && !node.subdivisionCode) usUnknown++;
    if (node.subdivisionCode) bySubdivision[node.subdivisionCode] = (bySubdivision[node.subdivisionCode] || 0) + 1;
  }
  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    geographicCore: 'v0.5.5-latam',
    spain,
    eu868UnknownOrOutsideSpain,
    usUnknown,
    bySubdivision
  });
});

app.get('/api/temporal-stats', (req, res) => {
  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    liveMaxAgeSeconds: LIVE_MAX_AGE_SECONDS,
    recentMaxAgeSeconds: RECENT_MAX_AGE_SECONDS,
    stats: { ...temporalStats }
  });
});

// ======================================================
// API - TRAFFIC ANALYZER
// ======================================================

app.get('/api/traffic-analyzer', (req, res) => {
  const gateway = String(req.query.gateway || '').trim();
  let gateways = getTrafficAnalyzerSummary();
  if (gateway) {
    const g = trafficGateways.get(gateway);
    gateways = g ? [trafficGatewaySummary(g)] : [];
  }
  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    enabled: TRAFFIC_ANALYZER,
    blocking: false,
    suspectThreshold: SUSPECT_GATEWAY_THRESHOLD,
    hashCacheMax: HASH_CACHE_MAX,
    trackedGateways: trafficGateways.size,
    suspects: getSuspectGateways(),
    gateways
  });
});

// ======================================================
app.get('/api/health-history', (req, res) => {
  const limit = Math.min(Math.max(Number(req.query.limit || 120), 1), HEALTH_HISTORY_MAX);
  res.json({ service:'VirtualMesh', mode:'READ_ONLY', count:Math.min(limit, healthHistory.length), samples:healthHistory.slice(-limit) });
});

// ======================================================
// PUBLIC PUERTO RICO API v0.5
// Privacy-first, READ ONLY. No directed/PKI messages, no keys,
// no exact coordinates, no raw MQTT diagnostics.
// ======================================================
function publicNodeName(node) {
  return node?.user?.longName || node?.mapReport?.longName || node?.longName || node?.user?.shortName || node?.mapReport?.shortName || node?.nodeHex || 'Meshtastic node';
}
function publicShortName(node) { return node?.user?.shortName || node?.mapReport?.shortName || node?.shortName || null; }
function publicApproxPosition(node) {
  const lat = Number(node?.latitude ?? node?.position?.latitude ?? node?.mapReport?.latitude);
  const lon = Number(node?.longitude ?? node?.position?.longitude ?? node?.mapReport?.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return { latitude: Math.round(lat * 10) / 10, longitude: Math.round(lon * 10) / 10, approximate: true };
}
function isPuertoRicoNode(node) {
  // Geographic Core represents Puerto Rico with subdivisionCode=PR.
  // Accept either countryCode=PR (current classifier) or US+PR for backward compatibility.
  return String(node?.subdivisionCode || '').toUpperCase() === 'PR' ||
    String(node?.countryCode || '').toUpperCase() === 'PR';
}

function publicPrNodes() {
  const now = Date.now();
  return Array.from(uniqueNodes.values()).filter(isPuertoRicoNode).map(node => {
    const seen = Date.parse(node.lastSeen || '');
    const ageSeconds = Number.isFinite(seen) ? Math.max(0, Math.floor((now-seen)/1000)) : null;
    const telemetry = node.telemetry || null;
    return { nodeId:node.nodeHex, longName:publicNodeName(node), shortName:publicShortName(node), lastSeen:node.lastSeen||null,
      ageSeconds, active:ageSeconds!==null && ageSeconds<=LIVE_MAX_AGE_SECONDS, position:publicApproxPosition(node),
      telemetry:telemetry ? { type:telemetry.telemetryType||null, time:telemetry.time||null, metrics:telemetry.metrics||null } : null };
  }).sort((a,b)=>String(b.lastSeen||'').localeCompare(String(a.lastSeen||'')));
}
function publicPrMessages() {
  return getMessagesNewestFirst().map(m=>({...m,...(nodeGeographyForMessage(m.from)||{})}))
    .filter(m=>String(m.subdivisionCode||'').toUpperCase()==='PR')
    .filter(m=>m.broadcast===true && m.directed!==true && m.pki!==true)
    .map(m=>{ const node=uniqueNodes.get(m.fromHex); return { id:m.key, from:m.fromHex, longName:publicNodeName(node), shortName:publicShortName(node), channel:m.channelId||null, text:m.text, firstSeen:m.firstSeen, lastSeen:m.lastSeen }; });
}
app.get('/api/public/pr/status',(req,res)=>{ const nodes=publicPrNodes(), messages=publicPrMessages();
  res.json({service:'VirtualMesh Puerto Rico',mode:'READ_ONLY',publicApi:'v0.5',privacy:{exactCoordinates:false,directedMessages:false,pkiMessages:false,publicKeys:false},updatedAt:new Date().toISOString(),mqtt:mqttState==='connected'?'connected':'degraded',nodes:{known:nodes.length,active:nodes.filter(n=>n.active).length,sensors:nodes.filter(n=>n.telemetry?.type==='environmentMetrics').length},messages:{publicStored:messages.length}}); });
app.get('/api/public/pr/nodes',(req,res)=>{const nodes=publicPrNodes();res.json({service:'VirtualMesh Puerto Rico',mode:'READ_ONLY',count:nodes.length,coordinatePrecision:'approximate',nodes});});
app.get('/api/public/pr/messages',(req,res)=>{const limit=Math.min(Math.max(Number(req.query.limit||100),1),250),messages=publicPrMessages().slice(0,limit);res.json({service:'VirtualMesh Puerto Rico',mode:'READ_ONLY',visibility:'PUBLIC_BROADCAST_ONLY',count:messages.length,messages});});
app.get('/api/public/pr/telemetry',(req,res)=>{const nodes=publicPrNodes().filter(n=>n.telemetry),environment=nodes.filter(n=>n.telemetry.type==='environmentMetrics'),device=nodes.filter(n=>n.telemetry.type==='deviceMetrics'),power=nodes.filter(n=>n.telemetry.type==='powerMetrics');res.json({service:'VirtualMesh Puerto Rico',mode:'READ_ONLY',count:nodes.length,environmentCount:environment.length,deviceCount:device.length,powerCount:power.length,environment,device,power});});

// API - DIAGNOSTICS
// ======================================================

app.get('/api/channel-discovery', (req, res) => {
  const source = req.query.source ? String(req.query.source).toUpperCase() : null;
  const channel = req.query.channel ? String(req.query.channel).toLowerCase() : null;
  let rows = getChannelDiscoveryObject();
  if (source) rows = rows.filter(r => String(r.source).toUpperCase() === source || String(r.regionId).toUpperCase() === source);
  if (channel) rows = rows.filter(r => String(r.channelId).toLowerCase().includes(channel));
  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    core: 'CHANNEL_DISCOVERY',
    attribution: 'MQTT_SOURCE_PLUS_CHANNEL_ID',
    geographyUsedForCorroboration: false,
    count: rows.length,
    channels: rows
  });
});


app.get('/api/channel-candidates', (req, res) => {
  const source = req.query.source ? String(req.query.source).toUpperCase() : null;
  const priority = req.query.priority ? String(req.query.priority).toUpperCase() : null;
  const minScoreRaw = Number(req.query.minScore ?? 0);
  const minScore = Number.isFinite(minScoreRaw) ? Math.max(0, Math.min(100, minScoreRaw)) : 0;
  const limitRaw = Number(req.query.limit ?? 100);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, Math.floor(limitRaw))) : 100;

  let candidates = getChannelCandidates();
  if (source) candidates = candidates.filter(r => String(r.source).toUpperCase() === source || String(r.regionId).toUpperCase() === source);
  if (priority) candidates = candidates.filter(r => r.candidate.priority === priority);
  candidates = candidates.filter(r => r.candidate.score >= minScore).slice(0, limit);

  const all = getChannelCandidates();
  const priorityCounts = { HIGH: 0, MEDIUM: 0, LOW: 0 };
  for (const row of all) priorityCounts[row.candidate.priority]++;

  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    core: 'CANDIDATE_ANALYZER',
    purpose: 'RESEARCH_PRIORITIZATION_ONLY',
    automaticPromotion: false,
    geographyUsed: false,
    scoring: {
      decodedText: '0-35',
      uniqueNodes: '0-25',
      packetActivity: '0-20',
      recurrenceCurrentRuntime: '0-15',
      dedicatedObservedSource: '0-5',
      high: '70-100',
      medium: '45-69',
      low: '0-44'
    },
    filters: { source, priority, minScore, limit },
    totalCandidates: all.length,
    priorityCounts,
    count: candidates.length,
    candidates
  });
});

app.get('/api/channel-classification', (req, res) => {
  const rows = getChannelDiscoveryObject();
  const groups = { VERIFIED: [], VERIFIED_SOURCE: [], OBSERVED: [], DISCOVERY: [] };
  for (const row of rows) {
    const status = row.classification?.status || 'DISCOVERY';
    (groups[status] ||= []).push(row);
  }
  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    core: 'CHANNEL_CORROBORATION',
    policy: 'NO_GEOLOCATION_FOR_COMMUNITY_CORROBORATION',
    verifiedRules: VERIFIED_CHANNEL_RULES.map(r => ({ source: r.source, channels: r.channels, community: r.community, countryCode: r.countryCode })),
    counts: Object.fromEntries(Object.entries(groups).map(([k,v]) => [k, v.length])),
    groups
  });
});

app.get('/api/diagnostics', (req, res) => {

  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    trafficMode: 'DIAGNOSTIC_TRY_ALL',
    observedNodes: observedNodes.size,
    uniqueNodes: uniqueNodes.size,
    messages: messageInbox.size,
    messageDedupStats: { ...messageDedupStats },
    nodeCounts: getNodeCountsByRegion(),
    publicKeyNodeCounts: getPublicKeyNodeCounts(),
    portStats: getPortStatsObject(),
    regionStats: getRegionStatsObject(),
    transportStats,
    decryptStats,
    publicKeyStats,
    jsonStats: { ...jsonStats },

    transportPolicy: {
      protobufServiceEnvelope: true,
      mapServiceEnvelopeAttempt: true,
      otherBinaryServiceEnvelopeAttempt: true,
      jsonInspection: true,
      preventiveBinarySkip: false
    },

    encryptedPacketPolicy: {
      attemptAll: true,
      keyUsed: 'LongFast public key',
      automaticPkiSkip: false,
      automaticNonLongFastSkip: false,
      directedPacketSkip: false
    },

    trafficAnalyzer: {
      enabled: TRAFFIC_ANALYZER,
      blocking: false,
      suspectThreshold: SUSPECT_GATEWAY_THRESHOLD,
      trackedGateways: trafficGateways.size,
      suspects: getSuspectGateways().slice(0, 10)
    },

    compressedText: {
      detection: true,
      compression: 'Unishox2',
      decompression: false
    }
  });
});

// ======================================================
// API - STATUS
// ======================================================

app.get('/api/status', (req, res) => {

  res.json({
    service: 'VirtualMesh',
    operationalCore: 'v0.5.5-latam',
    mqttState,
    topics: TOPICS,
    regions: REGIONS,
    mode: 'READ_ONLY',
    trafficMode: 'DIAGNOSTIC_TRY_ALL',
    observedNodes: observedNodes.size,
    uniqueNodes: uniqueNodes.size,
    messages: messageInbox.size,
    messageDedupStats: { ...messageDedupStats },
    nodeCounts: getNodeCountsByRegion(),
    publicKeyNodeCounts: getPublicKeyNodeCounts(),
    regionStats: getRegionStatsObject(),
    portStats: getPortStatsObject(),
    transportStats,
    decryptStats,
    publicKeyStats,
    jsonStats: { ...jsonStats },

    mqttTransport: {
      protobuf: true,
      json: true,
      mapAttempt: true,
      otherBinaryAttempt: true
    },

    decoders: {
      serviceEnvelopeTryAllBinary: true,
      encryptedPacketAttempt: true,
      automaticPkiSkip: false,
      automaticNonLongFastSkip: false,
      directedNodeToNodeSkip: false,
      position: true,
      nodeInfo: true,
      telemetry: true,
      text: true,
      compressedTextDetection: true,
      compressedTextDecompression: false,
      mapReport: true,
      nodeInfoPublicKey: true,
      meshPacketPublicKey: true
    }
  });
});

// ======================================================
// START SERVER
// ======================================================

server.listen(PORT, () => {

  console.log(`VirtualMesh Web listening on ${PORT}`);
  console.log('MQTT mode: READ ONLY');
  console.log('Traffic mode: DIAGNOSTIC TRY ALL');
  console.log('Listening regions:', TOPICS.join(', '));
  console.log('/2/e/: TRY SERVICE ENVELOPE');
  console.log('/2/map/: TRY SERVICE ENVELOPE');
  console.log('Other binary: TRY SERVICE ENVELOPE');
  console.log('/2/json/: INSPECT JSON');
  console.log('Encrypted packets: TRY LONGFAST KEY');
  console.log('PKI/non-LongFast skip: OFF');
  console.log('Node-to-node skip: OFF');
  console.log('MAP_REPORT_APP: DECODE');
  console.log('Public keys: CAPTURE');
  console.log('MQTT publish: DISABLED');
  console.log('Diagnostics endpoint: /api/diagnostics');
  console.log('Traffic analyzer endpoint: /api/traffic-analyzer');
  console.log('Channel candidate analyzer: /api/channel-candidates');
  console.log('Normalized message inbox: /api/messages');
  console.log('Global unique nodes: /api/unique-nodes');
  console.log('Live nodes (15 min): /api/live-nodes?country=ES or ?state=PR');
  console.log('Geographic summary: /api/geography');
  console.log('Temporal stats: /api/temporal-stats');
  console.log('Operational health: /api/health');
  console.log('Health history: /api/health-history');
  console.log('Gateway topology: /api/topology');
  console.log('Operational playback: /api/playback');
  console.log('Operational alerts: /api/alerts');
  console.log('Public PR portal: /puerto-rico.html');
  console.log('Public PR API: /api/public/pr/status | /nodes | /messages | /telemetry');
});

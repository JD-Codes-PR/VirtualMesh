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

// ======================================================
// REGIONS
// ======================================================

const REGIONS = [
  { id: 'PR', name: 'Puerto Rico', topic: 'msh/US/PR/#' },
  { id: 'FL', name: 'Florida', topic: 'msh/US/FL/#' },
  { id: 'TX', name: 'Texas', topic: 'msh/US/TX/#' },
  // Spain uses the EU_868 radio region. This subscribes to the public EU_868 root;
  // geographic Spain filtering can be applied from decoded position/map data.
  { id: 'EU868', name: 'Spain / Europe (EU_868 public root)', topic: 'msh/EU_868/2/#' },
  // Default root topic (gateways without a state sub-topic)
  { id: 'US', name: 'United States (default root)', topic: 'msh/US/2/#' }
];

const TOPICS = REGIONS.map(region => region.topic);

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

function recordMessageObservation({
  regionId, transport, topic, channelId, from, to, packetId,
  text, gatewayId, directed, pki = false, receivedAt = new Date().toISOString()
}) {
  if (typeof text !== 'string' || !text.length) return null;

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
    if (transport && !existing.transports.includes(transport)) existing.transports.push(transport);
    if (topic && !existing.topics.includes(topic) && existing.topics.length < 12) existing.topics.push(topic);
    existing.pki = existing.pki || pki;
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
    channelId: channelId || null,
    text,
    firstSeen: receivedAt,
    lastSeen: receivedAt,
    mqttCopies: 1,
    gateways: gateway ? [gateway] : [],
    regionsSeen: regionId ? [regionId] : [],
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

  broadcast({ type: 'message', message: item, messageDedupStats: { ...messageDedupStats } });
  return { isNew: true, message: item };
}

function getMessagesNewestFirst() {
  return Array.from(messageInbox.values()).sort((a, b) =>
    String(b.lastSeen).localeCompare(String(a.lastSeen))
  );
}

// ======================================================
// OBSERVED NODE
// ======================================================

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

  const updated = {
    ...existing,
    ...changes,
    region: regionId,
    nodeHex,
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
  uniqueNodes.set(nodeHex, {
    ...globalExisting,
    ...changes,
    nodeId: Number(nodeId),
    nodeHex,
    regionsSeen,
    lastRegion: regionId,
    lastSeen: updated.lastSeen
  });

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

function decryptWithLongFastKey(encrypted, packetId, fromNode) {

  const nonce = Buffer.alloc(16);

  nonce.writeBigUInt64LE(BigInt(packetId), 0);

  nonce.writeUInt32LE(Number(fromNode) >>> 0, 8);

  const decipher =
    crypto.createDecipheriv(
      'aes-128-ctr',
      LONGFAST_KEY,
      nonce
    );

  decipher.setAutoPadding(false);

  return Buffer.concat([
    decipher.update(Buffer.from(encrypted)),
    decipher.final()
  ]);
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
        plog('Normalized message:', recorded?.isNew ? 'NEW' : 'DUPLICATE');
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
console.log('PR + FL + TX + US(default root): ENABLED');
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

function handleMessage(topic, payload) {

  const region = getRegionFromTopic(topic);

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
        plog('Trying public LongFast key...');

        try {

          const plaintext =
            decryptWithLongFastKey(
              variant.value,
              packet.id,
              packet.from
            );

          plog('AES-CTR transform: OK');
          plog(
            'Plaintext candidate bytes:',
            plaintext.length
          );

          const data =
            fromBinary(Mesh.DataSchema, plaintext);

          // Random bytes can parse as a Data protobuf with portnum 0
          // (UNKNOWN_APP). That is NOT a valid decryption.
          if (data.portnum === 0) {
            variantStats.portnum0Rejected++;
            throw new Error('portnum 0 (UNKNOWN_APP): not a valid decrypt');
          }

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

        const normalizedMessage = recordMessageObservation({
          regionId: region.id,
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
        plog(normalizedMessage?.isNew ? 'TEXT MESSAGE RECEIVED' : 'DUPLICATE TEXT MESSAGE');
        plog('===================================');
        plog('Region:', region.id);
        plog('Channel ID:', envelope.channelId || '(none)');
        plog('From:', fromHex);
        plog('To:', isBroadcast ? 'BROADCAST' : toHex);
        plog('PortNum:', portnum, `(${portName})`);
        plog('Message:', application.text);
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
  let messages = getMessagesNewestFirst();
  if (directed === 'true') messages = messages.filter(m => m.directed);
  if (directed === 'false') messages = messages.filter(m => !m.directed);
  if (pki === 'true') messages = messages.filter(m => m.pki);
  if (pki === 'false') messages = messages.filter(m => !m.pki);
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
  const nodes = Array.from(uniqueNodes.values()).filter(node => {
    const t = Date.parse(node.lastSeen || node.updatedAt || node.lastHeard || '');
    return Number.isFinite(t) && (now - t) <= liveWindowMs;
  });

  res.json({
    service: 'VirtualMesh',
    mode: 'READ_ONLY',
    windowMinutes: LIVE_MAX_AGE_SECONDS / 60,
    count: nodes.length,
    temporalStats: { ...temporalStats },
    nodes
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
// API - DIAGNOSTICS
// ======================================================

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
  console.log('Normalized message inbox: /api/messages');
  console.log('Global unique nodes: /api/unique-nodes');
  console.log('Live nodes (15 min): /api/live-nodes');
  console.log('Temporal stats: /api/temporal-stats');
});

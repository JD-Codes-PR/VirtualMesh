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
// VIRTUALMESH - PUBLIC MESHTASTIC MONITOR & ANALYZER
// ======================================================

const PORT = process.env.PORT || 8080;

const MQTT_URL =
  process.env.MQTT_URL ||
  'mqtts://mqtt.meshtastic.org:8883';

const MQTT_USER = process.env.MQTT_USER || '';
const MQTT_PASS = process.env.MQTT_PASS || '';

// ======================================================
// REGIONS & TOPICS
// ======================================================

const REGIONS = [
  { id: 'US', name: 'United States / Territories', topic: 'msh/US/2/#' },
  { id: 'EU868', name: 'Spain via EU_868 public root', topic: 'msh/EU_868/2/#' }
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
  unknown: 0
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
// MQTT CLIENT & KEYS
// ======================================================

const CLIENT_ID = 'virtualmesh-' + crypto.randomBytes(6).toString('hex');

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
const wss = new WebSocketServer({ server, path: '/mesh' });
const clients = new Set();

let mqttState = 'disconnected';
let lastError = '';

wss.on('connection', (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({
    type: 'init',
    mqttState,
    messages: getMessagesNewestFirst(),
    nodesCount: uniqueNodes.size,
    dedupStats: messageDedupStats
  }));
  ws.on('close', () => clients.delete(ws));
});

function broadcast(data) {
  const message = JSON.stringify(data);
  for (const ws of clients) {
    if (ws.readyState === 1) ws.send(message);
  }
}

// ======================================================
// OBSERVED NODES & INBOX
// ======================================================

const observedNodes = new Map();
const uniqueNodes = new Map();
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

const portStats = new Map();
const regionStats = new Map();
for (const region of REGIONS) {
  regionStats.set(region.id, { packets: 0, bytes: 0, messages: 0 });
}

// ======================================================
// HELPER FUNCTIONS
// ======================================================

function getRegionFromTopic(topic) {
  for (const region of REGIONS) {
    const prefix = region.topic.replace('/#', '/');
    if (topic.startsWith(prefix)) return region;
  }
  return { id: 'UNKNOWN', name: 'Unknown', topic: null };
}

function getMqttTopicType(topic) {
  if (topic.includes('/2/e/')) return 'protobuf';
  if (topic.includes('/2/json/')) return 'json';
  if (topic.includes('/2/map/')) return 'map';
  return 'other';
}

function nodeIdToHex(nodeId) {
  if (typeof nodeId === 'string' && nodeId.startsWith('!')) return nodeId;
  return '!' + Number(nodeId || 0).toString(16).padStart(8, '0');
}

function normalizeNodeNumber(value) {
  if (typeof value === 'string' && value.startsWith('!')) {
    return parseInt(value.slice(1), 16) >>> 0;
  }
  const n = Number(value);
  return Number.isFinite(n) ? (n >>> 0) : 0;
}

function getPortNumName(portnum) {
  try {
    for (const [name, value] of Object.entries(Portnums.PortNum)) {
      if (value === portnum) return name;
    }
  } catch {}
  return `PORT_${portnum}`;
}

// ======================================================
// GEOGRAPHIC CLASSIFICATION CORE
// ======================================================

const US_GEO_BOUNDS = [
  ['PR','Puerto Rico',17.80,18.60,-67.35,-65.20],
  ['VI','U.S. Virgin Islands',17.60,18.50,-65.20,-64.45],
  ['GU','Guam',13.15,13.75,144.55,145.05],
  ['FL','Florida',24.35,31.10,-87.70,-79.80],
  ['TX','Texas',25.80,36.60,-106.70,-93.45],
  ['CA','California',32.45,42.10,-124.55,-114.00],
  ['NY','New York',40.45,45.10,-79.80,-71.75]
];

function inBox(lat, lon, minLat, maxLat, minLon, maxLon) {
  return Number.isFinite(lat) && Number.isFinite(lon) &&
    lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
}

function isSpainCoordinate(lat, lon) {
  return inBox(lat, lon, 35.70, 43.90, -9.55, 3.35) || // Mainland
         inBox(lat, lon, 38.55, 40.15, 1.00, 4.60) ||  // Balearic
         inBox(lat, lon, 27.45, 29.55, -18.30, -13.20); // Canary
}

function classifyGeography(regionId, latitude, longitude) {
  const lat = Number(latitude);
  const lon = Number(longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || (lat === 0 && lon === 0)) {
    return regionId === 'EU868'
      ? { countryCode: null, country: null, subdivisionCode: null, subdivision: null, geoStatus: 'EU868_UNKNOWN' }
      : { countryCode: 'US', country: 'United States', subdivisionCode: null, subdivision: null, geoStatus: 'US_UNKNOWN' };
  }

  if (regionId === 'EU868') {
    if (isSpainCoordinate(lat, lon)) {
      return { countryCode: 'ES', country: 'Spain', subdivisionCode: null, subdivision: null, geoStatus: 'SPAIN' };
    }
    return { countryCode: null, country: null, subdivisionCode: null, subdivision: null, geoStatus: 'NON_SPAIN_EU868' };
  }

  for (const [code, name, minLat, maxLat, minLon, maxLon] of US_GEO_BOUNDS) {
    if (inBox(lat, lon, minLat, maxLat, minLon, maxLon)) {
      return {
        countryCode: code === 'PR' ? 'PR' : 'US',
        country: code === 'PR' ? 'Puerto Rico' : 'United States',
        subdivisionCode: code,
        subdivision: name,
        geoStatus: code === 'PR' ? 'US_TERRITORY' : 'US_STATE'
      };
    }
  }

  return { countryCode: 'US', country: 'United States', subdivisionCode: null, subdivision: null, geoStatus: 'US_UNKNOWN' };
}

function updateObservedNode(regionId, nodeId, changes) {
  const nodeHex = nodeIdToHex(nodeId);
  const key = `${regionId}:${nodeHex}`;
  const existing = observedNodes.get(key) || { region: regionId, nodeId: Number(nodeId), nodeHex, firstSeen: new Date().toISOString() };

  const lat = changes.latitude ?? existing.latitude ?? null;
  const lon = changes.longitude ?? existing.longitude ?? null;
  const geography = classifyGeography(regionId, lat, lon);

  const updated = { ...existing, ...changes, ...geography, latitude: lat, longitude: lon, region: regionId, nodeHex, lastSeen: new Date().toISOString() };
  observedNodes.set(key, updated);

  const globalExisting = uniqueNodes.get(nodeHex) || { nodeId: Number(nodeId), nodeHex, firstSeen: updated.firstSeen, regionsSeen: [] };
  const regionsSeen = Array.from(new Set([...(globalExisting.regionsSeen || []), regionId]));

  uniqueNodes.set(nodeHex, {
    ...globalExisting,
    ...updated,
    regionsSeen,
    lastRegion: regionId,
    lastSeen: updated.lastSeen
  });

  return updated;
}

// ======================================================
// MESSAGE DE-DUPLICATION & INBOX
// ======================================================

function messageIdentity(from, packetId, text, to) {
  const f = normalizeNodeNumber(from);
  const id = normalizeNodeNumber(packetId);
  if (f && id) return `${f}:${id}`;
  return crypto.createHash('sha256').update(`${f}|${normalizeNodeNumber(to)}|${String(text || '')}`).digest('hex');
}

function validateTextMessage(text) {
  if (typeof text !== 'string' || !text.trim().length) return { valid: false, reason: 'EMPTY' };
  if (text.includes('\uFFFD')) return { valid: false, reason: 'INVALID_UTF8' };
  return { valid: true, reason: 'OK' };
}

function recordMessageObservation({
  regionId, transport, topic, channelId, from, to, packetId, text, gatewayId, directed, pki = false
}) {
  const textVal = validateTextMessage(text);
  if (!textVal.valid) return { isNew: false, rejected: true, reason: textVal.reason };

  messageDedupStats.observations++;
  if (transport === 'protobuf') messageDedupStats.protobuf++;
  if (transport === 'json') messageDedupStats.json++;

  const key = messageIdentity(from, packetId, text, to);
  const receivedAt = new Date().toISOString();
  const existing = messageInbox.get(key);

  if (existing) {
    messageDedupStats.duplicates++;
    existing.lastSeen = receivedAt;
    existing.mqttCopies++;
    if (gatewayId && !existing.gateways.includes(gatewayId)) existing.gateways.push(gatewayId);
    return { isNew: false, message: existing };
  }

  const nodeHex = nodeIdToHex(from);
  const node = uniqueNodes.get(nodeHex);

  const item = {
    key,
    packetId: normalizeNodeNumber(packetId),
    from: normalizeNodeNumber(from),
    fromHex: nodeHex,
    fromName: node?.longName || node?.shortName || nodeHex,
    to: normalizeNodeNumber(to),
    toHex: normalizeNodeNumber(to) === 0xffffffff ? 'BROADCAST' : nodeIdToHex(to),
    directed: Boolean(directed ?? (normalizeNodeNumber(to) !== 0xffffffff)),
    channelId: channelId || 'LongFast',
    text,
    country: node?.country || 'Unknown',
    subdivision: node?.subdivision || 'Unknown',
    latitude: node?.latitude || null,
    longitude: node?.longitude || null,
    firstSeen: receivedAt,
    lastSeen: receivedAt,
    mqttCopies: 1,
    gateways: gatewayId ? [gatewayId] : [],
    transports: [transport]
  };

  messageInbox.set(key, item);
  messageDedupStats.unique++;

  while (messageInbox.size > MESSAGE_INBOX_MAX) {
    messageInbox.delete(messageInbox.keys().next().value);
  }

  broadcast({ type: 'message', message: item, stats: messageDedupStats });
  return { isNew: true, message: item };
}

function getMessagesNewestFirst() {
  return Array.from(messageInbox.values()).sort((a, b) => b.lastSeen.localeCompare(a.lastSeen));
}

// ======================================================
// DECRYPTION & DECODING
// ======================================================

function decryptWithLongFastKey(encrypted, packetId, fromNode) {
  try {
    const nonce = Buffer.alloc(16);
    nonce.writeBigUInt64LE(BigInt(packetId), 0);
    nonce.writeUInt32LE(Number(fromNode) >>> 0, 8);

    const decipher = crypto.createDecipheriv('aes-128-ctr', LONGFAST_KEY, nonce);
    decipher.setAutoPadding(false);

    return Buffer.concat([decipher.update(Buffer.from(encrypted)), decipher.final()]);
  } catch (err) {
    return null;
  }
}

function decodeMapReport(payload) {
  try {
    const report = fromBinary(Mqtt.MapReportSchema, payload);
    return {
      longName: report.longName || null,
      shortName: report.shortName || null,
      role: report.role ?? null,
      hwModel: report.hwModel ?? null,
      firmwareVersion: report.firmwareVersion || null,
      latitude: report.latitudeI ? report.latitudeI * 1e-7 : null,
      longitude: report.longitudeI ? report.longitudeI * 1e-7 : null
    };
  } catch {
    return null;
  }
}

// ======================================================
// MQTT MESSAGE PROCESSOR
// ======================================================

function processMqttMessage(topic, payloadBuffer) {
  const region = getRegionFromTopic(topic);
  const topicType = getMqttTopicType(topic);

  regionStats.get(region.id).packets++;
  regionStats.get(region.id).bytes += payloadBuffer.length;

  if (topicType === 'map') {
    const mapReport = decodeMapReport(payloadBuffer);
    if (mapReport) {
      const parts = topic.split('/');
      const nodeIdHex = parts[parts.length - 1];
      const nodeId = parseInt(nodeIdHex.replace('!', ''), 16);
      if (Number.isFinite(nodeId)) {
        updateObservedNode(region.id, nodeId, mapReport);
      }
    }
    return;
  }

  if (topicType === 'json') {
    try {
      const json = JSON.parse(payloadBuffer.toString('utf-8'));
      if (json.type === 'text' && json.payload) {
        const textStr = typeof json.payload === 'string' ? json.payload : JSON.stringify(json.payload);
        recordMessageObservation({
          regionId: region.id,
          transport: 'json',
          topic,
          channelId: json.channelId || 'JSON',
          from: json.from,
          to: json.to || 0xffffffff,
          packetId: json.id || Math.floor(Math.random() * 1000000),
          text: textStr,
          gatewayId: json.sender
        });
      }
    } catch {}
    return;
  }

  if (topicType === 'protobuf') {
    try {
      const env = fromBinary(Mqtt.ServiceEnvelopeSchema, payloadBuffer);
      const packet = env.packet;
      if (!packet) return;

      classifyPacketAge(packet);
      const fromNode = packet.from;
      const toNode = packet.to;
      const packetId = packet.id;

      let decodedData = null;

      if (packet.payloadVariant?.case === 'decoded') {
        decodedData = packet.payloadVariant.value;
      } else if (packet.payloadVariant?.case === 'encrypted') {
        const decryptedRaw = decryptWithLongFastKey(packet.payloadVariant.value, packetId, fromNode);
        if (decryptedRaw) {
          try {
            decodedData = fromBinary(Mesh.DataSchema, decryptedRaw);
          } catch {}
        }
      }

      if (!decodedData) return;

      // Handle Port Numbers
      if (decodedData.portnum === Portnums.PortNum.TEXT_MESSAGE_APP) {
        const text = Buffer.from(decodedData.payload).toString('utf-8');
        recordMessageObservation({
          regionId: region.id,
          transport: 'protobuf',
          topic,
          channelId: env.channelId || 'LongFast',
          from: fromNode,
          to: toNode,
          packetId,
          text,
          gatewayId: env.gatewayId
        });
      } else if (decodedData.portnum === Portnums.PortNum.NODEINFO_APP) {
        try {
          const user = fromBinary(Mesh.UserSchema, decodedData.payload);
          updateObservedNode(region.id, fromNode, {
            longName: user.longName,
            shortName: user.shortName,
            hwModel: user.hwModel
          });
        } catch {}
      } else if (decodedData.portnum === Portnums.PortNum.POSITION_APP) {
        try {
          const pos = fromBinary(Mesh.PositionSchema, decodedData.payload);
          if (pos.latitudeI && pos.longitudeI) {
            updateObservedNode(region.id, fromNode, {
              latitude: pos.latitudeI * 1e-7,
              longitude: pos.longitudeI * 1e-7
            });
          }
        } catch {}
      }
    } catch (err) {
      // Byte corruption fallback
    }
  }
}

// ======================================================
// MQTT CLIENT INITIALIZATION
// ======================================================

const mqttOptions = {
  clientId: CLIENT_ID,
  clean: true,
  connectTimeout: 10000,
  reconnectPeriod: 5000
};

if (MQTT_USER) mqttOptions.username = MQTT_USER;
if (MQTT_PASS) mqttOptions.password = MQTT_PASS;

console.log(`Connecting to MQTT broker at ${MQTT_URL}...`);
const client = mqtt.connect(MQTT_URL, mqttOptions);

client.on('connect', () => {
  mqttState = 'connected';
  console.log('MQTT Connected. Subscribing to public mesh topics...');
  client.subscribe(TOPICS, (err) => {
    if (err) console.error('Subscription error:', err);
    else console.log('Successfully subscribed to Meshtastic regions:', REGIONS.map(r => r.id).join(', '));
  });
});

client.on('message', (topic, payload) => {
  processMqttMessage(topic, payload);
});

client.on('error', (err) => {
  mqttState = 'error';
  lastError = err.message;
  console.error('MQTT Error:', err.message);
});

client.on('close', () => {
  mqttState = 'disconnected';
});

// ======================================================
// SERVER START
// ======================================================

server.listen(PORT, () => {
  console.log(`VirtualMesh Monitoring Server live on port ${PORT}`);
  console.log(`WebSocket endpoint available at ws://localhost:${PORT}/mesh`);
});

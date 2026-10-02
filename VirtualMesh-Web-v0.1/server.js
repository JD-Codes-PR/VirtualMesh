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
// VIRTUALMESH CONFIGURATION
// ======================================================

const PORT = process.env.PORT || 8080;

const MQTT_URL =
  process.env.MQTT_URL ||
  'mqtts://mqtt.meshtastic.org:8883';

const MQTT_USER = process.env.MQTT_USER || '';
const MQTT_PASS = process.env.MQTT_PASS || '';

// ======================================================
// PUBLIC REGIONS
// ======================================================

const REGIONS = [
  {
    id: 'PR',
    name: 'Puerto Rico',
    topic: 'msh/US/PR/#'
  },
  {
    id: 'FL',
    name: 'Florida',
    topic: 'msh/US/FL/#'
  },
  {
    id: 'TX',
    name: 'Texas',
    topic: 'msh/US/TX/#'
  }
];

const TOPICS =
  REGIONS.map(region => region.topic);

const CLIENT_ID =
  'virtualmesh-' +
  crypto.randomBytes(6).toString('hex');

// ======================================================
// DEFAULT LONGFAST KEY
// ======================================================

const LONGFAST_KEY = Buffer.from([
  0xd4, 0xf1, 0xbb, 0x3a,
  0x20, 0x29, 0x07, 0x59,
  0xf0, 0xbc, 0xff, 0xab,
  0xcf, 0x4e, 0x69, 0x01
]);

// ======================================================
// WEB SERVER
// ======================================================

const app = express();

app.use(express.static('public'));

const server = http.createServer(app);

const wss = new WebSocketServer({
  server,
  path: '/mesh'
});

let mqttState = 'disconnected';
let lastError = '';

const clients = new Set();

// ======================================================
// OBSERVED NODES
// ======================================================

const observedNodes = new Map();

// ======================================================
// DIAGNOSTICS
// ======================================================

const portStats = new Map();

const jsonStats = {
  packets: 0,
  valid: 0,
  invalid: 0,
  possibleText: 0
};

const directedStats = {
  packets: 0,
  skipped: 0
};

// ======================================================
// PORT STATISTICS
// ======================================================

function addPortStat(
  portnum,
  portName,
  regionId
) {

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
        UNKNOWN: 0
      }
    };

  existing.total++;

  if (
    existing.byRegion[regionId] ===
    undefined
  ) {
    existing.byRegion[regionId] = 0;
  }

  existing.byRegion[regionId]++;

  existing.portName = portName;

  portStats.set(
    key,
    existing
  );
}

function getPortStatsObject() {

  return Array.from(
    portStats.values()
  ).sort(
    (a, b) =>
      b.total - a.total
  );
}

// ======================================================
// REGION STATISTICS
// ======================================================

const regionStats = new Map();

for (const region of REGIONS) {

  regionStats.set(
    region.id,
    {
      packets: 0,
      bytes: 0,
      messages: 0
    }
  );
}

function addRegionTraffic(
  regionId,
  bytes
) {

  const stats =
    regionStats.get(regionId);

  if (!stats) {
    return;
  }

  stats.packets++;
  stats.bytes += bytes;
}

function addRegionMessage(
  regionId
) {

  const stats =
    regionStats.get(regionId);

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

    result[regionId] = {
      ...stats
    };
  }

  return result;
}

// ======================================================
// WEBSOCKET BROADCAST
// ======================================================

function broadcast(data) {

  const message =
    JSON.stringify(data);

  for (const ws of clients) {

    if (ws.readyState === 1) {
      ws.send(message);
    }
  }
}

// ======================================================
// REGION FROM MQTT TOPIC
// ======================================================

function getRegionFromTopic(
  topic
) {

  for (const region of REGIONS) {

    const prefix =
      region.topic.replace(
        '/#',
        '/'
      );

    if (
      topic.startsWith(prefix)
    ) {
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

function getMqttTopicType(
  topic
) {

  if (
    topic.includes('/2/e/')
  ) {
    return 'protobuf';
  }

  if (
    topic.includes('/2/json/')
  ) {
    return 'json';
  }

  if (
    topic.includes('/2/map/')
  ) {
    return 'map';
  }

  return 'other';
}

// ======================================================
// NODE ID
// ======================================================

function nodeIdToHex(
  nodeId
) {

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

function getPortNumName(
  portnum
) {

  try {

    for (
      const [name, value]
      of Object.entries(
        Portnums.PortNum
      )
    ) {

      if (
        value === portnum
      ) {
        return name;
      }
    }

  } catch {
    // fallback
  }

  return `PORT_${portnum}`;
}

// ======================================================
// OBSERVED NODE
// ======================================================

function updateObservedNode(
  regionId,
  nodeId,
  changes
) {

  const nodeHex =
    nodeIdToHex(nodeId);

  const key =
    `${regionId}:${nodeHex}`;

  const existing =
    observedNodes.get(key) || {
      region: regionId,
      nodeId: Number(nodeId),
      nodeHex,
      firstSeen:
        new Date().toISOString()
    };

  const updated = {
    ...existing,
    ...changes,
    region: regionId,
    nodeHex,
    lastSeen:
      new Date().toISOString()
  };

  observedNodes.set(
    key,
    updated
  );

  return updated;
}

function getObservedNode(
  regionId,
  nodeId
) {

  const nodeHex =
    nodeIdToHex(nodeId);

  return (
    observedNodes.get(
      `${regionId}:${nodeHex}`
    ) || null
  );
}

function getNodeCountsByRegion() {

  const counts = {};

  for (const region of REGIONS) {
    counts[region.id] = 0;
  }

  for (
    const node
    of observedNodes.values()
  ) {

    if (
      counts[node.region] !==
      undefined
    ) {
      counts[node.region]++;
    }
  }

  return counts;
}

// ======================================================
// LONGFAST AES-128-CTR
// ======================================================

function decryptLongFast(
  encrypted,
  packetId,
  fromNode
) {

  const nonce =
    Buffer.alloc(16);

  nonce.writeBigUInt64LE(
    BigInt(packetId),
    0
  );

  nonce.writeUInt32LE(
    Number(fromNode) >>> 0,
    8
  );

  const decipher =
    crypto.createDecipheriv(
      'aes-128-ctr',
      LONGFAST_KEY,
      nonce
    );

  decipher.setAutoPadding(false);

  return Buffer.concat([
    decipher.update(
      Buffer.from(encrypted)
    ),
    decipher.final()
  ]);
}

// ======================================================
// POSITION DECODER
// ======================================================

function decodePosition(
  payload
) {

  const position =
    fromBinary(
      Mesh.PositionSchema,
      payload
    );

  let latitude = null;
  let longitude = null;

  if (
    position.latitudeI !==
    undefined &&
    position.latitudeI !== null
  ) {

    latitude =
      position.latitudeI * 1e-7;
  }

  if (
    position.longitudeI !==
    undefined &&
    position.longitudeI !== null
  ) {

    longitude =
      position.longitudeI * 1e-7;
  }

  return {
    latitude,
    longitude,

    altitude:
      position.altitude ?? null,

    time:
      position.time || null,

    satsInView:
      position.satsInView || null,

    precisionBits:
      position.precisionBits || null,

    groundSpeed:
      position.groundSpeed || null,

    groundTrack:
      position.groundTrack || null
  };
}

// ======================================================
// NODEINFO DECODER
// ======================================================

function decodeNodeInfo(
  payload
) {

  const user =
    fromBinary(
      Mesh.UserSchema,
      payload
    );

  return {
    id:
      user.id || null,

    longName:
      user.longName || null,

    shortName:
      user.shortName || null,

    hwModel:
      user.hwModel ?? null,

    isLicensed:
      user.isLicensed ?? false,

    role:
      user.role ?? null
  };
}

// ======================================================
// TELEMETRY DECODER
// ======================================================

function decodeTelemetry(
  payload
) {

  const telemetry =
    fromBinary(
      Telemetry.TelemetrySchema,
      payload
    );

  const variant =
    telemetry.variant;

  const result = {
    type: 'telemetry',

    time:
      telemetry.time || null,

    telemetryType:
      variant?.case || 'unknown',

    metrics: null
  };

  if (!variant?.case) {
    return result;
  }

  const value =
    variant.value;

  if (
    variant.case ===
    'deviceMetrics'
  ) {

    result.metrics = {
      batteryLevel:
        value.batteryLevel ?? null,

      voltage:
        value.voltage ?? null,

      channelUtilization:
        value.channelUtilization ?? null,

      airUtilTx:
        value.airUtilTx ?? null,

      uptimeSeconds:
        value.uptimeSeconds ?? null
    };
  }

  else if (
    variant.case ===
    'environmentMetrics'
  ) {

    result.metrics = {
      temperature:
        value.temperature ?? null,

      relativeHumidity:
        value.relativeHumidity ?? null,

      barometricPressure:
        value.barometricPressure ?? null,

      gasResistance:
        value.gasResistance ?? null,

      voltage:
        value.voltage ?? null,

      current:
        value.current ?? null
    };
  }

  else {

    result.metrics = {
      detected: true
    };
  }

  return result;
}

// ======================================================
// APPLICATION DECODER
// ======================================================

function decodeApplicationPayload(
  portName,
  payload
) {

  if (!payload) {
    return null;
  }

  // --------------------------------------------------
  // NORMAL TEXT
  // --------------------------------------------------

  if (
    portName ===
    'TEXT_MESSAGE_APP'
  ) {

    return {
      type: 'text',

      text:
        Buffer.from(payload)
          .toString('utf8')
    };
  }

  // --------------------------------------------------
  // COMPRESSED TEXT
  // Detection only
  // --------------------------------------------------

  if (
    portName ===
    'TEXT_MESSAGE_COMPRESSED_APP'
  ) {

    return {
      type:
        'compressed_text',

      compression:
        'Unishox2',

      bytes:
        payload.length,

      hex:
        Buffer.from(payload)
          .toString('hex'),

      base64:
        Buffer.from(payload)
          .toString('base64')
    };
  }

  // --------------------------------------------------
  // POSITION
  // --------------------------------------------------

  if (
    portName ===
    'POSITION_APP'
  ) {

    return {
      type: 'position',
      ...decodePosition(payload)
    };
  }

  // --------------------------------------------------
  // NODEINFO
  // --------------------------------------------------

  if (
    portName ===
    'NODEINFO_APP'
  ) {

    return {
      type: 'nodeinfo',
      ...decodeNodeInfo(payload)
    };
  }

  // --------------------------------------------------
  // TELEMETRY
  // --------------------------------------------------

  if (
    portName ===
    'TELEMETRY_APP'
  ) {

    return decodeTelemetry(
      payload
    );
  }

  return {
    type: 'unhandled',
    portName,
    bytes:
      payload.length
  };
}

// ======================================================
// JSON INSPECTION
// Broadcast only
// ======================================================

function inspectJsonPacket(
  region,
  topic,
  payload
) {

  jsonStats.packets++;

  const raw =
    Buffer.from(payload)
      .toString('utf8');

  console.log(
    'MQTT transport: JSON'
  );

  console.log(
    'JSON bytes:',
    payload.length
  );

  try {

    const obj =
      JSON.parse(raw);

    jsonStats.valid++;

    const to =
      obj.to !== undefined
        ? Number(obj.to)
        : null;

    const isBroadcast =
      to === null ||
      to === 0xffffffff ||
      to === 4294967295;

    // --------------------------------------------------
    // DO NOT INSPECT DIRECTED NODE-TO-NODE JSON
    // --------------------------------------------------

    if (!isBroadcast) {

      directedStats.packets++;
      directedStats.skipped++;

      console.log(
        'JSON packet: DIRECTED'
      );

      console.log(
        'Application inspection: SKIPPED'
      );

      console.log(
        'Reason: Node-to-node packet'
      );

      return {
        valid: true,
        directed: true,
        skipped: true
      };
    }

    const type =
      String(
        obj.type || ''
      ).toLowerCase();

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
      type.includes(
        'text_message'
      ) ||
      portnum === 1 ||
      portnum === 7;

    if (possibleText) {

      jsonStats.possibleText++;

      console.log(
        '==================================='
      );

      console.log(
        'POSSIBLE JSON TEXT MESSAGE'
      );

      console.log(
        'Region:',
        region.id
      );

      console.log(
        'Topic:',
        topic
      );

      console.log(
        'JSON:',
        JSON.stringify(obj)
      );

      console.log(
        '==================================='
      );
    }

    else {

      console.log(
        'JSON packet: OK'
      );

      console.log(
        'JSON keys:',
        Object.keys(obj)
          .join(', ') ||
        '(none)'
      );
    }

    return {
      valid: true,
      directed: false,
      possibleText,
      data: obj
    };

  }

  catch (err) {

    jsonStats.invalid++;

    console.log(
      'JSON parse: FAILED'
    );

    console.log(
      'Reason:',
      err.message
    );

    return {
      valid: false,
      error:
        err.message
    };
  }
}

// ======================================================
// MQTT OPTIONS
// ======================================================

const opts = {
  protocolVersion: 4,

  clientId:
    CLIENT_ID,

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

console.log(
  '==================================='
);

console.log('VirtualMesh');

console.log(
  'Public Broadcast Receiver'
);

console.log(
  '==================================='
);

console.log(
  'Broker:',
  MQTT_URL
);

console.log(
  'Client ID:',
  CLIENT_ID
);

console.log('Regions:');

for (const region of REGIONS) {

  console.log(
    ` - ${region.id} | ${region.name} | ${region.topic}`
  );
}

console.log(
  'Mode: READ ONLY'
);

console.log(
  'Traffic mode: BROADCAST APPLICATION DATA'
);

console.log(
  'Directed node-to-node application decoding: DISABLED'
);

console.log(
  'LongFast decoder: ENABLED'
);

console.log(
  'TEXT_MESSAGE_APP decoder: ENABLED'
);

console.log(
  'TEXT_MESSAGE_COMPRESSED_APP detector: ENABLED'
);

console.log(
  'POSITION_APP decoder: ENABLED'
);

console.log(
  'NODEINFO_APP decoder: ENABLED'
);

console.log(
  'TELEMETRY_APP decoder: ENABLED'
);

console.log(
  'Non-LongFast decryption: DISABLED'
);

console.log(
  'MQTT protocol: 3.1.1'
);

console.log(
  '==================================='
);

// ======================================================
// MQTT CLIENT
// ======================================================

const mc =
  mqtt.connect(
    MQTT_URL,
    opts
  );

// ======================================================
// MQTT CONNECT
// ======================================================

mc.on(
  'connect',
  connack => {

    console.log('');
    console.log(
      'MQTT CONNECTED'
    );

    console.log(
      'CONNACK:',
      JSON.stringify(connack)
    );

    mqttState =
      'connected';

    lastError = '';

    console.log(
      'SUBSCRIBING TO:'
    );

    for (
      const topic
      of TOPICS
    ) {

      console.log(
        ' -',
        topic
      );
    }

    mc.subscribe(
      TOPICS,
      {
        qos: 0
      },
      (err, granted) => {

        if (err) {

          console.error(
            'SUBSCRIBE ERROR:',
            err.message
          );

          return;
        }

        console.log(
          'SUBACK:',
          JSON.stringify(granted)
        );
      }
    );

    broadcast({
      type: 'status',
      mqttState,
      lastError,
      topics: TOPICS,
      regions: REGIONS
    });
  }
);

// ======================================================
// MQTT MESSAGE
// ======================================================

mc.on(
  'message',
  (topic, payload) => {

    const region =
      getRegionFromTopic(
        topic
      );

    const mqttTopicType =
      getMqttTopicType(
        topic
      );

    addRegionTraffic(
      region.id,
      payload.length
    );

    console.log('');
    console.log(
      '-----------------------------------'
    );

    console.log(
      `REGION: ${region.id} (${region.name})`
    );

    console.log(
      'MQTT PACKET:',
      topic,
      `${payload.length} bytes`
    );

    // ==================================================
    // JSON
    // ==================================================

    if (
      mqttTopicType ===
      'json'
    ) {

      const jsonResult =
        inspectJsonPacket(
          region,
          topic,
          payload
        );

      broadcast({
        type: 'packet',

        region:
          region.id,

        regionName:
          region.name,

        topic,

        topicType:
          mqttTopicType,

        receivedAt:
          new Date()
            .toISOString(),

        bytes:
          payload.length,

        decoded:
          jsonResult
      });

      return;
    }

    // ==================================================
    // MAP / OTHER
    // ==================================================

    if (
      mqttTopicType !==
      'protobuf'
    ) {

      console.log(
        'MQTT transport:',
        mqttTopicType.toUpperCase()
      );

      console.log(
        'ServiceEnvelope decode: SKIPPED'
      );

      console.log(
        'Reason: MQTT topic is not /2/e/'
      );

      return;
    }

    let result = null;

    try {

      // ================================================
      // SERVICE ENVELOPE
      // ================================================

      const envelope =
        fromBinary(
          Mqtt.ServiceEnvelopeSchema,
          payload
        );

      console.log(
        'SERVICE ENVELOPE: OK'
      );

      console.log(
        'Gateway ID:',
        envelope.gatewayId ||
        '(none)'
      );

      console.log(
        'Channel ID:',
        envelope.channelId ||
        '(none)'
      );

      const packet =
        envelope.packet;

      if (!packet) {

        console.log(
          'MESH PACKET: MISSING'
        );

        return;
      }

      console.log(
        'MESH PACKET: OK'
      );

      const fromHex =
        nodeIdToHex(
          packet.from
        );

      const isBroadcast =
        Number(packet.to) ===
        0xffffffff;

      const toHex =
        isBroadcast
          ? null
          : nodeIdToHex(
              packet.to
            );

      console.log(
        'From:',
        packet.from,
        `(${fromHex})`
      );

      console.log(
        'To:',
        packet.to,
        isBroadcast
          ? '(BROADCAST)'
          : `(${toHex})`
      );

      console.log(
        'Packet ID:',
        packet.id
      );

      console.log(
        'Channel:',
        packet.channel
      );

      console.log(
        'Hop Limit:',
        packet.hopLimit
      );

      console.log(
        'Hop Start:',
        packet.hopStart
      );

      // ================================================
      // ALWAYS RECORD THAT THE NODE WAS SEEN
      // ================================================

      updateObservedNode(
        region.id,
        packet.from,
        {
          lastGateway:
            envelope.gatewayId ||
            null,

          channelId:
            envelope.channelId ||
            null
        }
      );

      // ================================================
      // DIRECTED NODE-TO-NODE
      // DO NOT DECRYPT / DO NOT DECODE
      // ================================================

      if (!isBroadcast) {

        directedStats.packets++;
        directedStats.skipped++;

        console.log(
          'DIRECTED NODE-TO-NODE PACKET'
        );

        console.log(
          'Application decode: SKIPPED'
        );

        console.log(
          'Decryption: SKIPPED'
        );

        console.log(
          'Reason: VirtualMesh is processing broadcast application traffic only'
        );

        const nodeCounts =
          getNodeCountsByRegion();

        console.log(
          'Observed nodes:',
          observedNodes.size
        );

        console.log(
          'By region:',
          JSON.stringify(
            nodeCounts
          )
        );

        result = {
          serviceEnvelope: true,

          topicType:
            mqttTopicType,

          region:
            region.id,

          regionName:
            region.name,

          gatewayId:
            envelope.gatewayId ||
            null,

          channelId:
            envelope.channelId ||
            null,

          from:
            packet.from,

          fromHex,

          to:
            packet.to,

          toHex,

          broadcast:
            false,

          directed:
            true,

          applicationDecode:
            'SKIPPED',

          decryption:
            'SKIPPED',

          reason:
            'Node-to-node packet',

          observedNodeCount:
            observedNodes.size,

          nodeCounts
        };

        broadcast({
          type: 'packet',

          region:
            region.id,

          regionName:
            region.name,

          topic,

          topicType:
            mqttTopicType,

          receivedAt:
            new Date()
              .toISOString(),

          bytes:
            payload.length,

          decoded:
            result
        });

        return;
      }

      // ================================================
      // FROM THIS POINT: BROADCAST ONLY
      // ================================================

      const variant =
        packet.payloadVariant;

      let payloadType =
        'UNKNOWN';

      let portnum = null;
      let portName = null;

      let encryptedBytes = 0;
      let decodedBytes = 0;

      let decryptionSuccess =
        false;

      let decryptionSkipped =
        false;

      let application = null;

      // ================================================
      // ALREADY DECODED BROADCAST
      // ================================================

      if (
        variant?.case ===
        'decoded'
      ) {

        payloadType =
          'DECODED';

        const data =
          variant.value;

        portnum =
          data.portnum;

        portName =
          getPortNumName(
            portnum
          );

        decodedBytes =
          data.payload?.length ||
          0;

        console.log(
          'Payload: DECODED'
        );

        console.log(
          'PortNum:',
          portnum,
          `(${portName})`
        );

        console.log(
          'Application payload bytes:',
          decodedBytes
        );

        addPortStat(
          portnum,
          portName,
          region.id
        );

        console.log(
          'PORT STATS:',
          JSON.stringify(
            getPortStatsObject()
          )
        );

        try {

          application =
            decodeApplicationPayload(
              portName,
              data.payload
            );

        }

        catch (appError) {

          console.log(
            'APPLICATION DECODE FAILED:',
            appError.message
          );
        }
      }

      // ================================================
      // ENCRYPTED LONGFAST BROADCAST
      // ================================================

      else if (
        variant?.case ===
          'encrypted' &&
        envelope.channelId ===
          'LongFast'
      ) {

        payloadType =
          'ENCRYPTED';

        encryptedBytes =
          variant.value?.length ||
          0;

        console.log(
          'Payload: ENCRYPTED'
        );

        console.log(
          'Encrypted bytes:',
          encryptedBytes
        );

        try {

          const plaintext =
            decryptLongFast(
              variant.value,
              packet.id,
              packet.from
            );

          console.log(
            'AES-CTR decrypt: OK'
          );

          console.log(
            'Plaintext bytes:',
            plaintext.length
          );

          const data =
            fromBinary(
              Mesh.DataSchema,
              plaintext
            );

          decryptionSuccess =
            true;

          payloadType =
            'DECRYPTED';

          portnum =
            data.portnum;

          portName =
            getPortNumName(
              portnum
            );

          decodedBytes =
            data.payload?.length ||
            0;

          console.log(
            'DATA PROTOBUF: OK'
          );

          console.log(
            'PortNum:',
            portnum,
            `(${portName})`
          );

          console.log(
            'Application payload bytes:',
            decodedBytes
          );

          console.log(
            'Want response:',
            data.wantResponse
          );

          addPortStat(
            portnum,
            portName,
            region.id
          );

          console.log(
            'PORT STATS:',
            JSON.stringify(
              getPortStatsObject()
            )
          );

          try {

            application =
              decodeApplicationPayload(
                portName,
                data.payload
              );

          }

          catch (appError) {

            console.log(
              'APPLICATION DECODE FAILED:',
              appError.message
            );
          }
        }

        catch (decryptError) {

          console.log(
            'LONGFAST DECRYPT FAILED:',
            decryptError.message
          );
        }
      }

      // ================================================
      // NON-LONGFAST
      // ================================================

      else if (
        variant?.case ===
        'encrypted'
      ) {

        payloadType =
          'ENCRYPTED';

        encryptedBytes =
          variant.value?.length ||
          0;

        decryptionSkipped =
          true;

        console.log(
          'Payload: ENCRYPTED'
        );

        console.log(
          'Encrypted bytes:',
          encryptedBytes
        );

        console.log(
          'Decryption: SKIPPED'
        );

        console.log(
          'Reason: Channel is not LongFast'
        );
      }

      else {

        console.log(
          'Payload: NONE / UNKNOWN'
        );

        console.log(
          'payloadVariant case:',
          variant?.case ||
          '(none)'
        );
      }

      // ================================================
      // TEXT MESSAGE
      // ================================================

      if (
        application?.type ===
        'text'
      ) {

        addRegionMessage(
          region.id
        );

        console.log(
          '==================================='
        );

        console.log(
          'TEXT MESSAGE RECEIVED'
        );

        console.log(
          'Region:',
          region.id
        );

        console.log(
          'From:',
          fromHex
        );

        console.log(
          'To: BROADCAST'
        );

        console.log(
          'Message:',
          application.text
        );

        console.log(
          '==================================='
        );
      }

      // ================================================
      // COMPRESSED TEXT
      // ================================================

      if (
        application?.type ===
        'compressed_text'
      ) {

        addRegionMessage(
          region.id
        );

        console.log(
          '==================================='
        );

        console.log(
          'COMPRESSED TEXT MESSAGE DETECTED'
        );

        console.log(
          'Region:',
          region.id
        );

        console.log(
          'From:',
          fromHex
        );

        console.log(
          'To: BROADCAST'
        );

        console.log(
          'Compression:',
          application.compression
        );

        console.log(
          'Compressed bytes:',
          application.bytes
        );

        console.log(
          'Payload HEX:',
          application.hex
        );

        console.log(
          'Payload Base64:',
          application.base64
        );

        console.log(
          '==================================='
        );
      }

      // ================================================
      // POSITION
      // ================================================

      if (
        application?.type ===
        'position'
      ) {

        console.log(
          'POSITION APP: OK'
        );

        console.log(
          'Latitude:',
          application.latitude
        );

        console.log(
          'Longitude:',
          application.longitude
        );

        console.log(
          'Altitude:',
          application.altitude
        );

        console.log(
          'Satellites:',
          application.satsInView
        );

        console.log(
          'Precision bits:',
          application.precisionBits
        );

        updateObservedNode(
          region.id,
          packet.from,
          {
            position:
              application
          }
        );
      }

      // ================================================
      // NODEINFO
      // ================================================

      if (
        application?.type ===
        'nodeinfo'
      ) {

        console.log(
          'NODEINFO APP: OK'
        );

        console.log(
          'Node ID:',
          application.id
        );

        console.log(
          'Long Name:',
          application.longName
        );

        console.log(
          'Short Name:',
          application.shortName
        );

        console.log(
          'Hardware Model:',
          application.hwModel
        );

        console.log(
          'Role:',
          application.role
        );

        updateObservedNode(
          region.id,
          packet.from,
          {
            user:
              application
          }
        );
      }

      // ================================================
      // TELEMETRY
      // ================================================

      if (
        application?.type ===
        'telemetry'
      ) {

        console.log(
          'TELEMETRY APP: OK'
        );

        console.log(
          'Telemetry type:',
          application.telemetryType
        );

        console.log(
          'Telemetry time:',
          application.time
        );

        if (
          application.telemetryType ===
          'deviceMetrics'
        ) {

          console.log(
            'Battery:',
            application.metrics
              ?.batteryLevel
          );

          console.log(
            'Voltage:',
            application.metrics
              ?.voltage
          );

          console.log(
            'Channel utilization:',
            application.metrics
              ?.channelUtilization
          );

          console.log(
            'Air utilization TX:',
            application.metrics
              ?.airUtilTx
          );

          console.log(
            'Uptime seconds:',
            application.metrics
              ?.uptimeSeconds
          );
        }

        else if (
          application.telemetryType ===
          'environmentMetrics'
        ) {

          console.log(
            'Temperature:',
            application.metrics
              ?.temperature
          );

          console.log(
            'Humidity:',
            application.metrics
              ?.relativeHumidity
          );

          console.log(
            'Pressure:',
            application.metrics
              ?.barometricPressure
          );
        }

        updateObservedNode(
          region.id,
          packet.from,
          {
            telemetry:
              application
          }
        );
      }

      // ================================================
      // NODE COUNTS
      // ================================================

      const nodeCounts =
        getNodeCountsByRegion();

      console.log(
        'Observed nodes:',
        observedNodes.size
      );

      console.log(
        'By region:',
        JSON.stringify(
          nodeCounts
        )
      );

      // ================================================
      // RESULT
      // ================================================

      result = {
        serviceEnvelope: true,

        topicType:
          mqttTopicType,

        region:
          region.id,

        regionName:
          region.name,

        gatewayId:
          envelope.gatewayId ||
          null,

        channelId:
          envelope.channelId ||
          null,

        from:
          packet.from,

        fromHex,

        to:
          packet.to,

        toHex,

        broadcast:
          true,

        directed:
          false,

        id:
          packet.id,

        channel:
          packet.channel,

        hopLimit:
          packet.hopLimit,

        hopStart:
          packet.hopStart,

        payloadType,

        encryptedBytes,
        decodedBytes,
        decryptionSuccess,
        decryptionSkipped,

        portnum,
        portName,
        application,

        observedNode:
          getObservedNode(
            region.id,
            packet.from
          ),

        observedNodeCount:
          observedNodes.size,

        nodeCounts,

        regionStats:
          getRegionStatsObject(),

        portStats:
          getPortStatsObject(),

        jsonStats: {
          ...jsonStats
        },

        directedStats: {
          ...directedStats
        }
      };
    }

    catch (err) {

      console.error(
        'SERVICE ENVELOPE DECODE ERROR:',
        err.message
      );
    }

    // ==================================================
    // SEND TO BROWSER
    // ==================================================

    broadcast({
      type: 'packet',

      region:
        region.id,

      regionName:
        region.name,

      topic,

      topicType:
        mqttTopicType,

      receivedAt:
        new Date()
          .toISOString(),

      bytes:
        payload.length,

      decoded:
        result
    });
  }
);

// ======================================================
// MQTT CONNECTION DIAGNOSTICS
// ======================================================

mc.on(
  'reconnect',
  () => {

    console.log(
      'MQTT RECONNECTING'
    );

    mqttState =
      'reconnecting';
  }
);

mc.on(
  'offline',
  () => {

    console.log(
      'MQTT OFFLINE'
    );

    mqttState =
      'offline';
  }
);

mc.on(
  'close',
  () => {

    console.log(
      'MQTT CONNECTION CLOSED'
    );
  }
);

mc.on(
  'disconnect',
  packet => {

    console.log(
      'MQTT DISCONNECT:',
      JSON.stringify(packet)
    );
  }
);

mc.on(
  'error',
  err => {

    lastError =
      err.message;

    console.error(
      'MQTT ERROR:',
      err.message
    );
  }
);

// ======================================================
// WEBSOCKET
// ======================================================

wss.on(
  'connection',
  ws => {

    clients.add(ws);

    ws.send(
      JSON.stringify({
        type: 'status',

        mqttState,
        lastError,

        topics:
          TOPICS,

        regions:
          REGIONS,

        mode:
          'READ_ONLY',

        trafficMode:
          'BROADCAST_ONLY',

        observedNodeCount:
          observedNodes.size,

        nodeCounts:
          getNodeCountsByRegion(),

        regionStats:
          getRegionStatsObject(),

        portStats:
          getPortStatsObject(),

        jsonStats: {
          ...jsonStats
        },

        directedStats: {
          ...directedStats
        }
      })
    );

    ws.send(
      JSON.stringify({
        type: 'nodes',

        nodes:
          Array.from(
            observedNodes.values()
          ),

        nodeCounts:
          getNodeCountsByRegion()
      })
    );

    ws.on(
      'close',
      () => {
        clients.delete(ws);
      }
    );
  }
);

// ======================================================
// API - NODES
// ======================================================

app.get(
  '/api/nodes',
  (req, res) => {

    const requestedRegion =
      String(
        req.query.region || ''
      ).toUpperCase();

    let nodes =
      Array.from(
        observedNodes.values()
      );

    if (requestedRegion) {

      nodes =
        nodes.filter(
          node =>
            node.region ===
            requestedRegion
        );
    }

    res.json({
      mode:
        'READ_ONLY',

      trafficMode:
        'BROADCAST_ONLY',

      count:
        nodes.length,

      totalCount:
        observedNodes.size,

      nodeCounts:
        getNodeCountsByRegion(),

      regionStats:
        getRegionStatsObject(),

      nodes
    });
  }
);

// ======================================================
// API - DIAGNOSTICS
// ======================================================

app.get(
  '/api/diagnostics',
  (req, res) => {

    res.json({
      service:
        'VirtualMesh',

      mode:
        'READ_ONLY',

      trafficMode:
        'BROADCAST_ONLY',

      observedNodes:
        observedNodes.size,

      nodeCounts:
        getNodeCountsByRegion(),

      portStats:
        getPortStatsObject(),

      jsonStats: {
        ...jsonStats
      },

      directedTraffic: {
        decoding:
          false,

        packetsSeen:
          directedStats.packets,

        packetsSkipped:
          directedStats.skipped
      },

      compressedText: {
        detection:
          true,

        compression:
          'Unishox2',

        decompression:
          false
      }
    });
  }
);

// ======================================================
// API - STATUS
// ======================================================

app.get(
  '/api/status',
  (req, res) => {

    res.json({
      service:
        'VirtualMesh',

      mqttState,

      topics:
        TOPICS,

      regions:
        REGIONS,

      mode:
        'READ_ONLY',

      trafficMode:
        'BROADCAST_ONLY',

      observedNodes:
        observedNodes.size,

      nodeCounts:
        getNodeCountsByRegion(),

      regionStats:
        getRegionStatsObject(),

      portStats:
        getPortStatsObject(),

      jsonStats: {
        ...jsonStats
      },

      directedTraffic: {
        decoding:
          false,

        packetsSeen:
          directedStats.packets,

        packetsSkipped:
          directedStats.skipped
      },

      mqttTransport: {
        protobuf: true,
        json: true,
        map: false
      },

      decoders: {
        longFast: true,
        position: true,
        nodeInfo: true,
        telemetry: true,
        text: true,

        compressedTextDetection:
          true,

        compressedTextDecompression:
          false,

        directedNodeToNode:
          false
      }
    });
  }
);

// ======================================================
// START SERVER
// ======================================================

server.listen(
  PORT,
  () => {

    console.log(
      `VirtualMesh Web listening on ${PORT}`
    );

    console.log(
      'MQTT mode: READ ONLY'
    );

    console.log(
      'Traffic mode: BROADCAST ONLY'
    );

    console.log(
      'Directed node-to-node decode: DISABLED'
    );

    console.log(
      'Listening regions:',
      TOPICS.join(', ')
    );

    console.log(
      'Diagnostics endpoint: /api/diagnostics'
    );
  }
);

import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import mqtt from 'mqtt';
import crypto from 'crypto';
import { fromBinary } from '@bufbuild/protobuf';
import { Mqtt, Mesh, Portnums } from '@meshtastic/protobufs';

// ======================================================
// VIRTUALMESH CONFIGURATION
// ======================================================

const PORT = process.env.PORT || 8080;

const MQTT_URL =
  process.env.MQTT_URL ||
  'mqtts://mqtt.meshtastic.org:8883';

const MQTT_USER =
  process.env.MQTT_USER || '';

const MQTT_PASS =
  process.env.MQTT_PASS || '';

const TOPIC =
  process.env.MQTT_TOPIC ||
  'msh/US/PR/#';

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

const server =
  http.createServer(app);

const wss =
  new WebSocketServer({
    server,
    path: '/mesh'
  });

let mqttState = 'disconnected';
let lastError = '';

const clients = new Set();

// ======================================================
// OBSERVED NODES DATABASE
// Memory only - no persistence yet
// ======================================================

const observedNodes = new Map();

// ======================================================
// BROADCAST TO BROWSER
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
// NODE ID -> !xxxxxxxx
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
      of Object.entries(
        Portnums.PortNum
      )
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
// UPDATE OBSERVED NODE
// ======================================================

function updateObservedNode(
  nodeId,
  changes
) {

  const nodeHex =
    nodeIdToHex(nodeId);

  const existing =
    observedNodes.get(nodeHex) || {
      nodeId: Number(nodeId),
      nodeHex,
      firstSeen:
        new Date().toISOString()
    };

  const updated = {
    ...existing,
    ...changes,
    lastSeen:
      new Date().toISOString()
  };

  observedNodes.set(
    nodeHex,
    updated
  );

  return updated;

}

// ======================================================
// LONGFAST AES-128-CTR DECRYPTION
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

function decodePosition(payload) {

  const position =
    fromBinary(
      Mesh.PositionSchema,
      payload
    );

  let latitude = null;
  let longitude = null;

  if (
    position.latitudeI !== undefined &&
    position.latitudeI !== null
  ) {

    latitude =
      position.latitudeI * 1e-7;

  }

  if (
    position.longitudeI !== undefined &&
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

    locationSource:
      position.locationSource ?? null,

    altitudeSource:
      position.altitudeSource ?? null,

    timestamp:
      position.timestamp || null,

    timestampMillisAdjust:
      position.timestampMillisAdjust || null,

    altitudeHae:
      position.altitudeHae ?? null,

    altitudeGeoidalSeparation:
      position.altitudeGeoidalSeparation ?? null,

    pdop:
      position.pdop || null,

    hdop:
      position.hdop || null,

    vdop:
      position.vdop || null,

    gpsAccuracy:
      position.gpsAccuracy || null,

    groundSpeed:
      position.groundSpeed || null,

    groundTrack:
      position.groundTrack || null,

    fixQuality:
      position.fixQuality || null,

    fixType:
      position.fixType || null,

    satsInView:
      position.satsInView || null,

    precisionBits:
      position.precisionBits || null

  };

}

// ======================================================
// NODEINFO / USER DECODER
// ======================================================

function decodeNodeInfo(payload) {

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
      user.role ?? null,

    publicKey:
      user.publicKey &&
      user.publicKey.length > 0
        ? Buffer.from(
            user.publicKey
          ).toString('base64')
        : null

  };

}

// ======================================================
// APPLICATION PAYLOAD DECODER
// ======================================================

function decodeApplicationPayload(
  portName,
  payload
) {

  if (!payload) {
    return null;
  }

  // --------------------------------------------------
  // TEXT
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
  // NODE INFO
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

  return {
    type: 'unhandled',
    bytes:
      payload.length
  };

}

// ======================================================
// MQTT OPTIONS
// ======================================================

const opts = {

  protocolVersion: 4,

  clientId:
    CLIENT_ID,

  reconnectPeriod:
    5000,

  connectTimeout:
    15000,

  keepalive:
    60,

  clean:
    true

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
  'Meshtastic MQTT Receiver'
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

console.log(
  'Topic:',
  TOPIC
);

console.log(
  'Mode: READ ONLY'
);

console.log(
  'LongFast decoder: ENABLED'
);

console.log(
  'POSITION_APP decoder: ENABLED'
);

console.log(
  'NODEINFO_APP decoder: ENABLED'
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
  (connack) => {

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

    lastError =
      '';

    console.log(
      'SUBSCRIBE ->',
      TOPIC
    );

    mc.subscribe(
      TOPIC,
      { qos: 0 },
      (err, granted) => {

        if (err) {

          lastError =
            err.message;

          console.error(
            'MQTT SUBSCRIBE ERROR:',
            err.message
          );

        } else {

          console.log(
            'SUBACK:',
            JSON.stringify(granted)
          );

        }

        broadcast({
          type: 'status',
          mqttState,
          lastError,
          topic: TOPIC
        });

      }
    );

  }
);

// ======================================================
// MQTT PACKET RECEIVER
// ======================================================

mc.on(
  'message',
  (topic, payload) => {

    console.log('');

    console.log(
      '-----------------------------------'
    );

    console.log(
      'MQTT PACKET:',
      topic,
      payload.length,
      'bytes'
    );

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

      // ================================================
      // PACKET METADATA
      // ================================================

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

      // Record that node was seen
      updateObservedNode(
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
      // PAYLOAD
      // ================================================

      const variant =
        packet.payloadVariant;

      let payloadType =
        'UNKNOWN';

      let portnum =
        null;

      let portName =
        null;

      let encryptedBytes =
        0;

      let decodedBytes =
        0;

      let decryptionSuccess =
        false;

      let application =
        null;

      // ================================================
      // ALREADY DECODED DATA
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
          data.payload?.length || 0;

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

        try {

          application =
            decodeApplicationPayload(
              portName,
              data.payload
            );

        } catch (appError) {

          console.log(
            'APPLICATION DECODE FAILED:',
            appError.message
          );

        }

      }

      // ================================================
      // ENCRYPTED LONGFAST
      // ================================================

      else if (
        variant?.case ===
        'encrypted'
      ) {

        payloadType =
          'ENCRYPTED';

        encryptedBytes =
          variant.value?.length || 0;

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
            data.payload?.length || 0;

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

          console.log(
            'Request ID:',
            data.requestId
          );

          console.log(
            'Reply ID:',
            data.replyId
          );

          try {

            application =
              decodeApplicationPayload(
                portName,
                data.payload
              );

          } catch (appError) {

            console.log(
              'APPLICATION DECODE FAILED:',
              appError.message
            );

          }

        } catch (decryptError) {

          console.log(
            'LONGFAST DECRYPT FAILED:',
            decryptError.message
          );

        }

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
      // APPLICATION OUTPUT
      // ================================================

      if (
        application?.type ===
        'text'
      ) {

        console.log(
          'TEXT MESSAGE:',
          application.text
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

        console.log(
          'Licensed:',
          application.isLicensed
        );

        updateObservedNode(
          packet.from,
          {
            user:
              application
          }
        );

      }

      console.log(
        'Observed nodes:',
        observedNodes.size
      );

      // ================================================
      // RESULT FOR BROWSER
      // ================================================

      result = {

        serviceEnvelope:
          true,

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
          isBroadcast,

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

        portnum,

        portName,

        application,

        observedNode:
          observedNodes.get(
            fromHex
          ) || null,

        observedNodeCount:
          observedNodes.size

      };

    } catch (err) {

      console.error(
        'SERVICE ENVELOPE DECODE ERROR:',
        err.message
      );

    }

    // ================================================
    // SEND TO BROWSER
    // ================================================

    broadcast({

      type:
        'packet',

      topic,

      receivedAt:
        new Date()
          .toISOString(),

      bytes:
        payload.length,

      base64:
        payload.toString(
          'base64'
        ),

      decoded:
        result

    });

  }
);

// ======================================================
// MQTT DIAGNOSTICS
// ======================================================

mc.on(
  'reconnect',
  () => {

    console.log(
      'MQTT RECONNECTING'
    );

    mqttState =
      'reconnecting';

    broadcast({
      type: 'status',
      mqttState,
      lastError,
      topic: TOPIC
    });

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

    broadcast({
      type: 'status',
      mqttState,
      lastError,
      topic: TOPIC
    });

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
  (packet) => {

    console.log(
      'MQTT DISCONNECT:',
      JSON.stringify(
        packet
      )
    );

  }
);

mc.on(
  'error',
  (err) => {

    lastError =
      err.message;

    console.error(
      'MQTT ERROR:',
      err.message
    );

    broadcast({
      type: 'status',
      mqttState,
      lastError,
      topic: TOPIC
    });

  }
);

// ======================================================
// BROWSER <-> VIRTUALMESH
// ======================================================

wss.on(
  'connection',
  (ws) => {

    clients.add(ws);

    ws.send(
      JSON.stringify({

        type:
          'status',

        mqttState,

        lastError,

        topic:
          TOPIC,

        mode:
          'READ_ONLY',

        observedNodeCount:
          observedNodes.size

      })
    );

    ws.send(
      JSON.stringify({

        type:
          'nodes',

        nodes:
          Array.from(
            observedNodes.values()
          )

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
// HTTP API - OBSERVED NODES
// ======================================================

app.get(
  '/api/nodes',
  (req, res) => {

    res.json({

      mode:
        'READ_ONLY',

      count:
        observedNodes.size,

      nodes:
        Array.from(
          observedNodes.values()
        )

    });

  }
);

// ======================================================
// HEALTH
// ======================================================

app.get(
  '/api/status',
  (req, res) => {

    res.json({

      service:
        'VirtualMesh',

      mqttState,

      topic:
        TOPIC,

      mode:
        'READ_ONLY',

      observedNodes:
        observedNodes.size,

      decoders: {
        longFast:
          true,
        position:
          true,
        nodeInfo:
          true,
        text:
          true
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
      `VirtualMesh Web listening on ${PORT}; MQTT read-only topic ${TOPIC}`
    );

  }
);

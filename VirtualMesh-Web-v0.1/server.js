import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import mqtt from 'mqtt';
import crypto from 'crypto';
import { ServiceEnvelope } from '@meshtastic/protobufs';

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
  'msh/US/PR/2/e/LongFast/+';

// Identificador MQTT anónimo y único para esta instancia.
const CLIENT_ID =
  'virtualmesh-' + crypto.randomBytes(6).toString('hex');

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

function broadcast(data) {
  const message = JSON.stringify(data);

  for (const ws of clients) {
    if (ws.readyState === 1) {
      ws.send(message);
    }
  }
}

const opts = {
  protocolVersion: 4, // MQTT 3.1.1
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

console.log('-----------------------------------');
console.log('VirtualMesh MQTT diagnostics');
console.log('Broker:', MQTT_URL);
console.log('Client ID:', CLIENT_ID);
console.log('Topic:', TOPIC);
console.log('Mode: READ ONLY');
console.log('MQTT protocol: 3.1.1');
console.log('-----------------------------------');

const mc = mqtt.connect(MQTT_URL, opts);

// MQTT CONNECT / CONNACK
mc.on('connect', (connack) => {

  console.log('MQTT CONNECTED');
  console.log('CONNACK:', JSON.stringify(connack));

  mqttState = 'connected';
  lastError = '';

  console.log('SUBSCRIBE ->', TOPIC);

  mc.subscribe(
    TOPIC,
    { qos: 0 },
    (err, granted) => {

      if (err) {

        lastError = err.message;

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

  broadcast({
    type: 'status',
    mqttState,
    lastError,
    topic: TOPIC
  });
});

// ======================================================
// MESHTASTIC MQTT PACKET -> SERVICE ENVELOPE DECODER
// ======================================================

mc.on('message', (topic, payload) => {

  console.log('-----------------------------------');
  console.log(
    'MQTT PACKET:',
    topic,
    payload.length,
    'bytes'
  );

  let decoded = null;

  try {

    // MQTT Meshtastic payload = protobuf ServiceEnvelope
    const envelope =
      ServiceEnvelope.fromBinary(payload);

    const packet = envelope.packet;

    console.log('SERVICE ENVELOPE: OK');
    console.log(
      'Gateway ID:',
      envelope.gatewayId || '(none)'
    );
    console.log(
      'Channel ID:',
      envelope.channelId || '(none)'
    );

    if (packet) {

      console.log('MESH PACKET: OK');
      console.log('From:', packet.from);
      console.log('To:', packet.to);
      console.log('Packet ID:', packet.id);
      console.log('Channel:', packet.channel);

      const encrypted =
        packet.encrypted &&
        packet.encrypted.length > 0;

      console.log(
        'Payload:',
        encrypted
          ? `ENCRYPTED (${packet.encrypted.length} bytes)`
          : 'NOT ENCRYPTED'
      );

      decoded = {
        gatewayId:
          envelope.gatewayId || null,

        channelId:
          envelope.channelId || null,

        from:
          packet.from,

        to:
          packet.to,

        id:
          packet.id,

        channel:
          packet.channel,

        encrypted:
          Boolean(encrypted),

        encryptedBytes:
          encrypted
            ? packet.encrypted.length
            : 0
      };

    } else {

      console.log('MESH PACKET: MISSING');

    }

  } catch (err) {

    console.error(
      'SERVICE ENVELOPE DECODE ERROR:',
      err.message
    );

  }

  // Enviar paquete al navegador.
  // Seguimos únicamente recibiendo.
  broadcast({
    type: 'packet',
    topic,
    receivedAt:
      new Date().toISOString(),

    bytes:
      payload.length,

    base64:
      payload.toString('base64'),

    decoded
  });

});

// ======================================================

mc.on('reconnect', () => {

  console.log('MQTT RECONNECTING');

  mqttState = 'reconnecting';

  broadcast({
    type: 'status',
    mqttState,
    lastError,
    topic: TOPIC
  });
});

mc.on('offline', () => {

  console.log('MQTT OFFLINE');

  mqttState = 'offline';

  broadcast({
    type: 'status',
    mqttState,
    lastError,
    topic: TOPIC
  });
});

mc.on('close', () => {

  console.log('MQTT CONNECTION CLOSED');

});

mc.on('disconnect', (packet) => {

  console.log(
    'MQTT DISCONNECT:',
    JSON.stringify(packet)
  );

});

mc.on('error', (err) => {

  lastError = err.message;

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
});

// Browser -> VirtualMesh WebSocket bridge
wss.on('connection', (ws) => {

  clients.add(ws);

  ws.send(
    JSON.stringify({
      type: 'status',
      mqttState,
      lastError,
      topic: TOPIC,
      mode: 'READ_ONLY'
    })
  );

  ws.on('close', () => {
    clients.delete(ws);
  });
});

server.listen(PORT, () => {

  console.log(
    `VirtualMesh Web listening on ${PORT}; MQTT read-only topic ${TOPIC}`
  );

});

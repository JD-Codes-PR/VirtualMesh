import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import mqtt from 'mqtt';
import crypto from 'crypto';

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
// No contiene información personal ni institucional.
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
  protocolVersion: 4,          // MQTT 3.1.1
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

// MQTT CONNECT / CONNACK successful
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

mc.on('message', (topic, payload) => {

  console.log(
    'MQTT PACKET:',
    topic,
    payload.length,
    'bytes'
  );

  broadcast({
    type: 'packet',
    topic,
    receivedAt: new Date().toISOString(),
    bytes: payload.length,
    base64: payload.toString('base64')
  });
});

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

// Browser -> VirtualMesh bridge
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

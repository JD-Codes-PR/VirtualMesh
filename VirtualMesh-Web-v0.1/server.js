import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import mqtt from 'mqtt';
import crypto from 'crypto';
import { fromBinary } from '@bufbuild/protobuf';
import { Mqtt } from '@meshtastic/protobufs';

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

// ID anónimo para esta instancia de VirtualMesh
const CLIENT_ID =
  'virtualmesh-' +
  crypto.randomBytes(6).toString('hex');

// ======================================================
// WEB SERVER
// ======================================================

const app = express();

app.use(
  express.static('public')
);

const server =
  http.createServer(app);

const wss =
  new WebSocketServer({
    server,
    path: '/mesh'
  });

let mqttState =
  'disconnected';

let lastError =
  '';

const clients =
  new Set();

// ======================================================
// BROADCAST TO WEB CLIENTS
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
// MQTT CONFIGURATION
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

  opts.username =
    MQTT_USER;

}

if (MQTT_PASS) {

  opts.password =
    MQTT_PASS;

}

// ======================================================
// STARTUP INFO
// ======================================================

console.log(
  '==================================='
);

console.log(
  'VirtualMesh'
);

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
      {
        qos: 0
      },
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

          type:
            'status',

          mqttState,

          lastError,

          topic:
            TOPIC

        });

      }
    );

    broadcast({

      type:
        'status',

      mqttState,

      lastError,

      topic:
        TOPIC

    });

  }
);

// ======================================================
// MESHTASTIC SERVICE ENVELOPE DECODER
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

    let result =
      null;

    try {

      // --------------------------------
      // SERVICE ENVELOPE
      // --------------------------------

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

      // --------------------------------
      // MESH PACKET
      // --------------------------------

      if (!packet) {

        console.log(
          'MESH PACKET: MISSING'
        );

      } else {

        console.log(
          'MESH PACKET: OK'
        );

        // --------------------------------
        // NODE IDs
        // --------------------------------

        const fromHex =
          '!' +
          Number(packet.from)
            .toString(16)
            .padStart(
              8,
              '0'
            );

        const toHex =
          '!' +
          Number(packet.to)
            .toString(16)
            .padStart(
              8,
              '0'
            );

        const isBroadcast =
          Number(packet.to) ===
          0xffffffff;

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

        // --------------------------------
        // PAYLOAD DETECTION
        // --------------------------------

        const hasEncrypted =
          packet.encrypted &&
          packet.encrypted.length > 0;

        const hasDecoded =
          packet.decoded != null;

        let payloadType =
          'UNKNOWN';

        let portnum =
          null;

        let decodedBytes =
          0;

        // --------------------------------
        // DECODED PAYLOAD
        // --------------------------------

        if (hasDecoded) {

          payloadType =
            'DECODED';

          portnum =
            packet.decoded.portnum;

          decodedBytes =
            packet.decoded.payload
              ? packet.decoded.payload.length
              : 0;

          console.log(
            'Payload: DECODED'
          );

          console.log(
            'PortNum:',
            portnum
          );

          console.log(
            'Decoded payload bytes:',
            decodedBytes
          );

          console.log(
            'Want response:',
            packet.decoded.wantResponse
          );

          console.log(
            'Request ID:',
            packet.decoded.requestId
          );

          console.log(
            'Reply ID:',
            packet.decoded.replyId
          );

        }

        // --------------------------------
        // ENCRYPTED PAYLOAD
        // --------------------------------

        else if (
          hasEncrypted
        ) {

          payloadType =
            'ENCRYPTED';

          console.log(
            'Payload: ENCRYPTED'
          );

          console.log(
            'Encrypted bytes:',
            packet.encrypted.length
          );

        }

        // --------------------------------
        // UNKNOWN / EMPTY
        // --------------------------------

        else {

          console.log(
            'Payload: NONE / UNKNOWN'
          );

        }

        // --------------------------------
        // RESULT FOR WEB APP
        // --------------------------------

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

          toHex:
            isBroadcast
              ? null
              : toHex,

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

          portnum,

          decodedBytes,

          encryptedBytes:
            hasEncrypted
              ? packet.encrypted.length
              : 0

        };

      }

    }

    catch (err) {

      console.error(
        'SERVICE ENVELOPE DECODE ERROR:',
        err.message
      );

    }

    // --------------------------------
    // SEND TO BROWSER
    // --------------------------------

    broadcast({

      type:
        'packet',

      topic,

      receivedAt:
        new Date().toISOString(),

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

      type:
        'status',

      mqttState,

      lastError,

      topic:
        TOPIC

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

      type:
        'status',

      mqttState,

      lastError,

      topic:
        TOPIC

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
      JSON.stringify(packet)
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

      type:
        'status',

      mqttState,

      lastError,

      topic:
        TOPIC

    });

  }
);

// ======================================================
// WEB BROWSER <-> VIRTUALMESH
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
          'READ_ONLY'

      })
    );

    ws.on(
      'close',
      () => {

        clients.delete(
          ws
        );

      }
    );

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

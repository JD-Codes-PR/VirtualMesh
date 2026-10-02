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
// DEFAULT MESHTASTIC LONGFAST PSK
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
// BROADCAST
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
    // fallback below
  }

  return `PORT_${portnum}`;

}

// ======================================================
// MESHTASTIC AES-CTR DECRYPTION
// ======================================================

function decryptLongFast(
  encrypted,
  packetId,
  fromNode
) {

  const nonce =
    Buffer.alloc(16);

  /*
   * Meshtastic nonce:
   *
   * bytes 0-7:
   * packet_id uint64 LE
   *
   * bytes 8-11:
   * from_node uint32 LE
   *
   * bytes 12-15:
   * block counter = 0
   */

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
  'LongFast decoder: ENABLED'
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
    console.log('MQTT CONNECTED');

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
// MESHTASTIC PACKET RECEIVER
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
      // PACKET INFO
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

      // ================================================
      // PAYLOAD VARIANT
      // ================================================

      const variant =
        packet.payloadVariant;

      let payloadType =
        'UNKNOWN';

      let portnum =
        null;

      let portName =
        null;

      let decodedBytes =
        0;

      let encryptedBytes =
        0;

      let decryptionSuccess =
        false;

      let textMessage =
        null;

      // ================================================
      // ALREADY DECODED
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
          'Decoded bytes:',
          decodedBytes
        );

        // TEXT MESSAGE
        if (
          portName ===
          'TEXT_MESSAGE_APP'
        ) {

          textMessage =
            Buffer.from(
              data.payload
            ).toString(
              'utf8'
            );

          console.log(
            'TEXT MESSAGE:',
            textMessage
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

          // ============================================
          // AES-CTR DECRYPT
          // ============================================

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

          // ============================================
          // DATA PROTOBUF
          // ============================================

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

          // ============================================
          // TEXT_MESSAGE_APP
          // ============================================

          if (
            portName ===
            'TEXT_MESSAGE_APP'
          ) {

            textMessage =
              Buffer.from(
                data.payload
              ).toString(
                'utf8'
              );

            console.log(
              'TEXT MESSAGE:',
              textMessage
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
      // WEB RESULT
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

        textMessage

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

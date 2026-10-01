import express from 'express';
import http from 'http';
import {WebSocketServer} from 'ws';
import mqtt from 'mqtt';

const PORT=process.env.PORT||8080;
const MQTT_URL=process.env.MQTT_URL||'mqtts://mqtt.meshtastic.org:8883';
const MQTT_USER=process.env.MQTT_USER||'';
const MQTT_PASS=process.env.MQTT_PASS||'';
const TOPIC=process.env.MQTT_TOPIC||'msh/US/PR/2/e/LongFast/+';
const app=express(); app.use(express.static('public'));
const server=http.createServer(app); const wss=new WebSocketServer({server,path:'/mesh'});
let mqttState='disconnected'; let lastError='';
const clients=new Set();
function broadcast(o){const s=JSON.stringify(o); for(const ws of clients) if(ws.readyState===1) ws.send(s)}
const opts={protocolVersion:4,reconnectPeriod:5000,connectTimeout:15000,clean:true};
if(MQTT_USER) opts.username=MQTT_USER; if(MQTT_PASS) opts.password=MQTT_PASS;
const mc=mqtt.connect(MQTT_URL,opts);
mc.on('connect',()=>{mqttState='connected';lastError='';mc.subscribe(TOPIC,{qos:0},e=>{if(e){lastError=e.message;broadcast({type:'status',mqttState,lastError,topic:TOPIC})}});broadcast({type:'status',mqttState,lastError,topic:TOPIC})});
mc.on('reconnect',()=>{mqttState='reconnecting';broadcast({type:'status',mqttState,lastError,topic:TOPIC})});
mc.on('offline',()=>{mqttState='offline';broadcast({type:'status',mqttState,lastError,topic:TOPIC})});
mc.on('error',e=>{lastError=e.message;broadcast({type:'status',mqttState,lastError,topic:TOPIC})});
mc.on('message',(topic,payload)=>broadcast({type:'packet',topic,receivedAt:new Date().toISOString(),bytes:payload.length,base64:payload.toString('base64')}));
wss.on('connection',ws=>{clients.add(ws);ws.send(JSON.stringify({type:'status',mqttState,lastError,topic:TOPIC,mode:'READ_ONLY'}));ws.on('close',()=>clients.delete(ws));});
server.listen(PORT,()=>console.log(`VirtualMesh Web listening on ${PORT}; MQTT read-only topic ${TOPIC}`));

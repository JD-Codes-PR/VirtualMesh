VirtualMesh v1.1.4 — Packet Comparator + Target Watch Dedupe

CAMBIOS
- Packet Comparator PR en el panel privado.
- Compara un TX VirtualMesh con paquetes reales PR observados.
- Campos: from/to, packet ID, channel, channelId, hopLimit/hopStart, viaMqtt, wantAck, payload variant, bytes, PortNum, wantResponse, requestId/replyId, gatewayId y RX metadata disponible.
- Nuevo endpoint privado: <VNODE_ROUTE>/api/packet-comparator
- Target Watch deduplica por From + Packet ID + Port + Channel.
- Target Watch muestra paquetes únicos y observaciones MQTT por separado.
- Cada paquete único muestra su cantidad de copias/observaciones.

DESPLIEGUE
Reemplazar solamente:
1. server.js
2. virtual-node.html

No hay cambios en package.json ni nuevas variables de Render.
NO cambie VNODE_ID_SEED si desea conservar el Node ID actual.

NOTA
Los datos del Packet Comparator son en memoria. Tras reiniciar/deploy, debe esperar a que vuelvan a entrar paquetes PR y realizar/observar un TX nuevo para poblar las muestras.

VirtualMesh v1.1.3 — Gateway Inspector PR

Actualiza solamente:
- server.js
- virtual-node.html

Nuevo:
- Gateway Inspector privado para Puerto Rico.
- Mapa Node -> Gateway basado en tráfico realmente observado.
- Ranking por actividad reciente.
- Cuenta nodos transportados, paquetes únicos, observaciones MQTT y PortNums.
- Endpoint privado: <VNODE_ROUTE>/api/gateway-inspector
- Downlink se muestra UNKNOWN hasta que exista evidencia; no se infiere de uplink.

Se conserva:
- Directed Messaging PR
- Target Watch 10 min
- TX Tracker
- Node ID derivado del mismo VNODE_ID_SEED

No cambies VNODE_ID_SEED si deseas conservar !34860e4a.
No requiere nuevas variables de Render ni cambios en package.json.

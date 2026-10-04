VirtualMesh v1.1.1 — Directed Messaging PR

Cambios:
- Lista privada de nodos Puerto Rico observados por VirtualMesh.
- Muestra Node ID, Long/Short Name cuando están disponibles, estado temporal, última observación, Public Key conocida/no observada y gateways conocidos.
- Permite seleccionar un nodo LIVE/RECENT de PR y enviar TEXT_MESSAGE_APP dirigido sobre LongFast.
- El mensaje dirigido queda fuera del portal público PR por la política existente.
- TX Tracker distingue BROADCAST de DIRIGIDO y conserva el Node ID destino.
- No implementa todavía DM PKI. Publicación MQTT no equivale a recepción RF confirmada.

Archivos actualizados respecto a v1.1.0:
- server.js
- virtual-node.html

No cambiar VNODE_ID_SEED si desea conservar el mismo Node ID.

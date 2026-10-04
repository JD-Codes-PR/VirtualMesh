VirtualMesh v0.7.4 — LIVE Dashboard Fix

Cambios:
- Dashboard LIVE usa exactamente la ventana operacional de 15 minutos.
- Un timestamp futuro ya no cuenta como LIVE.
- LIVE solo incluye nodos con geografía operacional válida, igual que /api/live-nodes.
- Dashboard informa explícitamente la ventana LIVE.
- Indicador visual del estado MQTT.
- Se conserva Experience Layer v0.7.3, Node Identity, normalización/dedupe y READ ONLY.
- MQTT publish continúa deshabilitado.

Despliegue:
Reemplaza server.js y la carpeta public/ por los de este paquete y despliega en Render.
Verificación:
1. /api/dashboard
2. /api/live-nodes
Los valores summary.liveNodes y count deben coincidir.

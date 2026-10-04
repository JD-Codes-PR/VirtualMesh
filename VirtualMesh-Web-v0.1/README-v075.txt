VirtualMesh v0.7.5 — Dashboard Hotfix

Corrección principal:
- /api/dashboard ya no llama channelIntelligenceRows(), función inexistente.
- Usa getChannelDiscoveryObject(), la fuente real de canales ya existente.

Corrección temporal adicional:
- Node Directory ya no convierte timestamps futuros en edad 0/LIVE.
- Un timestamp futuro queda UNKNOWN.
- /api/dashboard conserva LIVE estricto: 0 <= edad <= 15 minutos.
- /api/live-nodes conserva la misma regla estricta.

Sin cambios:
- READ ONLY.
- MQTT publish deshabilitado.
- Normalización/dedupe, Node Identity y Experience Layer permanecen.

Verificar después de desplegar:
1. /api/dashboard debe devolver JSON, no Internal Server Error.
2. /api/live-nodes debe devolver JSON.
3. summary.liveNodes de /api/dashboard debe coincidir con count de /api/live-nodes.

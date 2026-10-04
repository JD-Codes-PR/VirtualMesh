VirtualMesh v0.6.7 — Message Normalization

Cambios:
- Corrige Node IDs inválidos para que nunca se genere !00000NaN.
- Acepta IDs numéricos, !hex y 0xhex de forma segura.
- JSON sin campo `to` ya no se clasifica automáticamente como dirigido; se trata como broadcast salvo PKI.
- Mejora deduplicación JSON ↔ protobuf cuando una copia carece de identificadores válidos, usando texto + canal + región dentro de una ventana de 15 s.
- Una copia posterior con mejores IDs puede completar el registro ya almacenado.
- Mantiene el monitor de TODOS los mensajes.
- Mantiene Candidate Analyzer v2 y Channel Corroboration sin cambios de política.
- READ ONLY. MQTT publish sigue deshabilitado.

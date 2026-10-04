VirtualMesh v0.7.3 — Experience Layer

Bundled upgrade from v0.6.8:
1. Node Directory: /nodos.html + /api/node-directory
2. Conversations: /conversaciones.html + /api/conversations
3. Network Explorer: /red.html + /api/network-explorer
4. Channel Intelligence: /canales.html + /api/channel-intelligence
5. Dashboard: /index.html + /api/dashboard

Preserved:
- v0.6.7 message normalization/deduplication
- v0.6.8 Node Identity
- existing diagnostics, channel discovery/corroboration/candidate analyzer
- existing MQTT sources and decoders
- READ ONLY; MQTT publish remains disabled

No PKI/X25519 decryption claim is added. PKI-visible traffic remains diagnostic observation only.

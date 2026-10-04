VirtualMesh v0.6.8 - Node Identity

Base: v0.6.7 Message Normalization
Mode: READ ONLY
MQTT publish: DISABLED

Changes:
- /api/messages enriches each message with fromIdentity and toIdentity when known.
- Identity is sourced only from NODEINFO_APP or MAP_REPORT_APP already observed by VirtualMesh.
- Monitor displays Long Name, Short Name and Node ID when available.
- Node ID remains visible as the stable fallback.
- Broadcast messages continue to display BROADCAST.
- No changes to MQTT publishing, encryption, PKI handling, channel discovery or deduplication.

Deploy:
Replace the existing project files with this package and redeploy Render.
Then open /mensajes-monitor.html and /api/messages.

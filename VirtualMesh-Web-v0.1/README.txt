VirtualMesh v0.5.3 - Spain MQTT Diagnostic

READ ONLY. MQTT publish remains disabled.

Spain community source:
- Broker: mqtt://mqtt.meshtastic.es:1883
- Topic: msh/EU_868/#
- TLS: disabled
- Defaults: public Meshtastic community credentials

New diagnostics:
- Explicit [SPAIN] startup/connect/subscribe/error/reconnect logs
- First 5 Spain packets and every 100th packet logged
- 60-second Spain connection status log
- GET /api/spain-status

Existing global sources remain enabled:
- msh/US/2/#
- msh/EU_868/2/#

Optional environment overrides:
SPAIN_MQTT_URL
SPAIN_MQTT_USER
SPAIN_MQTT_PASS
SPAIN_MQTT_TOPIC

=== v0.5.4 - Spain O Zulo diagnostic source ===

Adds a second independent Spain-community MQTT source while preserving the existing global and Spain-direct sources.

O Zulo defaults:
- URL: mqtt://mqtt.mesh.comunidadeozulo.org:1883
- Topic: msh/EU_868/#
- TLS: disabled
- READ ONLY: yes
- MQTT publish: disabled

Optional environment variables:
- OZULO_MQTT_URL
- OZULO_MQTT_USER
- OZULO_MQTT_PASS
- OZULO_MQTT_TOPIC

Diagnostics:
- /api/spain-status          direct mqtt.meshtastic.es source
- /api/spain-ozulo-status    O Zulo source

O Zulo packets are normalized into the SPAIN region for the existing VirtualMesh counters/decoders while retaining independent connection diagnostics.

v0.5.5 LATAM expansion
----------------------
READ ONLY. No MQTT publish calls.

Added country views: Mexico, Argentina, Chile, Colombia, Venezuela.
Added global ANZ discovery root: msh/ANZ/#.
Added dedicated READ-ONLY community sources:
- Chile: mqtt.meshchile.cl / msh/CL/#
- Colombia: mqtt.meshcolombia.co / msh/CO/#

Mexico is discovered through the US regional MQTT root plus decoded geography; the project-hosted broker no longer reliably supports msh/MX as a country root.
Argentina is discovered through ANZ plus decoded geography. Public regional channel keys included for diagnostic decryption: BairesMesh, RosarioMesh, NQNmesh, CordobaMesh, ERMesh, MendozaMesh.
Venezuela has country filtering/geographic readiness, but no dedicated public national Meshtastic MQTT broker/channel was added because none was verified from current public community sources.

Diagnostic endpoint:
/api/latin-america-status

Optional env vars:
CHILE_MQTT_URL, CHILE_MQTT_USER, CHILE_MQTT_PASS, CHILE_MQTT_TOPIC
COLOMBIA_MQTT_URL, COLOMBIA_MQTT_USER, COLOMBIA_MQTT_PASS, COLOMBIA_MQTT_TOPIC


VirtualMesh v0.6.0 Hispanic + US
- Puerto Rico priority: msh/US/PR/# + verified public LongFast
- US: msh/US/2/#
- Spain: global EU868 + Spain Direct + O Zulo
- Hispanic America: geographic classification for MX AR CL CO VE PE EC BO PY UY PA CR GT HN SV NI DO CU; Equatorial Guinea GQ.
- Dedicated community sources retained for Chile and Colombia; Argentina ANZ discovery retained.
- READ ONLY. No MQTT publish.

v0.6.3 - Channel Corroboration Core
- Adds VERIFIED / VERIFIED_SOURCE / OBSERVED / DISCOVERY classification.
- Community corroboration never uses geolocation.
- /api/channel-discovery now includes classification per observed source+channel.
- New /api/channel-classification groups evidence by status.
- /api/messages includes channelClassification and MQTT source evidence.
- mensajes-monitor.html consumes server-side classification; it no longer infers countries from coordinates.
- Backend discovery remains broad and READ ONLY. No MQTT publish added.

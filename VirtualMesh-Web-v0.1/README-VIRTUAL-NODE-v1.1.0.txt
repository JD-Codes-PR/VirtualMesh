VirtualMesh v1.1.0 - TX Tracker

Adds a 10-minute tracker for packets transmitted by the Virtual Node.
States:
- MQTT_PUBLISHED: broker publish callback succeeded.
- MQTT_OBSERVED: the same from+packetId was observed on an incoming subscribed MQTT source.
- GATEWAY_OBSERVED: the same packet was observed with a gateway ID other than the Virtual Node gateway ID.
- RF evidence POSSIBLE: a foreign gateway was observed. This is deliberately NOT labeled RF confirmed.

New private API:
  <VNODE_ROUTE>/api/tx-tracker

The existing private route, VNODE_TOKEN, VNODE_ID_SEED and node identity remain unchanged.

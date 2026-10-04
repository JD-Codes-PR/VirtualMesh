VirtualMesh v0.6.4 - Strict US Corroboration

READ ONLY. No MQTT publish.

Change from v0.6.3:
- US + LongFast => VERIFIED_SOURCE (US public root)
- US + any other channel => DISCOVERY
- PR/Chile/Colombia LongFast rules unchanged
- Argentina named-channel rules unchanged
- Spain/O Zulo verified-channel and observed-source rules unchanged
- EU868 and ANZ discovery behavior unchanged
- No geolocation is used to corroborate community membership.

The backend continues broad discovery; mensajes-monitor.html only displays VERIFIED and VERIFIED_SOURCE messages.

# VirtualMesh Web v0.1
PWA + WebSocket bridge para observar MQTT Meshtastic desde un navegador. **READ ONLY**: server.js no contiene llamadas `publish()`.

## Ejecutar
1. Instala Node.js 20+.
2. `npm install`
3. Configura las variables de `.env.example` en tu host (las credenciales MQTT, si son necesarias, deben vivir SOLO en el servidor).
4. `npm start`
5. Abre `http://localhost:8080`.

Para instalar como PWA en Android desde un servidor remoto se requiere HTTPS. El navegador se conecta a `/mesh` por WSS y el servidor se conecta al broker por MQTT/TLS 8883.

## Seguridad de prueba
- No hay endpoint HTTP para publicar.
- No existe llamada MQTT `publish()`.
- La identidad de prueba se genera en localStorage y no se envía al broker.
- Los paquetes se retransmiten al navegador en Base64 para inspección; el decoder protobuf/cifrado se añadirá en la siguiente fase.

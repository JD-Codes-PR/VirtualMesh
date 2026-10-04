VirtualMesh Virtual Node v1.0.0
================================

Base: VirtualMesh v0.7.5 Dashboard Hotfix.
El monitor y Dashboard siguen funcionando. Solo el panel privado puede publicar.

IMPORTANTE - variables de entorno en Render
-------------------------------------------
VNODE_ID_SEED = una cadena secreta aleatoria de 32+ caracteres (OBLIGATORIA para transmitir)
VNODE_TOKEN   = contraseña/token largo para proteger las API del panel (MUY RECOMENDADO)
VNODE_ROUTE   = ruta privada, por ejemplo /vm-xxxxxxxxxxxxxxxx (MUY RECOMENDADO)
VNODE_LONG_NAME = VirtualMesh
VNODE_SHORT_NAME = VM01

Opcional:
VNODE_ID = !1234abcd  (si deseas fijar manualmente el Node ID)
VNODE_PRIVATE_KEY_PEM = clave X25519 PEM privada, usando \\n para saltos de línea.

Si NO defines VNODE_ID, el Node ID se deriva de VNODE_ID_SEED y será persistente.
Si NO defines VNODE_PRIVATE_KEY_PEM, la identidad X25519 también se deriva de VNODE_ID_SEED y será persistente.
Cambiar VNODE_ID_SEED cambia la identidad criptográfica del nodo.

Acceso
------
El panel NO se enlaza desde index.html ni desde ningún menú.
La dirección es:
https://TU-SERVICIO.onrender.com + el valor de VNODE_ROUTE

Destinos v1.0
-------------
PR       -> msh/US/PR/2/e/LongFast/<nodeid>
US       -> msh/US/2/e/LongFast/<nodeid>
SPAIN    -> O Zulo: msh/EU_868/2/e/LongFast/<nodeid>
CHILE    -> msh/CL/2/e/LongFast/<nodeid>
COLOMBIA -> msh/CO/2/e/LongFast/<nodeid>

Los brokers pueden aplicar ACL de publicación. El panel reportará el resultado que entregue el cliente MQTT; no garantiza que un gateway LoRa haga downlink.

Funciones
---------
- Identidad Node ID persistente
- X25519 public/private identity persistente
- Public key incluida en NODEINFO_APP
- TEXT_MESSAGE_APP broadcast
- NODEINFO_APP announcement
- AES-CTR de canal LongFast
- ServiceEnvelope protobuf
- Packet IDs incrementales por proceso
- rate/size safety: mensajes máx. 220 bytes UTF-8
- topics/brokers no editables desde el navegador
- retain=false
- Dashboard/monitor permanecen separados

Seguridad
---------
Una ruta secreta por sí sola NO es autenticación. Configure VNODE_TOKEN.
No coloque VNODE_ID_SEED, VNODE_TOKEN o claves privadas en GitHub.

VirtualMesh v1.0.1 — Private Route Hotfix

FIX PRINCIPAL
- La ruta privada ya no depende de process.cwd() para encontrar el HTML.
- virtual-node.html se carga desde la ubicación real de server.js mediante import.meta.url.
- VNODE_ROUTE acepta con o sin / inicial y elimina / final.
- Ruta predeterminada: /vm-jd-control
- /virtual-node.html permanece bloqueado intencionalmente.
- Render Logs muestra: Virtual Node private route: ...

RENDER ENVIRONMENT
VNODE_ROUTE=/vm-jd-control
VNODE_TOKEN=<secreto>
VNODE_ID_SEED=<secreto de 24+ caracteres>

IMPORTANTE
Sube TODO el contenido de este paquete respetando la carpeta private/ y reemplaza server.js.
No subas VNODE_TOKEN ni VNODE_ID_SEED a GitHub.

PRUEBA
1. Espera a que Render diga Live.
2. En Logs confirma: Virtual Node private route: /vm-jd-control
3. Abre https://virtualmesh.onrender.com/vm-jd-control
4. El panel debe cargar. Si VNODE_TOKEN existe, introdúcelo dentro del panel para consultar estado/transmitir.

READ ONLY se mantiene para el Core receptor. Solo las rutas privadas del Virtual Node pueden publicar.

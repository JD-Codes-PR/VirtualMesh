VirtualMesh v1.0.2 - Private Route Startup Hotfix

Canonical deployment layout:
  server.js
  virtual-node.html
  public/

Fixes:
- server.js loads virtual-node.html from the same directory as server.js.
- Missing panel HTML no longer crashes the entire Render service.
- Private route remains controlled by VNODE_ROUTE (default /vm-jd-control).
- Direct /virtual-node.html remains blocked by Express.
- Status API reports v1.0.2.

Environment:
VNODE_ROUTE=/vm-jd-control
VNODE_TOKEN=<secret>
VNODE_ID_SEED=<secret 24+ chars>

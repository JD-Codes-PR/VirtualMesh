VirtualMesh v0.6.5 - Candidate Analyzer Core

READ ONLY. MQTT publish remains disabled.

New endpoint:
  /api/channel-candidates

Optional filters:
  ?priority=HIGH
  ?minScore=45
  ?source=EU868
  ?limit=50

Purpose:
- Rank DISCOVERY and OBSERVED source/channel combinations for manual research.
- Uses decoded text count, unique nodes, packet activity, recurrence during current runtime, and dedicated observed source evidence.
- Does NOT use geolocation.
- Does NOT automatically promote any channel to VERIFIED.
- Existing corroboration rules remain unchanged.

Scoring:
- decoded text: up to 35
- unique nodes: up to 25
- packet activity: up to 20
- recurrence in current runtime: up to 15
- dedicated OBSERVED source: 5

Priority:
- HIGH 70-100
- MEDIUM 45-69
- LOW 0-44

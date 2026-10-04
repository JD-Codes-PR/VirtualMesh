VirtualMesh v0.6.6 - Candidate Analyzer v2

READ ONLY. No MQTT publish.

Changes:
- Separates candidate traffic into COMMUNITY_CANDIDATE, PRESET_TRAFFIC and SPECIAL_TRAFFIC.
- PKI and other special protocol/diagnostic names are excluded from candidate scoring.
- Generic preset/modem-like names (LongFast, MediumFast, ShortFast, etc.) remain visible for network research but do not compete in communityRanking.
- Named channels remain COMMUNITY_CANDIDATE and are ranked for manual research.
- No automatic promotion to VERIFIED.
- No geolocation is used for community corroboration.
- Backend discovery remains broad.

Endpoint:
/api/channel-candidates

Filters:
?type=COMMUNITY_CANDIDATE
?type=PRESET_TRAFFIC
?type=SPECIAL_TRAFFIC
?priority=HIGH
?minScore=45
?source=EU868

The response also includes communityRanking (top 25 named community candidates).

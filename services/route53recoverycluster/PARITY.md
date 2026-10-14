---
service: route53recoverycluster
sdk_module: aws-sdk-go-v2/service/route53recoverycluster@v1.36.4
last_audit_commit: a65519ce2
last_audit_date: 2026-10-13
overall: A
ops:
  GetRoutingControlState: {wire: ok, errors: ok, state: ok, persist: ok, note: "On or Off; ResourceNotFound for controls outside the endpoint's cluster"}
  ListRoutingControls: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoutingControlState: {wire: ok, errors: ok, state: ok, persist: ok, note: "evaluates assertion and gating rules; honours SafetyRulesToOverride"}
  UpdateRoutingControlStates: {wire: ok, errors: ok, state: ok, persist: ok, note: "atomic batch; a violated rule rejects every entry"}
families:
  route_matching: {status: ok, note: "matches the ToggleCustomerAPI target prefix, or the signing name on POST"}
  errors: {status: ok, note: "JSON error bodies carry __type, message and Message; ConflictException adds resourceId and resourceType"}
gaps: []
items_still_open: []
structural_gaps: []
deferred: []
leaks: {status: clean, note: "no goroutines or janitors; expiry and lifecycle are derived from the clock at read time"}
---

## Notes

- Protocol: JSON 1.0 with `X-Amz-Target: ToggleCustomerAPI.<Op>`; SigV4 signing name `route53-recovery-cluster`.
- No state of its own: every call resolves `route53recoverycontrolconfig` at call time and reads or updates routing control state there.
- Clients are pointed at a cluster endpoint from `DescribeCluster`. Endpoints are `<server>/route53-recovery-cluster/<clusterId>/<region>` on this server, so the data plane is reachable through the normal router. A control that belongs to another cluster is ResourceNotFound on that endpoint.
- ConflictException names the blocking safety rule (message, `resourceId` is the rule ARN) and covers wait periods.


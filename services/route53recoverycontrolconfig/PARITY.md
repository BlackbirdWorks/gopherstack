---
service: route53recoverycontrolconfig
sdk_module: aws-sdk-go-v2/service/route53recoverycontrolconfig@v1.40.4
last_audit_commit: 80b7bb8b3
last_audit_date: 2026-10-14
overall: A
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "creates the default control panel with the cluster; ClientToken idempotent; PENDING->DEPLOYED via opt-in activation delay; DEPLOYED clusters expose five regional endpoints that route back to this server on the scheme and host the caller used, or on ROUTE53_RECOVERY_CONTROL_ENDPOINT when set"}
  DescribeCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while custom panels, routing controls or rules remain; PENDING_DELETION while the delay window runs"}
  CreateControlPanel: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeControlPanel: {wire: ok, errors: ok, state: ok, persist: ok}
  ListControlPanels: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateControlPanel: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteControlPanel: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException for the default panel and for panels that still hold routing controls or safety rules"}
  CreateRoutingControl: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRoutingControl: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRoutingControls: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoutingControl: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRoutingControl: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while a safety rule references the control"}
  ListAssociatedRoute53HealthChecks: {wire: ok, errors: ok, state: ok, persist: ok, note: "reads RECOVERY_CONTROL health checks from Route 53 at call time"}
  CreateSafetyRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "ATLEAST threshold bounded by control count; controls must live in the rule's panel; gating and target sets are disjoint"}
  DescribeSafetyRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSafetyRules: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSafetyRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSafetyRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "existence-checked; an existing resource returns no Policy (see structural_gaps)"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matching: {status: ok, note: "matches the SigV4 signing name or SDK user agent plus a known path shape; ARN labels arrive percent-encoded and are decoded per segment"}
  errors: {status: ok, note: "REST-JSON error bodies carry both message and Message plus X-Amzn-Errortype; messages never repeat the code"}
gaps: []
items_still_open: []
structural_gaps:
  - GetResourcePolicy cannot return a policy: ARC policies come from RAM sharing, but services/ram has no ARC shareable resource types or managed permissions, and the SDK does not publish the policy AWS attaches
deferred: []
leaks: {status: clean, note: "no goroutines or janitors; expiry and lifecycle are derived from the clock at read time"}
---

## Notes

- Protocol: REST-JSON, PascalCase members, SigV4 signing name `route53-recovery-control-config` (`services/_PROTOCOLS.md`). ARNs have no region segment (`arn:aws:route53-recovery-control::<acct>:cluster/<id>`).
- Routing control state (On/Off) lives here and is the single source of truth for the data plane (`route53recoverycluster`) and for Route 53 `RECOVERY_CONTROL` health checks.
- Safety rules are evaluated on every data-plane update against the resulting state of the controls the update touches. ASSERTION: the rule's controls must satisfy `RuleConfig` after the update. GATING: updates to target controls require the gating controls to satisfy `RuleConfig`. ATLEAST needs `Threshold` controls On, AND needs all, OR needs one; `Inverted` negates. `WaitPeriodMs` blocks further changes to governed controls after one lands. `SafetyRulesToOverride` skips both checks for the named rules. A rejected batch changes nothing.
- Lifecycle: `--lifecycle-route53-recovery` (or the global delay) keeps new resources PENDING and deleted ones PENDING_DELETION for the window; default 0 is instant.
- Quotas are not enforced; the SDK documents none per operation.


---
service: route53recoveryreadiness
sdk_module: aws-sdk-go-v2/service/route53recoveryreadiness@v1.34.4
last_audit_commit: a65519ce2
last_audit_date: 2026-10-13
overall: B
ops:
  CreateCell: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCell: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCells: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCell: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCell: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while the cell belongs to a recovery group or another cell"}
  GetCellReadinessSummary: {wire: ok, errors: ok, state: ok, persist: ok, note: "worst readiness of the checks whose resources are scoped to the cell or the cells below it; UNKNOWN when no check covers it"}
  CreateRecoveryGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRecoveryGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRecoveryGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRecoveryGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRecoveryGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  GetArchitectureRecommendations: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived from the group's real cell, resource set and readiness check layout"}
  GetRecoveryGroupReadinessSummary: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateResourceSet: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceSet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourceSets: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateResourceSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourceSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while a readiness check uses the set"}
  CreateReadinessCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  GetReadinessCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  ListReadinessChecks: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateReadinessCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReadinessCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  GetReadinessCheckStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "readiness is derived from the live state of the owning sibling service; UNKNOWN when no rule evaluates the type or the sibling is unreachable"}
  GetReadinessCheckResourceStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "per-rule results with timestamps; the resource is addressed by ARN or componentId"}
  CreateCrossAccountAuthorization: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCrossAccountAuthorizations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCrossAccountAuthorization: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "lists the rules this emulator evaluates (see Notes)"}
  ListTagsForResources: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matching: {status: ok, note: "matches the SigV4 signing name or SDK user agent plus a known path shape; /rules is shared with IoT and resolved by scope"}
  errors: {status: ok, note: "REST-JSON error bodies carry both message and Message plus X-Amzn-Errortype"}
gaps: []
items_still_open:
  - ListRules: the rule IDs and descriptions are gopherstack-defined; the real ARC rule catalog is not published in the SDK, so parity of IDs is unverifiable
  - GetReadinessCheckStatus: resource types without an evaluator (API Gateway stages, Auto Scaling groups, EC2 volumes and gateways, ELB classic, MSK, RDS clusters, SNS subscriptions) report UNKNOWN; each needs a sibling lookup
structural_gaps: []
deferred: []
leaks: {status: clean, note: "no goroutines or janitors; expiry and lifecycle are derived from the clock at read time"}
---

## Notes

- Protocol: REST-JSON, camelCase members, ISO-8601 timestamps, SigV4 signing name `route53-recovery-readiness`.
- Readiness is never assumed. Each resource is evaluated by looking the resource up in the sibling that owns its type (resolved at call time): ELBv2 load balancer (exists, active), Route 53 health check (exists, Healthy), SQS queue, SNS topic, Lambda function, EC2 VPC, CloudWatch alarm (exists, not in ALARM), DynamoDB table (exists, ACTIVE), Route 53 DNS target record (exists in the hosted zone). Anything else, or a sibling that is not reachable, is UNKNOWN with a message. A resource in another account is NOT_AUTHORIZED until a cross-account authorization naming that account exists.
- Check readiness: any NOT_READY gives NOT_READY, then NOT_AUTHORIZED, then UNKNOWN; READY only when every rule of every resource is READY. Cell and recovery group summaries use the same reduction over the checks scoped to them.
- Architecture recommendations are generated from configuration facts (single or missing cells, resource sets without checks, resource types present in some cells only, groups with no scoped resource sets).


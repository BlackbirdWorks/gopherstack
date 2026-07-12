---
service: ec2
sdk_module: aws-sdk-go-v2/service/ec2   # version: see go.mod (backfilled)
last_audit_commit: f1a54d26
last_audit_date: 2026-07-11
overall: A   # 672 LOC genuine fixes (prior pass); this pass re-audited the Phase 3.3 store refactor drift, 0 new bugs
protocol: ec2-query (AWS query -> XML)
families:
  tags:          {status: ok, note: FIXED — existence/type table covered only 9 of ~100 taggable types; now full (AMIs/snapshots/NACLs/TGW/VPN/endpoints/launch-templates/IPAM...). See backend_resource_types.go}
  instance_attrs:{status: ok, note: FIXED disguised stub — disableApiTermination/Stop, ebsOptimized, sourceDestCheck, instanceInitiatedShutdownBehavior were accepted-but-not-modeled; now persisted (sourceDestCheck on primary ENI). DescribeInstanceAttribute was hardcoded}
  instance_lifecycle: {status: ok, note: FIXED StateReason/StateTransitionReason wire shape (were absent); disableApiTermination/Stop now enforced (OperationNotPermitted); RunInstances launch attrs applied}
  sg_rules:      {status: ok, note: PROVEN correct — protocol/port/ICMP bounds, CIDR, dup detection match AWS}
  filters:       {status: ok, note: PROVEN — AND-across-names/OR-within-values, tag: prefix}
  storage_layer: {status: ok, note: RE-AUDITED — Parity sweep 3 (ce30166a) converted 147/153 backend maps map[string]*T -> pkgs/store.Table[T] via data-driven registerAllTables (store_setup.go); ~1150 access sites rewritten. Reviewed keyFn correctness (compiler-enforced per-type, cannot mismatch and still build), Snapshot/Restore version-guard round-trip, Reset->registry.ResetAll(), secondary-index rebuild after Restore, composite-key helpers (coipCidrKey/ipamResourceCidrKey/localGatewayRouteKey/networkPerformanceSubscriptionKey) for Put/Get/Delete consistency. No delete-during-live-iteration (All() returns a copied slice; no .Range() usage in ec2). 0 defects found — purely mechanical, gates green}
gaps:
  - DeleteVpc/DeleteSubnet force-cascade-delete instead of DependencyViolation (tracked: bd gopherstack-b5m; re-confirmed present post-refactor, logic unchanged — still deferred, large blast radius across 16 test files)
deferred:
  - VPC/RouteTable/IGW/NAT/TGW/VPCEndpoint (batch-4) op-by-op — flagged higher-defect-odds, UNAUDITED
  - EBS snapshot lineage, ENI attach/detach edge cases, pagination internals beyond tags/instances
leaks: {status: not-fully-audited, note: instance/tag paths clean; full janitor/goroutine audit pending with deferred families}
---

## Notes
- sourceDestCheck AWS default is **true** for VPC instances (must be explicitly disabled, e.g. NAT instances) — a prior test encoded false, corrected.
- kernel/ramdisk return "" for HVM (not "stop").
- 2026-07-11 re-audit: only local drift since c18fa9b1 was the Phase 3.3 pkgs/store refactor (ce30166a) — a mechanical, compiler/type-checked, ~1150-site conversion of resource maps to store.Table. Audited it directly (not exhaustively re-walking unchanged op families per protocol); found no correctness regressions. All gates green: build, vet, test -race, go fix -diff (empty), golangci-lint (0 issues).
- NEXT PASS priority: VPC/TGW/Endpoint family sweep (still UNAUDITED) + DeleteVpc/DeleteSubnet DependencyViolation (gopherstack-b5m).

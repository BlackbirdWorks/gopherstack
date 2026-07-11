---
service: ec2
sdk_module: aws-sdk-go-v2/service/ec2   # version: see go.mod (backfilled)
last_audit_commit: c18fa9b1
last_audit_date: 2026-07-04
overall: A   # 672 LOC genuine fixes; ec2 is ~96k LOC/~180 maps — audit was SCOPED, not exhaustive
protocol: ec2-query (AWS query -> XML)
families:
  tags:          {status: ok, note: FIXED — existence/type table covered only 9 of ~100 taggable types; now full (AMIs/snapshots/NACLs/TGW/VPN/endpoints/launch-templates/IPAM...). See backend_resource_types.go}
  instance_attrs:{status: ok, note: FIXED disguised stub — disableApiTermination/Stop, ebsOptimized, sourceDestCheck, instanceInitiatedShutdownBehavior were accepted-but-not-modeled; now persisted (sourceDestCheck on primary ENI). DescribeInstanceAttribute was hardcoded}
  instance_lifecycle: {status: ok, note: FIXED StateReason/StateTransitionReason wire shape (were absent); disableApiTermination/Stop now enforced (OperationNotPermitted); RunInstances launch attrs applied}
  sg_rules:      {status: ok, note: PROVEN correct — protocol/port/ICMP bounds, CIDR, dup detection match AWS}
  filters:       {status: ok, note: PROVEN — AND-across-names/OR-within-values, tag: prefix}
gaps:
  - DeleteVpc/DeleteSubnet force-cascade-delete instead of DependencyViolation (bd: ec2 DeleteVpc follow-up)
deferred:
  - VPC/RouteTable/IGW/NAT/TGW/VPCEndpoint (batch-4) op-by-op — flagged higher-defect-odds, UNAUDITED
  - EBS snapshot lineage, ENI attach/detach edge cases, pagination internals beyond tags/instances
leaks: {status: not-fully-audited, note: instance/tag paths clean; full janitor/goroutine audit pending with deferred families}
---

## Notes
- sourceDestCheck AWS default is **true** for VPC instances (must be explicitly disabled, e.g. NAT instances) — a prior test encoded false, corrected.
- kernel/ramdisk return "" for HVM (not "stop").
- NEXT PASS priority: VPC/TGW/Endpoint family sweep + DeleteVpc DependencyViolation.

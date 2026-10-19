---
service: timestreaminfluxdb
sdk_module: aws-sdk-go-v2/service/timestreaminfluxdb@v1.29.4
last_audit_commit: c1af87ba3
last_audit_date: 2026-10-18
overall: B            # all 24 SDK operations are real state verified through the SDK client; B until the engine integration test has run against Docker in CI and the open items close.
ops:
  CreateDbBackup: {wire: ok, errors: ok, state: ok, persist: ok, note: "IN_PROGRESS then COMPLETED; snapshots the placement of the instance or cluster; ExpiresAfter is an RFC 3339 timestamp"}
  CreateDbCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "three member instances (PRIMARY, REPLICA, REPLICA) named <cluster>-1..3; INFLUXDB_V2 engine type; MULTI_NODE_READ_REPLICAS only"}
  CreateDbInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING until the lifecycle dwell (500ms default), or until the InfluxDB server accepts connections when the docker engine is on; Endpoint appears once past CREATING; defaults port 8086, SINGLE_AZ, InfluxIOIncludedT1, IPV4, maintenance Sun:02:00-Sun:06:00 UTC; name unique per account and Region (case-insensitive)"}
  CreateDbParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "union checked: exactly one of InfluxDBv2, InfluxDBv3Core, InfluxDBv3Enterprise holding known parameter names; logLevel, tracingType and Duration shapes validated"}
  DeleteDbBackup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDbCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDbInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETING then gone; the engine container is removed; only AVAILABLE or FAILED instances"}
  GetDbBackup: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDbCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDbInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDbParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDbBackups: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDbClusters: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDbInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDbInstancesForCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "members ordered by name"}
  ListDbParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootDbCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootDbInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "REBOOTING then AVAILABLE; the engine container is not restarted"}
  RestoreFromDbBackup: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW_RESOURCE creates an instance or cluster in RESTORING; REPLACE_EXISTING resets the original resource to the backup settings; restoreToTime is only valid for a CONTINUOUS backup"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDbCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDbInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "UPDATING, UPDATING_INSTANCE_TYPE or UPDATING_DEPLOYMENT_TYPE; storage can only grow; cluster members are refused"}
families:
  DbInstance: {status: ok, note: "Status is a lazy deadline (pending kind until it elapses, then AVAILABLE); with the docker engine the status leaves CREATING only when the container reports healthy, FAILED when it cannot start."}
  DbCluster: {status: ok, note: "Member instances share the cluster status; updates to type and port propagate to members; reboot can target a subset of members."}
  DbParameterGroup: {status: ok, note: "No delete or update operation exists in the SDK; groups are referenced by instances and clusters by Id."}
  DbBackup: {status: ok, note: "On-demand backups only."}
  Tags: {status: ok, note: "Instances, clusters, parameter groups and backups are taggable."}
  DockerEngine: {status: ok, note: "TIMESTREAM_INFLUXDB_ENGINE=docker runs one influxdb:2 container per CreateDbInstance, initialised with the requested username, password, organization and bucket (DOCKER_INFLUXDB_INIT_*); the instance reports the host and the published port as Endpoint and Port. Unit-tested with a fake runtime; Docker itself is exercised only by the integration environment."}
gaps: []
items_still_open:
  - DockerEngine: only CreateDbInstance starts a container; cluster members and restored instances stay metadata-only, and a restored instance starts empty because backups carry no data
  - DockerEngine: RebootDbInstance does not restart the container and UpdateDbInstance does not reconfigure it (no volume is kept, so a restart would lose data)
  - InfluxAuthParametersSecretArn: no Secrets Manager secret is created, so instances report no secret ARN; log in to a docker-backed instance with the username and password
  - DbParameterGroup: parameters are stored and returned but not applied to the engine; InfluxDBv3 parameter groups have no matching engine
  - DbBackupConfigurations: schedules are stored and NextAutomatedBackupTime is computed for HOURLY, DAILY, WEEKLY and MONTHLY, but automated and CONTINUOUS backups are never taken, so ListDbBackups only shows ON_DEMAND backups
  - LogDeliveryConfiguration: the S3 bucket name is checked for shape only and no logs are delivered
  - MaintenanceSchedule: NextMaintenanceTime is derived from the window but no maintenance ever runs (LastMaintenanceTime is never set)
  - VpcSubnetIds: existence is checked through EC2 when wired, but all subnets belonging to one VPC and the security groups are not verified
  - AccessDeniedException: never raised; no IAM authorization is modeled
  - ResourceGroupsTaggingAPI: Timestream for InfluxDB tags are not exposed through the tagging API
structural_gaps:
  - "InfluxDB v3 Core and Enterprise engines and multi-node clusters have no container image the emulator can run; cluster data planes cannot exist."
  - "Multi-AZ standby failover, storage throughput tiers and instance classes are labels only: a single local container has no standby or hardware."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; engine starts run in tracked goroutines that Close cancels and waits for."}
---

## Notes

Protocol is awsjson1.0 (target prefix `AmazonTimestreamInfluxDB.`). Unlike most JSON services, this SDK parses every timestamp with `smithytime.ParseDateTime`: timestamps on the wire are RFC 3339 strings, not epoch-seconds numbers (deserializers.go, `restoreToTime` is serialized with `FormatDateTime`). Shapes come from the pinned SDK serializers and types.

Errors use the modeled HTTP statuses: ResourceNotFound 404, Conflict 409, Validation 400, ServiceQuotaExceeded 402, Throttling 429, AccessDenied 403.

Emulator choices the SDK does not document: ids are 10 lowercase hex characters; the endpoint is `<id>-<account>.timestream-influxdb.<region>.on.aws` (cluster reader endpoint `<id>-ro-<account>...`); quotas enforced are 50 instances, 10 clusters, 50 parameter groups, 100 backups and 50 tags; default page size is 20; the lifecycle dwell follows `TIMESTREAMINFLUXDB_LIFECYCLE_DELAY` (default 500ms).

Engine settings are read from the environment by the provider: `TIMESTREAM_INFLUXDB_ENGINE` (`stub` by default, `docker` to run influxdb:2) and `TIMESTREAM_INFLUXDB_HOST` (address advertised for containers, default 127.0.0.1).

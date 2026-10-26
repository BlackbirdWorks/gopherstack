---
service: timestreaminfluxdb
sdk_module: aws-sdk-go-v2/service/timestreaminfluxdb@v1.29.4
last_audit_commit: c1af87ba3
last_audit_date: 2026-10-18
overall: B            # all 24 SDK operations are real state verified through the SDK client; B until the engine integration test has run against Docker in CI and the open items close.
ops:
  CreateDbBackup: {wire: ok, errors: ok, state: ok, persist: ok, note: "IN_PROGRESS then COMPLETED; snapshots the placement of the instance or cluster; with the docker engine runs influx backup inside the server (FAILED if it errors) and a restore replays it; ExpiresAfter is an RFC 3339 timestamp"}
  CreateDbCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "three member instances (PRIMARY, REPLICA, REPLICA) named <cluster>-1..3; INFLUXDB_V2 engine type; MULTI_NODE_READ_REPLICAS only"}
  CreateDbInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING until the lifecycle dwell (500ms default), or until the InfluxDB server accepts connections when the docker engine is on; Endpoint appears once past CREATING; InfluxAuthParametersSecretArn is a Secrets Manager secret (username, password, organization, bucket, token; key names not verified against AWS) deleted with the instance; defaults port 8086, SINGLE_AZ, InfluxIOIncludedT1, IPV4, maintenance Sun:02:00-Sun:06:00 UTC; name unique per account and Region (case-insensitive)"}
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
  RebootDbInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "REBOOTING then AVAILABLE; with the docker engine the container is restarted in place (same port, data kept)"}
  RestoreFromDbBackup: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW_RESOURCE creates an instance or cluster in RESTORING; REPLACE_EXISTING resets the original resource to the backup settings; restoreToTime is only valid for a CONTINUOUS backup"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDbCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDbInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "UPDATING, UPDATING_INSTANCE_TYPE or UPDATING_DEPLOYMENT_TYPE; storage can only grow; cluster members are refused; with the docker engine a changed DbParameterGroupIdentifier recreates the container on the same port and data directory with the new INFLUXD_* settings, instance type and storage changes are metadata"}
families:
  DbInstance: {status: ok, note: "Status is a lazy deadline (pending kind until it elapses, then AVAILABLE); with the docker engine the status leaves CREATING only when the container reports healthy, FAILED when it cannot start."}
  DbCluster: {status: ok, note: "Member instances share the cluster status; updates to type and port propagate to members; reboot can target a subset of members."}
  DbParameterGroup: {status: ok, note: "No delete or update operation exists in the SDK; groups are referenced by instances and clusters by Id."}
  DbBackup: {status: ok, note: "On-demand backups and scheduled HOURLY, DAILY, WEEKLY, MONTHLY and CUSTOM_SCHEDULE (cron(min hour dom month dow year), pattern from the CLI reference, dom or dow must be ?, UTC) backups (taken on the backend clock by RunScheduled, every 30s in the app), expired after RetentionDays."}
  Tags: {status: ok, note: "Instances, clusters, parameter groups and backups are taggable."}
  DockerEngine: {status: ok, note: "TIMESTREAM_INFLUXDB_ENGINE=docker runs one influxdb:2 container per instance and per cluster member, initialised with the requested credentials and an admin token (DOCKER_INFLUXDB_INIT_*) and run as the emulator's user on a bind-mounted data directory (under the data dir with --persist, else a temporary directory removed on delete/Close). Reboot restarts the container; a parameter group change recreates it on the same port and directory; restored instances replay the backup with influx restore --full. Parameters map to INFLUXD_* settings (every InfluxDBv2 parameter name is the influxd flag in camelCase). Container output is shipped to the LogDeliveryConfiguration bucket every minute when the bucket exists. After a restart with --persist, instances are started again on their data. Unit-tested with a fake runtime; TestDockerRebootKeepsData runs create, write, reboot, read, backup and restore against a real influxdb:2 image (skipped without Docker or the image)."}
  MaintenanceSchedule: {status: ok, note: "LastMaintenanceTime is set when the window start passes on the backend clock; maintenance itself changes nothing in the container."}
gaps: []
items_still_open:
  - DbParameterGroup: InfluxDBv3 parameter groups are stored and returned but have no matching engine, so they are not applied
  - DbBackupConfigurations: CONTINUOUS configurations are stored but never run
  - VpcSubnetIds: existence is checked through EC2 when wired, but all subnets belonging to one VPC and the security groups are not verified
  - AccessDeniedException: never raised; no IAM authorization is modeled
structural_gaps:
  - "InfluxDB v3 Core and Enterprise engines and multi-node clusters have no container image the emulator can run; cluster data planes cannot exist."
  - "Cluster members run as independent servers: there is no replication between them, and a cluster backup is taken from the PRIMARY member."
  - "Multi-AZ standby failover, storage throughput tiers and instance classes are labels only: a single local container has no standby or hardware."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; engine starts run in tracked goroutines that Close cancels and waits for."}
---

## Notes

Protocol is awsjson1.0 (target prefix `AmazonTimestreamInfluxDB.`). Unlike most JSON services, this SDK parses every timestamp with `smithytime.ParseDateTime`: timestamps on the wire are RFC 3339 strings, not epoch-seconds numbers (deserializers.go, `restoreToTime` is serialized with `FormatDateTime`). Shapes come from the pinned SDK serializers and types.

Errors use the modeled HTTP statuses: ResourceNotFound 404, Conflict 409, Validation 400, ServiceQuotaExceeded 402, Throttling 429, AccessDenied 403.

Emulator choices the SDK does not document: ids are 10 lowercase hex characters; the endpoint is `<id>-<account>.timestream-influxdb.<region>.on.aws` (cluster reader endpoint `<id>-ro-<account>...`); quotas enforced are 50 instances, 10 clusters, 50 parameter groups, 100 backups and 50 tags; default page size is 20; the lifecycle dwell follows `TIMESTREAMINFLUXDB_LIFECYCLE_DELAY` (default 500ms).

Engine settings are read from the environment by the provider: `TIMESTREAM_INFLUXDB_ENGINE` (`stub` by default, `docker` to run influxdb:2) and `TIMESTREAM_INFLUXDB_HOST` (address advertised for containers, default 127.0.0.1).

## 2026-10-22 engine pass

Engine-backed instances keep data across reboot and parameter changes, run scheduled backups with restore, deliver logs to S3 and publish an auth secret. Seams: SecretStore (Secrets Manager adapter), SetLogSink (S3 backend) and SetDataDir, wired in internal/app/timestreaminfluxdb_wiring.go.

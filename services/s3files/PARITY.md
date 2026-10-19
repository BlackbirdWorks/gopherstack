---
service: s3files
sdk_module: aws-sdk-go-v2/service/s3files@v1.8.1
last_audit_commit: d52fe4ac6
last_audit_date: 2026-10-18
overall: B            # all 21 SDK operations are real and driven through the SDK client behind the production router; B until the open items close.
ops:
  CreateAccessPoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent on ClientToken; uid/gid, path and octal permissions validated; Name taken from the Name tag"}
  CreateFileSystem: {wire: ok, errors: ok, state: ok, persist: ok, note: "Bucket must be an arn:aws:s3:::name ARN naming a bucket that exists in the S3 emulator (when wired); RoleArn must be an IAM role ARN; ClientToken replays, a changed parameter set is ConflictException"}
  CreateMountTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "file system must be available; subnet resolved against EC2 when wired (VPC, AZ id, CIDR); IPv4 allocated from the subnet CIDR or validated; one mount target per file system and subnet"}
  DeleteAccessPoint: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFileSystem: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while mount targets or access points remain"}
  DeleteFileSystemPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceNotFoundException when no policy is set"}
  DeleteMountTarget: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessPoint: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFileSystem: {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts a file system ID or ARN"}
  GetFileSystemPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceNotFoundException when no policy is set"}
  GetMountTarget: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSynchronizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceNotFoundException until a configuration is put"}
  ListAccessPoints: {wire: ok, errors: ok, state: ok, persist: ok, note: "page.Page tokens, MaxResults"}
  ListFileSystems: {wire: ok, errors: ok, state: ok, persist: ok, note: "filters by bucket ARN, page.Page tokens, MaxResults"}
  ListMountTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "filters by file system and/or access point (ID or ARN)"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "file systems and access points; capitalised MaxResults/NextToken query keys per the SDK"}
  PutFileSystemPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "policy must be a JSON object with a Statement, at most 20 KiB"}
  PutSynchronizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "LatestVersionNumber is optimistic concurrency: omitted on first put, then must equal the current version"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "at most 50 tags, aws: prefix reserved"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "absent keys ignored"}
  UpdateMountTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "replaces security groups, 1 to 5 sg- IDs"}
gaps: []
items_still_open:
  - "CreateMountTarget: when SecurityGroups is omitted the VPC default security group is not looked up, so none is reported; supplied group IDs are shape-checked only, not resolved against EC2 or checked for VPC membership."
  - "CreateMountTarget: IPv4 allocation avoids only this backend's mount targets, not addresses EC2 holds; IPv6 is never allocated; the network interface ID is minted here and no ENI exists in EC2."
  - "CreateFileSystem: RoleArn is shape-checked, not looked up in IAM; KmsKeyId is not looked up in KMS; AcceptBucketWarning is accepted but no bucket warnings exist."
  - "GetSynchronizationConfiguration returns ResourceNotFoundException until a configuration is put; the real service may seed a default configuration at creation, which the SDK docs do not state."
  - "DeleteFileSystem: ForceDelete is accepted but there is no file data plane, so no export is ever pending and the flag changes nothing."
  - "ServiceQuotaExceededException, ThrottlingException and InternalServerException are never returned: the S3 Files quotas are not published in the SDK, so no limit is enforced."
  - "Error HTTP statuses (400/404/409) are inferred; the SDK dispatches on the X-Amzn-Errortype header and does not pin statuses. ConflictException carries resourceId and resourceType."
  - "Access point and file system ARN resource paths (access-point/<id>, file-system/<id>) are assumed from sibling services; the SDK documents ARNs only as opaque strings."
  - "The creating/deleting dwell is zero by default and is not exposed as a CLI lifecycle setting; tests drive it with SetLifecycleDelay."
structural_gaps:
  - "No NFS/file data plane: files are not served from or exported to the S3 bucket, and no import or expiration rule is ever evaluated."
deferred: []
leaks: {status: clean, note: "lifecycle transitions are lazy deadlines read under the backend lock; no goroutines or timers."}
---

## Notes

Protocol: **restjson1**, unversioned paths (`/file-systems`, `/access-points`, `/mount-targets`, `/resource-tags/{id}`), SigV4 signing name `s3files`. The paths carry no service prefix, so the route matcher requires the `s3files` signing name or an `s3files.` endpoint host; unsigned requests to these paths fall through to S3.

Timestamps are epoch-seconds numbers. IDs are `fs-`, `fsap-`, `fsmt-` plus 17 hex characters; any input that takes an ID also takes the ARN (URL-escaped in the path).

Cross-service: bucket existence is checked against the S3 emulator and mount-target subnets against EC2 (`internal/app/s3files_wiring.go`); unwired backends accept any bucket and subnet.

---
service: cloudhsmv2
sdk_module: aws-sdk-go-v2/service/cloudhsmv2@v1.43.4
last_audit_commit: acf0513c8
last_audit_date: 2026-10-18
overall: B            # all 18 SDK operations are backed by real state and verified through the real SDK client in-package; B until the open items below close.
ops:
  CopyBackupToRegion: {wire: ok, errors: ok, state: ok, persist: ok, note: "READY backups only, different valid region; the copy gets its own ID in the destination region (CopyTimestamp, SourceBackup/Cluster/Region set); source tags copied unless TagList is given"}
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATE_IN_PROGRESS until the lifecycle dwell (500ms) then UNINITIALIZED; subnets validated through EC2 (one per AZ, one VPC), security group created in the VPC; real X.509 CSR (ECDSA P-256) in Certificates.ClusterCsr; SourceBackupId (ID or shared ARN) restores an initialized cluster; mode defaults FIPS (hsm1.medium) / NON_FIPS (hsm2m.medium); quota 4 clusters per region"}
  CreateHsm: {wire: ok, errors: ok, state: ok, persist: ok, note: "one HSM per cluster AZ; ENI (with optional IpAddress) created in the cluster subnet through EC2 and attached to the cluster security group; CREATE_IN_PROGRESS then ACTIVE; at most 28 per cluster"}
  DeleteBackup: {wire: ok, errors: ok, state: ok, persist: ok, note: "READY -> PENDING_DELETION with DeleteTimestamp 7 days out; DELETED and gone afterwards"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "refused (CloudHsmInvalidRequestException) while HSMs exist; DELETE_IN_PROGRESS then gone, releasing the security group"}
  DeleteHsm: {wire: ok, errors: ok, state: ok, persist: ok, note: "by HsmId, EniId or EniIp; DELETE_IN_PROGRESS then gone, releasing the ENI; deleting the last HSM of an initialized cluster takes a backup"}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "backups only; CloudHsmResourceNotFoundException when no policy is attached"}
  DescribeBackups: {wire: ok, errors: ok, state: ok, persist: ok, note: "filters backupIds/sourceBackupIds/clusterIds/states/neverExpires/modes; Shared returns other accounts' backups whose resource policy grants the caller; SortAscending; paginated"}
  DescribeClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "filters clusterIds/vpcIds/states; DELETED clusters are hidden; paginated"}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "backups only"}
  InitializeCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "UNINITIALIZED only; the signed certificate must certify the stored CSR key and chain to a self-signed TrustAnchor (crypto/x509); then INITIALIZE_IN_PROGRESS, INITIALIZED, and ACTIVE once an HSM is ACTIVE"}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "clusters and backups by ID or ARN; paginated"}
  ModifyBackupAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "NeverExpires toggles exemption from the cluster retention policy captured at backup time"}
  ModifyCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "BackupRetentionPolicy (7-379 days) and/or HsmType (hsm1.medium to hsm2m.medium, rollback inside the rollback window); MODIFY_IN_PROGRESS for the dwell"}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "READY backups only, JSON with a Statement; grants consumed by Shared DescribeBackups and cross-account CreateCluster SourceBackupId"}
  RestoreBackup: {wire: ok, errors: ok, state: ok, persist: ok, note: "cancels PENDING_DELETION inside the 7 day window; on a live backup it is a no-op that returns the backup"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "clusters and backups; 50 tags, aws: prefix reserved (CloudHsmTagException)"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "clusters and backups"}
families:
  clusters: {status: ok, note: "lifecycle derived from the clock (CREATE_IN_PROGRESS, UNINITIALIZED, INITIALIZE_IN_PROGRESS, MODIFY_IN_PROGRESS, DELETE_IN_PROGRESS) plus HSM presence (INITIALIZED vs ACTIVE); EC2 subnet, security group and ENI wiring through internal/app/cloudhsmv2_wiring.go"}
  backups: {status: ok, note: "taken when an initialized cluster gains its first HSM, when it initializes with an HSM, and when its last HSM is deleted; copies and cross-account sharing via resource policy"}
  tags: {status: ok, note: "cluster and backup tags; backups inherit the cluster tags at backup time"}
items_still_open:
  - Certificates: AwsHardwareCertificate, HsmCertificate and ManufacturerHardwareCertificate are not emitted
  - Hsm.EniIpV6: DUALSTACK clusters get an IPv4 ENI only; no IPv6 address is allocated
  - Backups: no daily automatic backups; only the lifecycle points listed in the backups family
  - DescribeBackups Shared: only PutResourcePolicy grants are consulted; RAM resource shares are not
  - CopyBackupToRegion: same-account copies only; the destination region is a label on the stored backup, not a separate backend
  - ModifyCluster: the 7 day HsmTypeRollbackExpiration window is an assumption, the SDK does not state it
  - CloudHsmAccessDeniedException and CloudHsmServiceException: never raised; no IAM is modeled
  - "CloudFormation: AWS::CloudHSM::Cluster is provisioned; HSMs are created through the API only, as in AWS. AWS::CloudHSMV2::Backup has no writable properties in CloudFormation resource specification v268, so it is not provisioned. Cluster Fn::GetAtt SubnetMapping is not supported"
gaps: []
structural_gaps:
  - Cluster.PreCoPassword: the HSM user model (PRECO, CO, CU) does not exist, so no password can be real
  - Certificates hardware chain: there is no physical HSM to carry AWS or manufacturer hardware certificates
  - Cluster ACTIVE from a customer activation: real clusters become ACTIVE after the CloudHSM CLI activates them on the HSM; the emulator derives ACTIVE from an initialized cluster with an ACTIVE HSM
deferred: []
leaks: {status: clean, note: "no goroutines or timers; transitional states are computed from the clock and reaped on the next write"}
---

## Notes
Protocol awsjson1.1, target prefix `BaldrApiService.`, signing name `cloudhsm`; timestamps are epoch-seconds numbers.
The CSR key is dropped after the CSR is generated (only the HSM would hold it); InitializeCluster compares the signed certificate's public key with the CSR's.
Account and region come from the request; resources are scoped by both, and a backup copy lives in the destination region.

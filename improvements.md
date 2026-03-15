# Gopherstack Improvements & Gap Analysis

Comprehensive audit of all services comparing against LocalStack and real AWS behavior.
Covers: resource leaks, memory leaks, race conditions, performance optimizations, missing features, and bugs.

All findings have been filed as GitHub issues.

---

## Filed Issues

### P0 — Immediate (production-breaking)
- [#654](https://github.com/BlackbirdWorks/gopherstack/issues/654) SES VerifyEmailIdentity mutex hang
- [#655](https://github.com/BlackbirdWorks/gopherstack/issues/655) Lambda container process leak on timeout
- [#656](https://github.com/BlackbirdWorks/gopherstack/issues/656) ECS multi-container tracking bug
- [#657](https://github.com/BlackbirdWorks/gopherstack/issues/657) Persistence FileStore.Save() missing fsync
- [#658](https://github.com/BlackbirdWorks/gopherstack/issues/658) Firehose empty record panic and flush race
- [#659](https://github.com/BlackbirdWorks/gopherstack/issues/659) CloudFormation DeleteStack memory leak

### P1 — High (data loss / resource exhaustion)
- [#660](https://github.com/BlackbirdWorks/gopherstack/issues/660) STS session tokens stored forever
- [#661](https://github.com/BlackbirdWorks/gopherstack/issues/661) CloudWatch Logs unbounded event storage
- [#662](https://github.com/BlackbirdWorks/gopherstack/issues/662) Kinesis records never expire
- [#663](https://github.com/BlackbirdWorks/gopherstack/issues/663) Secrets Manager unbounded version accumulation
- [#664](https://github.com/BlackbirdWorks/gopherstack/issues/664) EventBridge goroutine leak in delivery
- [#665](https://github.com/BlackbirdWorks/gopherstack/issues/665) SNS HTTP delivery goroutine leak
- [#666](https://github.com/BlackbirdWorks/gopherstack/issues/666) SES unbounded email storage
- [#667](https://github.com/BlackbirdWorks/gopherstack/issues/667) EC2 ENI not cleaned on instance termination
- [#688](https://github.com/BlackbirdWorks/gopherstack/issues/688) SSM unbounded parameter history
- [#692](https://github.com/BlackbirdWorks/gopherstack/issues/692) ACM autoValidate goroutine leak and tags not persisted
- [#693](https://github.com/BlackbirdWorks/gopherstack/issues/693) EKS nested map race — nil map panic
- [#698](https://github.com/BlackbirdWorks/gopherstack/issues/698) DocumentDB tag cleanup missing on all delete paths
- [#699](https://github.com/BlackbirdWorks/gopherstack/issues/699) Neptune DeleteDBCluster orphans instances
- [#701](https://github.com/BlackbirdWorks/gopherstack/issues/701) Organizations missing cascading cleanup
- [#711](https://github.com/BlackbirdWorks/gopherstack/issues/711) Kafka shared cluster pointer leaks
- [#714](https://github.com/BlackbirdWorks/gopherstack/issues/714) IoT race condition in dispatchActions
- [#713](https://github.com/BlackbirdWorks/gopherstack/issues/713) DMS data race in AddTagsToResource
- [#720](https://github.com/BlackbirdWorks/gopherstack/issues/720) LakeFormation orphaned permissions on deregistration
- [#723](https://github.com/BlackbirdWorks/gopherstack/issues/723) AppConfig tag memory leak and nil pointer dereference
- [#722](https://github.com/BlackbirdWorks/gopherstack/issues/722) ServiceDiscovery orphaned instances on deletion
- [#721](https://github.com/BlackbirdWorks/gopherstack/issues/721) TimestreamWrite tags memory leak on deletion

### P2 — Medium (correctness / parity)
- [#668](https://github.com/BlackbirdWorks/gopherstack/issues/668) SQS deduplication ID accumulation
- [#669](https://github.com/BlackbirdWorks/gopherstack/issues/669) KMS key material leak
- [#670](https://github.com/BlackbirdWorks/gopherstack/issues/670) IAM access key leak on user deletion
- [#671](https://github.com/BlackbirdWorks/gopherstack/issues/671) CloudWatch alarm context leak
- [#672](https://github.com/BlackbirdWorks/gopherstack/issues/672) Step Functions execution history unbounded
- [#673](https://github.com/BlackbirdWorks/gopherstack/issues/673) RDS DNS deregistration race
- [#674](https://github.com/BlackbirdWorks/gopherstack/issues/674) DynamoDB stream memory pressure
- [#675](https://github.com/BlackbirdWorks/gopherstack/issues/675) Lambda race conditions and missing event sources
- [#676](https://github.com/BlackbirdWorks/gopherstack/issues/676) Cognito IDP missing auth flows
- [#677](https://github.com/BlackbirdWorks/gopherstack/issues/677) S3 missing features
- [#678](https://github.com/BlackbirdWorks/gopherstack/issues/678) ECS task definition accumulation
- [#679](https://github.com/BlackbirdWorks/gopherstack/issues/679) AppSync nil pointer and schema caching
- [#680](https://github.com/BlackbirdWorks/gopherstack/issues/680) Route 53 DNS record leak
- [#681](https://github.com/BlackbirdWorks/gopherstack/issues/681) AWS Config missing rules engine
- [#682](https://github.com/BlackbirdWorks/gopherstack/issues/682) API Gateway resource tree orphaning
- [#683](https://github.com/BlackbirdWorks/gopherstack/issues/683) ElastiCache miniredis instance leak
- [#684](https://github.com/BlackbirdWorks/gopherstack/issues/684) OpenSearch silent error suppression
- [#685](https://github.com/BlackbirdWorks/gopherstack/issues/685) Redshift missing state transitions
- [#686](https://github.com/BlackbirdWorks/gopherstack/issues/686) ECR essentially non-functional
- [#687](https://github.com/BlackbirdWorks/gopherstack/issues/687) Batch job execution non-functional
- [#689](https://github.com/BlackbirdWorks/gopherstack/issues/689) Shared packages metric leak and panic risk
- [#690](https://github.com/BlackbirdWorks/gopherstack/issues/690) ACM-PCA no persistence, shallow copy, missing CRL
- [#691](https://github.com/BlackbirdWorks/gopherstack/issues/691) Scheduler tags race, no execution logic
- [#694](https://github.com/BlackbirdWorks/gopherstack/issues/694) CloudTrail no event storage, EventSelectors copy bug
- [#695](https://github.com/BlackbirdWorks/gopherstack/issues/695) CloudFront ETag race, missing invalidation tracking
- [#696](https://github.com/BlackbirdWorks/gopherstack/issues/696) EFS lifecycle policy leak on deletion
- [#697](https://github.com/BlackbirdWorks/gopherstack/issues/697) Backup plan deletion logic bug, jobs never complete
- [#700](https://github.com/BlackbirdWorks/gopherstack/issues/700) WAFv2 O(n) ARN lookups, missing tag cleanup
- [#703](https://github.com/BlackbirdWorks/gopherstack/issues/703) Shield O(n) duplicate detection, nil in UntagResource
- [#704](https://github.com/BlackbirdWorks/gopherstack/issues/704) Athena nil pointer in UntagResource, empty query results
- [#705](https://github.com/BlackbirdWorks/gopherstack/issues/705) Glue nil pointer in UntagResource, shallow copy leak
- [#706](https://github.com/BlackbirdWorks/gopherstack/issues/706) SES v2 request body not closed, unbounded emails
- [#707](https://github.com/BlackbirdWorks/gopherstack/issues/707) Cognito Identity silent crypto random failures
- [#708](https://github.com/BlackbirdWorks/gopherstack/issues/708) Pipes O(n) ARN lookups
- [#709](https://github.com/BlackbirdWorks/gopherstack/issues/709) Transfer silent JSON unmarshal errors
- [#710](https://github.com/BlackbirdWorks/gopherstack/issues/710) MWAA missing environment status transitions
- [#712](https://github.com/BlackbirdWorks/gopherstack/issues/712) IoT DataPlane missing shadow versioning
- [#715](https://github.com/BlackbirdWorks/gopherstack/issues/715) CodePipeline O(n) ARN lookups
- [#716](https://github.com/BlackbirdWorks/gopherstack/issues/716) CodeCommit O(n) ARN lookups
- [#717](https://github.com/BlackbirdWorks/gopherstack/issues/717) CodeBuild orphaned builds on project deletion
- [#718](https://github.com/BlackbirdWorks/gopherstack/issues/718) Glacier O(n) filtering, nil dereference
- [#719](https://github.com/BlackbirdWorks/gopherstack/issues/719) QLDB O(n) ARN lookups, no tag cleanup
- [#724](https://github.com/BlackbirdWorks/gopherstack/issues/724) TimestreamQuery all ARN operations O(n)
- [#725](https://github.com/BlackbirdWorks/gopherstack/issues/725) EMR terminated clusters never removed

---

## Services Audited

| # | Service | Issues Filed | Key Findings |
|---|---------|:---:|---|
| 1 | S3 | #677 | Tag gaps, TOCTOU, missing bucket policy enforcement |
| 2 | DynamoDB | #674 | Stream memory, txn token cleanup, missing GSI backfill |
| 3 | SQS | #668 | Dedup ID accumulation, missing validations |
| 4 | SNS | #665 | HTTP goroutine leak, silent failures, missing FIFO |
| 5 | Lambda | #655, #675 | Container leak, race conditions, async drop |
| 6 | IAM | #670 | Access key leak, missing group ops |
| 7 | STS | #660 | Session tokens stored forever |
| 8 | KMS | #669 | Key material leak, no rotation |
| 9 | Secrets Manager | #663 | Unbounded versions, staging label race |
| 10 | EventBridge | #664 | Goroutine leak, no DLQ |
| 11 | CloudWatch | #671 | Alarm context leak, circular dependency |
| 12 | RDS | #673 | DNS deregistration race |
| 13 | EC2 | #667 | ENI leak on termination |
| 14 | ECS | #656, #678 | Multi-container bug, task def accumulation |
| 15 | Step Functions | #672 | Unbounded history, goroutine explosion |
| 16 | Kinesis | #662 | Records never expire |
| 17 | CloudWatch Logs | #661 | Unbounded events, no retention |
| 18 | CloudFormation | #659 | DeleteStack leaks resources map |
| 19 | Cognito IDP | #676 | Missing MFA, challenges, refresh tokens |
| 20 | Firehose | #658 | Empty record panic, flush race |
| 21 | Route 53 | #680 | DNS record leak, missing record types |
| 22 | AppSync | #679 | Nil pointer, schema re-parsing |
| 23 | ElastiCache | #683 | Miniredis instance leak |
| 24 | SSM | #688 | Unbounded history, no command expiry |
| 25 | SES | #654, #666 | Mutex hang, unbounded email storage |
| 26 | AWS Config | #681 | Missing rules engine |
| 27 | API Gateway | #682 | Resource tree orphaning |
| 28 | ECR | #686 | Non-functional for image operations |
| 29 | Redshift | #685 | Missing state transitions |
| 30 | OpenSearch | #684 | Silent error suppression |
| 31 | Batch | #687 | Job execution non-functional |
| 32 | ACM | #692 | Goroutine leak, tags not persisted |
| 33 | ACM-PCA | #690 | No persistence, missing CRL |
| 34 | Scheduler | #691 | Tags race, no execution logic |
| 35 | CloudFront | #695 | ETag race, missing invalidation |
| 36 | CloudTrail | #694 | No event storage |
| 37 | EKS | #693 | Nested map race — nil panic |
| 38 | EFS | #696 | Lifecycle policy leak |
| 39 | Backup | #697 | Deletion logic bug, jobs never complete |
| 40 | DocumentDB | #698 | Tag cleanup missing on all deletes |
| 41 | Neptune | #699 | Orphaned instances on cluster delete |
| 42 | Organizations | #701 | Missing cascading cleanup |
| 43 | WAFv2 | #700 | O(n) ARN lookups, no tag cleanup |
| 44 | Glue | #705 | Nil pointer, shallow copy |
| 45 | Athena | #704 | Nil pointer, empty query results |
| 46 | Shield | #703 | O(n) duplicate detection |
| 47 | SES v2 | #706 | Request body leak, unbounded emails |
| 48 | Cognito Identity | #707 | Silent crypto random failures |
| 49 | Pipes | #708 | O(n) ARN lookups |
| 50 | Kafka (MSK) | #711 | Shared cluster pointer leaks |
| 51 | Transfer | #709 | Silent JSON unmarshal errors |
| 52 | MWAA | #710 | Missing status transitions |
| 53 | IoT | #714 | Race in dispatchActions |
| 54 | IoT DataPlane | #712 | Missing shadow versioning |
| 55 | DMS | #713 | Tags data race, nil pointer |
| 56 | CodeBuild | #717 | Orphaned builds, no deletion API |
| 57 | CodePipeline | #715 | O(n) ARN lookups |
| 58 | CodeCommit | #716 | O(n) ARN lookups |
| 59 | QLDB | #719 | O(n) ARN lookups, no tag cleanup |
| 60 | Glacier | #718 | O(n) filtering, nil dereference |
| 61 | LakeFormation | #720 | Orphaned permissions |
| 62 | AppConfig | #723 | Tag leak, nil pointer, panic |
| 63 | ServiceDiscovery | #722 | Orphaned instances |
| 64 | TimestreamWrite | #721 | Tags leak on deletion |
| 65 | TimestreamQuery | #724 | All ARN ops O(n) |
| 66 | EMR | #725 | Terminated clusters never removed |
| 67 | Shared Packages | #689 | Prometheus leak, event panic, status code bug |
| 68 | Persistence | #657 | Missing fsync before rename |

---

## Cross-Cutting Patterns Found

### 1. Tag Cleanup on Deletion (Most Common Issue)
Nearly every service fails to clean up tags when resources are deleted. Services with separate tag storage (`b.tags` map) are especially vulnerable. Affected: DocumentDB, Neptune, Organizations, AppConfig, ServiceDiscovery, TimestreamWrite, EFS, Backup, CodeBuild, QLDB, Glacier, EMR, and more.

### 2. O(n) ARN Lookups (Pervasive Performance Issue)
Most services lack an ARN→resource reverse index. Tag operations, describe-by-ARN, and delete-by-ARN all do linear scans. Affected: CloudFront, CloudTrail, EKS, WAFv2, Shield, Pipes, MWAA, DMS, CodeBuild, CodePipeline, CodeCommit, QLDB, Glacier, TimestreamQuery, EMR, OpenSearch, ServiceDiscovery, and more.

### 3. Missing Persistence
Several services lack Snapshot/Restore: ACM-PCA, DocumentDB, Amplify, ServiceDiscovery. State is completely lost on restart.

### 4. Shallow Copy / Shared Pointer Leaks
List operations in many services return direct pointers to internal state instead of deep copies. Callers can corrupt backend state. Affected: Kafka, Glue, EKS, and more.

### 5. Missing State Machine Transitions
Many services create resources in final state (e.g., "available", "ACTIVE") without transitioning through creating → active → deleting lifecycle. Affected: Redshift, EKS, MWAA, Transfer, DMS, EMR, and more.

---

## Summary Matrix

| Service | Resource Leaks | Memory Leaks | Race Conditions | Performance | Missing Features |
|---------|:-:|:-:|:-:|:-:|:-:|
| **S3** | ⚠️ | ⚠️ | ⚠️ | ⚠️ | 🔴 |
| **DynamoDB** | — | 🔴 | ⚠️ | ⚠️ | ⚠️ |
| **SQS** | — | 🔴 | ⚠️ | — | ⚠️ |
| **SNS** | 🔴 | ⚠️ | ⚠️ | ⚠️ | 🔴 |
| **Lambda** | 🔴 | 🔴 | 🔴 | ⚠️ | 🔴 |
| **IAM** | — | ⚠️ | ⚠️ | ⚠️ | ⚠️ |
| **STS** | — | 🔴 | ⚠️ | — | ⚠️ |
| **KMS** | — | ⚠️ | — | ⚠️ | ⚠️ |
| **Secrets Manager** | — | 🔴 | ⚠️ | ⚠️ | ⚠️ |
| **EventBridge** | 🔴 | ⚠️ | ⚠️ | — | ⚠️ |
| **CloudWatch** | — | 🔴 | 🔴 | — | ⚠️ |
| **RDS** | — | — | ⚠️ | ⚠️ | 🔴 |
| **EC2** | 🔴 | ⚠️ | — | — | ⚠️ |
| **ECS** | 🔴 | ⚠️ | ⚠️ | — | ⚠️ |
| **Step Functions** | — | 🔴 | ⚠️ | ⚠️ | ⚠️ |
| **Kinesis** | — | 🔴 | — | 🔴 | ⚠️ |
| **CW Logs** | — | 🔴 | ⚠️ | ⚠️ | ⚠️ |
| **CloudFormation** | — | 🔴 | — | ⚠️ | 🔴 |
| **Cognito IDP** | — | ⚠️ | — | ⚠️ | 🔴 |
| **Firehose** | — | ⚠️ | ⚠️ | — | ⚠️ |
| **Route 53** | ⚠️ | — | — | ⚠️ | ⚠️ |
| **AppSync** | — | — | ⚠️ | ⚠️ | 🔴 |
| **ElastiCache** | 🔴 | ⚠️ | ⚠️ | ⚠️ | 🔴 |
| **SSM** | — | 🔴 | — | ⚠️ | ⚠️ |
| **SES** | — | 🔴 | 🔴 | ⚠️ | 🔴 |
| **Config** | — | — | — | — | 🔴 |
| **API Gateway** | ⚠️ | — | — | ⚠️ | 🔴 |
| **ECR** | — | — | ⚠️ | — | 🔴 |
| **Redshift** | — | — | ⚠️ | — | 🔴 |
| **OpenSearch** | — | — | ⚠️ | ⚠️ | ⚠️ |
| **Batch** | — | ⚠️ | — | ⚠️ | 🔴 |
| **ACM** | ⚠️ | ⚠️ | ⚠️ | — | ⚠️ |
| **ACM-PCA** | — | — | ⚠️ | ⚠️ | 🔴 |
| **Scheduler** | — | — | ⚠️ | — | 🔴 |
| **CloudFront** | — | ⚠️ | ⚠️ | ⚠️ | 🔴 |
| **CloudTrail** | — | — | ⚠️ | ⚠️ | 🔴 |
| **EKS** | — | ⚠️ | 🔴 | ⚠️ | 🔴 |
| **EFS** | — | ⚠️ | — | ⚠️ | ⚠️ |
| **Backup** | — | ⚠️ | — | — | ⚠️ |
| **DocumentDB** | — | 🔴 | ⚠️ | — | 🔴 |
| **Neptune** | 🔴 | ⚠️ | ⚠️ | — | ⚠️ |
| **Organizations** | — | 🔴 | — | ⚠️ | ⚠️ |
| **WAFv2** | — | ⚠️ | — | 🔴 | 🔴 |
| **Glue** | — | ⚠️ | ⚠️ | ⚠️ | ⚠️ |
| **Athena** | — | ⚠️ | — | ⚠️ | ⚠️ |
| **Shield** | — | — | — | ⚠️ | ⚠️ |
| **SES v2** | ⚠️ | ⚠️ | — | — | ⚠️ |
| **Cognito Identity** | — | — | — | ⚠️ | ⚠️ |
| **Pipes** | — | — | — | ⚠️ | ⚠️ |
| **Kafka (MSK)** | — | 🔴 | ⚠️ | — | ⚠️ |
| **Transfer** | — | — | — | — | ⚠️ |
| **MWAA** | — | — | — | ⚠️ | ⚠️ |
| **IoT** | — | — | 🔴 | ⚠️ | ⚠️ |
| **IoT DataPlane** | — | — | — | ⚠️ | ⚠️ |
| **DMS** | — | — | 🔴 | ⚠️ | ⚠️ |
| **CodeBuild** | 🔴 | ⚠️ | ⚠️ | ⚠️ | ⚠️ |
| **CodePipeline** | — | — | ⚠️ | ⚠️ | ⚠️ |
| **CodeCommit** | — | — | — | ⚠️ | — |
| **QLDB** | — | ⚠️ | — | ⚠️ | — |
| **Glacier** | — | ⚠️ | — | ⚠️ | — |
| **LakeFormation** | — | 🔴 | — | ⚠️ | ⚠️ |
| **AppConfig** | — | 🔴 | — | — | ⚠️ |
| **ServiceDiscovery** | 🔴 | ⚠️ | — | 🔴 | ⚠️ |
| **TimestreamWrite** | — | 🔴 | — | — | — |
| **TimestreamQuery** | — | — | — | 🔴 | — |
| **EMR** | — | ⚠️ | — | ⚠️ | — |
| **Shared Pkgs** | — | ⚠️ | — | ⚠️ | — |

**Legend:** 🔴 Critical | ⚠️ Moderate | — None/Minimal

---

**Total Issues Filed:** 72 (issues #654–#725)
**Services Audited:** 68 (including shared packages and persistence layer)
**Audit Date:** 2026-03-15

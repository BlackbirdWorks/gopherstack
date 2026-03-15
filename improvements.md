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
- [#742](https://github.com/BlackbirdWorks/gopherstack/issues/742) AppConfig Data map mutation during iteration
- [#743](https://github.com/BlackbirdWorks/gopherstack/issues/743) Application Auto Scaling O(n) ARN lookups in tag and policy operations
- [#744](https://github.com/BlackbirdWorks/gopherstack/issues/744) Bedrock unsafe pointer returns and O(n) ARN lookups
- [#745](https://github.com/BlackbirdWorks/gopherstack/issues/745) Bedrock Runtime unbounded invocation history
- [#746](https://github.com/BlackbirdWorks/gopherstack/issues/746) CloudControl unbounded requests map — never cleaned up
- [#750](https://github.com/BlackbirdWorks/gopherstack/issues/750) CodeConnections returns internal pointers — caller can corrupt state
- [#751](https://github.com/BlackbirdWorks/gopherstack/issues/751) Elastic Transcoder O(n) duplicate checks, no persistence
- [#753](https://github.com/BlackbirdWorks/gopherstack/issues/753) Elastic Beanstalk triple-nested O(n) ARN lookups, pointer leak
- [#755](https://github.com/BlackbirdWorks/gopherstack/issues/755) FIS O(n) ARN lookups in 3 tag methods
- [#756](https://github.com/BlackbirdWorks/gopherstack/issues/756) ELBv2 missing tag cleanup on all 4 delete paths
- [#758](https://github.com/BlackbirdWorks/gopherstack/issues/758) Kinesis Analytics v1 O(n) ARN lookups, missing tag cleanup
- [#759](https://github.com/BlackbirdWorks/gopherstack/issues/759) Identity Store returns internal pointers — race condition
- [#760](https://github.com/BlackbirdWorks/gopherstack/issues/760) MediaConvert tag map leaks on queue/job template deletion
- [#761](https://github.com/BlackbirdWorks/gopherstack/issues/761) MediaStore O(n) ARN lookups, missing tag cleanup
- [#762](https://github.com/BlackbirdWorks/gopherstack/issues/762) Managed Blockchain O(n²) ARN lookups, ARN map leak
- [#764](https://github.com/BlackbirdWorks/gopherstack/issues/764) Pinpoint returns internal pointers — race condition
- [#765](https://github.com/BlackbirdWorks/gopherstack/issues/765) MQ unbounded configuration revisions and O(n²) tag operations
- [#767](https://github.com/BlackbirdWorks/gopherstack/issues/767) RDS Data unbounded executed statements
- [#768](https://github.com/BlackbirdWorks/gopherstack/issues/768) RAM unbounded association growth
- [#769](https://github.com/BlackbirdWorks/gopherstack/issues/769) SageMaker O(n) ARN lookups in all tag operations
- [#773](https://github.com/BlackbirdWorks/gopherstack/issues/773) SageMaker Runtime unbounded invocation history
- [#774](https://github.com/BlackbirdWorks/gopherstack/issues/774) SSO Admin returns internal pointers, unbounded status maps
- [#775](https://github.com/BlackbirdWorks/gopherstack/issues/775) Verified Permissions missing tag cleanup on DeletePolicyStore
- [#776](https://github.com/BlackbirdWorks/gopherstack/issues/776) Transcribe persistence snapshot isolation violation
- [#777](https://github.com/BlackbirdWorks/gopherstack/issues/777) Textract unbounded job accumulation — no delete method
- [#779](https://github.com/BlackbirdWorks/gopherstack/issues/779) X-Ray unbounded trace/segment accumulation
- [#780](https://github.com/BlackbirdWorks/gopherstack/issues/780) ECR persistence snapshot isolation bug
- [#781](https://github.com/BlackbirdWorks/gopherstack/issues/781) IoT Wireless shared tags map race condition
- [#782](https://github.com/BlackbirdWorks/gopherstack/issues/782) S3 Tables 12 O(n) table scans
- [#783](https://github.com/BlackbirdWorks/gopherstack/issues/783) SWF unbounded workflow execution growth
- [#785](https://github.com/BlackbirdWorks/gopherstack/issues/785) Route53 Resolver missing tag storage, no cascade delete

### P2 — Medium (correctness / parity)
- [#668](https://github.com/BlackbirdWorks/gopherstack/issues/668) SQS deduplication ID accumulation
- [#669](https://github.com/BlackbirdWorks/gopherstack/issues/669) KMS key material leak
- [#670](https://github.com/BlackbirdWorks/gopherstack/issues/670) IAM access key leak on user deletion
- [#671](https://github.com/BlackbirdWorks/gopherstack/issues/671) CloudWatch alarm context leak
- [#672](https://github.com/BlackbirdWorks/gopherstack/issues/672) Step Functions execution history unbounded
- [#673](https://github.com/BlackbirdWorks/gopherstack/issues/673) RDS DNS deregistration race
- [#674](https://github.com/BlackbirdWorks/gopherstack/issues/674) DynamoDB stream memory pressure
- [#739](https://github.com/BlackbirdWorks/gopherstack/issues/739) API Gateway Management API unbounded message growth
- [#740](https://github.com/BlackbirdWorks/gopherstack/issues/740) API Gateway v2 missing persistence layer
- [#741](https://github.com/BlackbirdWorks/gopherstack/issues/741) Amplify O(n) ARN lookups
- [#747](https://github.com/BlackbirdWorks/gopherstack/issues/747) CodeArtifact tag leaks on deletion, O(n) ARN lookups
- [#748](https://github.com/BlackbirdWorks/gopherstack/issues/748) CodeStar Connections O(n) ARN lookups
- [#749](https://github.com/BlackbirdWorks/gopherstack/issues/749) CodeDeploy tag leaks on DeleteApplication
- [#752](https://github.com/BlackbirdWorks/gopherstack/issues/752) Elasticsearch O(n) ARN lookups, missing tag cleanup
- [#754](https://github.com/BlackbirdWorks/gopherstack/issues/754) EMR Serverless O(n²) nested ARN lookups
- [#757](https://github.com/BlackbirdWorks/gopherstack/issues/757) Kinesis Analytics v2 O(n) findByARN
- [#763](https://github.com/BlackbirdWorks/gopherstack/issues/763) QLDB Session unbounded session accumulation
- [#766](https://github.com/BlackbirdWorks/gopherstack/issues/766) Redshift Data unbounded statement accumulation
- [#770](https://github.com/BlackbirdWorks/gopherstack/issues/770) Resource Groups O(n) ARN lookups
- [#771](https://github.com/BlackbirdWorks/gopherstack/issues/771) Resource Groups Tagging API O(n) pagination token lookup
- [#772](https://github.com/BlackbirdWorks/gopherstack/issues/772) Support unbounded attachment sets
- [#778](https://github.com/BlackbirdWorks/gopherstack/issues/778) Cost Explorer missing persistence
- [#784](https://github.com/BlackbirdWorks/gopherstack/issues/784) Serverless Application Repository missing persistence
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
| 28 | ECR | #686, #780 | Non-functional, persistence snapshot isolation bug |
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
| 69 | Amplify | #741 | O(n) ARN lookups |
| 70 | API Gateway v2 | #740 | Missing persistence |
| 71 | API GW Mgmt API | #739 | Unbounded message growth |
| 72 | AppConfig Data | #742 | Map mutation during iteration |
| 73 | App Auto Scaling | #743 | O(n) ARN lookups in tag and policy ops |
| 74 | Bedrock | #744 | Unsafe pointer returns, O(n) ARN lookups |
| 75 | Bedrock Runtime | #745 | Unbounded invocation history |
| 76 | CloudControl | #746 | Unbounded requests map |
| 77 | CodeArtifact | #747 | Tag leaks on deletion, O(n) ARN lookups |
| 78 | CodeConnections | #750 | Returns internal pointers |
| 79 | CodeDeploy | #749 | Tag leaks on DeleteApplication |
| 80 | CodeStar Conns | #748 | O(n) ARN lookups in tag ops |
| 81 | Elastic Beanstalk | #753 | Triple-nested O(n) ARN lookups |
| 82 | Elasticsearch | #752 | O(n) ARN lookups, missing tag cleanup |
| 83 | Elastic Transcoder | #751 | O(n) dup checks, no persistence, no tags |
| 84 | ELBv2 | #756 | Missing tag cleanup on 4 delete paths |
| 85 | EMR Serverless | #754 | O(n²) nested ARN lookups |
| 86 | FIS | #755 | O(n) ARN lookups in 3 tag methods |
| 87 | Identity Store | #759 | Returns internal pointers, O(n) lookups |
| 88 | IoT Wireless | #781 | Shared tags map race condition |
| 89 | Kinesis Analytics | #758 | O(n) ARN lookups, missing tag cleanup |
| 90 | Kinesis Analytics v2 | #757 | O(n) findByARN |
| 91 | Managed Blockchain | #762 | O(n²) ARN lookups, ARN map leak |
| 92 | MediaConvert | #760 | Tag map leaks on deletion |
| 93 | MediaStore | #761 | O(n) ARN lookups, missing tag cleanup |
| 94 | MQ | #765 | Unbounded config revisions, O(n²) tags |
| 95 | Pinpoint | #764 | Returns internal pointers |
| 96 | QLDB Session | #763 | Unbounded session accumulation |
| 97 | RAM | #768 | Unbounded association growth |
| 98 | RDS Data | #767 | Unbounded executed statements |
| 99 | Redshift Data | #766 | Unbounded statement accumulation |
| 100 | Resource Groups | #770 | O(n) ARN lookups, pointer leaks |
| 101 | RG Tagging API | #771 | O(n) pagination token lookup |
| 102 | Route53 Resolver | #785 | Missing tag storage, no cascade delete |
| 103 | S3 Tables | #782 | 12 O(n) table scans |
| 104 | SageMaker | #769 | O(n) ARN lookups in tag ops |
| 105 | SageMaker Runtime | #773 | Unbounded invocation history |
| 106 | Serverless Repo | #784 | Missing persistence |
| 107 | SSO Admin | #774 | Internal pointers, unbounded status maps |
| 108 | Support | #772 | Unbounded attachment sets |
| 109 | SWF | #783 | Unbounded execution growth |
| 110 | Textract | #777 | Unbounded job accumulation |
| 111 | Transcribe | #776 | Persistence snapshot isolation violation |
| 112 | Verified Perms | #775 | Missing tag cleanup on delete |
| 113 | X-Ray | #779 | Unbounded trace/segment accumulation |
| 114 | Cost Explorer | #778 | Missing persistence |

### Services Not Requiring Issues (Clean or Minimal)

| Service | Notes |
|---------|-------|
| Autoscaling | Well-designed, has persistence |
| DynamoDB Streams | Delegates to DynamoDB backend |
| ELB (Classic) | Proper tag cleanup, clean implementation |
| IoT Analytics | Proper tag cleanup, O(1) ARN lookups |
| MediaStore Data | Proper copying, no tags |
| MemoryDB | Excellent: O(1) lookups, proper cleanup |
| S3 Control | Simple, has persistence |

---

## Cross-Cutting Patterns Found

### 1. Tag Cleanup on Deletion (Most Common Issue)
Nearly every service fails to clean up tags when resources are deleted. Services with separate tag storage (`b.tags` map) are especially vulnerable. Affected: DocumentDB, Neptune, Organizations, AppConfig, ServiceDiscovery, TimestreamWrite, EFS, Backup, CodeBuild, QLDB, Glacier, EMR, ELBv2, CodeArtifact, CodeDeploy, Elasticsearch, MediaConvert, MediaStore, Verified Permissions, and more.

### 2. O(n) ARN Lookups (Pervasive Performance Issue)
Most services lack an ARN→resource reverse index. Tag operations, describe-by-ARN, and delete-by-ARN all do linear scans. Affected: CloudFront, CloudTrail, EKS, WAFv2, Shield, Pipes, MWAA, DMS, CodeBuild, CodePipeline, CodeCommit, QLDB, Glacier, TimestreamQuery, EMR, OpenSearch, ServiceDiscovery, Amplify, Application Auto Scaling, Bedrock, CodeArtifact, CodeStar Connections, Elastic Beanstalk, Elasticsearch, EMR Serverless, FIS, Kinesis Analytics, Managed Blockchain, MediaStore, Pinpoint, Resource Groups, SageMaker, S3 Tables, and more.

### 3. Missing Persistence
Several services lack Snapshot/Restore: ACM-PCA, DocumentDB, Amplify, ServiceDiscovery, API Gateway v2, CodeConnections, CodeDeploy, CodeStar Connections, Elastic Beanstalk, Elastic Transcoder, FIS, Identity Store, IoT Wireless, Pinpoint, Cost Explorer, Serverless Repo, SageMaker Runtime, Textract, Verified Permissions, X-Ray.

### 4. Shallow Copy / Shared Pointer Leaks
List operations in many services return direct pointers to internal state instead of deep copies. Callers can corrupt backend state. Affected: Kafka, Glue, EKS, Bedrock, CodeConnections, Identity Store, IoT Wireless, Pinpoint, Resource Groups, SSO Admin, and more.

### 5. Unbounded Growth (Memory Leaks)
Many services store history, invocations, statements, or requests without any TTL or cleanup. Affected: Bedrock Runtime, CloudControl, MQ (config revisions), QLDB Session, RAM, RDS Data, Redshift Data, SageMaker Runtime, SWF, Textract, X-Ray, Support.

### 6. Missing State Machine Transitions
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
| **Amplify** | — | — | — | ⚠️ | — |
| **API GW v2** | — | — | — | — | ⚠️ |
| **API GW Mgmt** | — | ⚠️ | — | — | — |
| **AppConfig Data** | — | — | ⚠️ | — | — |
| **App Auto Scaling** | — | — | — | 🔴 | — |
| **Bedrock** | — | — | 🔴 | 🔴 | — |
| **Bedrock Runtime** | — | 🔴 | — | — | — |
| **CloudControl** | — | 🔴 | — | — | — |
| **CodeArtifact** | ⚠️ | — | — | ⚠️ | — |
| **CodeConnections** | — | — | 🔴 | — | — |
| **CodeDeploy** | ⚠️ | — | — | — | — |
| **CodeStar Conns** | — | — | — | ⚠️ | — |
| **Elastic Beanstalk** | — | — | ⚠️ | 🔴 | — |
| **Elasticsearch** | ⚠️ | — | — | ⚠️ | — |
| **Elastic Transcoder** | — | — | — | ⚠️ | ⚠️ |
| **ELBv2** | ⚠️ | — | — | — | — |
| **EMR Serverless** | — | — | — | ⚠️ | — |
| **FIS** | — | — | — | ⚠️ | — |
| **Identity Store** | — | — | 🔴 | 🔴 | — |
| **IoT Wireless** | — | — | 🔴 | — | — |
| **Kinesis Analytics** | ⚠️ | — | — | ⚠️ | — |
| **Kinesis Analytics v2** | — | — | — | ⚠️ | — |
| **Managed Blockchain** | ⚠️ | — | — | 🔴 | — |
| **MediaConvert** | ⚠️ | — | — | — | — |
| **MediaStore** | ⚠️ | — | — | ⚠️ | — |
| **MQ** | — | 🔴 | — | 🔴 | — |
| **Pinpoint** | — | — | 🔴 | ⚠️ | — |
| **QLDB Session** | — | ⚠️ | — | — | — |
| **RAM** | — | 🔴 | — | ⚠️ | — |
| **RDS Data** | — | 🔴 | — | — | — |
| **Redshift Data** | — | ⚠️ | — | ⚠️ | — |
| **Resource Groups** | — | — | ⚠️ | ⚠️ | — |
| **RG Tagging API** | — | — | — | ⚠️ | — |
| **Route53 Resolver** | ⚠️ | — | — | ⚠️ | ⚠️ |
| **S3 Tables** | — | — | — | 🔴 | — |
| **SageMaker** | — | — | — | ⚠️ | — |
| **SageMaker Runtime** | — | 🔴 | — | — | — |
| **Serverless Repo** | — | — | — | — | ⚠️ |
| **SSO Admin** | — | ⚠️ | 🔴 | ⚠️ | — |
| **Support** | — | ⚠️ | — | — | — |
| **SWF** | — | 🔴 | — | ⚠️ | ⚠️ |
| **Textract** | — | 🔴 | — | — | — |
| **Transcribe** | — | — | — | — | ⚠️ |
| **Verified Perms** | ⚠️ | — | — | — | — |
| **X-Ray** | — | 🔴 | — | — | — |
| **Cost Explorer** | — | — | — | — | ⚠️ |

**Legend:** 🔴 Critical | ⚠️ Moderate | — None/Minimal

---

**Total Issues Filed:** 119 (issues #654–#785)
**Services Audited:** 121 (114 with issues filed + 7 clean services)
**Audit Date:** 2026-03-15

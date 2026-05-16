# LocalStack Parity Findings

Audit performed against gopherstack `services/*` handlers vs the AWS SDK v2 surface and LocalStack feature coverage. Items below are confirmed by reading the relevant handler/backend files or by integration-test failures.

## Critical (wrong wire protocol — clients hard-fail)

1. **DAX** — `services/dax/handler.go:71` routes on `X-Amz-Target: AmazonDAXV3.*`, but the AWS DAX SDK speaks a bespoke TLS/framed binary protocol on a non-HTTP listener. Real SDK never reaches this handler. (Surfaced via failing integration test.)
2. **XRay** — `services/xray/handler.go:265` accepts `POST /EncryptionConfig` for `PutEncryptionConfig`, but the AWS SDK targets the operation-named path (`POST /PutEncryptionConfig`). Same handler returns HTML 400 to real SDK callers. (Surfaced via failing integration test.)
3. **ELBv2** — `AddTags`, `RemoveTags`, `RegisterTargets`, `DeregisterTargets` originally omitted the `*Result` XML wrapper required by AWS Query protocol. SDK deserialiser raised `… node not found`. (Fixed in this PR.)
4. **Classic ELB** — `AddTags`, `RemoveTags` had the same missing `*Result` wrappers. (Fixed in this PR.)

## SDK ops missing from gopherstack handlers (new upstream surface)

5. **EMR Serverless `GetResourceDashboard`** — interactive Spark UI not implemented.
6. **EMR Serverless `StartSession`** — interactive sessions API not implemented.
7. **EMR Serverless `GetSession`** — sessions API not implemented.
8. **EMR Serverless `GetSessionEndpoint`** — sessions API not implemented.
9. **EMR Serverless `ListSessions`** — sessions API not implemented.
10. **EMR Serverless `TerminateSession`** — sessions API not implemented.

## WAFv2 — handlers exist but return only the empty AWS Query envelope (no state mutation)

11. **WAFv2 `AssociateWebACL`** — `services/wafv2/handler.go` ignores input and returns empty response.
12. **WAFv2 `DisassociateWebACL`** — empty stub.
13. **WAFv2 `DeleteAPIKey`** — empty stub.
14. **WAFv2 `DeleteIPSet`** — empty stub (does not remove from backend).
15. **WAFv2 `DeleteLoggingConfiguration`** — empty stub.
16. **WAFv2 `DeletePermissionPolicy`** — empty stub.
17. **WAFv2 `DeleteRegexPatternSet`** — empty stub.
18. **WAFv2 `DeleteRuleGroup`** — empty stub.
19. **WAFv2 `DeleteWebACL`** — empty stub.
20. **WAFv2 `PutPermissionPolicy`** — empty stub.
21. **WAFv2 `TagResource`** — does not persist tags.
22. **WAFv2 `UntagResource`** — does not remove tags.

## S3 Tables — full management surface is stubbed

23. **S3Tables `DeleteNamespace`** — empty stub; namespace persists.
24. **S3Tables `DeleteTable`** — empty stub.
25. **S3Tables `DeleteTableBucket`** — empty stub.
26. **S3Tables `DeleteTableBucketEncryption`** — empty stub.
27. **S3Tables `DeleteTableBucketMetricsConfiguration`** — empty stub.
28. **S3Tables `DeleteTableBucketPolicy`** — empty stub.
29. **S3Tables `DeleteTableBucketReplication`** — empty stub.
30. **S3Tables `DeleteTablePolicy`** — empty stub.
31. **S3Tables `DeleteTableReplication`** — empty stub.
32. **S3Tables `PutTableBucketEncryption`** — empty stub.
33. **S3Tables `PutTableBucketMaintenanceConfiguration`** — empty stub.
34. **S3Tables `PutTableBucketMetricsConfiguration`** — empty stub.
35. **S3Tables `PutTableBucketPolicy`** — empty stub.
36. **S3Tables `PutTableBucketReplication`** — empty stub.
37. **S3Tables `PutTableBucketStorageClass`** — empty stub.
38. **S3Tables `PutTableMaintenanceConfiguration`** — empty stub.
39. **S3Tables `PutTablePolicy`** — empty stub.
40. **S3Tables `PutTableRecordExpirationConfiguration`** — empty stub.
41. **S3Tables `RenameTable`** — empty stub.
42. **S3Tables `TagResource`** — empty stub.
43. **S3Tables `UntagResource`** — empty stub.

## SES (Classic) — stubbed control-plane mutations

44. **SES `PutIdentityPolicy`** — empty stub (`services/ses/handler.go:2001`).
45. **SES `DeleteIdentityPolicy`** — empty stub.
46. **SES `DeleteVerifiedEmailAddress`** — empty stub (verified address list unchanged).
47. **SES `VerifyEmailAddress`** — empty stub.
48. **SES `SetIdentityDkimEnabled`** — empty stub.
49. **SES `SetIdentityFeedbackForwardingEnabled`** — empty stub.
50. **SES `SetIdentityHeadersInNotificationsEnabled`** — empty stub.
51. **SES `SetIdentityMailFromDomain`** — empty stub.
52. **SES `SetIdentityNotificationTopic`** — empty stub.
53. **SES `PutConfigurationSetDeliveryOptions`** — empty stub.
54. **SES `UpdateAccountSendingEnabled`** — empty stub.
55. **SES `UpdateConfigurationSetEventDestination`** — empty stub.
56. **SES `UpdateConfigurationSetReputationMetricsEnabled`** — empty stub.
57. **SES `UpdateConfigurationSetSendingEnabled`** — empty stub.
58. **SES `UpdateConfigurationSetTrackingOptions`** — empty stub.
59. **SES `UpdateCustomVerificationEmailTemplate`** — empty stub.
60. **SES `UpdateReceiptRule`** — empty stub.
61. **SES `SetReceiptRulePosition`** — empty stub.
62. **SES `ReorderReceiptRuleSet`** — empty stub.

## Integration-test coverage gaps for popular services

Services with substantial handlers but no AWS-SDK-driven integration test under `test/integration/`:

63. **dynamodbstreams** — no SDK-driven test (DDB stream consumption regressions undetected).
64. **databrew** — no SDK-driven test.
65. **iotdataplane** — no SDK-driven test.
66. **sagemakerruntime** — no SDK-driven test.
67. **bedrockruntime** — no SDK-driven test.
68. **appconfigdata** — no SDK-driven test.
69. **apigatewaymanagementapi** — no SDK-driven test (no WebSocket integration coverage).
70. **acmpca** — no SDK-driven test.
71. **elastictranscoder** — no SDK-driven test.

## Tests added in this PR

- `test/integration/ce_test.go` — Cost Explorer anomaly monitor lifecycle.
- `test/integration/textract_test.go` — Textract synchronous text detection.
- `test/integration/timestreamquery_test.go` — Timestream endpoint discovery.
- `test/integration/ssoadmin_test.go` — SSO Admin instance + permission set lifecycle.
- `test/integration/codestarconnections_test.go` — CodeStar Connections lifecycle.
- `test/integration/cloudcontrol_test.go` — CloudControl resource lifecycle.
- `test/integration/support_test.go` — Support case lifecycle.

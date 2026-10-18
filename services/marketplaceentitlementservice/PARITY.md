---
service: marketplaceentitlementservice
sdk_module: aws-sdk-go-v2/service/marketplaceentitlementservice@v1.46.4
last_audit_commit: 8d07d8213
last_audit_date: 2026-10-18
overall: B            # the single SDK operation is backed by real state and verified through the SDK client in-package; B until an SDK-driven integration suite exists and the open items close.
ops:
  GetEntitlements: {wire: ok, errors: ok, state: ok, persist: ok, note: "filters CUSTOMER_IDENTIFIER, DIMENSION, CUSTOMER_AWS_ACCOUNT_ID, LICENSE_ARN (values of one key unioned, keys intersected; the two customer filters are mutually exclusive); MaxResults 1-100, default 25; Value is the IntegerValue/BooleanValue/DoubleValue/StringValue union; ExpirationDate omitted when unset"}
families:
  Protocol: {status: ok, note: "The pinned SDK speaks Smithy RPCv2 CBOR (POST /service/AWSMPEntitlementService/operation/GetEntitlements, ExpirationDate as CBOR tag 1); the legacy awsjson1.1 X-Amz-Target form is served too. Both run the same operation code."}
gaps: []
items_still_open:
  - "Entitlements come only from Marketplace Metering subscriptions (ResolveCustomer, Subscribe on a declared product) or Backend.GrantEntitlement; there is no seller-side catalog, so a plain emulator process returns none until a customer is subscribed"
  - "The calling account is not checked against the product owner; any caller can list any product's entitlements"
  - "ThrottlingException and InternalServiceErrorException are never raised on their own"
structural_gaps:
  - "No AWS Marketplace listing or contract system exists, so entitlement values and expiry dates cannot come from real buyer agreements."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; there are no goroutines or timers."}
---

## Notes

SigV4 signing name `aws-marketplace`. The matcher accepts the `AWSMPEntitlementService.` target prefix or the RPCv2 CBOR service path; because it also matches a path it is not listed in `internal/routegates` TargetGates. Invalid filters, MaxResults and tokens are `InvalidParameterException` (400).

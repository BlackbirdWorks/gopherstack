---
service: marketplacemetering
sdk_module: aws-sdk-go-v2/service/marketplacemetering@v1.46.2
last_audit_commit: 8d07d8213
last_audit_date: 2026-10-18
overall: B            # all 4 SDK operations are backed by real state and verified through the SDK client in-package; B until an SDK-driven integration suite exists and the open items close.
ops:
  BatchMeterUsage: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-25 records; per-record Success, DuplicateRecord (same product/customer/dimension/timestamp, original MeteringRecordId returned) or CustomerNotSubscribed; invalid dimension, timestamp outside the window, negative quantity or bad allocations go to UnprocessedRecords"}
  MeterUsage: {wire: ok, errors: ok, state: ok, persist: ok, note: "timestamp window now-6h..now+1h (TimestampOutOfBoundsException); same key + same quantity returns the original MeteringRecordId, different quantity is DuplicateRequestException; ClientToken replays and conflicts (IdempotencyConflictException, 409); DryRun validates then returns DryRunOperation; UsageAllocations must add up to UsageQuantity with unique tag sets"}
  RegisterUsage: {wire: ok, errors: ok, state: ok, persist: ok, note: "PublicKeyVersion must equal the product's current version; declared products require a subscription for the caller account (CustomerNotEntitledException); returns an HS256 JWT signature over product, nonce and account"}
  ResolveCustomer: {wire: ok, errors: ok, state: ok, persist: ok, note: "registration tokens are HMAC-signed with a 4 hour life (InvalidTokenException / ExpiredTokenException); resolving subscribes the account and grants the product's entitlements"}
families:
  Products: {status: ok, note: "A declared product (RegisterProduct) enforces its dimensions and requires a subscription. An undeclared product is created implicitly on first sight and accepts any dimension and customer."}
  Subscriptions: {status: ok, note: "Created by ResolveCustomer or Subscribe; CustomerIdentifier is a stable hash of product and account. They feed the Entitlement Service backend through EntitlementSink."}
  Usage: {status: ok, note: "Accepted records are stored and readable with Backend.UsageRecords(product); RegisterUsage calls with Backend.Registrations(product)."}
gaps: []
items_still_open:
  - "License-based metering (LicenseArn / concurrent agreements): BatchMeterUsage needs a ProductCode and LicenseArn is only echoed; there is no license catalog"
  - "Seeding is Go-API only (RegisterProduct, Subscribe, IssueRegistrationToken); there is no HTTP or config surface to declare products for out-of-process clients"
  - "RegisterUsage signature is HS256 with an emulator key and PublicKeyRotationTimestamp is the product's first-seen time plus 90 days; clients cannot verify it against the AWS Marketplace public key"
  - "Usage is deduplicated per second-resolution timestamp; the real service aggregates per hour"
  - "InvalidRegionException, InvalidEndpointRegionException, PlatformNotSupportedException, DisabledApiException, InvalidLicenseException and ThrottlingException are never raised"
structural_gaps:
  - "No EC2 instance, ECS task or EKS pod identity exists to bind a product code to, so the region/platform checks of MeterUsage and RegisterUsage have no data source."
  - "No billing system: metered usage is stored and queryable but never invoiced."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; there are no goroutines or timers."}
---

## Notes

Protocol is awsjson1.1 (`X-Amz-Target: AWSMPMeteringService.<Op>`, SigV4 signing name `aws-marketplace`). Timestamps are epoch-seconds numbers. The caller account is the SigV4 account of the request. Exceptions are 400 except IdempotencyConflictException (409) and InternalServiceErrorException (500). ValidationException and DryRunOperation are used where the SDK documents the condition but ships no typed exception.

Wiring: `internal/app/marketplace_wiring.go` sets the Entitlement Service backend as the sink, so a ResolveCustomer on a declared product shows up in GetEntitlements.

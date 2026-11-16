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
items_still_open: []
structural_gaps:
  - "No EC2 instance, ECS task or EKS pod identity exists to bind a product code to, so the region/platform checks of MeterUsage and RegisterUsage have no data source."
  - "No billing system: metered usage is stored and queryable but never invoiced."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; there are no goroutines or timers."}
---

## Notes

Protocol is awsjson1.1 (`X-Amz-Target: AWSMPMeteringService.<Op>`, SigV4 signing name `aws-marketplace`). Timestamps are epoch-seconds numbers. The caller account is the SigV4 account of the request. Exceptions are 400 except IdempotencyConflictException (409) and InternalServiceErrorException (500). ValidationException and DryRunOperation are used where the SDK documents the condition but ships no typed exception.

Wiring: `internal/app/marketplace_wiring.go` sets the Entitlement Service backend as the sink, so a ResolveCustomer on a declared product shows up in GetEntitlements.

## 2026-11-13 MeterUsage hour rounding

MeterUsage rounds Timestamp down to the hour for the once-per-hour idempotency/DuplicateRequestException rule (API_MeterUsage.html); IdempotencyConflictException answers 400 as documented (was 409).

## Emulator choices where the vendor is silent

- BatchMeterUsage deduplicates per second-resolution timestamp: API_BatchMeterUsage.html (checked 2026-11-13) states idempotency for identical requests but no hourly aggregation rule; only MeterUsage rounds down to the hour (API_MeterUsage.html).
- Seeding: there is no seller portal, so products, subscriptions, registration tokens and licenses are declared through POST /_gopherstack/marketplace/{products,subscriptions,registration-tokens,licenses} (or the Go API).
- License-based BatchMeterUsage (no request ProductCode, LicenseArn per record) validates the license (InvalidLicenseException when unknown, for another product, another customer or outside its activation period) as API_BatchMeterUsage.html and types.InvalidLicenseException describe; a license defaults to activating at the start of the accepted metering window.
- InvalidRegionException, InvalidEndpointRegionException and PlatformNotSupportedException need a container or instance identity the emulator does not have (see structural_gaps); DisabledApiException and ThrottlingException are never raised because no condition or limit for them is published.

## External effects that are recorded, not performed

- Client limitation: RegisterUsage signs with an emulator key (HS256), so clients cannot verify it against the AWS Marketplace public key.

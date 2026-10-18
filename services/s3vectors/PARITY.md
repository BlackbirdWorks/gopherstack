---
service: s3vectors
sdk_module: aws-sdk-go-v2/service/s3vectors@v1.16.3
last_audit_commit: c1af87ba3
last_audit_date: 2026-10-18
overall: B            # all 21 operations hold real state and are driven through the SDK client, queries are exact nearest-neighbour; limits and a few filter semantics come from AWS documentation rather than SDK evidence.
ops:
  CreateIndex: {wire: ok, errors: ok, state: ok, persist: ok, note: "dimension 1-4096, float32, cosine or euclidean, up to 10 non-filterable metadata keys; inherits the bucket default index mode and encryption"}
  CreateVectorBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "name 3-63 lowercase letters, digits and hyphens, unique per account and Region; AES256 or aws:kms; 10000 buckets per Region"}
  DeleteIndex: {wire: ok, errors: ok, state: ok, persist: ok, note: "removes the index's vectors"}
  DeleteVectorBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while indexes exist"}
  DeleteVectorBucketPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-100 keys; absent keys are ignored"}
  GetIndex: {wire: ok, errors: ok, state: ok, persist: ok, note: "bucket name or bucket ARN; index ARN accepted"}
  GetVectorBucket: {wire: ok, errors: ok, state: ok, persist: ok}
  GetVectorBucketPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "NotFoundException when no policy is set"}
  GetVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-100 keys; missing keys are omitted; data and metadata only on request"}
  ListIndexes: {wire: ok, errors: ok, state: ok, persist: ok, note: "prefix, maxResults 1-500, nextToken"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "bucket or index ARN"}
  ListVectorBuckets: {wire: ok, errors: ok, state: ok, persist: ok, note: "prefix, maxResults 1-500, nextToken"}
  ListVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "maxResults 1-1000 (default 500); segmentCount 1-16 with a key hash assigning each vector to one segment"}
  PutVectorBucketDefaultIndexMode: {wire: ok, errors: ok, state: ok, persist: ok, note: "applies to indexes created afterwards; CLASSIC only for buckets created before 2026-09-30"}
  PutVectorBucketPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "must be a JSON document with a Statement"}
  PutVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-500 vectors, all-or-nothing; dimension checked; zero vectors rejected for cosine; metadata 40 KB total, 2 KB filterable, 50 keys; existing keys are overwritten"}
  QueryVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "exact k-NN with the index metric; topK 1-100; returnDistance and returnMetadata; filter operators $eq $ne $gt $gte $lt $lte $in $nin $exists $and $or; nextToken pages through the ranked matches"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "50 tags, aws: prefix reserved"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateIndexMode: {wire: ok, errors: ok, state: ok, persist: ok, note: "CLASSIC only for buckets created before 2026-09-30"}
families:
  Buckets: {status: ok, note: "Scoped by account and Region; ARNs are arn:aws:s3vectors:<region>:<account>:bucket/<name>[/index/<index>]."}
  Indexes: {status: ok, note: "Index mode CLASSIC or ENHANCED; QueryMode CLASSIC against an ENHANCED index is rejected."}
  Vectors: {status: ok, note: "Stored as float32; persistence writes each vector as base64 little-endian bytes (4 bytes per dimension) rather than a JSON number list."}
  Query: {status: ok, note: "Brute force over every vector, distance float64 internally and float32 on the wire; ties break on key."}
  Filters: {status: ok, note: "A filter on a non-filterable key, an unknown operator, a non-numeric range operand or an empty $in is a ValidationException."}
  Wire: {status: ok, note: "Every route is POST /<Operation> except tagging (/tags/{resourceArn}); timestamps are epoch seconds (the SDK's four deserializers use ParseEpochSeconds)."}
gaps: []
items_still_open:
  - Limits: topK 100, batch sizes 500 and 100, metadata byte limits and the 10000 bucket and index quotas come from AWS documentation; the SDK does not encode them
  - Filter semantics: a missing field satisfies $ne and $nin and fails every other operator, and list-valued metadata matches when any element matches; both are taken from the documented operator behaviour, not SDK evidence
  - PutVectors: a key repeated inside one batch keeps the last occurrence; AWS behaviour is unverified
  - Index mode cutoff: the 2026-09-30 rule for CLASSIC comes from SDK documentation text and compares against the emulator clock
  - Encryption: sseType and kmsKeyArn are stored but nothing is encrypted and the KMS key is not looked up, so the Kms*Exception errors are never raised
  - Bucket policies: stored and returned, not enforced
  - Throttling: TooManyRequestsException and ServiceUnavailableException are never produced
  - Memory: vectors live in process memory (4 bytes per dimension plus metadata) with no spill to disk
structural_gaps:
  - QueryVectors in CLASSIC mode: AWS filters during an approximate search and may return fewer than topK matches; the emulator is exact in both modes
deferred: []
leaks: {status: clean, note: "No goroutines; testleak verifies the package."}
---

## Notes

REST-JSON 1. Routing is gated by the SigV4 signing name `s3vectors` (or the SDK user agent marker) plus a known path shape. Several operation paths (`/CreateIndex`, `/GetIndex`, `/ListIndexes`, `/DeleteIndex`) are shared with Resource Explorer 2, so the signing name is the only discriminator; priority is never raised to win a collision.

Not-found errors use the code `NotFoundException` (not `ResourceNotFoundException`), conflicts `ConflictException` (409), quota errors `ServiceQuotaExceededException` (402).

Distances: euclidean is the L2 distance, cosine is `1 - cos(a, b)`.

---
service: s3vectors
sdk_module: aws-sdk-go-v2/service/s3vectors@v1.16.3
last_audit_commit: c1af87ba3
last_audit_date: 2026-10-18
overall: B            # all 21 operations hold real state and are driven through the SDK client, queries are exact nearest-neighbour; a missing-field filter rule and duplicate keys in a batch are not documented.
ops:
  CreateIndex: {wire: ok, errors: ok, state: ok, persist: ok, note: "dimension 1-4096, float32, cosine or euclidean, up to 10 non-filterable metadata keys; inherits the bucket default index mode and encryption"}
  CreateVectorBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "name 3-63 lowercase letters, digits and hyphens, unique per account and Region; AES256 or aws:kms; 10000 buckets per Region"}
  DeleteIndex: {wire: ok, errors: ok, state: ok, persist: ok, note: "removes the index's vectors"}
  DeleteVectorBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while indexes exist"}
  DeleteVectorBucketPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-500 keys (API_S3VectorBuckets_DeleteVectors.html); absent keys are ignored; Kms*Exception when the index key is unusable; counts toward the write rate"}
  GetIndex: {wire: ok, errors: ok, state: ok, persist: ok, note: "bucket name or bucket ARN; index ARN accepted"}
  GetVectorBucket: {wire: ok, errors: ok, state: ok, persist: ok}
  GetVectorBucketPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "NotFoundException when no policy is set"}
  GetVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-100 keys; Kms*Exception when the index key is unusable; missing keys are omitted; data and metadata only on request"}
  ListIndexes: {wire: ok, errors: ok, state: ok, persist: ok, note: "prefix, maxResults 1-500, nextToken"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "bucket or index ARN"}
  ListVectorBuckets: {wire: ok, errors: ok, state: ok, persist: ok, note: "prefix, maxResults 1-500, nextToken"}
  ListVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "maxResults 1-1000 (default 500); segmentCount 1-16 with a key hash assigning each vector to one segment"}
  PutVectorBucketDefaultIndexMode: {wire: ok, errors: ok, state: ok, persist: ok, note: "applies to indexes created afterwards; CLASSIC only for buckets created before 2026-09-30"}
  PutVectorBucketPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "must be a JSON document with a Statement"}
  PutVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-500 vectors, all-or-nothing; dimension checked; zero vectors rejected for cosine; metadata 40 KB total, 2 KB filterable, 50 keys; existing keys are overwritten; at most 1000 write requests and 2500 vectors per second per index, then 429 TooManyRequestsException (s3-vectors-limitations.html); Kms*Exception when the index key is unusable"}
  QueryVectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "exact k-NN with the index metric; topK 1-10000 with pages of at most 100 and nextToken (s3-vectors-limitations.html); returnDistance and returnMetadata; filter operators $eq $ne $gt $gte $lt $lte $startsWith (ENHANCED queries only) $in $nin $exists $and $or; at most 100 filter constraints on ENHANCED indexes; Kms*Exception when the index key is unusable"}
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
  - "Filter semantics: a missing field satisfies $ne and $nin and fails every other operator; s3-vectors-metadata-filtering.html only says '$ne: Not equal comparison' and '$nin: Match none of the values in array' and does not state how absent fields behave"
  - PutVectors: a key repeated inside one batch keeps the last occurrence; API_S3VectorBuckets_PutVectors.html does not state the behaviour
  - Encryption: sseType and kmsKeyArn are stored and the key is checked on data operations, but vector data is not encrypted at rest
  - Bucket policies: stored and returned, not enforced
  - ServiceUnavailableException: never produced; the PutVectors page ties it to a request exceeding capacity without publishing a size
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

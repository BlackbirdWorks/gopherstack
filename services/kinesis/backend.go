package kinesis

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"math/rand/v2"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// StorageBackend defines the interface for a Kinesis backend.
//
// Every method takes a context.Context so the per-request AWS region can be
// threaded through and resources kept isolated per region. The region is read
// from the context via getRegion, falling back to the backend's default region
// when the context carries no region.
type StorageBackend interface {
	CreateStream(ctx context.Context, input *CreateStreamInput) error
	DeleteStream(ctx context.Context, input *DeleteStreamInput) error
	DescribeStream(ctx context.Context, input *DescribeStreamInput) (*DescribeStreamOutput, error)
	ListStreams(ctx context.Context, input *ListStreamsInput) (*ListStreamsOutput, error)
	PutRecord(ctx context.Context, input *PutRecordInput) (*PutRecordOutput, error)
	PutRecords(ctx context.Context, input *PutRecordsInput) (*PutRecordsOutput, error)
	GetShardIterator(ctx context.Context, input *GetShardIteratorInput) (*GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, input *GetRecordsInput) (*GetRecordsOutput, error)
	ListShards(ctx context.Context, input *ListShardsInput) (*ListShardsOutput, error)
	RegisterStreamConsumer(
		ctx context.Context,
		input *RegisterStreamConsumerInput,
	) (*RegisterStreamConsumerOutput, error)
	DescribeStreamConsumer(
		ctx context.Context,
		input *DescribeStreamConsumerInput,
	) (*DescribeStreamConsumerOutput, error)
	ListStreamConsumers(ctx context.Context, input *ListStreamConsumersInput) (*ListStreamConsumersOutput, error)
	DeregisterStreamConsumer(ctx context.Context, input *DeregisterStreamConsumerInput) error
	SubscribeToShard(ctx context.Context, input *SubscribeToShardInput) (*SubscribeToShardOutput, error)
	UpdateShardCount(ctx context.Context, input *UpdateShardCountInput) (*UpdateShardCountOutput, error)
	EnableEnhancedMonitoring(
		ctx context.Context,
		input *EnableEnhancedMonitoringInput,
	) (*EnableEnhancedMonitoringOutput, error)
	DisableEnhancedMonitoring(
		ctx context.Context,
		input *DisableEnhancedMonitoringInput,
	) (*DisableEnhancedMonitoringOutput, error)
	IncreaseStreamRetentionPeriod(ctx context.Context, input *IncreaseStreamRetentionPeriodInput) error
	DecreaseStreamRetentionPeriod(ctx context.Context, input *DecreaseStreamRetentionPeriodInput) error
	MergeShards(ctx context.Context, input *MergeShardsInput) error
	SplitShard(ctx context.Context, input *SplitShardInput) error
	StartStreamEncryption(ctx context.Context, input *StartStreamEncryptionInput) error
	StopStreamEncryption(ctx context.Context, input *StopStreamEncryptionInput) error
	DeleteResourcePolicy(ctx context.Context, input *DeleteResourcePolicyInput) error
	GetResourcePolicy(ctx context.Context, input *GetResourcePolicyInput) (*GetResourcePolicyOutput, error)
	PutResourcePolicy(ctx context.Context, input *PutResourcePolicyInput) error
	ListTagsForResource(ctx context.Context, input *ListTagsForResourceInput) (*ListTagsForResourceOutput, error)
	TagResource(ctx context.Context, input *TagResourceInput) error
	UntagResource(ctx context.Context, input *UntagResourceInput) error
	UpdateStreamMode(ctx context.Context, input *UpdateStreamModeInput) error
	UpdateAccountSettings(ctx context.Context, input *UpdateAccountSettingsInput) error
	UpdateMaxRecordSize(ctx context.Context, input *UpdateMaxRecordSizeInput) error
	UpdateStreamWarmThroughput(ctx context.Context, input *UpdateStreamWarmThroughputInput) error
	DescribeAccountSettings(ctx context.Context) (*DescribeAccountSettingsOutput, error)
	CountOpenShards(ctx context.Context) int
	ListAll(ctx context.Context) []StreamInfo
}

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// contextWithRegion returns ctx with the given region attached under regionContextKey.
func contextWithRegion(ctx context.Context, region string) context.Context {
	return context.WithValue(ctx, regionContextKey{}, region)
}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARNOrCtx resolves the region for an ARN-addressed operation: it
// prefers the region embedded in the ARN (so the resource is found in the
// region it actually lives in), then the ctx region, then defaultRegion.
func regionFromARNOrCtx(ctx context.Context, a, defaultRegion string) string {
	if r := regionFromARN(a); r != "" {
		return r
	}

	return getRegion(ctx, defaultRegion)
}

// regionFromARN extracts the region segment from an AWS ARN.
// ARN format: arn:partition:service:region:account:resource. Returns "" when
// the input is not a well-formed ARN with a non-empty region segment.
func regionFromARN(a string) string {
	const (
		arnRegionIdx = 3
		arnMinParts  = 6
	)

	parts := strings.Split(a, ":")
	if len(parts) < arnMinParts {
		return ""
	}

	return parts[arnRegionIdx]
}

// Compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// resetter is implemented by backends that support a full in-memory reset.
type resetter interface {
	Reset()
}

// purger is implemented by backends that support time-based purging.
type purger interface {
	Purge(ctx context.Context, cutoff time.Time)
}

// streamNameRe validates Kinesis stream names: 1–128 alphanumeric, hyphen, underscore, or dot chars.
var streamNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)

// consumerNameRe validates Kinesis consumer names: same charset as stream names.
var consumerNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)

// isValidStreamName reports whether s is a valid Kinesis stream name.
func isValidStreamName(s string) bool {
	return streamNameRe.MatchString(s)
}

// isValidConsumerName reports whether s is a valid Kinesis consumer name.
func isValidConsumerName(s string) bool {
	return consumerNameRe.MatchString(s)
}

// sortedKeys returns the keys of map m in sorted order.
func sortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	return keys
}

// kinesisThrottleFault holds the state of an active FIS throughput-exception fault on a stream.
type kinesisThrottleFault struct {
	expiry      time.Time
	probability float64 // fraction of calls to throttle (0.0–1.0); 1.0 = always throttle
}

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// All resource maps are nested by region (outer key = region) so that
// same-named streams in different regions are fully isolated — including their
// shards, records, consumers, FIS throttle faults, and resource policies.
type InMemoryBackend struct {
	streams                  map[string]map[string]*Stream               // region → stream name → stream
	fisThroughputFaults      map[string]map[string]*kinesisThrottleFault // region → stream name → fault
	faultsMu                 *lockmetrics.RWMutex
	resourcePolicies         map[string]map[string]string // region → resource ARN → policy
	mu                       *lockmetrics.RWMutex
	OnStreamPurged           func(string)
	accountID                string
	region                   string
	onDemandStreamCountLimit int
}

// NewInMemoryBackend creates a new empty InMemoryBackend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		streams:                  make(map[string]map[string]*Stream),
		fisThroughputFaults:      make(map[string]map[string]*kinesisThrottleFault),
		faultsMu:                 lockmetrics.New("kinesis.faults"),
		resourcePolicies:         make(map[string]map[string]string),
		accountID:                accountID,
		region:                   region,
		mu:                       lockmetrics.New("kinesis"),
		onDemandStreamCountLimit: defaultOnDemandStreamCountLimit,
	}
}

// Region returns the AWS region this backend is configured to use as its default.
func (b *InMemoryBackend) Region() string { return b.region }

// streamsStore returns the stream map for the given region, lazily creating it.
// Callers must hold b.mu (write lock).
func (b *InMemoryBackend) streamsStore(region string) map[string]*Stream {
	if b.streams[region] == nil {
		b.streams[region] = make(map[string]*Stream)
	}

	return b.streams[region]
}

// streamsView returns the existing stream map for the given region without
// creating it. A nil map (region never written) reads as empty. Safe under a
// read lock. Callers must hold b.mu (read or write lock).
func (b *InMemoryBackend) streamsView(region string) map[string]*Stream {
	return b.streams[region]
}

// faultsStore returns the FIS throttle-fault map for the given region, lazily
// creating it. Callers must hold b.faultsMu.
func (b *InMemoryBackend) faultsStore(region string) map[string]*kinesisThrottleFault {
	if b.fisThroughputFaults[region] == nil {
		b.fisThroughputFaults[region] = make(map[string]*kinesisThrottleFault)
	}

	return b.fisThroughputFaults[region]
}

// policiesStore returns the resource-policy map for the given region, lazily
// creating it. Callers must hold b.mu.
func (b *InMemoryBackend) policiesStore(region string) map[string]string {
	if b.resourcePolicies[region] == nil {
		b.resourcePolicies[region] = make(map[string]string)
	}

	return b.resourcePolicies[region]
}

func newStreamLock(streamName string) *lockmetrics.RWMutex {
	return lockmetrics.New(fmt.Sprintf("kinesis.stream.%s", streamName))
}

func initializeStreamRuntime(stream *Stream, streamName string) {
	if stream.Name == "" {
		stream.Name = streamName
	}
	if stream.Consumers == nil {
		stream.Consumers = map[string]*Consumer{}
	}
	if stream.mu == nil {
		stream.mu = newStreamLock(stream.Name)
	}
	if stream.Shards == nil {
		stream.Shards = []*Shard{}
	}
	if stream.StreamMode == "" {
		stream.StreamMode = streamModeProvisioned
	}
	if stream.EnhancedMonitoring == nil {
		stream.EnhancedMonitoring = []string{}
	}
	if stream.MaxRecordSizeBytes <= 0 {
		stream.MaxRecordSizeBytes = defaultMaxRecordSizeBytes
	}
}

// hashKey computes a numeric hash key for a partition key using a simple mapping.
// The result is in the range [0, 2^128-1] as required by Kinesis.
func hashKey(partitionKey string) *big.Int {
	// Use a simple deterministic hash by interpreting the UUID v5 of the partition key.
	sum := uuid.NewSHA1(uuid.NameSpaceOID, []byte(partitionKey))

	return new(big.Int).SetBytes(sum[:])
}

// shardForHashKey selects the open shard whose hash key range contains h.
func shardForHashKey(shards []*Shard, h *big.Int) *Shard {
	for _, s := range shards {
		if s.Closed {
			continue
		}
		start := new(big.Int)
		start.SetString(s.HashKeyRangeStart, hashKeyDecimalBase)
		end := new(big.Int)
		end.SetString(s.HashKeyRangeEnd, hashKeyDecimalBase)
		if h.Cmp(start) >= 0 && h.Cmp(end) <= 0 {
			return s
		}
	}
	// fallback: first open shard
	for _, s := range shards {
		if !s.Closed {
			return s
		}
	}
	if len(shards) > 0 {
		return shards[0]
	}

	return nil
}

// shardForPartitionKey selects the open shard for the given partition key by hash.
func shardForPartitionKey(shards []*Shard, partitionKey string) *Shard {
	return shardForHashKey(shards, hashKey(partitionKey))
}

// nextSequenceNumber generates a new sequence number for a shard.
func (s *Shard) nextSequenceNumber() string {
	s.NextSeq++

	return fmt.Sprintf("%020d", s.NextSeq)
}

// CreateStream creates a new Kinesis stream.
func (b *InMemoryBackend) CreateStream(ctx context.Context, input *CreateStreamInput) error {
	region := getRegion(ctx, b.region)
	if input.Region != "" {
		region = input.Region
	}

	b.mu.Lock("CreateStream")
	defer b.mu.Unlock()

	if !isValidStreamName(input.StreamName) {
		return ErrValidation
	}

	streams := b.streamsStore(region)
	if _, exists := streams[input.StreamName]; exists {
		return ErrStreamAlreadyExists
	}

	// Clamp shardCount to a safe range before using it for allocations or shard math.
	shardCount := input.ShardCount
	if shardCount <= 0 {
		shardCount = defaultShardCount
	}
	if shardCount > maxShardsPerStream {
		return ErrInvalidArgument
	}
	if shardCount > maxShardCount {
		shardCount = maxShardCount
	}

	maxHashKey := new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), maxHashKeyBits),
		big.NewInt(1),
	)
	shardRange := new(big.Int).Div(
		new(big.Int).Add(maxHashKey, big.NewInt(1)),
		big.NewInt(int64(shardCount)),
	)

	// No capacity hint — user-derived values in the make capacity position
	// trigger CodeQL go/slice-memory-allocation-excessive-size even after
	// clamping. shardCount is only used for the loop count below (safe).
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	shards := make([]*Shard, 0)
	for i := range shardCount {
		start := new(big.Int).Mul(shardRange, big.NewInt(int64(i)))
		var end *big.Int
		if i == shardCount-1 {
			end = maxHashKey
		} else {
			end = new(big.Int).Sub(
				new(big.Int).Mul(shardRange, big.NewInt(int64(i+1))),
				big.NewInt(1),
			)
		}

		shards = append(shards, &Shard{
			ID:                fmt.Sprintf("shardId-%012d", i),
			HashKeyRangeStart: start.String(),
			HashKeyRangeEnd:   end.String(),
		})
	}

	accountID := b.accountID
	if input.AccountID != "" {
		accountID = input.AccountID
	}

	streamMode := input.StreamMode
	if streamMode == "" {
		streamMode = streamModeProvisioned
	}

	if streamMode == streamModeOnDemand {
		onDemandCount := 0
		for _, s := range streams {
			if s.StreamMode == streamModeOnDemand {
				onDemandCount++
			}
		}
		if onDemandCount >= b.onDemandStreamCountLimit {
			return ErrLimitExceeded
		}
	}

	streamARN := arn.Build("kinesis", region, accountID, "stream/"+input.StreamName)

	streams[input.StreamName] = &Stream{
		Name:               input.StreamName,
		ARN:                streamARN,
		Status:             streamStatusActive,
		Shards:             shards,
		mu:                 newStreamLock(input.StreamName),
		Tags:               tags.New("kinesis.stream." + input.StreamName + ".tags"),
		CreatedAt:          time.Now(),
		RetentionPeriod:    defaultRetentionHours,
		Consumers:          make(map[string]*Consumer),
		StreamMode:         streamMode,
		MaxRecordSizeBytes: defaultMaxRecordSizeBytes,
	}

	return nil
}

// DeleteStream removes a stream.
func (b *InMemoryBackend) DeleteStream(ctx context.Context, input *DeleteStreamInput) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteStream")

	streams := b.streamsStore(region)
	stream, exists := streams[input.StreamName]
	if !exists {
		b.mu.Unlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("DeleteStream.stream")
	defer stream.mu.Unlock()

	if stream.Tags != nil {
		stream.Tags.Close()
	}

	// Mark the stream as deleting before removing it (AWS-realistic status transition).
	stream.Status = streamStatusDeleting
	delete(streams, input.StreamName)
	b.mu.Unlock()
	b.faultsMu.Lock("DeleteStream.faults")
	delete(b.faultsStore(region), input.StreamName)
	b.faultsMu.Unlock()

	if b.OnStreamPurged != nil {
		b.OnStreamPurged(input.StreamName)
	}

	// Release lockmetrics resources for the deleted stream to prevent memory leaks.
	stream.mu.Close()

	return nil
}

// DescribeStream returns full stream details including shards.
func (b *InMemoryBackend) DescribeStream(
	ctx context.Context,
	input *DescribeStreamInput,
) (*DescribeStreamOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeStream")

	stream, exists := b.streamsView(region)[input.StreamName]
	if !exists {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("DescribeStream.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	shards := make([]ShardDescription, len(stream.Shards))
	for i, s := range stream.Shards {
		shards[i] = shardDescription(s)
	}

	encType := stream.EncryptionType
	if encType == "" {
		encType = encryptionTypeNone
	}

	return &DescribeStreamOutput{
		StreamName:              stream.Name,
		StreamARN:               stream.ARN,
		StreamStatus:            stream.Status,
		Shards:                  shards,
		RetentionPeriodHours:    stream.RetentionPeriod,
		EncryptionType:          encType,
		KeyID:                   stream.KeyID,
		EnhancedMonitoring:      append([]string{}, stream.EnhancedMonitoring...),
		StreamCreationTimestamp: stream.CreatedAt,
		StreamMode:              stream.StreamMode,
	}, nil
}

// ListStreams returns stream names with optional pagination.
//
// AWS contract: results are returned in alphabetical order. When `Limit` is
// set the response contains at most that many names. Pagination is keyed on
// either `ExclusiveStartStreamName` or the opaque `NextToken` (which we treat
// as the previously returned last stream name) so that callers can iterate
// over arbitrarily large account inventories.
func (b *InMemoryBackend) ListStreams(ctx context.Context, input *ListStreamsInput) (*ListStreamsOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListStreams")
	defer b.mu.RUnlock()

	// AWS returns stream names in alphabetical order.
	names := sortedKeys(b.streamsView(region))

	// Apply pagination start point: prefer ExclusiveStartStreamName, then NextToken.
	start := input.ExclusiveStartStreamName
	if start == "" {
		start = input.NextToken
	}

	if start != "" {
		idx := sort.SearchStrings(names, start)
		// Skip the matched name itself; SearchStrings returns the insertion point
		// so equal entries land at idx — advance past it.
		if idx < len(names) && names[idx] == start {
			idx++
		}

		names = names[idx:]
	}

	limit := input.Limit
	if limit <= 0 {
		limit = len(names)
	}

	if limit > len(names) {
		limit = len(names)
	}

	page := names[:limit]
	hasMore := len(names) > limit

	var nextToken string
	if hasMore && len(page) > 0 {
		nextToken = page[len(page)-1]
	}

	return &ListStreamsOutput{
		StreamNames:    page,
		HasMoreStreams: hasMore,
		NextToken:      nextToken,
	}, nil
}

// PutRecord writes a single record to a stream shard.
func (b *InMemoryBackend) PutRecord(ctx context.Context, input *PutRecordInput) (*PutRecordOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("PutRecord")

	stream, exists := b.streamsView(region)[input.StreamName]
	if !exists {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("PutRecord.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if b.isThroughputFaultActive(region, input.StreamName) {
		return nil, ErrProvisionedThroughputExceeded
	}

	// Reject writes if the stream is not active (e.g. CREATING/DELETING).
	if stream.Status != streamStatusActive {
		return nil, ErrInvalidArgument
	}

	// Validate partition key length (AWS requires 1–256 chars).
	if len(input.PartitionKey) == 0 || len(input.PartitionKey) > maxPartitionKeyLen {
		return nil, ErrInvalidArgument
	}

	// Enforce per-record data size limit (default 1 MiB; updatable via UpdateMaxRecordSize).
	maxSize := stream.MaxRecordSizeBytes
	if maxSize <= 0 {
		maxSize = defaultMaxRecordSizeBytes
	}
	if len(input.Data) > maxSize {
		return nil, ErrInvalidArgument
	}

	if len(stream.Shards) == 0 {
		return nil, ErrInvalidArgument
	}

	var shard *Shard
	if input.ExplicitHashKey != "" {
		routingHash := new(big.Int)
		if _, ok := routingHash.SetString(input.ExplicitHashKey, hashKeyDecimalBase); !ok {
			return nil, ErrInvalidArgument
		}
		// Validate range [0, 2^128-1].
		maxHashKey := new(big.Int).Sub(
			new(big.Int).Lsh(big.NewInt(1), maxHashKeyBits),
			big.NewInt(1),
		)
		if routingHash.Sign() < 0 || routingHash.Cmp(maxHashKey) > 0 {
			return nil, ErrInvalidArgument
		}
		shard = shardForHashKey(stream.Shards, routingHash)
	} else {
		shard = shardForPartitionKey(stream.Shards, input.PartitionKey)
	}
	if shard == nil {
		return nil, ErrInvalidArgument
	}

	seq := shard.nextSequenceNumber()
	record := &Record{
		PartitionKey:                input.PartitionKey,
		Data:                        input.Data,
		SequenceNumber:              seq,
		ApproximateArrivalTimestamp: time.Now(),
	}

	shard.Records.push(record)

	enc := stream.EncryptionType
	if enc == "" {
		enc = encryptionTypeNone
	}

	return &PutRecordOutput{
		ShardID:        shard.ID,
		SequenceNumber: seq,
		EncryptionType: enc,
	}, nil
}

// putRecordErrorCode maps a per-record PutRecord error to the AWS error code string
// that should appear in a PutRecords result entry.
func putRecordErrorCode(err error) string {
	if errors.Is(err, ErrProvisionedThroughputExceeded) {
		return "ProvisionedThroughputExceededException"
	}
	if errors.Is(err, ErrInvalidArgument) {
		return "ValidationException"
	}

	return "InternalFailure"
}

// PutRecords writes multiple records to a stream.
func (b *InMemoryBackend) PutRecords(ctx context.Context, input *PutRecordsInput) (*PutRecordsOutput, error) {
	// AWS PutRecords caps a request at 500 records and 5 MiB total payload
	// (sum of partition-key + data bytes across every entry).
	const (
		maxRecordsPerRequest = 500
		maxBatchPayloadBytes = 5 * 1024 * 1024
	)

	if len(input.Records) > maxRecordsPerRequest {
		return nil, ErrInvalidArgument
	}

	totalBytes := 0
	for _, r := range input.Records {
		totalBytes += len(r.PartitionKey) + len(r.Data)
	}

	if totalBytes > maxBatchPayloadBytes {
		return nil, ErrInvalidArgument
	}

	results := make([]PutRecordsResultEntry, len(input.Records))
	failedCount := 0

	for i, entry := range input.Records {
		out, err := b.PutRecord(ctx, &PutRecordInput{
			StreamName:      input.StreamName,
			PartitionKey:    entry.PartitionKey,
			ExplicitHashKey: entry.ExplicitHashKey,
			Data:            entry.Data,
		})
		if err != nil {
			errCode := putRecordErrorCode(err)
			results[i] = PutRecordsResultEntry{
				ErrorCode:    errCode,
				ErrorMessage: err.Error(),
			}
			failedCount++
		} else {
			results[i] = PutRecordsResultEntry{
				ShardID:        out.ShardID,
				SequenceNumber: out.SequenceNumber,
			}
		}
	}

	return &PutRecordsOutput{
		Records:           results,
		FailedRecordCount: failedCount,
	}, nil
}

// iteratorToken encodes a shard iterator as a base64 JSON token.
func encodeIterator(it *ShardIterator) (string, error) {
	data, err := json.Marshal(it)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

// decodeIterator decodes a base64 JSON shard iterator token.
func decodeIterator(token string) (*ShardIterator, error) {
	data, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrShardIteratorExpired
	}

	var it ShardIterator
	if err = json.Unmarshal(data, &it); err != nil {
		return nil, ErrShardIteratorExpired
	}

	if !it.CreatedAt.IsZero() && time.Since(it.CreatedAt) > iteratorTTL {
		return nil, ErrShardIteratorExpired
	}

	return &it, nil
}

// GetShardIterator returns an iterator for reading records from a shard.
func (b *InMemoryBackend) GetShardIterator(
	ctx context.Context,
	input *GetShardIteratorInput,
) (*GetShardIteratorOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetShardIterator")

	stream, exists := b.streamsView(region)[input.StreamName]
	if !exists {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("GetShardIterator.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	// Find the shard
	shard := findShard(stream.Shards, input.ShardID)

	if shard == nil {
		return nil, ErrInvalidArgument
	}

	var position int

	switch input.ShardIteratorType {
	case iteratorTypeTrimHorizon:
		position = 0
	case iteratorTypeLatest:
		position = shard.Records.len()
	case iteratorTypeAtSequenceNumber, iteratorTypeAfterSequenceNumber:
		if input.StartingSequenceNumber == "" {
			return nil, ErrInvalidArgument
		}
		position = findSequencePosition(
			&shard.Records,
			input.StartingSequenceNumber,
			input.ShardIteratorType == iteratorTypeAfterSequenceNumber,
		)
	case iteratorTypeAtTimestamp:
		position = findTimestampPosition(&shard.Records, input.Timestamp)
	default:
		return nil, ErrInvalidArgument
	}

	it := &ShardIterator{
		StreamName:     input.StreamName,
		ShardID:        input.ShardID,
		Position:       position,
		SequenceNumber: input.StartingSequenceNumber,
		Region:         region,
		CreatedAt:      time.Now(),
	}

	token, err := encodeIterator(it)
	if err != nil {
		return nil, err
	}

	return &GetShardIteratorOutput{ShardIterator: token}, nil
}

// findShard returns the shard with the given ID from a slice, or nil if not found.
func findShard(shards []*Shard, shardID string) *Shard {
	for _, s := range shards {
		if s.ID == shardID {
			return s
		}
	}

	return nil
}

// GetRecords retrieves records starting at the given shard iterator position.
//
// The region is taken from the iterator token (encoded by GetShardIterator),
// not from ctx, so an iterator issued for one region always reads that region's
// records even if the GetRecords call carries a different ctx region.
func (b *InMemoryBackend) GetRecords(ctx context.Context, input *GetRecordsInput) (*GetRecordsOutput, error) {
	it, err := decodeIterator(input.ShardIterator)
	if err != nil {
		return nil, err
	}

	region := it.Region
	if region == "" {
		// Legacy token without an embedded region: fall back to the ctx region.
		region = getRegion(ctx, b.region)
	}

	b.mu.RLock("GetRecords")

	stream, exists := b.streamsView(region)[it.StreamName]
	if !exists {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("GetRecords.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	if b.isThroughputFaultActive(region, it.StreamName) {
		return nil, ErrProvisionedThroughputExceeded
	}

	shard := findShard(stream.Shards, it.ShardID)

	if shard == nil {
		return nil, ErrInvalidArgument
	}

	limit := input.Limit
	if limit <= 0 {
		limit = defaultGetRecordsLimit
	}

	if limit > maxGetRecordsLimit {
		limit = maxGetRecordsLimit
	}

	start := min(it.Position, shard.Records.len())

	end := min(start+limit, shard.Records.len())

	// Apply 10 MiB response cap: stop before end if accumulated payload exceeds the limit.
	totalBytes := 0
	actualEnd := start
	results := make([]GetRecordResult, 0, end-start)
	for i := start; i < end; i++ {
		r := shard.Records.at(i)
		recordBytes := len(r.Data)
		if totalBytes+recordBytes > maxGetRecordsResponseBytes && len(results) > 0 {
			break
		}
		totalBytes += recordBytes
		results = append(results, GetRecordResult{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: r.ApproximateArrivalTimestamp,
		})
		actualEnd = i + 1
	}

	// Advance iterator position
	newIt := &ShardIterator{
		StreamName: it.StreamName,
		ShardID:    it.ShardID,
		Region:     region,
		Position:   actualEnd,
	}

	// AWS returns an empty NextShardIterator when a shard has been closed
	// (due to MergeShards or SplitShard) and all records have been consumed.
	nextToken := ""
	if !shard.Closed || actualEnd < shard.Records.len() {
		var tokenErr error
		nextToken, tokenErr = encodeIterator(newIt)
		if tokenErr != nil {
			return nil, tokenErr
		}
	}

	// MillisBehindLatest is the age of the last record in the shard (tip of stream).
	millisBehind := int64(0)
	if actualEnd < shard.Records.len() {
		millisBehind = time.Since(shard.Records.last().ApproximateArrivalTimestamp).Milliseconds()
	}

	return &GetRecordsOutput{
		Records:            results,
		NextShardIterator:  nextToken,
		MillisBehindLatest: millisBehind,
	}, nil
}

// shardFilterIncludesAll reports whether the given ShardFilter value requests all shards
// shardDescription builds a ShardDescription from a Shard.
func shardDescription(s *Shard) ShardDescription {
	seqStart := "0"
	if s.Records.len() > 0 {
		seqStart = s.Records.at(0).SequenceNumber
	}

	// EndingSequenceNumber is reported only for CLOSED shards. AWS leaves it
	// absent on open shards regardless of whether they currently hold records —
	// KCL-style consumers treat its presence as the signal that a shard is
	// closed and they should advance to the child shards. Reporting it on an
	// open shard with records would make a consumer prematurely abandon the shard.
	var seqEnd string
	if s.Closed {
		seqEnd = "0"
		if s.Records.len() > 0 {
			seqEnd = s.Records.last().SequenceNumber
		}
	}

	return ShardDescription{
		ShardID:                  s.ID,
		HashKeyRangeStart:        s.HashKeyRangeStart,
		HashKeyRangeEnd:          s.HashKeyRangeEnd,
		SequenceNumberRangeStart: seqStart,
		SequenceNumberRangeEnd:   seqEnd,
		ParentShardID:            s.ParentShardID,
		AdjacentParentShardID:    s.AdjacentParentShardID,
		Closed:                   s.Closed,
	}
}

// listShardsAtShardID returns shards matching the AT_SHARD_ID filter.
func listShardsAtShardID(shards []*Shard, targetID string) []ShardDescription {
	result := make([]ShardDescription, 0, len(shards))
	for _, s := range shards {
		if s.ID != targetID && s.ParentShardID != targetID && s.AdjacentParentShardID != targetID {
			continue
		}
		result = append(result, shardDescription(s))
	}

	return result
}

// (open and closed). Used by ListShards.
func shardFilterIncludesAll(filter string) bool {
	return filter == "FROM_TRIM_HORIZON" || filter == "AT_TIMESTAMP" || filter == "FROM_TIMESTAMP"
}

// ListShards returns the shards for a stream.
func (b *InMemoryBackend) ListShards(ctx context.Context, input *ListShardsInput) (*ListShardsOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListShards")

	stream, exists := b.streamsView(region)[input.StreamName]
	if !exists {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("ListShards.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	if input.ShardFilterType == "AT_SHARD_ID" && input.ShardFilterShardID != "" {
		return &ListShardsOutput{Shards: listShardsAtShardID(stream.Shards, input.ShardFilterShardID)}, nil
	}

	includeAll := shardFilterIncludesAll(input.ShardFilter)

	// NextToken acts as an exclusive start shard ID for pagination.
	startShardID := input.ExclusiveStartShardID
	if startShardID == "" {
		startShardID = input.NextToken
	}

	skip := startShardID != ""
	result := make([]ShardDescription, 0, len(stream.Shards))

	for _, s := range stream.Shards {
		if skip {
			if s.ID == startShardID {
				skip = false
			}

			continue
		}

		// By default only open shards are returned. Include closed shards when
		// the caller requests FROM_TRIM_HORIZON or other inclusive filter types.
		if s.Closed && !includeAll {
			continue
		}

		result = append(result, shardDescription(s))
	}

	// Apply MaxResults pagination.
	if input.MaxResults > 0 && input.MaxResults < len(result) {
		nextToken := result[input.MaxResults-1].ShardID

		return &ListShardsOutput{Shards: result[:input.MaxResults], NextToken: nextToken}, nil
	}

	return &ListShardsOutput{Shards: result}, nil
}

// ListAll returns a snapshot of all streams as StreamInfo values across every
// region. It is used by the dashboard, which presents a global inventory.
func (b *InMemoryBackend) ListAll(_ context.Context) []StreamInfo {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()

	result := make([]StreamInfo, 0)
	for _, regionStreams := range b.streams {
		for _, s := range regionStreams {
			s.mu.RLock("ListAll.stream")
			result = append(result, StreamInfo{
				Name:       s.Name,
				ARN:        s.ARN,
				Status:     s.Status,
				ShardCount: len(s.Shards),
			})
			s.mu.RUnlock()
		}
	}

	return result
}

// isThroughputFaultActive reports whether a FIS throughput exception should be
// applied to the current request for the given stream name, using probability-based
// sampling when percentage < 100.
// The method lazily removes expired fault entries to prevent unbounded map growth.
func (b *InMemoryBackend) isThroughputFaultActive(region, streamName string) bool {
	b.faultsMu.Lock("isThroughputFaultActive")
	defer b.faultsMu.Unlock()

	regionFaults := b.fisThroughputFaults[region]
	fault, ok := regionFaults[streamName]
	if !ok || fault == nil {
		return false
	}

	if !fault.expiry.IsZero() && time.Now().After(fault.expiry) {
		// Lazily evict expired entry.
		delete(regionFaults, streamName)

		return false
	}

	if fault.probability <= 0 {
		return false
	}

	if fault.probability >= 1.0 {
		return true
	}

	//nolint:gosec // weak random is intentional for fault injection
	return rand.Float64() < fault.probability
}

// streamNameFromARN extracts the stream name from a Kinesis stream ARN.
// Stream ARN format: arn:aws:kinesis:{region}:{account}:stream/{name}.
func streamNameFromARN(streamARN string) string {
	parts := strings.Split(streamARN, ":")
	const arnResourceIdx = 5
	if len(parts) <= arnResourceIdx {
		return ""
	}

	return strings.TrimPrefix(parts[arnResourceIdx], "stream/")
}

// consumerInfoFromARN extracts the stream name and consumer name from a consumer ARN.
// Consumer ARN format: arn:aws:kinesis:{region}:{account}:stream/{stream}/consumer/{name}:{timestamp}.
func consumerInfoFromARN(consumerARN string) (string, string) {
	parts := strings.Split(consumerARN, ":")
	const arnConsumerResourceIdx = 5
	if len(parts) <= arnConsumerResourceIdx {
		return "", ""
	}

	resourcePath := parts[arnConsumerResourceIdx]
	segments := strings.Split(resourcePath, "/")
	// segments: ["stream", "{streamName}", "consumer", "{consumerName}"]
	const expectedSegments = 4
	if len(segments) < expectedSegments {
		return "", ""
	}

	return segments[1], segments[3]
}

// buildConsumerARN builds a Kinesis consumer ARN from stream ARN, consumer name, and creation timestamp.
func buildConsumerARN(streamARN, consumerName string, createdAt time.Time) string {
	return fmt.Sprintf("%s/consumer/%s:%d", streamARN, consumerName, createdAt.Unix())
}

// uniqueStrings returns a deduplicated copy of ss, preserving order.
func uniqueStrings(ss []string) []string {
	seen := make(map[string]struct{}, len(ss))
	out := make([]string, 0, len(ss))

	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}

	return out
}

// removeStrings returns a copy of ss with all elements in remove deleted.
func removeStrings(ss, remove []string) []string {
	removeSet := make(map[string]struct{}, len(remove))
	for _, s := range remove {
		removeSet[s] = struct{}{}
	}

	out := make([]string, 0, len(ss))

	for _, s := range ss {
		if _, ok := removeSet[s]; !ok {
			out = append(out, s)
		}
	}

	return out
}

// RegisterStreamConsumer registers a new enhanced fan-out consumer on a stream.
func (b *InMemoryBackend) RegisterStreamConsumer(
	ctx context.Context,
	input *RegisterStreamConsumerInput,
) (*RegisterStreamConsumerOutput, error) {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.RLock("RegisterStreamConsumer")

	streamName := streamNameFromARN(input.StreamARN)
	stream, ok := b.streamsView(region)[streamName]

	if !ok {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("RegisterStreamConsumer.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if stream.Consumers == nil {
		stream.Consumers = make(map[string]*Consumer)
	}

	if !isValidConsumerName(input.ConsumerName) {
		return nil, ErrInvalidArgument
	}

	if _, exists := stream.Consumers[input.ConsumerName]; exists {
		return nil, ErrConsumerAlreadyExists
	}

	now := time.Now()
	consumerARN := buildConsumerARN(input.StreamARN, input.ConsumerName, now)

	consumer := &Consumer{
		ConsumerName:              input.ConsumerName,
		ConsumerARN:               consumerARN,
		ConsumerStatus:            consumerStatusActive,
		ConsumerCreationTimestamp: now,
		StreamARN:                 input.StreamARN,
	}
	stream.Consumers[input.ConsumerName] = consumer

	return &RegisterStreamConsumerOutput{Consumer: *consumer}, nil
}

// DescribeStreamConsumer returns details about a registered consumer.
// Lookup is by ConsumerARN, or by StreamARN + ConsumerName.
func (b *InMemoryBackend) DescribeStreamConsumer(
	ctx context.Context,
	input *DescribeStreamConsumerInput,
) (*DescribeStreamConsumerOutput, error) {
	var sName string
	var cName string
	var arnForRegion string
	if input.ConsumerARN != "" {
		sName, cName = consumerInfoFromARN(input.ConsumerARN)
		arnForRegion = input.ConsumerARN
	} else {
		sName = streamNameFromARN(input.StreamARN)
		cName = input.ConsumerName
		arnForRegion = input.StreamARN
	}

	region := regionFromARNOrCtx(ctx, arnForRegion, b.region)

	b.mu.RLock("DescribeStreamConsumer")
	stream, ok := b.streamsView(region)[sName]
	if !ok {
		b.mu.RUnlock()
		if input.ConsumerARN != "" {
			return nil, ErrConsumerNotFound
		}

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("DescribeStreamConsumer.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	consumer, ok := stream.Consumers[cName]
	if !ok {
		return nil, ErrConsumerNotFound
	}

	return &DescribeStreamConsumerOutput{ConsumerDescription: *consumer}, nil
}

// ListStreamConsumers lists all registered consumers for a stream.
func (b *InMemoryBackend) ListStreamConsumers(
	ctx context.Context,
	input *ListStreamConsumersInput,
) (*ListStreamConsumersOutput, error) {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.RLock("ListStreamConsumers")

	streamName := streamNameFromARN(input.StreamARN)
	stream, ok := b.streamsView(region)[streamName]

	if !ok {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("ListStreamConsumers.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	consumers := make([]Consumer, 0, len(stream.Consumers))
	for _, c := range stream.Consumers {
		consumers = append(consumers, *c)
	}

	// Sort for deterministic ordering.
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].ConsumerName < consumers[j].ConsumerName
	})

	// Apply NextToken as exclusive start consumer name.
	if input.NextToken != "" {
		start := 0
		for start < len(consumers) && consumers[start].ConsumerName <= input.NextToken {
			start++
		}
		consumers = consumers[start:]
	}

	// Apply MaxResults cap.
	var nextToken string
	if input.MaxResults > 0 && input.MaxResults < len(consumers) {
		nextToken = consumers[input.MaxResults-1].ConsumerName
		consumers = consumers[:input.MaxResults]
	}

	return &ListStreamConsumersOutput{Consumers: consumers, NextToken: nextToken}, nil
}

// DeregisterStreamConsumer removes a registered consumer from a stream.
func (b *InMemoryBackend) DeregisterStreamConsumer(ctx context.Context, input *DeregisterStreamConsumerInput) error {
	sName, cName := func() (string, string) {
		if input.ConsumerARN != "" {
			return consumerInfoFromARN(input.ConsumerARN)
		}

		return streamNameFromARN(input.StreamARN), input.ConsumerName
	}()

	arnForRegion := input.StreamARN
	if input.ConsumerARN != "" {
		arnForRegion = input.ConsumerARN
	}
	region := regionFromARNOrCtx(ctx, arnForRegion, b.region)

	b.mu.RLock("DeregisterStreamConsumer")
	stream, ok := b.streamsView(region)[sName]
	if !ok {
		b.mu.RUnlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("DeregisterStreamConsumer.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if _, ok = stream.Consumers[cName]; !ok {
		return ErrConsumerNotFound
	}

	delete(stream.Consumers, cName)

	return nil
}

// SubscribeToShard delivers records from a shard to an enhanced fan-out consumer.
// For mock purposes this is a single-shot delivery of all available records.
func (b *InMemoryBackend) SubscribeToShard(
	ctx context.Context,
	input *SubscribeToShardInput,
) (*SubscribeToShardOutput, error) {
	sName, cName := consumerInfoFromARN(input.ConsumerARN)
	region := regionFromARNOrCtx(ctx, input.ConsumerARN, b.region)

	b.mu.RLock("SubscribeToShard")

	stream, ok := b.streamsView(region)[sName]
	if !ok {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("SubscribeToShard.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	if _, exists := stream.Consumers[cName]; !exists {
		return nil, ErrConsumerNotFound
	}

	shard := findShard(stream.Shards, input.ShardID)
	if shard == nil {
		return nil, ErrInvalidArgument
	}

	var startPos int

	switch input.StartingPosition.Type {
	case iteratorTypeTrimHorizon:
		startPos = 0
	case iteratorTypeLatest:
		startPos = shard.Records.len()
	case iteratorTypeAtSequenceNumber:
		startPos = findSequencePosition(&shard.Records, input.StartingPosition.SequenceNumber, false)
	case iteratorTypeAfterSequenceNumber:
		startPos = findSequencePosition(&shard.Records, input.StartingPosition.SequenceNumber, true)
	case iteratorTypeAtTimestamp:
		ts := time.Time{}
		if input.StartingPosition.Timestamp != nil {
			ts = *input.StartingPosition.Timestamp
		}

		startPos = findTimestampPosition(&shard.Records, ts)
	default:
		return nil, ErrInvalidArgument
	}

	n := shard.Records.len()
	records := make([]GetRecordResult, 0, n-startPos)

	for i := startPos; i < n; i++ {
		r := shard.Records.at(i)
		records = append(records, GetRecordResult{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: r.ApproximateArrivalTimestamp,
		})
	}

	var continuationSeq string
	if len(records) > 0 {
		continuationSeq = records[len(records)-1].SequenceNumber
	}

	millisBehind := int64(0)
	if n > 0 && startPos < n {
		millisBehind = time.Since(shard.Records.last().ApproximateArrivalTimestamp).Milliseconds()
	}

	return &SubscribeToShardOutput{
		Event: SubscribeToShardEvent{
			Records:                    records,
			ContinuationSequenceNumber: continuationSeq,
			MillisBehindLatest:         millisBehind,
		},
	}, nil
}

// UpdateShardCount resizes a stream to the given number of shards.
// Existing records in the stream are not migrated; new shards start empty.
func (b *InMemoryBackend) UpdateShardCount(
	ctx context.Context,
	input *UpdateShardCountInput,
) (*UpdateShardCountOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateShardCount")
	defer b.mu.Unlock()

	stream, ok := b.streamsStore(region)[input.StreamName]
	if !ok {
		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("UpdateShardCount.stream")
	defer stream.mu.Unlock()

	if stream.StreamMode == streamModeOnDemand {
		return nil, ErrInvalidArgument
	}

	if input.TargetShardCount <= 0 {
		return nil, ErrInvalidArgument
	}

	if input.ScalingType != "" && input.ScalingType != scalingTypeUniformScaling {
		return nil, ErrInvalidArgument
	}

	// Count only open shards as the current count (AWS semantics).
	currentCount := 0
	for _, s := range stream.Shards {
		if !s.Closed {
			currentCount++
		}
	}
	targetCount := input.TargetShardCount

	maxHashKey := new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), maxHashKeyBits),
		big.NewInt(1),
	)
	shardRange := new(big.Int).Div(
		new(big.Int).Add(maxHashKey, big.NewInt(1)),
		big.NewInt(int64(targetCount)),
	)

	// Assign new shard IDs from beyond the existing maximum to avoid collisions.
	startIdx := nextShardIDIndex(stream.Shards)

	newShards := make([]*Shard, targetCount)
	for i := range targetCount {
		start := new(big.Int).Mul(shardRange, big.NewInt(int64(i)))

		var end *big.Int
		if i == targetCount-1 {
			end = maxHashKey
		} else {
			end = new(big.Int).Sub(
				new(big.Int).Mul(shardRange, big.NewInt(int64(i+1))),
				big.NewInt(1),
			)
		}

		newShards[i] = &Shard{
			ID:                fmt.Sprintf("shardId-%012d", startIdx+i),
			HashKeyRangeStart: start.String(),
			HashKeyRangeEnd:   end.String(),
		}
	}

	// Mark all currently open shards as CLOSED (AWS semantics: old shards
	// remain visible in DescribeStream/ListShards with CLOSED status).
	for _, s := range stream.Shards {
		if !s.Closed {
			s.Closed = true
		}
	}

	allShards := make([]*Shard, 0, len(stream.Shards)+targetCount)
	allShards = append(allShards, stream.Shards...)
	allShards = append(allShards, newShards...)
	stream.Shards = allShards

	return &UpdateShardCountOutput{
		StreamName:        input.StreamName,
		CurrentShardCount: currentCount,
		TargetShardCount:  targetCount,
	}, nil
}

// EnableEnhancedMonitoring adds shard-level metrics to a stream.
func (b *InMemoryBackend) EnableEnhancedMonitoring(
	ctx context.Context,
	input *EnableEnhancedMonitoringInput,
) (*EnableEnhancedMonitoringOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("EnableEnhancedMonitoring")
	defer b.mu.Unlock()

	stream, ok := b.streamsStore(region)[input.StreamName]
	if !ok {
		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("EnableEnhancedMonitoring.stream")
	defer stream.mu.Unlock()

	current := make([]string, len(stream.EnhancedMonitoring))
	copy(current, stream.EnhancedMonitoring)

	combined := make([]string, 0, len(current)+len(input.ShardLevelMetrics))
	combined = append(combined, current...)
	combined = append(combined, input.ShardLevelMetrics...)
	desired := uniqueStrings(combined)
	stream.EnhancedMonitoring = desired

	return &EnableEnhancedMonitoringOutput{
		StreamName:               stream.Name,
		CurrentShardLevelMetrics: current,
		DesiredShardLevelMetrics: desired,
	}, nil
}

// DisableEnhancedMonitoring removes shard-level metrics from a stream.
func (b *InMemoryBackend) DisableEnhancedMonitoring(
	ctx context.Context,
	input *DisableEnhancedMonitoringInput,
) (*DisableEnhancedMonitoringOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DisableEnhancedMonitoring")
	defer b.mu.Unlock()

	stream, ok := b.streamsStore(region)[input.StreamName]
	if !ok {
		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("DisableEnhancedMonitoring.stream")
	defer stream.mu.Unlock()

	current := make([]string, len(stream.EnhancedMonitoring))
	copy(current, stream.EnhancedMonitoring)

	desired := removeStrings(current, input.ShardLevelMetrics)
	stream.EnhancedMonitoring = desired

	return &DisableEnhancedMonitoringOutput{
		StreamName:               stream.Name,
		CurrentShardLevelMetrics: current,
		DesiredShardLevelMetrics: desired,
	}, nil
}

// IncreaseStreamRetentionPeriod increases the retention period for a stream.
// If the new value equals the current retention period the call is a no-op
// and returns success — this matches the idempotent behaviour expected by the
// AWS Terraform provider, which may call this with the default value (24h) even
// on freshly created streams. The new value must not exceed maxRetentionHours.
func (b *InMemoryBackend) IncreaseStreamRetentionPeriod(
	ctx context.Context,
	input *IncreaseStreamRetentionPeriodInput,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("IncreaseStreamRetentionPeriod")
	defer b.mu.Unlock()

	stream, ok := b.streamsStore(region)[input.StreamName]
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("IncreaseStreamRetentionPeriod.stream")
	defer stream.mu.Unlock()

	// Idempotent: same value → no-op.
	if input.RetentionPeriodHours == stream.RetentionPeriod {
		return nil
	}

	if input.RetentionPeriodHours < minRetentionHours ||
		input.RetentionPeriodHours < stream.RetentionPeriod ||
		input.RetentionPeriodHours > maxRetentionHours {
		return ErrInvalidArgument
	}

	stream.RetentionPeriod = input.RetentionPeriodHours

	return nil
}

// DecreaseStreamRetentionPeriod decreases the retention period for a stream.
// If the new value equals the current retention period the call is a no-op
// and returns success. The new value must be at least minRetentionHours.
func (b *InMemoryBackend) DecreaseStreamRetentionPeriod(
	ctx context.Context,
	input *DecreaseStreamRetentionPeriodInput,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DecreaseStreamRetentionPeriod")
	defer b.mu.Unlock()

	stream, ok := b.streamsStore(region)[input.StreamName]
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("DecreaseStreamRetentionPeriod.stream")
	defer stream.mu.Unlock()

	// Idempotent: same value → no-op.
	if input.RetentionPeriodHours == stream.RetentionPeriod {
		return nil
	}

	if input.RetentionPeriodHours > stream.RetentionPeriod ||
		input.RetentionPeriodHours < minRetentionHours {
		return ErrInvalidArgument
	}

	stream.RetentionPeriod = input.RetentionPeriodHours

	return nil
}

// nextShardIDIndex returns the numeric index for the next unique shard ID.
func nextShardIDIndex(shards []*Shard) int {
	maxIdx := len(shards) // safe lower bound
	for _, s := range shards {
		var n int
		if _, err := fmt.Sscanf(s.ID, "shardId-%012d", &n); err == nil && n+1 > maxIdx {
			maxIdx = n + 1
		}
	}

	return maxIdx
}

// nextShardID computes the next unique shard ID for a stream by finding the
// maximum existing shard index and incrementing it. This ensures IDs remain
// unique across merge/split operations even after shards are removed.
func nextShardID(shards []*Shard) string {
	return fmt.Sprintf("shardId-%012d", nextShardIDIndex(shards))
}

// MergeShards merges two adjacent shards into one.
// The merged shard spans the combined hash key range of both parent shards.
func (b *InMemoryBackend) MergeShards(ctx context.Context, input *MergeShardsInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("MergeShards")
	defer b.mu.Unlock()

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streamsStore(region)[streamName]
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("MergeShards.stream")
	defer stream.mu.Unlock()

	if stream.StreamMode == streamModeOnDemand {
		return ErrInvalidArgument
	}

	shard1 := findShard(stream.Shards, input.ShardToMerge)
	shard2 := findShard(stream.Shards, input.AdjacentShardToMerge)

	if shard1 == nil || shard2 == nil {
		return ErrInvalidArgument
	}

	// Determine the merged range: min start, max end.
	s1Start := new(big.Int)
	s1Start.SetString(shard1.HashKeyRangeStart, hashKeyDecimalBase)
	s2Start := new(big.Int)
	s2Start.SetString(shard2.HashKeyRangeStart, hashKeyDecimalBase)
	s1End := new(big.Int)
	s1End.SetString(shard1.HashKeyRangeEnd, hashKeyDecimalBase)
	s2End := new(big.Int)
	s2End.SetString(shard2.HashKeyRangeEnd, hashKeyDecimalBase)

	s1EndPlusOne := new(big.Int).Add(s1End, big.NewInt(1))
	s2EndPlusOne := new(big.Int).Add(s2End, big.NewInt(1))
	if s1EndPlusOne.Cmp(s2Start) != 0 && s2EndPlusOne.Cmp(s1Start) != 0 {
		return ErrInvalidArgument
	}

	startKey := s1Start
	if s2Start.Cmp(s1Start) < 0 {
		startKey = s2Start
	}

	endKey := s1End
	if s2End.Cmp(s1End) > 0 {
		endKey = s2End
	}

	mergedID := nextShardID(stream.Shards)
	merged := &Shard{
		ID:                mergedID,
		HashKeyRangeStart: startKey.String(),
		HashKeyRangeEnd:   endKey.String(),
	}

	// Mark parents as closed (keep them in the list)
	shard1.Closed = true
	shard2.Closed = true

	merged.ParentShardID = input.ShardToMerge
	merged.AdjacentParentShardID = input.AdjacentShardToMerge

	newShards := make([]*Shard, 0, len(stream.Shards)+1)
	newShards = append(newShards, stream.Shards...)
	newShards = append(newShards, merged)
	stream.Shards = newShards

	return nil
}

// SplitShard splits a shard into two at the given new starting hash key.
func (b *InMemoryBackend) SplitShard(ctx context.Context, input *SplitShardInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("SplitShard")
	defer b.mu.Unlock()

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streamsStore(region)[streamName]
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("SplitShard.stream")
	defer stream.mu.Unlock()

	if stream.StreamMode == streamModeOnDemand {
		return ErrInvalidArgument
	}

	shard := findShard(stream.Shards, input.ShardToSplit)
	if shard == nil {
		return ErrInvalidArgument
	}

	newStart := new(big.Int)
	if _, valid := newStart.SetString(input.NewStartingHashKey, hashKeyDecimalBase); !valid {
		return ErrInvalidArgument
	}

	shardStart := new(big.Int)
	shardStart.SetString(shard.HashKeyRangeStart, hashKeyDecimalBase)
	shardEnd := new(big.Int)
	shardEnd.SetString(shard.HashKeyRangeEnd, hashKeyDecimalBase)

	// NewStartingHashKey must be strictly inside the shard's range.
	if newStart.Cmp(shardStart) <= 0 || newStart.Cmp(shardEnd) >= 0 {
		return ErrInvalidArgument
	}

	shard1End := new(big.Int).Sub(newStart, big.NewInt(1))

	shard1ID := nextShardID(stream.Shards)

	var shard1Idx int
	if _, err := fmt.Sscanf(shard1ID, "shardId-%012d", &shard1Idx); err != nil {
		// nextShardID guarantees the format; this path is unreachable in practice.
		shard1Idx = len(stream.Shards)
	}

	shard2ID := fmt.Sprintf("shardId-%012d", shard1Idx+1)
	shard1 := &Shard{
		ID:                shard1ID,
		HashKeyRangeStart: shard.HashKeyRangeStart,
		HashKeyRangeEnd:   shard1End.String(),
	}
	shard2 := &Shard{
		ID:                shard2ID,
		HashKeyRangeStart: input.NewStartingHashKey,
		HashKeyRangeEnd:   shard.HashKeyRangeEnd,
	}

	shard.Closed = true

	shard1.ParentShardID = input.ShardToSplit
	shard2.ParentShardID = input.ShardToSplit

	// splitShardResultCount is the number of new shards produced by a split.
	const splitShardResultCount = 2

	newShards := make([]*Shard, 0, len(stream.Shards)+splitShardResultCount)
	newShards = append(newShards, stream.Shards...)
	newShards = append(newShards, shard1, shard2)
	stream.Shards = newShards

	return nil
}

// StartStreamEncryption enables server-side encryption on a stream.
func (b *InMemoryBackend) StartStreamEncryption(ctx context.Context, input *StartStreamEncryptionInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("StartStreamEncryption")
	defer b.mu.Unlock()

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streamsStore(region)[streamName]
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("StartStreamEncryption.stream")
	defer stream.mu.Unlock()

	if input.EncryptionType != encryptionTypeKMS {
		return ErrInvalidArgument
	}

	stream.EncryptionType = input.EncryptionType
	stream.KeyID = input.KeyID

	return nil
}

// StopStreamEncryption disables server-side encryption on a stream.
func (b *InMemoryBackend) StopStreamEncryption(ctx context.Context, input *StopStreamEncryptionInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("StopStreamEncryption")
	defer b.mu.Unlock()

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streamsStore(region)[streamName]
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("StopStreamEncryption.stream")
	defer stream.mu.Unlock()

	stream.EncryptionType = encryptionTypeNone
	stream.KeyID = ""

	return nil
}

// PutResourcePolicy stores a resource-based policy for the given stream or consumer ARN.
func (b *InMemoryBackend) PutResourcePolicy(ctx context.Context, input *PutResourcePolicyInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.policiesStore(region)[input.ResourceARN] = input.Policy

	return nil
}

// GetResourcePolicy retrieves the resource-based policy for the given stream or consumer ARN.
func (b *InMemoryBackend) GetResourcePolicy(
	ctx context.Context,
	input *GetResourcePolicyInput,
) (*GetResourcePolicyOutput, error) {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	policy, ok := b.resourcePolicies[region][input.ResourceARN]
	if !ok {
		return nil, ErrResourcePolicyNotFound
	}

	return &GetResourcePolicyOutput{Policy: policy}, nil
}

// DeleteResourcePolicy removes the resource-based policy for the given stream or consumer ARN.
func (b *InMemoryBackend) DeleteResourcePolicy(ctx context.Context, input *DeleteResourcePolicyInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	regionPolicies := b.resourcePolicies[region]
	if _, ok := regionPolicies[input.ResourceARN]; !ok {
		return ErrResourcePolicyNotFound
	}

	delete(regionPolicies, input.ResourceARN)

	return nil
}

// ListTagsForResource returns the tags associated with a stream identified by its ARN.
// Tags are those stored on the stream's internal Tags store (set via TagResource).
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	input *ListTagsForResourceInput,
) (*ListTagsForResourceOutput, error) {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	b.mu.RLock("ListTagsForResource")

	streamName := streamNameFromARN(input.ResourceARN)
	stream, ok := b.streamsView(region)[streamName]

	if !ok {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("ListTagsForResource.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	result := map[string]string{}
	if stream.Tags != nil {
		result = stream.Tags.Clone()
	}

	return &ListTagsForResourceOutput{Tags: result}, nil
}

// DescribeAccountSettings returns account-level limits for this Kinesis account.
// The ON_DEMAND stream count is reported per region (AWS account-level limits
// are tracked per region), using the region carried on ctx.
func (b *InMemoryBackend) DescribeAccountSettings(ctx context.Context) (*DescribeAccountSettingsOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAccountSettings")
	defer b.mu.RUnlock()

	onDemandCount := 0
	for _, s := range b.streamsView(region) {
		s.mu.RLock("DescribeAccountSettings.stream")
		if s.StreamMode == streamModeOnDemand {
			onDemandCount++
		}
		s.mu.RUnlock()
	}

	return &DescribeAccountSettingsOutput{
		ShardLimit:               kinesisDefaultShardLimit,
		OnDemandStreamCount:      onDemandCount,
		OnDemandStreamCountLimit: b.onDemandStreamCountLimit,
	}, nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, regionStreams := range b.streams {
		for _, stream := range regionStreams {
			stream.mu.Lock("Reset.stream")
			if stream.Tags != nil {
				stream.Tags.Close()
			}
			stream.mu.Unlock()
		}
	}

	b.streams = make(map[string]map[string]*Stream)
	b.faultsMu.Lock("Reset.faults")
	b.fisThroughputFaults = make(map[string]map[string]*kinesisThrottleFault)
	b.faultsMu.Unlock()
	b.resourcePolicies = make(map[string]map[string]string)
}

// purgeStreamEntry removes s from the given region's stream map when it predates
// cutoff, or evicts its stale consumers otherwise. Must be called with b.mu held.
// Returns true when the stream was removed.
func (b *InMemoryBackend) purgeStreamEntry(region, name string, s *Stream, cutoff time.Time) bool {
	s.mu.Lock("Purge.stream")
	defer s.mu.Unlock()

	if s.CreatedAt.Before(cutoff) {
		if s.Tags != nil {
			s.Tags.Close()
		}
		delete(b.streamsStore(region), name)
		b.faultsMu.Lock("Purge.faults")
		delete(b.faultsStore(region), name)
		b.faultsMu.Unlock()

		return true
	}

	// Stream is still live; evict stale consumers only.
	for cName, c := range s.Consumers {
		if c.ConsumerCreationTimestamp.Before(cutoff) {
			delete(s.Consumers, cName)
		}
	}

	return false
}

// fireStreamPurgedCallbacks invokes OnStreamPurged for every name, without holding any lock.
func (b *InMemoryBackend) fireStreamPurgedCallbacks(names []string) {
	if b.OnStreamPurged == nil {
		return
	}
	for _, name := range names {
		b.OnStreamPurged(name)
	}
}

// Purge removes all Kinesis streams and consumers created before the cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	var purgedNames []string

	b.mu.Lock("Purge")
	for region, regionStreams := range b.streams {
		if ctx.Err() != nil {
			break
		}
		for name, s := range regionStreams {
			if ctx.Err() != nil {
				break
			}
			if b.purgeStreamEntry(region, name, s, cutoff) {
				purgedNames = append(purgedNames, name)
			}
		}
	}
	b.mu.Unlock()

	b.fireStreamPurgedCallbacks(purgedNames)
}

// CountOpenShards returns the total number of open (non-closed) shards across
// every stream in the region carried on ctx. DescribeLimits is region-scoped in
// AWS, so this counts within a single region.
func (b *InMemoryBackend) CountOpenShards(ctx context.Context) int {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CountOpenShards")
	defer b.mu.RUnlock()

	count := 0
	for _, s := range b.streamsView(region) {
		s.mu.RLock("CountOpenShards.stream")
		for _, sh := range s.Shards {
			if !sh.Closed {
				count++
			}
		}
		s.mu.RUnlock()
	}

	return count
}

// UpdateAccountSettings updates account-level settings such as the ON_DEMAND stream count limit.
func (b *InMemoryBackend) UpdateAccountSettings(_ context.Context, input *UpdateAccountSettingsInput) error {
	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	if input.OnDemandStreamCountLimit < 0 {
		return ErrInvalidArgument
	}

	if input.OnDemandStreamCountLimit > 0 {
		b.onDemandStreamCountLimit = input.OnDemandStreamCountLimit
	}

	return nil
}

// UpdateMaxRecordSize changes the per-record data payload size limit for a stream.
// The value must be between defaultMaxRecordSizeBytes (1 MiB) and
// absoluteMaxRecordSizeBytes (10 MiB).
func (b *InMemoryBackend) UpdateMaxRecordSize(ctx context.Context, input *UpdateMaxRecordSizeInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.RLock("UpdateMaxRecordSize")

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streamsView(region)[streamName]
	if !ok {
		b.mu.RUnlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("UpdateMaxRecordSize.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if input.MaxRecordSizeBytes < defaultMaxRecordSizeBytes || input.MaxRecordSizeBytes > absoluteMaxRecordSizeBytes {
		return ErrInvalidArgument
	}

	stream.MaxRecordSizeBytes = input.MaxRecordSizeBytes

	return nil
}

// UpdateStreamWarmThroughput configures pre-warmed throughput for a stream.
// This is a no-op in the in-memory backend (no actual warm-up is needed).
func (b *InMemoryBackend) UpdateStreamWarmThroughput(
	ctx context.Context,
	input *UpdateStreamWarmThroughputInput,
) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.RLock("UpdateStreamWarmThroughput")

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	_, ok := b.streamsView(region)[streamName]
	b.mu.RUnlock()

	if !ok {
		return ErrStreamNotFound
	}

	return nil
}

// TagResource adds or updates tags on a stream identified by its ARN.
// This is the ARN-based counterpart to AddTagsToStream.
func (b *InMemoryBackend) TagResource(ctx context.Context, input *TagResourceInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	b.mu.RLock("TagResource")

	streamName := streamNameFromARN(input.ResourceARN)
	stream, ok := b.streamsView(region)[streamName]

	if !ok {
		b.mu.RUnlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("TagResource.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if stream.Tags == nil {
		stream.Tags = tags.New("kinesis.stream." + streamName + ".tags")
	}

	stream.Tags.Merge(input.Tags)

	return nil
}

// UntagResource removes tags from a stream identified by its ARN.
// This is the ARN-based counterpart to RemoveTagsFromStream.
func (b *InMemoryBackend) UntagResource(ctx context.Context, input *UntagResourceInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	b.mu.RLock("UntagResource")

	streamName := streamNameFromARN(input.ResourceARN)
	stream, ok := b.streamsView(region)[streamName]

	if !ok {
		b.mu.RUnlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("UntagResource.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if stream.Tags != nil {
		stream.Tags.DeleteKeys(input.TagKeys)
	}

	return nil
}

// AddStreamInternal seeds a stream directly into the backend for testing.
// Caller must provide a non-nil stream with at least Name and ARN set. The
// stream is placed in the region encoded in its ARN, falling back to the
// backend's default region when the ARN carries none.
func (b *InMemoryBackend) AddStreamInternal(stream *Stream) {
	b.mu.Lock("AddStreamInternal")
	defer b.mu.Unlock()

	initializeStreamRuntime(stream, stream.Name)

	region := regionFromARN(stream.ARN)
	if region == "" {
		region = b.region
	}

	b.streamsStore(region)[stream.Name] = stream
}

// UpdateStreamMode changes the mode of a stream identified by its ARN.
func (b *InMemoryBackend) UpdateStreamMode(ctx context.Context, input *UpdateStreamModeInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("UpdateStreamMode")
	defer b.mu.Unlock()

	streamName := streamNameFromARN(input.StreamARN)
	stream, ok := b.streamsStore(region)[streamName]
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("UpdateStreamMode.stream")
	defer stream.mu.Unlock()

	if input.StreamModeDetails.StreamMode != streamModeProvisioned &&
		input.StreamModeDetails.StreamMode != streamModeOnDemand {
		return ErrInvalidArgument
	}

	stream.StreamMode = input.StreamModeDetails.StreamMode

	return nil
}

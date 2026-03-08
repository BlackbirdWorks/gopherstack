package kinesis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// StorageBackend defines the interface for a Kinesis backend.
type StorageBackend interface {
	CreateStream(input *CreateStreamInput) error
	DeleteStream(input *DeleteStreamInput) error
	DescribeStream(input *DescribeStreamInput) (*DescribeStreamOutput, error)
	ListStreams(input *ListStreamsInput) (*ListStreamsOutput, error)
	PutRecord(input *PutRecordInput) (*PutRecordOutput, error)
	PutRecords(input *PutRecordsInput) (*PutRecordsOutput, error)
	GetShardIterator(input *GetShardIteratorInput) (*GetShardIteratorOutput, error)
	GetRecords(input *GetRecordsInput) (*GetRecordsOutput, error)
	ListShards(input *ListShardsInput) (*ListShardsOutput, error)
	RegisterStreamConsumer(input *RegisterStreamConsumerInput) (*RegisterStreamConsumerOutput, error)
	DescribeStreamConsumer(input *DescribeStreamConsumerInput) (*DescribeStreamConsumerOutput, error)
	ListStreamConsumers(input *ListStreamConsumersInput) (*ListStreamConsumersOutput, error)
	DeregisterStreamConsumer(input *DeregisterStreamConsumerInput) error
	SubscribeToShard(input *SubscribeToShardInput) (*SubscribeToShardOutput, error)
	UpdateShardCount(input *UpdateShardCountInput) (*UpdateShardCountOutput, error)
	EnableEnhancedMonitoring(input *EnableEnhancedMonitoringInput) (*EnableEnhancedMonitoringOutput, error)
	DisableEnhancedMonitoring(input *DisableEnhancedMonitoringInput) (*DisableEnhancedMonitoringOutput, error)
	ListAll() []StreamInfo
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	streams             map[string]*Stream
	fisThroughputFaults map[string]time.Time // keyed by stream name; value is expiry (zero = no expiry)
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new empty InMemoryBackend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		streams:             make(map[string]*Stream),
		fisThroughputFaults: make(map[string]time.Time),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("kinesis"),
	}
}

// hashKey computes a numeric hash key for a partition key using a simple mapping.
// The result is in the range [0, 2^128-1] as required by Kinesis.
func hashKey(partitionKey string) *big.Int {
	// Use a simple deterministic hash by interpreting the UUID v5 of the partition key.
	sum := uuid.NewSHA1(uuid.NameSpaceOID, []byte(partitionKey))

	return new(big.Int).SetBytes(sum[:])
}

// shardForPartitionKey selects a shard index for the given partition key by hash.
func shardForPartitionKey(shards []*Shard, partitionKey string) int {
	if len(shards) == 0 {
		return 0
	}

	h := hashKey(partitionKey)
	idx := new(big.Int).Mod(h, big.NewInt(int64(len(shards))))

	return int(idx.Int64())
}

// nextSequenceNumber generates a new sequence number for a shard.
func (s *Shard) nextSequenceNumber() string {
	s.nextSeq++

	return fmt.Sprintf("%020d", s.nextSeq)
}

// CreateStream creates a new Kinesis stream.
func (b *InMemoryBackend) CreateStream(input *CreateStreamInput) error {
	b.mu.Lock("CreateStream")
	defer b.mu.Unlock()

	if _, exists := b.streams[input.StreamName]; exists {
		return ErrStreamAlreadyExists
	}

	shardCount := input.ShardCount
	if shardCount <= 0 {
		shardCount = defaultShardCount
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

	shards := make([]*Shard, shardCount)
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

		shards[i] = &Shard{
			ID:                fmt.Sprintf("shardId-%012d", i),
			HashKeyRangeStart: start.String(),
			HashKeyRangeEnd:   end.String(),
			Records:           make([]*Record, 0),
		}
	}

	accountID := b.accountID
	if input.AccountID != "" {
		accountID = input.AccountID
	}

	region := b.region
	if input.Region != "" {
		region = input.Region
	}

	streamARN := arn.Build("kinesis", region, accountID, "stream/"+input.StreamName)

	b.streams[input.StreamName] = &Stream{
		Name:            input.StreamName,
		ARN:             streamARN,
		Status:          streamStatusActive,
		Shards:          shards,
		Tags:            tags.New("kinesis.stream." + input.StreamName + ".tags"),
		CreatedAt:       time.Now(),
		RetentionPeriod: defaultRetentionHours,
		Consumers:       make(map[string]*Consumer),
	}

	return nil
}

// DeleteStream removes a stream.
func (b *InMemoryBackend) DeleteStream(input *DeleteStreamInput) error {
	b.mu.Lock("DeleteStream")
	defer b.mu.Unlock()

	if _, exists := b.streams[input.StreamName]; !exists {
		return ErrStreamNotFound
	}

	delete(b.streams, input.StreamName)

	return nil
}

// DescribeStream returns full stream details including shards.
func (b *InMemoryBackend) DescribeStream(input *DescribeStreamInput) (*DescribeStreamOutput, error) {
	b.mu.RLock("DescribeStream")
	defer b.mu.RUnlock()

	stream, exists := b.streams[input.StreamName]
	if !exists {
		return nil, ErrStreamNotFound
	}

	shards := make([]ShardDescription, len(stream.Shards))
	for i, s := range stream.Shards {
		var seqEnd string
		if len(s.Records) > 0 {
			seqEnd = s.Records[len(s.Records)-1].SequenceNumber
		}

		seqStart := "0"
		if len(s.Records) > 0 {
			seqStart = s.Records[0].SequenceNumber
		}

		shards[i] = ShardDescription{
			ShardID:                  s.ID,
			HashKeyRangeStart:        s.HashKeyRangeStart,
			HashKeyRangeEnd:          s.HashKeyRangeEnd,
			SequenceNumberRangeStart: seqStart,
			SequenceNumberRangeEnd:   seqEnd,
		}
	}

	return &DescribeStreamOutput{
		StreamName:           stream.Name,
		StreamARN:            stream.ARN,
		StreamStatus:         stream.Status,
		Shards:               shards,
		RetentionPeriodHours: stream.RetentionPeriod,
	}, nil
}

// ListStreams returns stream names with optional pagination.
func (b *InMemoryBackend) ListStreams(input *ListStreamsInput) (*ListStreamsOutput, error) {
	b.mu.RLock("ListStreams")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.streams))
	for name := range b.streams {
		names = append(names, name)
	}

	limit := input.Limit
	if limit <= 0 {
		limit = len(names)
	}

	if limit > len(names) {
		limit = len(names)
	}

	return &ListStreamsOutput{
		StreamNames:    names[:limit],
		HasMoreStreams: len(names) > limit,
	}, nil
}

// PutRecord writes a single record to a stream shard.
func (b *InMemoryBackend) PutRecord(input *PutRecordInput) (*PutRecordOutput, error) {
	b.mu.Lock("PutRecord")
	defer b.mu.Unlock()

	stream, exists := b.streams[input.StreamName]
	if !exists {
		return nil, ErrStreamNotFound
	}

	if b.isThroughputFaultActiveLocked(input.StreamName) {
		return nil, ErrProvisionedThroughputExceeded
	}

	if len(stream.Shards) == 0 {
		return nil, ErrInvalidArgument
	}

	shardIdx := shardForPartitionKey(stream.Shards, input.PartitionKey)
	shard := stream.Shards[shardIdx]

	seq := shard.nextSequenceNumber()
	record := &Record{
		PartitionKey:                input.PartitionKey,
		Data:                        input.Data,
		SequenceNumber:              seq,
		ApproximateArrivalTimestamp: time.Now(),
	}

	// Trim if over limit
	if len(shard.Records) >= maxRecordsPerShard {
		shard.Records = shard.Records[1:]
	}

	shard.Records = append(shard.Records, record)

	return &PutRecordOutput{
		ShardID:        shard.ID,
		SequenceNumber: seq,
	}, nil
}

// PutRecords writes multiple records to a stream.
func (b *InMemoryBackend) PutRecords(input *PutRecordsInput) (*PutRecordsOutput, error) {
	results := make([]PutRecordsResultEntry, len(input.Records))
	failedCount := 0

	for i, entry := range input.Records {
		out, err := b.PutRecord(&PutRecordInput{
			StreamName:   input.StreamName,
			PartitionKey: entry.PartitionKey,
			Data:         entry.Data,
		})
		if err != nil {
			results[i] = PutRecordsResultEntry{
				ErrorCode:    "InternalFailure",
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

	return &it, nil
}

// GetShardIterator returns an iterator for reading records from a shard.
func (b *InMemoryBackend) GetShardIterator(input *GetShardIteratorInput) (*GetShardIteratorOutput, error) {
	b.mu.RLock("GetShardIterator")
	defer b.mu.RUnlock()

	stream, exists := b.streams[input.StreamName]
	if !exists {
		return nil, ErrStreamNotFound
	}

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
		position = len(shard.Records)
	case iteratorTypeAtSequenceNumber:
		position = findSequencePosition(shard.Records, input.StartingSequenceNumber, false)
	case iteratorTypeAfterSequenceNumber:
		position = findSequencePosition(shard.Records, input.StartingSequenceNumber, true)
	case iteratorTypeAtTimestamp:
		position = findTimestampPosition(shard.Records, input.Timestamp)
	default:
		return nil, ErrInvalidArgument
	}

	it := &ShardIterator{
		StreamName:     input.StreamName,
		ShardID:        input.ShardID,
		Position:       position,
		SequenceNumber: input.StartingSequenceNumber,
	}

	token, err := encodeIterator(it)
	if err != nil {
		return nil, err
	}

	return &GetShardIteratorOutput{ShardIterator: token}, nil
}

// findSequencePosition returns the record index for the given sequence number.
// If after is true, returns the index after the matching record.
func findSequencePosition(records []*Record, seqNum string, after bool) int {
	for i, r := range records {
		if r.SequenceNumber == seqNum {
			if after {
				return i + 1
			}

			return i
		}

		// Sequence numbers are zero-padded integers; compare lexicographically when exact match fails
		if strings.Compare(r.SequenceNumber, seqNum) > 0 {
			return i
		}
	}

	return len(records)
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
func (b *InMemoryBackend) GetRecords(input *GetRecordsInput) (*GetRecordsOutput, error) {
	it, err := decodeIterator(input.ShardIterator)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetRecords")
	defer b.mu.RUnlock()

	stream, exists := b.streams[it.StreamName]
	if !exists {
		return nil, ErrStreamNotFound
	}

	if b.isThroughputFaultActiveLocked(it.StreamName) {
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

	start := min(it.Position, len(shard.Records))

	end := min(start+limit, len(shard.Records))

	results := make([]GetRecordResult, 0, end-start)
	for _, r := range shard.Records[start:end] {
		results = append(results, GetRecordResult{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: r.ApproximateArrivalTimestamp,
		})
	}

	// Advance iterator position
	newIt := &ShardIterator{
		StreamName: it.StreamName,
		ShardID:    it.ShardID,
		Position:   end,
	}

	nextToken, err := encodeIterator(newIt)
	if err != nil {
		return nil, err
	}

	millisBehind := int64(0)
	if end < len(shard.Records) {
		millisBehind = time.Since(shard.Records[len(shard.Records)-1].ApproximateArrivalTimestamp).Milliseconds()
	}

	return &GetRecordsOutput{
		Records:            results,
		NextShardIterator:  nextToken,
		MillisBehindLatest: millisBehind,
	}, nil
}

// ListShards returns the shards for a stream.
func (b *InMemoryBackend) ListShards(input *ListShardsInput) (*ListShardsOutput, error) {
	b.mu.RLock("ListShards")
	defer b.mu.RUnlock()

	stream, exists := b.streams[input.StreamName]
	if !exists {
		return nil, ErrStreamNotFound
	}

	shards := make([]ShardDescription, len(stream.Shards))
	for i, s := range stream.Shards {
		var seqEnd string
		if len(s.Records) > 0 {
			seqEnd = s.Records[len(s.Records)-1].SequenceNumber
		}

		seqStart := "0"
		if len(s.Records) > 0 {
			seqStart = s.Records[0].SequenceNumber
		}

		shards[i] = ShardDescription{
			ShardID:                  s.ID,
			HashKeyRangeStart:        s.HashKeyRangeStart,
			HashKeyRangeEnd:          s.HashKeyRangeEnd,
			SequenceNumberRangeStart: seqStart,
			SequenceNumberRangeEnd:   seqEnd,
		}
	}

	return &ListShardsOutput{Shards: shards}, nil
}

// ListAll returns a snapshot of all streams as StreamInfo values.
func (b *InMemoryBackend) ListAll() []StreamInfo {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()

	result := make([]StreamInfo, 0, len(b.streams))
	for _, s := range b.streams {
		result = append(result, StreamInfo{
			Name:       s.Name,
			ARN:        s.ARN,
			Status:     s.Status,
			ShardCount: len(s.Shards),
		})
	}

	return result
}

// isThroughputFaultActiveLocked reports whether a FIS throughput exception fault
// is currently active for the given stream name.
// Caller MUST hold at least a read lock on b.mu.
func (b *InMemoryBackend) isThroughputFaultActiveLocked(streamName string) bool {
	exp, ok := b.fisThroughputFaults[streamName]
	if !ok {
		return false
	}

	if !exp.IsZero() && time.Now().After(exp) {
		return false
	}

	return true
}

// findTimestampPosition returns the index of the first record whose arrival
// timestamp is not before ts. Returns len(records) if all records are before ts.
func findTimestampPosition(records []*Record, ts time.Time) int {
	for i, r := range records {
		if !r.ApproximateArrivalTimestamp.Before(ts) {
			return i
		}
	}

	return len(records)
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
	input *RegisterStreamConsumerInput,
) (*RegisterStreamConsumerOutput, error) {
	b.mu.Lock("RegisterStreamConsumer")
	defer b.mu.Unlock()

	streamName := streamNameFromARN(input.StreamARN)
	stream, ok := b.streams[streamName]

	if !ok {
		return nil, ErrStreamNotFound
	}

	if stream.Consumers == nil {
		stream.Consumers = make(map[string]*Consumer)
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
	input *DescribeStreamConsumerInput,
) (*DescribeStreamConsumerOutput, error) {
	b.mu.RLock("DescribeStreamConsumer")
	defer b.mu.RUnlock()

	consumer, err := b.findConsumer(input.StreamARN, input.ConsumerARN, input.ConsumerName)
	if err != nil {
		return nil, err
	}

	return &DescribeStreamConsumerOutput{ConsumerDescription: *consumer}, nil
}

// findConsumer locates a consumer by ARN or by (streamARN + consumerName).
// The caller must hold a read lock.
func (b *InMemoryBackend) findConsumer(streamARN, consumerARN, consumerName string) (*Consumer, error) {
	if consumerARN != "" {
		sName, cName := consumerInfoFromARN(consumerARN)
		stream, ok := b.streams[sName]

		if !ok {
			return nil, ErrConsumerNotFound
		}

		c, ok := stream.Consumers[cName]
		if !ok {
			return nil, ErrConsumerNotFound
		}

		return c, nil
	}

	sName := streamNameFromARN(streamARN)
	stream, ok := b.streams[sName]

	if !ok {
		return nil, ErrStreamNotFound
	}

	c, ok := stream.Consumers[consumerName]
	if !ok {
		return nil, ErrConsumerNotFound
	}

	return c, nil
}

// ListStreamConsumers lists all registered consumers for a stream.
func (b *InMemoryBackend) ListStreamConsumers(input *ListStreamConsumersInput) (*ListStreamConsumersOutput, error) {
	b.mu.RLock("ListStreamConsumers")
	defer b.mu.RUnlock()

	streamName := streamNameFromARN(input.StreamARN)
	stream, ok := b.streams[streamName]

	if !ok {
		return nil, ErrStreamNotFound
	}

	consumers := make([]Consumer, 0, len(stream.Consumers))
	for _, c := range stream.Consumers {
		consumers = append(consumers, *c)
	}

	// Sort for deterministic ordering.
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].ConsumerName < consumers[j].ConsumerName
	})

	return &ListStreamConsumersOutput{Consumers: consumers}, nil
}

// DeregisterStreamConsumer removes a registered consumer from a stream.
func (b *InMemoryBackend) DeregisterStreamConsumer(input *DeregisterStreamConsumerInput) error {
	b.mu.Lock("DeregisterStreamConsumer")
	defer b.mu.Unlock()

	if _, err := b.findConsumer(input.StreamARN, input.ConsumerARN, input.ConsumerName); err != nil {
		return err
	}

	sName, cName := func() (string, string) {
		if input.ConsumerARN != "" {
			return consumerInfoFromARN(input.ConsumerARN)
		}

		return streamNameFromARN(input.StreamARN), input.ConsumerName
	}()

	delete(b.streams[sName].Consumers, cName)

	return nil
}

// SubscribeToShard delivers records from a shard to an enhanced fan-out consumer.
// For mock purposes this is a single-shot delivery of all available records.
func (b *InMemoryBackend) SubscribeToShard(input *SubscribeToShardInput) (*SubscribeToShardOutput, error) {
	sName, cName := consumerInfoFromARN(input.ConsumerARN)

	b.mu.RLock("SubscribeToShard")
	defer b.mu.RUnlock()

	stream, ok := b.streams[sName]
	if !ok {
		return nil, ErrStreamNotFound
	}

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
		startPos = len(shard.Records)
	case iteratorTypeAtSequenceNumber:
		startPos = findSequencePosition(shard.Records, input.StartingPosition.SequenceNumber, false)
	case iteratorTypeAfterSequenceNumber:
		startPos = findSequencePosition(shard.Records, input.StartingPosition.SequenceNumber, true)
	case iteratorTypeAtTimestamp:
		ts := time.Time{}
		if input.StartingPosition.Timestamp != nil {
			ts = *input.StartingPosition.Timestamp
		}

		startPos = findTimestampPosition(shard.Records, ts)
	default:
		return nil, ErrInvalidArgument
	}

	records := make([]GetRecordResult, 0, len(shard.Records)-startPos)
	for _, r := range shard.Records[startPos:] {
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
	if len(shard.Records) > 0 && startPos < len(shard.Records) {
		millisBehind = time.Since(shard.Records[len(shard.Records)-1].ApproximateArrivalTimestamp).Milliseconds()
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
func (b *InMemoryBackend) UpdateShardCount(input *UpdateShardCountInput) (*UpdateShardCountOutput, error) {
	b.mu.Lock("UpdateShardCount")
	defer b.mu.Unlock()

	stream, ok := b.streams[input.StreamName]
	if !ok {
		return nil, ErrStreamNotFound
	}

	if input.TargetShardCount <= 0 {
		return nil, ErrInvalidArgument
	}

	if input.ScalingType != "" && input.ScalingType != scalingTypeUniformScaling {
		return nil, ErrInvalidArgument
	}

	currentCount := len(stream.Shards)
	targetCount := input.TargetShardCount

	maxHashKey := new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), maxHashKeyBits),
		big.NewInt(1),
	)
	shardRange := new(big.Int).Div(
		new(big.Int).Add(maxHashKey, big.NewInt(1)),
		big.NewInt(int64(targetCount)),
	)

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
			ID:                fmt.Sprintf("shardId-%012d", i),
			HashKeyRangeStart: start.String(),
			HashKeyRangeEnd:   end.String(),
			Records:           make([]*Record, 0),
		}
	}

	stream.Shards = newShards

	return &UpdateShardCountOutput{
		StreamName:        input.StreamName,
		CurrentShardCount: currentCount,
		TargetShardCount:  targetCount,
	}, nil
}

// EnableEnhancedMonitoring adds shard-level metrics to a stream.
func (b *InMemoryBackend) EnableEnhancedMonitoring(
	input *EnableEnhancedMonitoringInput,
) (*EnableEnhancedMonitoringOutput, error) {
	b.mu.Lock("EnableEnhancedMonitoring")
	defer b.mu.Unlock()

	stream, ok := b.streams[input.StreamName]
	if !ok {
		return nil, ErrStreamNotFound
	}

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
	input *DisableEnhancedMonitoringInput,
) (*DisableEnhancedMonitoringOutput, error) {
	b.mu.Lock("DisableEnhancedMonitoring")
	defer b.mu.Unlock()

	stream, ok := b.streams[input.StreamName]
	if !ok {
		return nil, ErrStreamNotFound
	}

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

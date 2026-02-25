package kinesis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	ListAll() []StreamInfo
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	streams   map[string]*Stream
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new empty InMemoryBackend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(kinesisAccountID, kinesisRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		streams:   make(map[string]*Stream),
		accountID: accountID,
		region:    region,
	}
}

// streamARN builds the ARN for a Kinesis stream.
func (b *InMemoryBackend) streamARN(name string) string {
	return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", b.region, b.accountID, name)
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
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.streams[input.StreamName]; exists {
		return ErrStreamAlreadyExists
	}

	shardCount := input.ShardCount
	if shardCount <= 0 {
		shardCount = defaultShardCount
	}

	maxHashKey := new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), 128),
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

	arn := fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", region, accountID, input.StreamName)

	b.streams[input.StreamName] = &Stream{
		Name:            input.StreamName,
		ARN:             arn,
		Status:          streamStatusActive,
		Shards:          shards,
		Tags:            make(map[string]string),
		CreatedAt:       time.Now(),
		RetentionPeriod: 24,
	}

	return nil
}

// DeleteStream removes a stream.
func (b *InMemoryBackend) DeleteStream(input *DeleteStreamInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.streams[input.StreamName]; !exists {
		return ErrStreamNotFound
	}

	delete(b.streams, input.StreamName)

	return nil
}

// DescribeStream returns full stream details including shards.
func (b *InMemoryBackend) DescribeStream(input *DescribeStreamInput) (*DescribeStreamOutput, error) {
	b.mu.RLock()
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
	b.mu.RLock()
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
		StreamNames:   names[:limit],
		HasMoreStreams: len(names) > limit,
	}, nil
}

// PutRecord writes a single record to a stream shard.
func (b *InMemoryBackend) PutRecord(input *PutRecordInput) (*PutRecordOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	stream, exists := b.streams[input.StreamName]
	if !exists {
		return nil, ErrStreamNotFound
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
		Records:          results,
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	stream, exists := b.streams[input.StreamName]
	if !exists {
		return nil, ErrStreamNotFound
	}

	// Find the shard
	var shard *Shard
	for _, s := range stream.Shards {
		if s.ID == input.ShardID {
			shard = s

			break
		}
	}

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

// GetRecords retrieves records starting at the given shard iterator position.
func (b *InMemoryBackend) GetRecords(input *GetRecordsInput) (*GetRecordsOutput, error) {
	it, err := decodeIterator(input.ShardIterator)
	if err != nil {
		return nil, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	stream, exists := b.streams[it.StreamName]
	if !exists {
		return nil, ErrStreamNotFound
	}

	var shard *Shard
	for _, s := range stream.Shards {
		if s.ID == it.ShardID {
			shard = s

			break
		}
	}

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

	start := it.Position
	if start > len(shard.Records) {
		start = len(shard.Records)
	}

	end := start + limit
	if end > len(shard.Records) {
		end = len(shard.Records)
	}

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
		millisBehind = time.Since(shard.Records[end-1].ApproximateArrivalTimestamp).Milliseconds()
	}

	return &GetRecordsOutput{
		Records:           results,
		NextShardIterator: nextToken,
		MillisBehindLatest: millisBehind,
	}, nil
}

// ListShards returns the shards for a stream.
func (b *InMemoryBackend) ListShards(input *ListShardsInput) (*ListShardsOutput, error) {
	b.mu.RLock()
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]StreamInfo, 0, len(b.streams))
	for _, s := range b.streams {
		result = append(result, StreamInfo{
			Name:   s.Name,
			ARN:    s.ARN,
			Status: s.Status,
		})
	}

	return result
}

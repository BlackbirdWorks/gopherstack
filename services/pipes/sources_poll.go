package pipes

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// millisToSeconds converts Unix milliseconds to a float64 second timestamp.
const millisToSeconds = 1000.0

// kinesisStreamNameFromARN extracts the stream name from a Kinesis ARN.
// Example: arn:aws:kinesis:us-east-1:000000000000:stream/my-stream -> my-stream.
func kinesisStreamNameFromARN(arn string) string {
	const marker = "stream/"

	idx := strings.LastIndex(arn, marker)
	if idx < 0 {
		return ""
	}

	return arn[idx+len(marker):]
}

// shardIteratorKey builds the safemap key used to cache a shard's in-flight
// iterator token. Keyed by pipe ARN (globally unique) rather than pipe name,
// since names are only unique within a single region+account.
func shardIteratorKey(p *Pipe, shardID string) string {
	return p.ARN + ":" + shardID
}

// sweepStaleShardIterators drops cached shard iterator entries for pipes that
// are no longer in the currently-polled RUNNING set, so a stopped or deleted
// pipe's iterator state does not grow the cache unboundedly.
func (r *Runner) sweepStaleShardIterators(runningARNs map[string]struct{}) {
	for _, key := range r.shardIterators.Keys() {
		// The pipe ARN itself contains colons, so the pipe ARN portion of the
		// "<pipeARN>:<shardID>" key is everything before the trailing segment
		// appended by shardIteratorKey.
		lastColon := strings.LastIndex(key, ":")
		if lastColon < 0 {
			continue
		}
		pipeARN := key[:lastColon]

		if _, active := runningARNs[pipeARN]; !active {
			r.shardIterators.Delete(key)
		}
	}
}

// kinesisStartingPosition returns the configured Kinesis StartingPosition for
// the pipe, defaulting to TRIM_HORIZON (read from the beginning) when unset.
func kinesisStartingPosition(p *Pipe) string {
	if p.SourceParameters != nil && p.SourceParameters.KinesisStreamParameters != nil &&
		p.SourceParameters.KinesisStreamParameters.StartingPosition != "" {
		return p.SourceParameters.KinesisStreamParameters.StartingPosition
	}

	return "TRIM_HORIZON"
}

// dynamoDBStartingPosition returns the configured DynamoDB Streams StartingPosition
// for the pipe, defaulting to TRIM_HORIZON when unset.
func dynamoDBStartingPosition(p *Pipe) string {
	if p.SourceParameters != nil && p.SourceParameters.DynamoDBStreamParameters != nil &&
		p.SourceParameters.DynamoDBStreamParameters.StartingPosition != "" {
		return p.SourceParameters.DynamoDBStreamParameters.StartingPosition
	}

	return "TRIM_HORIZON"
}

// pollKinesisPipe polls every shard of a Kinesis-sourced RUNNING pipe and
// forwards any new records (after filtering/enrichment) to the pipe's target.
func (r *Runner) pollKinesisPipe(ctx context.Context, p *Pipe) {
	if r.kinesisReader == nil {
		return
	}

	streamName := kinesisStreamNameFromARN(p.Source)
	if streamName == "" {
		return
	}

	shardIDs, err := r.kinesisReader.GetShardIDs(streamName)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to get Kinesis shard IDs",
			"pipe", p.Name, "source", p.Source, "error", err)

		return
	}

	batchSize := p.effectiveBatchSize()
	startingPosition := kinesisStartingPosition(p)

	for _, shardID := range shardIDs {
		r.pollKinesisShard(ctx, p, shardID, startingPosition, batchSize)
	}
}

func (r *Runner) pollKinesisShard(ctx context.Context, p *Pipe, shardID, startingPosition string, batchSize int) {
	iterKey := shardIteratorKey(p, shardID)

	it, exists := r.shardIterators.Get(iterKey)
	if !exists {
		newIt, err := r.kinesisReader.GetShardIterator(
			kinesisStreamNameFromARN(p.Source), shardID, startingPosition, "",
		)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "pipes: failed to get Kinesis shard iterator",
				"pipe", p.Name, "shard", shardID, "error", err)

			return
		}

		it = newIt
	}

	records, nextIt, err := r.kinesisReader.GetRecords(it, batchSize)
	if err != nil {
		// Iterator may have expired; drop it so the next poll re-initializes
		// from the pipe's configured starting position.
		r.shardIterators.Delete(iterKey)
		logger.Load(ctx).WarnContext(ctx, "pipes: Kinesis GetRecords failed, resetting iterator",
			"pipe", p.Name, "shard", shardID, "error", err)

		return
	}

	// Advance the iterator unconditionally once GetRecords succeeds, matching
	// this codebase's established Kinesis event-source-poller precedent
	// (services/lambda/event_source_poller.go's processMapping): checkpointing
	// only on invoke success would let one poison record wedge the shard
	// forever, and there is no per-record DLQ/redrive granularity to fall
	// back on for a stream-shaped source.
	r.shardIterators.Set(iterKey, nextIt)

	if len(records) == 0 {
		return
	}

	records = filterKinesisRecords(p, records)
	if len(records) == 0 {
		return
	}

	payload, err := json.Marshal(kinesisPipeEvent{Records: buildKinesisRecords(p, shardID, records)})
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to marshal Kinesis records", "pipe", p.Name, "error", err)

		return
	}

	r.deliverStreamPayload(ctx, p, payload)
}

// pollDynamoDBStreamPipe polls every shard of a DynamoDB-Streams-sourced RUNNING
// pipe and forwards any new records (after filtering/enrichment) to the target.
func (r *Runner) pollDynamoDBStreamPipe(ctx context.Context, p *Pipe) {
	if r.ddbStreamsReader == nil {
		return
	}

	shardIDs, err := r.ddbStreamsReader.DescribeStreamShards(p.Source)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to describe DynamoDB stream shards",
			"pipe", p.Name, "source", p.Source, "error", err)

		return
	}

	batchSize := p.effectiveBatchSize()
	startingPosition := dynamoDBStartingPosition(p)

	for _, shardID := range shardIDs {
		r.pollDynamoDBShard(ctx, p, shardID, startingPosition, batchSize)
	}
}

func (r *Runner) pollDynamoDBShard(ctx context.Context, p *Pipe, shardID, startingPosition string, batchSize int) {
	iterKey := shardIteratorKey(p, shardID)

	it, exists := r.shardIterators.Get(iterKey)
	if !exists {
		newIt, err := r.ddbStreamsReader.GetStreamShardIterator(p.Source, shardID, startingPosition)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "pipes: failed to get DynamoDB stream shard iterator",
				"pipe", p.Name, "shard", shardID, "error", err)

			return
		}

		it = newIt
	}

	records, nextIt, err := r.ddbStreamsReader.GetStreamRecords(it, batchSize)
	if err != nil {
		r.shardIterators.Delete(iterKey)
		logger.Load(ctx).WarnContext(ctx, "pipes: DynamoDB GetStreamRecords failed, resetting iterator",
			"pipe", p.Name, "shard", shardID, "error", err)

		return
	}

	// See pollKinesisShard for why the iterator advances unconditionally.
	r.shardIterators.Set(iterKey, nextIt)

	if len(records) == 0 {
		return
	}

	records = filterDynamoDBRecords(p, records)
	if len(records) == 0 {
		return
	}

	payload, err := json.Marshal(dynamoDBPipeEvent{Records: buildDynamoDBRecords(p, records)})
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to marshal DynamoDB stream records",
			"pipe", p.Name, "error", err)

		return
	}

	r.deliverStreamPayload(ctx, p, payload)
}

// deliverStreamPayload runs the shared enrichment -> target-dispatch -> DLQ
// pipeline for a stream-shaped source (Kinesis, DynamoDB Streams) that has
// already been marshalled into its default event payload. Unlike the SQS path
// there is no message-level delete step: successful delivery has nothing
// further to do, and a delivery failure only attempts DLQ forwarding (the
// shard iterator has already advanced past these records).
func (r *Runner) deliverStreamPayload(ctx context.Context, p *Pipe, payload []byte) {
	if p.Enrichment != "" {
		pipeCtx := context.WithValue(ctx, regionContextKey{}, p.Region)
		r.backend.RecordEnrichmentCall(pipeCtx, p.Name)

		enriched, enrichErr := r.invokeEnrichment(ctx, p, payload)
		if enrichErr != nil {
			logger.Load(ctx).WarnContext(ctx, "pipes: enrichment invocation failed",
				"pipe", p.Name, "enrichment", p.Enrichment, "error", enrichErr)
			r.sendToDLQIfConfigured(ctx, p, payload, enrichErr)

			return
		}
		if enriched != nil {
			payload = enriched
		}
	}

	if err := r.dispatchTarget(ctx, p, payload); err != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: target invocation failed",
			"pipe", p.Name, "target", p.Target, "error", err)
		r.sendToDLQIfConfigured(ctx, p, payload, err)
	}
}

// sendToDLQIfConfigured attempts DLQ delivery for a failed pipe event batch
// from a source with no message-level delete/ack semantics (Kinesis, DynamoDB
// Streams). Unlike handlePipeFailure (SQS), there are no source receipt
// handles to delete after a successful DLQ send.
func (r *Runner) sendToDLQIfConfigured(ctx context.Context, p *Pipe, payload []byte, cause error) {
	if p.DeadLetterConfig == nil || p.DeadLetterConfig.Arn == "" {
		return
	}

	if err := r.sendToDLQ(ctx, p.DeadLetterConfig.Arn, payload); err != nil {
		logger.Load(ctx).WarnContext(ctx, "pipes: failed to send to DLQ",
			"pipe", p.Name, "dlq", p.DeadLetterConfig.Arn, "error", err, "cause", cause)
	}
}

// filterKinesisRecords applies the pipe's FilterCriteria (if any) to the raw
// record Data, matching the same body-based matching engine SQS sources use.
func filterKinesisRecords(p *Pipe, records []KinesisRecord) []KinesisRecord {
	filters := filterCriteriaFilters(p)
	if len(filters) == 0 {
		return records
	}

	out := make([]KinesisRecord, 0, len(records))
	for _, rec := range records {
		if matchesAnyFilter(string(rec.Data), filters) {
			out = append(out, rec)
		}
	}

	return out
}

// filterDynamoDBRecords applies the pipe's FilterCriteria (if any) against a
// JSON view of each DynamoDB stream record ({"eventName":..., "dynamodb":{...}}),
// matching real EventBridge Pipes' pattern matching against the whole record.
func filterDynamoDBRecords(p *Pipe, records []DynamoDBStreamRecord) []DynamoDBStreamRecord {
	filters := filterCriteriaFilters(p)
	if len(filters) == 0 {
		return records
	}

	out := make([]DynamoDBStreamRecord, 0, len(records))
	for _, rec := range records {
		body, err := json.Marshal(dynamoDBFilterView{
			EventName: rec.EventName,
			Dynamodb: dynamoDBFilterViewInner{
				Keys:     rec.Keys,
				NewImage: rec.NewImage,
				OldImage: rec.OldImage,
			},
		})
		if err == nil && matchesAnyFilter(string(body), filters) {
			out = append(out, rec)
		}
	}

	return out
}

// dynamoDBFilterView/dynamoDBFilterViewInner mirror the shape of a DynamoDB
// stream event record for FilterCriteria pattern matching purposes.
type dynamoDBFilterView struct {
	Dynamodb  dynamoDBFilterViewInner `json:"dynamodb"`
	EventName string                  `json:"eventName"`
}

type dynamoDBFilterViewInner struct {
	Keys     map[string]any `json:"Keys,omitempty"`
	NewImage map[string]any `json:"NewImage,omitempty"`
	OldImage map[string]any `json:"OldImage,omitempty"`
}

// kinesisPipeEvent/kinesisPipeRecord mirror the Records-array event shape AWS
// delivers for a Kinesis-sourced pipe (identical to the Lambda Kinesis event
// source shape) absent a custom InputTemplate.
type kinesisPipeEvent struct {
	Records []kinesisPipeRecord `json:"Records"`
}

type kinesisPipeRecord struct {
	EventSource       string                `json:"eventSource"`
	EventVersion      string                `json:"eventVersion"`
	EventID           string                `json:"eventID"`
	EventName         string                `json:"eventName"`
	InvokeIdentityArn string                `json:"invokeIdentityArn"`
	AWSRegion         string                `json:"awsRegion"`
	EventSourceARN    string                `json:"eventSourceARN"`
	Kinesis           kinesisPipeRecordData `json:"kinesis"`
}

type kinesisPipeRecordData struct {
	KinesisSchemaVersion        string  `json:"kinesisSchemaVersion"`
	PartitionKey                string  `json:"partitionKey"`
	SequenceNumber              string  `json:"sequenceNumber"`
	Data                        string  `json:"data"`
	ApproximateArrivalTimestamp float64 `json:"approximateArrivalTimestamp"`
}

func buildKinesisRecords(p *Pipe, shardID string, records []KinesisRecord) []kinesisPipeRecord {
	out := make([]kinesisPipeRecord, len(records))

	for i, rec := range records {
		out[i] = kinesisPipeRecord{
			Kinesis: kinesisPipeRecordData{
				KinesisSchemaVersion:        "1.0",
				PartitionKey:                rec.PartitionKey,
				SequenceNumber:              rec.SequenceNumber,
				Data:                        base64.StdEncoding.EncodeToString(rec.Data),
				ApproximateArrivalTimestamp: float64(rec.ArrivalTime.UnixMilli()) / millisToSeconds,
			},
			EventSource:    "aws:kinesis",
			EventVersion:   "1.0",
			EventID:        shardID + ":" + rec.SequenceNumber,
			EventName:      "aws:kinesis:record",
			AWSRegion:      p.Region,
			EventSourceARN: p.Source,
		}
	}

	return out
}

// dynamoDBPipeEvent/dynamoDBPipeRecord mirror the Records-array event shape
// AWS delivers for a DynamoDB-Streams-sourced pipe.
type dynamoDBPipeEvent struct {
	Records []dynamoDBPipeRecord `json:"Records"`
}

type dynamoDBPipeRecord struct {
	EventID        string                   `json:"eventID"`
	EventName      string                   `json:"eventName"`
	EventVersion   string                   `json:"eventVersion"`
	EventSource    string                   `json:"eventSource"`
	AWSRegion      string                   `json:"awsRegion"`
	EventSourceARN string                   `json:"eventSourceARN"`
	Dynamodb       dynamoDBStreamRecordData `json:"dynamodb"`
}

type dynamoDBStreamRecordData struct {
	Keys                        map[string]any `json:"Keys,omitempty"`
	NewImage                    map[string]any `json:"NewImage,omitempty"`
	OldImage                    map[string]any `json:"OldImage,omitempty"`
	SequenceNumber              string         `json:"SequenceNumber"`
	StreamViewType              string         `json:"StreamViewType,omitempty"`
	ApproximateCreationDateTime float64        `json:"ApproximateCreationDateTime,omitempty"`
	SizeBytes                   int64          `json:"SizeBytes,omitempty"`
}

func buildDynamoDBRecords(p *Pipe, records []DynamoDBStreamRecord) []dynamoDBPipeRecord {
	out := make([]dynamoDBPipeRecord, len(records))

	for i, rec := range records {
		out[i] = dynamoDBPipeRecord{
			EventID:      rec.EventID,
			EventName:    rec.EventName,
			EventVersion: "1.1",
			EventSource:  "aws:dynamodb",
			AWSRegion:    p.Region,
			Dynamodb: dynamoDBStreamRecordData{
				SequenceNumber:              rec.SequenceNumber,
				StreamViewType:              rec.StreamViewType,
				ApproximateCreationDateTime: rec.ApproximateCreationDateTime,
				SizeBytes:                   rec.SizeBytes,
				Keys:                        rec.Keys,
				NewImage:                    rec.NewImage,
				OldImage:                    rec.OldImage,
			},
			EventSourceARN: p.Source,
		}
	}

	return out
}

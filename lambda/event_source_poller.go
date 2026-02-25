package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// KinesisReader is the interface for reading Kinesis records.
// It is implemented by the kinesis backend.
type KinesisReader interface {
	// GetShardIDs returns the shard IDs for the given stream.
	GetShardIDs(streamName string) ([]string, error)
	// GetShardIterator returns an iterator token for a shard.
	GetShardIterator(streamName, shardID, iteratorType, startingSeqNum string) (string, error)
	// GetRecords reads up to limit records from the given iterator, returning records and next iterator.
	GetRecords(iteratorToken string, limit int) ([]KinesisRecord, string, error)
}

// KinesisRecord is a single record from a Kinesis shard.
type KinesisRecord struct {
	PartitionKey   string
	SequenceNumber string
	Data           []byte
	ArrivalTime    time.Time
}

// EventSourcePoller polls Kinesis streams for new records and invokes Lambda functions.
type EventSourcePoller struct {
	lambdaBackend *InMemoryBackend
	kinesisReader KinesisReader
	logger        *slog.Logger
	// shardIterators holds the current iterator position per "uuid:shardID" key.
	shardIterators map[string]string
}

// NewEventSourcePoller creates a new EventSourcePoller.
func NewEventSourcePoller(lambdaBackend *InMemoryBackend, kinesisReader KinesisReader, log *slog.Logger) *EventSourcePoller {
	return &EventSourcePoller{
		lambdaBackend:  lambdaBackend,
		kinesisReader:  kinesisReader,
		logger:         log,
		shardIterators: make(map[string]string),
	}
}

const (
	// defaultPollInterval is how often the poller ticks to check for new records.
	defaultPollInterval = 1 * time.Second
)

// Start runs the event source poller as a background goroutine.
// It returns immediately; the goroutine stops when ctx is cancelled.
func (p *EventSourcePoller) Start(ctx context.Context) {
	go p.run(ctx)
}

func (p *EventSourcePoller) run(ctx context.Context) {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

// poll iterates over all enabled event source mappings and processes new records.
func (p *EventSourcePoller) poll(ctx context.Context) {
	mappings := p.lambdaBackend.ListEventSourceMappings("")
	for _, m := range mappings {
		if m.State != ESMStateEnabled {
			continue
		}

		streamName := streamNameFromARN(m.EventSourceARN)
		if streamName == "" {
			continue
		}

		p.processMapping(ctx, m, streamName)
	}
}

// processMapping reads new records from all shards and invokes Lambda.
func (p *EventSourcePoller) processMapping(ctx context.Context, m *EventSourceMapping, streamName string) {
	shardIDs, err := p.kinesisReader.GetShardIDs(streamName)
	if err != nil {
		p.logger.WarnContext(ctx, "event source poller: failed to get shard IDs",
			"stream", streamName, "error", err)

		return
	}

	for _, shardID := range shardIDs {
		iterKey := m.UUID + ":" + shardID

		it, exists := p.shardIterators[iterKey]
		if !exists {
			// Initialize iterator at starting position
			it, err = p.kinesisReader.GetShardIterator(streamName, shardID, m.StartingPosition, "")
			if err != nil {
				p.logger.WarnContext(ctx, "event source poller: failed to get shard iterator",
					"stream", streamName, "shard", shardID, "error", err)

				continue
			}

			p.shardIterators[iterKey] = it
		}

		records, nextIt, err := p.kinesisReader.GetRecords(it, m.BatchSize)
		if err != nil {
			// Iterator may have expired; reset it
			delete(p.shardIterators, iterKey)
			p.logger.WarnContext(ctx, "event source poller: GetRecords failed, resetting iterator",
				"stream", streamName, "shard", shardID, "error", err)

			continue
		}

		p.shardIterators[iterKey] = nextIt

		if len(records) == 0 {
			continue
		}

		p.invokeLambda(ctx, m, streamName, shardID, records)
	}
}

// invokeLambda formats the Kinesis records as a Lambda event and invokes the function.
func (p *EventSourcePoller) invokeLambda(
	ctx context.Context,
	m *EventSourceMapping,
	streamName, shardID string,
	records []KinesisRecord,
) {
	type kinesisRecord struct {
		KinesisSchemaVersion         string  `json:"kinesisSchemaVersion"`
		PartitionKey                 string  `json:"partitionKey"`
		SequenceNumber               string  `json:"sequenceNumber"`
		Data                         string  `json:"data"`
		ApproximateArrivalTimestamp  float64 `json:"approximateArrivalTimestamp"`
	}
	type lambdaRecord struct {
		Kinesis          kinesisRecord `json:"kinesis"`
		EventSource      string        `json:"eventSource"`
		EventVersion     string        `json:"eventVersion"`
		EventID          string        `json:"eventID"`
		EventName        string        `json:"eventName"`
		InvokeIdentityArn string       `json:"invokeIdentityArn"`
		AWSRegion        string        `json:"awsRegion"`
		EventSourceARN   string        `json:"eventSourceARN"`
	}
	type lambdaEvent struct {
		Records []lambdaRecord `json:"Records"`
	}

	eventRecords := make([]lambdaRecord, len(records))
	for i, r := range records {
		eventRecords[i] = lambdaRecord{
			Kinesis: kinesisRecord{
				KinesisSchemaVersion:        "1.0",
				PartitionKey:                r.PartitionKey,
				SequenceNumber:              r.SequenceNumber,
				Data:                        base64.StdEncoding.EncodeToString(r.Data),
				ApproximateArrivalTimestamp: float64(r.ArrivalTime.UnixMilli()) / 1000.0,
			},
			EventSource:       "aws:kinesis",
			EventVersion:      "1.0",
			EventID:           fmt.Sprintf("%s:%s", shardID, r.SequenceNumber),
			EventName:         "aws:kinesis:record",
			InvokeIdentityArn: m.FunctionARN,
			AWSRegion:         p.lambdaBackend.region,
			EventSourceARN:    m.EventSourceARN,
		}
	}

	payload, err := json.Marshal(lambdaEvent{Records: eventRecords})
	if err != nil {
		p.logger.WarnContext(ctx, "event source poller: failed to marshal event", "error", err)

		return
	}

	// Extract function name from ARN
	fnName := functionNameFromARN(m.FunctionARN)
	if fnName == "" {
		fnName = m.FunctionARN
	}

	_, _, err = p.lambdaBackend.InvokeFunction(ctx, fnName, InvocationTypeEvent, payload)
	if err != nil {
		p.logger.WarnContext(ctx, "event source poller: Lambda invocation failed",
			"function", fnName, "stream", streamName, "error", err)
	} else {
		p.logger.DebugContext(ctx, "event source poller: invoked Lambda",
			"function", fnName, "records", len(records))
	}
}

// streamNameFromARN extracts the stream name from a Kinesis ARN.
// Example: arn:aws:kinesis:us-east-1:000000000000:stream/my-stream → my-stream
func streamNameFromARN(arn string) string {
	const prefix = "arn:aws:kinesis:"
	if len(arn) <= len(prefix) {
		return ""
	}

	// Format: arn:aws:kinesis:region:account:stream/name
	parts := splitN(arn, ":", 6) //nolint:mnd // ARN has 6 parts
	if len(parts) < 6 {
		return ""
	}

	last := parts[5]
	const streamPrefix = "stream/"
	if len(last) <= len(streamPrefix) {
		return ""
	}

	return last[len(streamPrefix):]
}

// functionNameFromARN extracts the function name from a Lambda ARN.
// Example: arn:aws:lambda:us-east-1:000000000000:function:my-func → my-func
func functionNameFromARN(arn string) string {
	parts := splitN(arn, ":", 7) //nolint:mnd // Lambda ARN has up to 7 parts
	if len(parts) < 7 {
		return ""
	}

	return parts[6]
}

// splitN splits s by sep into at most n parts.
func splitN(s, sep string, n int) []string {
	var parts []string
	for i := 0; i < n-1; i++ {
		idx := indexOf(s, sep)
		if idx < 0 {
			break
		}

		parts = append(parts, s[:idx])
		s = s[idx+len(sep):]
	}

	parts = append(parts, s)

	return parts
}

// indexOf returns the index of sep in s, or -1 if not found.
func indexOf(s, sep string) int {
	for i := range len(s) - len(sep) + 1 {
		if s[i:i+len(sep)] == sep {
			return i
		}
	}

	return -1
}

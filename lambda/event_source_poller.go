package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const (
	// arnKinesisPartCount is the number of colon-separated parts in a Kinesis ARN.
	arnKinesisPartCount = 6
	// arnLambdaPartCount is the number of colon-separated parts in a Lambda ARN.
	arnLambdaPartCount = 7
	// millisToSeconds converts Unix milliseconds to a float64 second timestamp.
	millisToSeconds = 1000.0
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
	ArrivalTime    time.Time
	PartitionKey   string
	SequenceNumber string
	Data           []byte
}

// EventSourcePoller polls Kinesis streams for new records and invokes Lambda functions.
type EventSourcePoller struct {
	kinesisReader  KinesisReader
	lambdaBackend  *InMemoryBackend
	logger         *slog.Logger
	shardIterators map[string]string
	mu             sync.Mutex
}

// NewEventSourcePoller creates a new EventSourcePoller.
func NewEventSourcePoller(
	lambdaBackend *InMemoryBackend,
	kinesisReader KinesisReader,
	log *slog.Logger,
) *EventSourcePoller {
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

		p.mu.Lock()
		it, exists := p.shardIterators[iterKey]
		p.mu.Unlock()

		if !exists {
			// Initialize iterator at starting position
			it, err = p.kinesisReader.GetShardIterator(streamName, shardID, m.StartingPosition, "")
			if err != nil {
				p.logger.WarnContext(ctx, "event source poller: failed to get shard iterator",
					"stream", streamName, "shard", shardID, "error", err)

				continue
			}

			p.mu.Lock()
			p.shardIterators[iterKey] = it
			p.mu.Unlock()
		}

		records, nextIt, readErr := p.kinesisReader.GetRecords(it, m.BatchSize)
		if readErr != nil {
			// Iterator may have expired; reset it
			p.mu.Lock()
			delete(p.shardIterators, iterKey)
			p.mu.Unlock()
			p.logger.WarnContext(ctx, "event source poller: GetRecords failed, resetting iterator",
				"stream", streamName, "shard", shardID, "error", readErr)

			continue
		}

		p.mu.Lock()
		p.shardIterators[iterKey] = nextIt
		p.mu.Unlock()

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
		KinesisSchemaVersion        string  `json:"kinesisSchemaVersion"`
		PartitionKey                string  `json:"partitionKey"`
		SequenceNumber              string  `json:"sequenceNumber"`
		Data                        string  `json:"data"`
		ApproximateArrivalTimestamp float64 `json:"approximateArrivalTimestamp"`
	}
	type lambdaRecord struct {
		EventSource       string        `json:"eventSource"`
		EventVersion      string        `json:"eventVersion"`
		EventID           string        `json:"eventID"`
		EventName         string        `json:"eventName"`
		InvokeIdentityArn string        `json:"invokeIdentityArn"`
		AWSRegion         string        `json:"awsRegion"`
		EventSourceARN    string        `json:"eventSourceARN"`
		Kinesis           kinesisRecord `json:"kinesis"`
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
				ApproximateArrivalTimestamp: float64(r.ArrivalTime.UnixMilli()) / millisToSeconds,
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
// Example: arn:aws:kinesis:us-east-1:000000000000:stream/my-stream → my-stream.
func streamNameFromARN(arn string) string {
	const prefix = "arn:aws:kinesis:"
	if len(arn) <= len(prefix) {
		return ""
	}

	// Format: arn:aws:kinesis:region:account:stream/name
	parts := strings.SplitN(arn, ":", arnKinesisPartCount)
	if len(parts) < arnKinesisPartCount {
		return ""
	}

	last := parts[arnKinesisPartCount-1]
	const streamPrefix = "stream/"
	if len(last) <= len(streamPrefix) {
		return ""
	}

	return last[len(streamPrefix):]
}

// functionNameFromARN extracts the function name from a Lambda ARN.
// Example: arn:aws:lambda:us-east-1:000000000000:function:my-func → my-func.
func functionNameFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", arnLambdaPartCount)
	if len(parts) < arnLambdaPartCount {
		return ""
	}

	return parts[arnLambdaPartCount-1]
}

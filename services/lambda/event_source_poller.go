package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	// arnKinesisPartCount is the number of colon-separated parts in a Kinesis ARN.
	arnKinesisPartCount = 6
	// arnLambdaPartCount is the number of colon-separated parts in a Lambda ARN.
	arnLambdaPartCount = 7
	// millisToSeconds converts Unix milliseconds to a float64 second timestamp.
	millisToSeconds = 1000.0
	// ddbStreamShardID is the single fixed shard ID used by all DynamoDB streams.
	ddbStreamShardID = "shardId-00000000000000000001-00000001"
)

// DynamoDBStreamRecord is a single record from a DynamoDB stream.
// The image fields use DynamoDB JSON wire format (e.g. {"pk": {"S": "value"}}).
type DynamoDBStreamRecord struct {
	NewImage                    map[string]any
	OldImage                    map[string]any
	Keys                        map[string]any
	EventID                     string
	EventName                   string // INSERT, MODIFY, or REMOVE
	SequenceNumber              string
	StreamViewType              string
	ApproximateCreationDateTime float64 // Unix epoch seconds
	SizeBytes                   int64
}

// DynamoDBStreamsReader reads records from a DynamoDB stream.
// It is implemented by the DynamoDB backend adapter in the CLI wiring layer.
type DynamoDBStreamsReader interface {
	// GetStreamShardIterator returns an iterator for the single shard of the stream.
	// streamARN is the full DynamoDB stream ARN. iteratorType is TRIM_HORIZON or LATEST.
	GetStreamShardIterator(streamARN, iteratorType string) (string, error)
	// GetStreamRecords reads up to limit records from the given iterator.
	GetStreamRecords(iteratorToken string, limit int) ([]DynamoDBStreamRecord, string, error)
}

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

// SQSMessage is a single SQS message delivered to a Lambda function via ESM.
type SQSMessage struct {
	Attributes    map[string]string
	MessageID     string
	ReceiptHandle string
	Body          string
	MD5OfBody     string
}

// SQSReader is the interface for consuming SQS messages in the ESM poller.
// It is implemented by the SQS backend adapter in the CLI wiring layer.
type SQSReader interface {
	// ReceiveMessagesLocal pulls up to maxMessages from the queue identified by queueARN.
	ReceiveMessagesLocal(queueARN string, maxMessages int) ([]*SQSMessage, error)
	// DeleteMessagesLocal removes the messages identified by receiptHandles from the queue.
	DeleteMessagesLocal(queueARN string, receiptHandles []string) error
}

// EventSourcePoller polls Kinesis streams, SQS queues, and DynamoDB streams for
// new records and invokes Lambda functions for enabled event source mappings.
type EventSourcePoller struct {
	kinesisReader    KinesisReader
	sqsReader        SQSReader
	ddbStreamsReader DynamoDBStreamsReader
	lambdaBackend    *InMemoryBackend
	shardIterators   map[string]string
	mu               *lockmetrics.RWMutex
	// sqsInvoker is an optional override for the Lambda invocation step used
	// when processing SQS messages. When nil the real InMemoryBackend is used.
	// Intended for use in unit tests only.
	sqsInvoker func(ctx context.Context, fnName string) error
	// ddbInvoker is an optional override for the Lambda invocation step used
	// when processing DynamoDB stream records. When nil the real InMemoryBackend is used.
	// Intended for use in unit tests only.
	ddbInvoker func(ctx context.Context, fnName string, payload []byte) error
}

// NewEventSourcePoller creates a new EventSourcePoller.
func NewEventSourcePoller(
	lambdaBackend *InMemoryBackend,
	kinesisReader KinesisReader,
) *EventSourcePoller {
	return &EventSourcePoller{
		lambdaBackend:  lambdaBackend,
		kinesisReader:  kinesisReader,
		shardIterators: make(map[string]string),
		mu:             lockmetrics.New("lambda.esm"),
	}
}

// SetSQSReader sets the SQS reader used to poll SQS queues for ESM delivery.
func (p *EventSourcePoller) SetSQSReader(r SQSReader) {
	p.mu.Lock("SetSQSReader")
	defer p.mu.Unlock()

	p.sqsReader = r
}

// SetDynamoDBStreamsReader sets the DynamoDB Streams reader used to poll DynamoDB
// streams for ESM delivery.
func (p *EventSourcePoller) SetDynamoDBStreamsReader(r DynamoDBStreamsReader) {
	p.mu.Lock("SetDynamoDBStreamsReader")
	defer p.mu.Unlock()

	p.ddbStreamsReader = r
}

// getDDBStreamsReader returns the DynamoDB Streams reader under a read lock.
func (p *EventSourcePoller) getDDBStreamsReader() DynamoDBStreamsReader {
	p.mu.RLock("getDDBStreamsReader")
	defer p.mu.RUnlock()

	return p.ddbStreamsReader
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
	mappings := p.lambdaBackend.ListEventSourceMappings("", "", 0).Data
	for _, m := range mappings {
		if m.State != ESMStateEnabled {
			continue
		}

		if isSQSARN(m.EventSourceARN) {
			p.mu.RLock("poll")
			sqsR := p.sqsReader
			p.mu.RUnlock()

			if sqsR != nil {
				p.processSQSMapping(ctx, m, sqsR)
			}

			continue
		}

		if isDynamoDBStreamARN(m.EventSourceARN) {
			ddbR := p.getDDBStreamsReader()
			if ddbR != nil {
				p.processDynamoDBStreamMapping(ctx, m, ddbR)
			}

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
		logger.Load(ctx).WarnContext(ctx, "event source poller: failed to get shard IDs",
			"stream", streamName, "error", err)

		return
	}

	for _, shardID := range shardIDs {
		iterKey := m.UUID + ":" + shardID

		p.mu.Lock("processMapping")
		it, exists := p.shardIterators[iterKey]
		p.mu.Unlock()

		if !exists {
			// Initialize iterator at starting position
			it, err = p.kinesisReader.GetShardIterator(streamName, shardID, m.StartingPosition, "")
			if err != nil {
				logger.Load(ctx).WarnContext(ctx, "event source poller: failed to get shard iterator",
					"stream", streamName, "shard", shardID, "error", err)

				continue
			}

			p.mu.Lock("processMapping")
			p.shardIterators[iterKey] = it
			p.mu.Unlock()
		}

		records, nextIt, readErr := p.kinesisReader.GetRecords(it, m.BatchSize)
		if readErr != nil {
			// Iterator may have expired; reset it
			p.mu.Lock("processMapping")
			delete(p.shardIterators, iterKey)
			p.mu.Unlock()
			logger.Load(ctx).WarnContext(ctx, "event source poller: GetRecords failed, resetting iterator",
				"stream", streamName, "shard", shardID, "error", readErr)

			continue
		}

		p.mu.Lock("processMapping")
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
		logger.Load(ctx).WarnContext(ctx, "event source poller: failed to marshal event", "error", err)

		return
	}

	// Extract function name from ARN
	fnName := functionNameFromARN(m.FunctionARN)
	if fnName == "" {
		fnName = m.FunctionARN
	}

	_, _, err = p.lambdaBackend.InvokeFunction(ctx, fnName, InvocationTypeEvent, payload)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "event source poller: Lambda invocation failed",
			"function", fnName, "stream", streamName, "error", err)
	} else {
		logger.Load(ctx).DebugContext(ctx, "event source poller: invoked Lambda",
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

// isSQSARN reports whether the given ARN identifies an SQS queue.
func isSQSARN(resourceARN string) bool {
	return strings.HasPrefix(resourceARN, "arn:aws:sqs:")
}

// isDynamoDBStreamARN reports whether the given ARN identifies a DynamoDB stream.
func isDynamoDBStreamARN(resourceARN string) bool {
	return strings.HasPrefix(resourceARN, "arn:aws:dynamodb:") && strings.Contains(resourceARN, "/stream/")
}

// processDynamoDBStreamMapping polls a DynamoDB stream's single shard and invokes Lambda.
func (p *EventSourcePoller) processDynamoDBStreamMapping(
	ctx context.Context,
	m *EventSourceMapping,
	reader DynamoDBStreamsReader,
) {
	iterKey := m.UUID + ":" + ddbStreamShardID

	p.mu.RLock("processDDBMapping.read")
	it, exists := p.shardIterators[iterKey]
	p.mu.RUnlock()

	var err error

	if !exists {
		it, err = reader.GetStreamShardIterator(m.EventSourceARN, m.StartingPosition)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "event source poller: failed to get DDB shard iterator",
				"stream", m.EventSourceARN, "error", err)

			return
		}

		p.mu.Lock("processDDBMapping.initIter")
		p.shardIterators[iterKey] = it
		p.mu.Unlock()
	}

	records, nextIt, readErr := reader.GetStreamRecords(it, m.BatchSize)
	if readErr != nil {
		p.mu.Lock("processDDBMapping.resetIter")
		delete(p.shardIterators, iterKey)
		p.mu.Unlock()
		logger.Load(ctx).WarnContext(ctx, "event source poller: DDB GetStreamRecords failed, resetting iterator",
			"stream", m.EventSourceARN, "error", readErr)

		return
	}

	p.mu.Lock("processDDBMapping.advanceIter")
	p.shardIterators[iterKey] = nextIt
	p.mu.Unlock()

	if len(records) == 0 {
		return
	}

	p.invokeLambdaForDDB(ctx, m, records)
}

// invokeLambdaForDDB formats DynamoDB stream records as a Lambda event and invokes the function.
func (p *EventSourcePoller) invokeLambdaForDDB(
	ctx context.Context,
	m *EventSourceMapping,
	records []DynamoDBStreamRecord,
) {
	type ddbStreamRecord struct {
		Keys                        map[string]any `json:"Keys,omitempty"`
		NewImage                    map[string]any `json:"NewImage,omitempty"`
		OldImage                    map[string]any `json:"OldImage,omitempty"`
		SequenceNumber              string         `json:"SequenceNumber"`
		StreamViewType              string         `json:"StreamViewType,omitempty"`
		ApproximateCreationDateTime float64        `json:"ApproximateCreationDateTime,omitempty"`
		SizeBytes                   int64          `json:"SizeBytes,omitempty"`
	}
	type lambdaRecord struct {
		EventID        string          `json:"eventID"`
		EventName      string          `json:"eventName"`
		EventVersion   string          `json:"eventVersion"`
		EventSource    string          `json:"eventSource"`
		AWSRegion      string          `json:"awsRegion"`
		EventSourceARN string          `json:"eventSourceARN"`
		Dynamodb       ddbStreamRecord `json:"dynamodb"`
	}
	type lambdaEvent struct {
		Records []lambdaRecord `json:"Records"`
	}

	eventRecords := make([]lambdaRecord, len(records))
	for i, r := range records {
		eventRecords[i] = lambdaRecord{
			EventID:        r.EventID,
			EventName:      r.EventName,
			EventVersion:   "1.1",
			EventSource:    "aws:dynamodb",
			AWSRegion:      p.lambdaBackend.region,
			EventSourceARN: m.EventSourceARN,
			Dynamodb: ddbStreamRecord{
				SequenceNumber:              r.SequenceNumber,
				ApproximateCreationDateTime: r.ApproximateCreationDateTime,
				StreamViewType:              r.StreamViewType,
				SizeBytes:                   r.SizeBytes,
				Keys:                        r.Keys,
				NewImage:                    r.NewImage,
				OldImage:                    r.OldImage,
			},
		}
	}

	payload, err := json.Marshal(lambdaEvent{Records: eventRecords})
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "event source poller: failed to marshal DDB event", "error", err)

		return
	}

	fnName := functionNameFromARN(m.FunctionARN)
	if fnName == "" {
		fnName = m.FunctionARN
	}

	var invokeErr error
	if p.ddbInvoker != nil {
		invokeErr = p.ddbInvoker(ctx, fnName, payload)
	} else {
		_, _, invokeErr = p.lambdaBackend.InvokeFunction(ctx, fnName, InvocationTypeEvent, payload)
	}

	if invokeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "event source poller: DDB Lambda invocation failed",
			"function", fnName, "stream", m.EventSourceARN, "error", invokeErr)
	} else {
		logger.Load(ctx).DebugContext(ctx, "event source poller: invoked Lambda for DDB stream",
			"function", fnName, "records", len(records))
	}
}

// processSQSMapping polls an SQS queue, invokes Lambda with the messages, and
// deletes the messages on successful invocation.
func (p *EventSourcePoller) processSQSMapping(ctx context.Context, m *EventSourceMapping, reader SQSReader) {
	msgs, err := reader.ReceiveMessagesLocal(m.EventSourceARN, m.BatchSize)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "esm sqs: failed to receive messages",
			"queue", m.EventSourceARN, "error", err)

		return
	}

	if len(msgs) == 0 {
		return
	}

	receiptHandles, invErr := p.invokeLambdaForSQS(ctx, m, msgs)
	if invErr != nil {
		logger.Load(ctx).WarnContext(ctx, "esm sqs: Lambda invocation failed",
			"function", m.FunctionARN, "error", invErr)

		return
	}

	if delErr := reader.DeleteMessagesLocal(m.EventSourceARN, receiptHandles); delErr != nil {
		logger.Load(ctx).WarnContext(ctx, "esm sqs: failed to delete messages",
			"queue", m.EventSourceARN, "error", delErr)
	}
}

// invokeLambdaForSQS formats SQS messages as a Lambda SQS event and invokes the function.
// On success it returns the receipt handles of the delivered messages.
func (p *EventSourcePoller) invokeLambdaForSQS(
	ctx context.Context,
	m *EventSourceMapping,
	msgs []*SQSMessage,
) ([]string, error) {
	type sqsEventRecord struct {
		Attributes     map[string]string `json:"attributes,omitempty"`
		MessageID      string            `json:"messageId"`
		ReceiptHandle  string            `json:"receiptHandle"`
		Body           string            `json:"body"`
		MD5OfBody      string            `json:"md5OfBody"`
		EventSource    string            `json:"eventSource"`
		EventSourceARN string            `json:"eventSourceARN"`
		AWSRegion      string            `json:"awsRegion"`
	}
	type sqsEvent struct {
		Records []sqsEventRecord `json:"Records"`
	}

	records := make([]sqsEventRecord, len(msgs))
	receiptHandles := make([]string, len(msgs))

	for i, msg := range msgs {
		records[i] = sqsEventRecord{
			MessageID:      msg.MessageID,
			ReceiptHandle:  msg.ReceiptHandle,
			Body:           msg.Body,
			Attributes:     msg.Attributes,
			MD5OfBody:      msg.MD5OfBody,
			EventSource:    "aws:sqs",
			EventSourceARN: m.EventSourceARN,
			AWSRegion:      p.lambdaBackend.region,
		}
		receiptHandles[i] = msg.ReceiptHandle
	}

	payload, err := json.Marshal(sqsEvent{Records: records})
	if err != nil {
		return nil, fmt.Errorf("marshal sqs event: %w", err)
	}

	fnName := functionNameFromARN(m.FunctionARN)
	if fnName == "" {
		fnName = m.FunctionARN
	}

	var invokeErr error
	if p.sqsInvoker != nil {
		invokeErr = p.sqsInvoker(ctx, fnName)
	} else {
		_, _, invokeErr = p.lambdaBackend.InvokeFunction(ctx, fnName, InvocationTypeEvent, payload)
	}

	if invokeErr != nil {
		return nil, invokeErr
	}

	logger.Load(ctx).DebugContext(ctx, "esm sqs: invoked Lambda",
		"function", fnName, "messages", len(msgs))

	return receiptHandles, nil
}

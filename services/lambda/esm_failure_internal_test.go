package lambda

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deliveredMsg struct {
	target string
	body   []byte
}

type recordingDelivery struct {
	msgs []deliveredMsg
	mu   sync.Mutex
}

func (r *recordingDelivery) DeliverToTarget(
	_ context.Context,
	target string,
	payload []byte,
	_ map[string]string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.msgs = append(r.msgs, deliveredMsg{target: target, body: payload})

	return nil
}

type s3Put struct {
	bucket, key string
	body        []byte
}

type recordingS3 struct {
	puts []s3Put
	mu   sync.Mutex
}

func (r *recordingS3) PutObject(_ context.Context, bucket, key string, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.puts = append(r.puts, s3Put{bucket: bucket, key: key, body: body})

	return nil
}

type onceKinesisReader struct {
	records []KinesisRecord
	mu      sync.Mutex
	served  bool
}

func (*onceKinesisReader) GetShardIDs(string) ([]string, error) {
	return []string{"shardId-000000000001"}, nil
}

func (*onceKinesisReader) GetShardIterator(_, _, _, _ string) (string, error) { return "it", nil }

func (r *onceKinesisReader) GetRecords(string, int) ([]KinesisRecord, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.served {
		return nil, "it", nil
	}

	r.served = true

	return r.records, "it", nil
}

type failureHarness struct {
	b        *InMemoryBackend
	p        *EventSourcePoller
	delivery *recordingDelivery
	s3       *recordingS3
	metrics  *kafkaPointRecorder
	calls    *[]int
	m        *EventSourceMapping
}

const (
	testKinesisARN = "arn:aws:kinesis:us-east-1:000000000000:stream/s"
	testFnARN      = "arn:aws:lambda:us-east-1:000000000000:function:f"
)

func newFailureHarness(
	t *testing.T, in *CreateEventSourceMappingInput, failOn func(payload string) bool, records []KinesisRecord,
) *failureHarness {
	t.Helper()

	b := newKafkaTestBackend(t)
	h := &failureHarness{b: b, delivery: &recordingDelivery{}, s3: &recordingS3{}, metrics: &kafkaPointRecorder{}}
	h.calls = new([]int)
	b.SetMetricEmitter(h.metrics)
	b.SetAsyncDestinationDelivery(h.delivery)
	b.SetESMS3Destination(h.s3)

	require.NoError(t, b.CreateFunction(&FunctionConfiguration{FunctionName: "f"}))

	in.FunctionName = "f"
	in.Enabled = true
	in.StartingPosition = "TRIM_HORIZON"
	in.BatchSize = 10
	in.MetricsConfig = &ESMMetricsConfig{Metrics: []string{"EventCount"}}

	m, err := b.CreateEventSourceMapping(in)
	require.NoError(t, err)

	h.m = m
	h.p = NewEventSourcePoller(b, &onceKinesisReader{records: records})

	var mu sync.Mutex

	h.p.streamInvoker = func(_ context.Context, _ string, payload []byte) invokeOutcome {
		mu.Lock()
		defer mu.Unlock()

		var ev struct {
			Records []json.RawMessage `json:"Records"`
		}

		require.NoError(t, json.Unmarshal(payload, &ev))
		*h.calls = append(*h.calls, len(ev.Records))

		if failOn(string(payload)) {
			return invokeOutcome{functionError: "Unhandled"}
		}

		return invokeOutcome{}
	}

	return h
}

func kinesisRecords(n int, arrival time.Time) []KinesisRecord {
	recs := make([]KinesisRecord, n)
	for i := range recs {
		recs[i] = KinesisRecord{
			PartitionKey:   "pk",
			SequenceNumber: "seq" + strconv.Itoa(i+1),
			Data:           []byte("d" + strconv.Itoa(i+1)),
			ArrivalTime:    arrival,
		}
	}

	return recs
}

func TestStreamRetryAndOnFailureDestination(t *testing.T) {
	t.Parallel()

	always := func(string) bool { return true }
	never := func(string) bool { return false }
	poisonSeq2 := func(p string) bool { return strings.Contains(p, "seq2") }

	tests := []struct {
		fail          func(string) bool
		wantMetrics   map[string]float64
		name          string
		dest          string
		wantCalls     []int
		recs          []KinesisRecord
		in            CreateEventSourceMappingInput
		polls         int
		wantSQS       int
		wantS3        int
		wantInvokeCnt int
		wantBatchSize int
	}{
		{
			name:          "retries exhausted to sqs",
			in:            CreateEventSourceMappingInput{MaximumRetryAttempts: new(2)},
			dest:          "arn:aws:sqs:us-east-1:000000000000:dlq",
			fail:          always,
			recs:          kinesisRecords(3, time.Now()),
			polls:         5,
			wantCalls:     []int{3, 3, 3},
			wantSQS:       1,
			wantInvokeCnt: 3,
			wantBatchSize: 3,
			wantMetrics:   map[string]float64{"DroppedEventCount": 3, "OnFailureDestinationDeliveredEventCount": 3},
		},
		{
			name:          "zero retries drops after first failure to s3",
			in:            CreateEventSourceMappingInput{MaximumRetryAttempts: new(0)},
			dest:          "arn:aws:s3:::dlq-bucket",
			fail:          always,
			recs:          kinesisRecords(2, time.Now()),
			polls:         2,
			wantCalls:     []int{2},
			wantS3:        1,
			wantInvokeCnt: 1,
			wantBatchSize: 2,
			wantMetrics:   map[string]float64{"DroppedEventCount": 2, "OnFailureDestinationDeliveredEventCount": 2},
		},
		{
			name:        "success delivers nothing",
			in:          CreateEventSourceMappingInput{MaximumRetryAttempts: new(0)},
			dest:        "arn:aws:sqs:us-east-1:000000000000:dlq",
			fail:        never,
			recs:        kinesisRecords(2, time.Now()),
			polls:       2,
			wantCalls:   []int{2},
			wantMetrics: map[string]float64{"InvokedEventCount": 2},
		},
		{
			name: "bisect isolates the poison record",
			in: CreateEventSourceMappingInput{
				MaximumRetryAttempts:       new(0),
				BisectBatchOnFunctionError: true,
			},
			dest:          "arn:aws:sqs:us-east-1:000000000000:dlq",
			fail:          poisonSeq2,
			recs:          kinesisRecords(4, time.Now()),
			polls:         1,
			wantCalls:     []int{4, 2, 1, 1, 2},
			wantSQS:       1,
			wantInvokeCnt: 1,
			wantBatchSize: 1,
		},
		{
			name:          "expired records are dropped without invoking",
			in:            CreateEventSourceMappingInput{MaximumRecordAgeInSeconds: 60},
			dest:          "arn:aws:sqs:us-east-1:000000000000:dlq",
			fail:          never,
			recs:          kinesisRecords(2, time.Now().Add(-time.Hour)),
			polls:         1,
			wantSQS:       1,
			wantInvokeCnt: 0,
			wantBatchSize: 2,
			wantMetrics:   map[string]float64{"DroppedEventCount": 2, "OnFailureDestinationDeliveredEventCount": 2},
		},
		{
			name:        "no destination still counts drops",
			in:          CreateEventSourceMappingInput{MaximumRetryAttempts: new(0)},
			fail:        always,
			recs:        kinesisRecords(2, time.Now()),
			polls:       2,
			wantCalls:   []int{2},
			wantMetrics: map[string]float64{"DroppedEventCount": 2},
		},
		{
			name:        "unlimited retries keep the shard blocked",
			in:          CreateEventSourceMappingInput{},
			dest:        "arn:aws:sqs:us-east-1:000000000000:dlq",
			fail:        always,
			recs:        kinesisRecords(1, time.Now()),
			polls:       4,
			wantCalls:   []int{1, 1, 1, 1},
			wantMetrics: map[string]float64{"FailedInvokeEventCount": 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := tt.in
			in.EventSourceARN = testKinesisARN

			if tt.dest != "" {
				in.DestinationConfig = &ESMDestinationConfig{OnFailure: &ESMDestination{Destination: tt.dest}}
			}

			h := newFailureHarness(t, &in, tt.fail, tt.recs)

			for range tt.polls {
				h.p.poll(t.Context())
			}

			assert.Equal(t, tt.wantCalls, *h.calls)

			sums := h.metrics.sums()
			for k, v := range tt.wantMetrics {
				assert.InDelta(t, v, sums[k], 0, k)
			}

			assert.Len(t, h.delivery.msgs, tt.wantSQS)
			assert.Len(t, h.s3.puts, tt.wantS3)

			var body []byte

			switch {
			case tt.wantSQS > 0:
				assert.Equal(t, tt.dest, h.delivery.msgs[0].target)
				body = h.delivery.msgs[0].body
			case tt.wantS3 > 0:
				assert.Equal(t, "dlq-bucket", h.s3.puts[0].bucket)
				body = h.s3.puts[0].body
			default:
				return
			}

			var rec struct {
				KinesisBatchInfo struct {
					ShardID             string `json:"shardId"`
					StartSequenceNumber string `json:"startSequenceNumber"`
					StreamArn           string `json:"streamArn"`
					BatchSize           int    `json:"batchSize"`
				} `json:"KinesisBatchInfo"`
				Payload        any    `json:"payload"`
				Version        string `json:"version"`
				RequestContext struct {
					Condition   string `json:"condition"`
					FunctionArn string `json:"functionArn"`
					Count       int    `json:"approximateInvokeCount"`
				} `json:"requestContext"`
			}

			require.NoError(t, json.Unmarshal(body, &rec))
			assert.Equal(t, "RetryAttemptsExhausted", rec.RequestContext.Condition)
			assert.Equal(t, testFnARN, rec.RequestContext.FunctionArn)
			assert.Equal(t, tt.wantInvokeCnt, rec.RequestContext.Count)
			assert.Equal(t, "1.0", rec.Version)
			assert.Equal(t, "shardId-000000000001", rec.KinesisBatchInfo.ShardID)
			assert.Equal(t, testKinesisARN, rec.KinesisBatchInfo.StreamArn)
			assert.Equal(t, tt.wantBatchSize, rec.KinesisBatchInfo.BatchSize)

			if tt.wantS3 > 0 {
				assert.Contains(t, h.s3.puts[0].key, "aws/lambda/"+h.m.UUID+"/shardId-000000000001/")
				assert.IsType(t, "", rec.Payload)
			} else {
				assert.Nil(t, rec.Payload)
			}
		})
	}
}

type producingConsumer struct {
	*fakeKafkaConsumer
	produced []producedMsg
	pmu      sync.Mutex
}

type producedMsg struct {
	topic string
	key   []byte
	value []byte
}

func (c *producingConsumer) ProduceFailed(_ context.Context, topic string, key, value []byte) error {
	c.pmu.Lock()
	defer c.pmu.Unlock()

	c.produced = append(c.produced, producedMsg{topic: topic, key: key, value: value})

	return nil
}

func TestKafkaRetryExhaustionDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		dest         string
		policy       retryPolicy
		wantInvokes  int
		wantProduced int
		wantSQS      int
		wantS3       int
	}{
		{
			name:         "kafka topic receives each record",
			dest:         "kafka://dlq",
			policy:       retryPolicy{limited: true, maxRetries: 1},
			wantInvokes:  2,
			wantProduced: 3,
		},
		{
			name:        "sqs gets per record metadata",
			dest:        "arn:aws:sqs:us-east-1:000000000000:dlq",
			policy:      retryPolicy{limited: true, maxRetries: 0},
			wantInvokes: 1,
			wantSQS:     3,
		},
		{
			name:        "s3 stores the payload",
			dest:        "arn:aws:s3:::dlq-bucket",
			policy:      retryPolicy{limited: true, maxRetries: 0},
			wantInvokes: 1,
			wantS3:      3,
		},
		{
			name:         "infinite retries are capped at ten with a destination",
			dest:         "kafka://dlq",
			policy:       retryPolicy{},
			wantInvokes:  11,
			wantProduced: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newKafkaTestBackend(t)
				rec := &kafkaPointRecorder{}
				del := &recordingDelivery{}
				s3 := &recordingS3{}
				b.SetMetricEmitter(rec)
				b.SetAsyncDestinationDelivery(del)
				b.SetESMS3Destination(s3)

				p := NewEventSourcePoller(b, nil)
				invokes := 0
				p.kafkaInvoker = func(context.Context, string, []byte) error {
					invokes++

					return errKafkaTestFunction
				}

				cons := &producingConsumer{fakeKafkaConsumer: newFakeKafkaConsumer()}
				spec := kafkaWorkerSpec{
					UUID: "u-1", FunctionARN: testFnARN, EventSource: kafkaEventSourceSelfManaged,
					BootstrapServers: "b:9092", Policy: tt.policy, Destination: tt.dest,
					Metrics: &ESMMetricsConfig{Metrics: []string{"EventCount"}},
				}

				p.deliverKafkaBatch(t.Context(), cons, spec, kafkaOffsets(3))

				assert.Equal(t, tt.wantInvokes, invokes)
				assert.Len(t, cons.produced, tt.wantProduced)
				assert.Len(t, del.msgs, tt.wantSQS)
				assert.Len(t, s3.puts, tt.wantS3)
				assert.Equal(t, []int64{0, 1, 2}, cons.committedOffsets())

				sums := rec.sums()
				assert.InDelta(t, 3, sums["DroppedEventCount"], 0)
				assert.InDelta(t, 3, sums["OnFailureDestinationDeliveredEventCount"], 0)

				var body []byte

				switch {
				case tt.wantProduced > 0:
					assert.Equal(t, "dlq", cons.produced[0].topic)
					assert.Equal(t, []byte("k"), cons.produced[0].key)
					body = cons.produced[0].value
				case tt.wantSQS > 0:
					body = del.msgs[0].body
				default:
					body = s3.puts[0].body
				}

				var out struct {
					Payload        any `json:"payload"`
					RequestContext struct {
						Condition string `json:"condition"`
					} `json:"requestContext"`
					KafkaBatchInfo struct {
						RecordInfo struct {
							Offset string `json:"offset"`
						} `json:"recordInfo"`
						BootstrapServers string `json:"bootstrapServers"`
						BatchSize        int    `json:"batchSize"`
					} `json:"KafkaBatchInfo"`
				}

				require.NoError(t, json.Unmarshal(body, &out))
				assert.Equal(t, "RetryAttemptsExhausted", out.RequestContext.Condition)
				assert.Equal(t, "0", out.KafkaBatchInfo.RecordInfo.Offset)
				assert.Equal(t, 1, out.KafkaBatchInfo.BatchSize)
				assert.Equal(t, "b:9092", out.KafkaBatchInfo.BootstrapServers)

				switch {
				case tt.wantProduced > 0:
					assert.IsType(t, map[string]any{}, out.Payload)
				case tt.wantS3 > 0:
					assert.IsType(t, "", out.Payload)
				default:
					assert.Nil(t, out.Payload)
				}
			})
		})
	}
}

func TestKafkaRecordAgeAndBisect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		recs         func() []KafkaRecord
		failWhen     func(payload string) bool
		name         string
		policy       retryPolicy
		wantInvokes  int
		wantProduced int
	}{
		{
			name:   "expired records skip the function",
			policy: retryPolicy{maxAge: time.Minute},
			recs: func() []KafkaRecord {
				return []KafkaRecord{
					{Topic: "t", Offset: 0, Timestamp: time.Now().Add(-time.Hour), Value: []byte("a")},
					{Topic: "t", Offset: 1, Timestamp: time.Now(), Value: []byte("b")},
				}
			},
			failWhen:     func(string) bool { return false },
			wantInvokes:  1,
			wantProduced: 1,
		},
		{
			name:         "bisect isolates the failing record",
			policy:       retryPolicy{bisect: true, limited: true},
			recs:         func() []KafkaRecord { return kafkaOffsets(4) },
			failWhen:     func(p string) bool { return strings.Contains(p, `"offset":2,`) },
			wantInvokes:  5,
			wantProduced: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newKafkaTestBackend(t)
				p := NewEventSourcePoller(b, nil)
				invokes := 0
				p.kafkaInvoker = func(_ context.Context, _ string, payload []byte) error {
					invokes++

					if tt.failWhen(string(payload)) {
						return errKafkaTestFunction
					}

					return nil
				}

				cons := &producingConsumer{fakeKafkaConsumer: newFakeKafkaConsumer()}
				spec := kafkaWorkerSpec{
					UUID: "u-2", FunctionARN: testFnARN, EventSource: kafkaEventSourceSelfManaged,
					Policy: tt.policy, Destination: "kafka://dlq",
				}

				recs := tt.recs()

				p.deliverKafkaBatch(t.Context(), cons, spec, recs)

				assert.Equal(t, tt.wantInvokes, invokes)
				assert.Len(t, cons.produced, tt.wantProduced)
				assert.Len(t, cons.committedOffsets(), len(recs))
			})
		})
	}
}

type memCWLogs struct {
	lines map[string][]string
	mu    sync.Mutex
}

func (m *memCWLogs) EnsureLogGroupAndStream(string, string) error { return nil }

func (m *memCWLogs) PutLogLines(group, stream string, msgs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.lines == nil {
		m.lines = map[string][]string{}
	}

	m.lines[group+"|"+stream] = append(m.lines[group+"|"+stream], msgs...)

	return nil
}

func (m *memCWLogs) all() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []string
	for _, v := range m.lines {
		out = append(out, v...)
	}

	return out
}

func TestESMSystemLogLevels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		level      string
		wantEvents []string
	}{
		{name: "debug logs everything", level: "DEBUG", wantEvents: []string{
			"POLLER_STATUS_EVENT", "ESM_PROCESSING_EVENT", "KAFKA_STATUS_EVENT",
		}},
		{name: "info drops offsets", level: "INFO", wantEvents: []string{
			"POLLER_STATUS_EVENT", "ESM_PROCESSING_EVENT",
		}},
		{name: "warn only errors", level: "WARN", wantEvents: []string{"ESM_PROCESSING_EVENT"}},
		{name: "unset logs nothing", level: "", wantEvents: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)
			cw := &memCWLogs{}
			b.SetCWLogsBackend(cw)

			spec := kafkaWorkerSpec{
				UUID: "u-3", FunctionARN: testFnARN, EventSourceARN: "arn:aws:kafka:us-east-1:000000000000:cluster/c/x",
				ResourceARN: "arn:aws:lambda:us-east-1:000000000000:event-source-mapping:u-3", LogLevel: tt.level,
				BootstrapServers: "b:9092", Consumer: kafkaConsumerConfig{Topics: []string{"t"}, GroupID: "g"},
			}

			lg := newESMLogger(b, spec)
			lg.consumerBuilt(t.Context(), spec)
			lg.warn(t.Context(), "boom", "Unhandled")
			lg.committed(t.Context(), kafkaOffsets(3))

			lines := cw.all()
			got := make([]string, 0, len(lines))

			for _, line := range lines {
				var rec struct {
					Offsets *struct {
						Partition       string `json:"partition"`
						CommittedOffset int64  `json:"committedOffset"`
					} `json:"kafkaPartitionOffsets"`
					EventType string `json:"eventType"`
					LogLevel  string `json:"logLevel"`
					Processor string `json:"eventProcessorId"`
				}

				require.NoError(t, json.Unmarshal([]byte(line), &rec))
				assert.Equal(t, "u-3/0", rec.Processor)
				got = append(got, rec.EventType)

				if rec.Offsets != nil {
					assert.Equal(t, "t-0", rec.Offsets.Partition)
					assert.Equal(t, int64(3), rec.Offsets.CommittedOffset)
				}
			}

			assert.ElementsMatch(t, tt.wantEvents, got)
		})
	}
}

func TestValidateFailureDestination(t *testing.T) {
	t.Parallel()

	kafkaDest := &ESMDestinationConfig{OnFailure: &ESMDestination{Destination: "kafka://dlq"}}
	sqsDest := &ESMDestinationConfig{OnFailure: &ESMDestination{Destination: "arn:aws:sqs:us-east-1:0:q"}}

	tests := []struct {
		dc      *ESMDestinationConfig
		name    string
		arn     string
		topics  []string
		self    bool
		wantErr bool
	}{
		{
			name:   "kafka topic on msk",
			dc:     kafkaDest,
			arn:    "arn:aws:kafka:us-east-1:0:cluster/c/x",
			topics: []string{"src"},
		},
		{name: "kafka topic on self managed", dc: kafkaDest, self: true, topics: []string{"src"}},
		{name: "kafka topic on kinesis", dc: kafkaDest, arn: testKinesisARN, wantErr: true},
		{name: "kafka topic equals source", dc: kafkaDest, self: true, topics: []string{"dlq"}, wantErr: true},
		{name: "sqs anywhere", dc: sqsDest, arn: testKinesisARN},
		{name: "none", arn: testKinesisARN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateFailureDestination(tt.arn, tt.self, tt.topics, tt.dc)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidParameterValue)

				return
			}

			require.NoError(t, err)
		})
	}
}

type onceDDBReader struct {
	records []DynamoDBStreamRecord
	mu      sync.Mutex
	served  bool
}

func (*onceDDBReader) DescribeStreamShards(string) ([]string, error) {
	return []string{"shardId-00000001573689847184-864758bb"}, nil
}

func (*onceDDBReader) GetStreamShardIterator(_, _, _ string) (string, error) { return "it", nil }

func (r *onceDDBReader) GetStreamRecords(string, int) ([]DynamoDBStreamRecord, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.served {
		return nil, "it", nil
	}

	r.served = true

	return r.records, "it", nil
}

func TestDynamoDBStreamOnFailureDestination(t *testing.T) {
	t.Parallel()

	b := newKafkaTestBackend(t)
	del := &recordingDelivery{}
	rec := &kafkaPointRecorder{}
	b.SetAsyncDestinationDelivery(del)
	b.SetMetricEmitter(rec)
	require.NoError(t, b.CreateFunction(&FunctionConfiguration{FunctionName: "f"}))

	streamARN := "arn:aws:dynamodb:us-east-1:000000000000:table/t/stream/2019-11-14T00:04:06"
	_, err := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
		EventSourceARN: streamARN, FunctionName: "f", Enabled: true, BatchSize: 10,
		MaximumRetryAttempts: new(1),
		MetricsConfig:        &ESMMetricsConfig{Metrics: []string{"EventCount"}},
		DestinationConfig: &ESMDestinationConfig{
			OnFailure: &ESMDestination{Destination: "arn:aws:sns:us-east-1:000000000000:dlq"},
		},
	})
	require.NoError(t, err)

	p := NewEventSourcePoller(b, nil)
	p.ddbStreamsReader = &onceDDBReader{records: []DynamoDBStreamRecord{
		{
			EventID:                     "e1",
			EventName:                   "INSERT",
			SequenceNumber:              "800000000003126276362",
			ApproximateCreationDateTime: 1573690399,
		},
	}}

	invokes := 0
	p.ddbInvoker = func(context.Context, string, []byte) error {
		invokes++

		return errKafkaTestFunction
	}

	for range 4 {
		p.poll(t.Context())
	}

	assert.Equal(t, 2, invokes)
	require.Len(t, del.msgs, 1)

	var out struct {
		Info struct {
			Start     string `json:"startSequenceNumber"`
			First     string `json:"approximateArrivalOfFirstRecord"`
			StreamArn string `json:"streamArn"`
		} `json:"DDBStreamBatchInfo"`
		RequestContext struct {
			Count int `json:"approximateInvokeCount"`
		} `json:"requestContext"`
	}

	require.NoError(t, json.Unmarshal(del.msgs[0].body, &out))
	assert.Equal(t, "800000000003126276362", out.Info.Start)
	assert.Equal(t, "2019-11-14T00:13:19Z", out.Info.First)
	assert.Equal(t, streamARN, out.Info.StreamArn)
	assert.Equal(t, 2, out.RequestContext.Count)
	assert.InDelta(t, 1, rec.sums()["OnFailureDestinationDeliveredEventCount"], 0)
}

func TestESMMaximumRetryAttemptsWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   *int
		want string
	}{
		{name: "unset omitted", in: nil, want: ""},
		{name: "zero echoed", in: new(0), want: `"MaximumRetryAttempts":0`},
		{name: "infinite echoed", in: new(-1), want: `"MaximumRetryAttempts":-1`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := &EventSourceMapping{FunctionARN: testFnARN, MaximumRetryAttempts: tt.in}
			body, err := json.Marshal(toJSONESMResponse(m))
			require.NoError(t, err)

			if tt.want == "" {
				assert.NotContains(t, string(body), "MaximumRetryAttempts")

				return
			}

			assert.Contains(t, string(body), tt.want)
		})
	}
}

func TestKafkaOffsetLagMetrics(t *testing.T) {
	t.Parallel()

	mk := func(part int32, off, hw int64) KafkaRecord {
		r := kafkaTestRecord("t", part, off, "v")
		r.HighWatermark = hw

		return r
	}

	tests := []struct {
		want    map[string]float64
		name    string
		metrics []string
		recs    []KafkaRecord
	}{
		{
			name:    "lag per partition",
			metrics: []string{"KafkaMetrics"},
			recs:    []KafkaRecord{mk(0, 4, 10), mk(0, 5, 10), mk(1, 9, 10)},
			want:    map[string]float64{"MaxOffsetLag": 4, "SumOffsetLag": 4},
		},
		{
			name:    "caught up",
			metrics: []string{"KafkaMetrics"},
			recs:    []KafkaRecord{mk(0, 9, 10)},
			want:    map[string]float64{"MaxOffsetLag": 0, "SumOffsetLag": 0},
		},
		{
			name:    "unknown watermark",
			metrics: []string{"KafkaMetrics"},
			recs:    []KafkaRecord{mk(0, 1, 0)},
			want:    map[string]float64{},
		},
		{
			name:    "group not enabled",
			metrics: []string{"EventCount"},
			recs:    []KafkaRecord{mk(0, 1, 5)},
			want:    map[string]float64{"CommittedEventCount": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)
			rec := &kafkaPointRecorder{}
			b.SetMetricEmitter(rec)

			p := NewEventSourcePoller(b, nil)
			spec := kafkaWorkerSpec{
				UUID:        "u-4",
				FunctionARN: testFnARN,
				Metrics:     &ESMMetricsConfig{Metrics: tt.metrics},
			}

			p.commitKafka(t.Context(), newFakeKafkaConsumer(), spec, tt.recs)

			assert.Equal(t, tt.want, rec.sums())
		})
	}
}

package lambda

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptedKinesisReader struct {
	polls [][]KinesisRecord
	mu    sync.Mutex
}

func (*scriptedKinesisReader) GetShardIDs(string) ([]string, error) {
	return []string{"shardId-000000000001"}, nil
}

func (*scriptedKinesisReader) GetShardIterator(_, _, _, _ string) (string, error) { return "it", nil }

func (r *scriptedKinesisReader) GetRecords(string, int) ([]KinesisRecord, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.polls) == 0 {
		return nil, "it", nil
	}

	recs := r.polls[0]
	r.polls = r.polls[1:]

	return recs, "it", nil
}

type streamCall struct {
	Window                  *windowRange      `json:"window"`
	State                   map[string]any    `json:"state"`
	ShardID                 string            `json:"shardId"`
	EventSourceARN          string            `json:"eventSourceARN"`
	Records                 []json.RawMessage `json:"Records"`
	IsFinalInvokeForWindow  bool              `json:"isFinalInvokeForWindow"`
	IsWindowTerminatedEarly bool              `json:"isWindowTerminatedEarly"`
}

func (c streamCall) seqs(t *testing.T) []string {
	t.Helper()

	out := make([]string, 0, len(c.Records))

	for _, r := range c.Records {
		var rec struct {
			Kinesis struct {
				Seq string `json:"sequenceNumber"`
			} `json:"kinesis"`
		}

		require.NoError(t, json.Unmarshal(r, &rec))
		out = append(out, rec.Kinesis.Seq)
	}

	return out
}

type streamHarness struct {
	p     *EventSourcePoller
	now   time.Time
	calls []streamCall
	mu    sync.Mutex
}

func newStreamHarness(
	t *testing.T, in *CreateEventSourceMappingInput, polls [][]KinesisRecord, base time.Time,
	respond func(h *streamHarness, c streamCall) invokeOutcome,
) *streamHarness {
	t.Helper()

	b := newKafkaTestBackend(t)
	require.NoError(t, b.CreateFunction(&FunctionConfiguration{FunctionName: "f"}))

	in.FunctionName, in.EventSourceARN, in.Enabled = "f", testKinesisARN, true
	in.StartingPosition = "TRIM_HORIZON"

	if in.BatchSize == 0 {
		in.BatchSize = 100
	}

	_, err := b.CreateEventSourceMapping(in)
	require.NoError(t, err)

	h := &streamHarness{now: base}
	h.p = NewEventSourcePoller(b, &scriptedKinesisReader{polls: polls})
	h.p.clock = func() time.Time {
		h.mu.Lock()
		defer h.mu.Unlock()

		return h.now
	}
	h.p.streamInvoker = func(_ context.Context, _ string, payload []byte) invokeOutcome {
		var c streamCall

		require.NoError(t, json.Unmarshal(payload, &c))

		h.mu.Lock()
		h.calls = append(h.calls, c)
		h.mu.Unlock()

		return respond(h, c)
	}

	return h
}

func (h *streamHarness) advance(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.now = h.now.Add(d)
}

func (h *streamHarness) snapshot() []streamCall {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]streamCall(nil), h.calls...)
}

func keyedRecords(base time.Time, keys ...string) []KinesisRecord {
	recs := make([]KinesisRecord, len(keys))
	for i, k := range keys {
		recs[i] = KinesisRecord{
			PartitionKey:   k,
			SequenceNumber: strconv.Itoa(i + 1),
			Data:           []byte(k),
			ArrivalTime:    base.Add(time.Duration(i) * time.Second),
		}
	}

	return recs
}

func TestStreamParallelizationFactor(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	keys := []string{"a", "b", "c", "d", "e", "f", "a", "b", "c", "d", "e", "f"}

	tests := []struct {
		name      string
		factor    int
		wantPeak  int
		wantLanes int
	}{
		{name: "factor one is one batch", factor: 1, wantPeak: 1, wantLanes: 1},
		{name: "factor three runs lanes concurrently", factor: 3, wantLanes: 3},
		{name: "factor ten", factor: 10, wantLanes: 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lanes := map[int]struct{}{}
			for _, k := range keys {
				lanes[laneIndex(k, tt.factor)] = struct{}{}
			}

			want := len(lanes)

			var (
				mu       sync.Mutex
				active   int
				peak     int
				release  = make(chan struct{})
				once     sync.Once
				timedOut bool
			)

			h := newStreamHarness(t,
				&CreateEventSourceMappingInput{ParallelizationFactor: tt.factor},
				[][]KinesisRecord{keyedRecords(base, keys...)}, base,
				func(*streamHarness, streamCall) invokeOutcome {
					mu.Lock()
					active++
					peak = max(peak, active)

					if active == want {
						once.Do(func() { close(release) })
					}

					mu.Unlock()

					select {
					case <-release:
					case <-time.After(10 * time.Second):
						mu.Lock()
						timedOut = true
						mu.Unlock()
					}

					mu.Lock()
					active--
					mu.Unlock()

					return invokeOutcome{}
				})

			h.p.poll(t.Context())

			assert.False(t, timedOut, "lanes did not run concurrently")
			assert.Equal(t, want, peak)
			assert.Len(t, h.snapshot(), want)

			perKey := map[string][]string{}

			for _, c := range h.snapshot() {
				laneOf := -1

				for _, r := range c.Records {
					var rec struct {
						Kinesis struct {
							PK  string `json:"partitionKey"`
							Seq string `json:"sequenceNumber"`
						} `json:"kinesis"`
					}

					require.NoError(t, json.Unmarshal(r, &rec))

					l := laneIndex(rec.Kinesis.PK, tt.factor)
					if laneOf >= 0 {
						assert.Equal(t, laneOf, l, "batch mixes lanes")
					}

					laneOf = l
					perKey[rec.Kinesis.PK] = append(perKey[rec.Kinesis.PK], rec.Kinesis.Seq)
				}
			}

			for k, seqs := range perKey {
				assert.Len(t, seqs, 2, k)
				first, _ := strconv.Atoi(seqs[0])
				second, _ := strconv.Atoi(seqs[1])
				assert.Less(t, first, second, "partition key %s out of order", k)
			}
		})
	}
}

func TestStreamTumblingWindow(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	type step struct {
		recs    []KinesisRecord
		advance time.Duration
	}

	type want struct {
		state   string
		records int
		final   bool
	}

	counting := func(_ *streamHarness, c streamCall) invokeOutcome {
		n := 0
		if v, ok := c.State["n"].(float64); ok {
			n = int(v)
		}

		return invokeOutcome{body: []byte(`{"state":{"n":` + strconv.Itoa(n+len(c.Records)) + `}}`)}
	}

	tests := []struct {
		respond func(h *streamHarness, c streamCall) invokeOutcome
		name    string
		steps   []step
		want    []want
	}{
		{
			name: "state carried and final invoke at window end",
			steps: []step{
				{recs: keyedRecords(base.Add(5*time.Second), "k", "k")},
				{recs: keyedRecords(base.Add(30*time.Second), "k")},
				{advance: 30 * time.Second},
				{advance: 31 * time.Second},
			},
			want: []want{
				{records: 2, state: `{}`},
				{records: 1, state: `{"n":2}`},
				{records: 0, state: `{"n":3}`, final: true},
			},
			respond: counting,
		},
		{
			name: "record in next window closes the previous one",
			steps: []step{
				{recs: keyedRecords(base.Add(5*time.Second), "k")},
				{recs: keyedRecords(base.Add(65*time.Second), "k")},
			},
			want: []want{
				{records: 1, state: `{}`},
				{records: 0, state: `{"n":1}`, final: true},
				{records: 1, state: `{}`},
			},
			respond: counting,
		},
		{
			name: "response without state is a failure and retries with the same state",
			steps: []step{
				{recs: keyedRecords(base.Add(5*time.Second), "k")},
				{},
			},
			want: []want{
				{records: 1, state: `{}`},
				{records: 1, state: `{}`},
			},
			respond: func(h *streamHarness, c streamCall) invokeOutcome {
				if len(h.snapshot()) == 1 {
					return invokeOutcome{body: []byte(`{}`)}
				}

				return counting(h, c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			polls := make([][]KinesisRecord, 0, len(tt.steps))
			for _, s := range tt.steps {
				polls = append(polls, s.recs)
			}

			in := &CreateEventSourceMappingInput{TumblingWindowInSeconds: 60}
			h := newStreamHarness(t, in, polls, base, tt.respond)
			h.advance(10 * time.Second)

			for _, s := range tt.steps {
				h.advance(s.advance)
				h.p.poll(t.Context())
			}

			calls := h.snapshot()
			require.Len(t, calls, len(tt.want))

			for i, w := range tt.want {
				c := calls[i]
				assert.Len(t, c.Records, w.records, "call %d", i)
				assert.Equal(t, w.final, c.IsFinalInvokeForWindow, "call %d", i)
				assert.False(t, c.IsWindowTerminatedEarly)
				assert.Equal(t, "shardId-000000000001", c.ShardID)
				assert.Equal(t, testKinesisARN, c.EventSourceARN)
				require.NotNil(t, c.Window)

				state, err := json.Marshal(c.State)
				require.NoError(t, err)
				assert.JSONEq(t, w.state, string(state), "call %d", i)
			}

			assert.Equal(t, "2026-01-01T10:00:00Z", calls[0].Window.Start)
			assert.Equal(t, "2026-01-01T10:01:00Z", calls[0].Window.End)
		})
	}
}

func TestStreamPartialBatchResponse(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fail := func(body string) func(*streamHarness, streamCall) invokeOutcome {
		return func(h *streamHarness, _ streamCall) invokeOutcome {
			if len(h.snapshot()) == 1 {
				return invokeOutcome{body: []byte(body)}
			}

			return invokeOutcome{}
		}
	}

	tests := []struct {
		respond   func(*streamHarness, streamCall) invokeOutcome
		name      string
		types     []string
		wantSeqs  [][]string
		bisect    bool
		retryOnce bool
	}{
		{
			name:     "retries from the failed record",
			types:    []string{"ReportBatchItemFailures"},
			respond:  fail(`{"batchItemFailures":[{"itemIdentifier":"3"}]}`),
			wantSeqs: [][]string{{"1", "2", "3", "4"}, {"3", "4"}},
		},
		{
			name:     "lowest failed sequence is the checkpoint",
			types:    []string{"ReportBatchItemFailures"},
			respond:  fail(`{"batchItemFailures":[{"itemIdentifier":"4"},{"itemIdentifier":"2"}]}`),
			wantSeqs: [][]string{{"1", "2", "3", "4"}, {"2", "3", "4"}},
		},
		{
			name:     "bisect with partial response retries only the remainder",
			types:    []string{"ReportBatchItemFailures"},
			bisect:   true,
			respond:  fail(`{"batchItemFailures":[{"itemIdentifier":"3"}]}`),
			wantSeqs: [][]string{{"1", "2", "3", "4"}, {"3", "4"}},
		},
		{
			name:     "empty identifier is a complete failure",
			types:    []string{"ReportBatchItemFailures"},
			respond:  fail(`{"batchItemFailures":[{"itemIdentifier":""}]}`),
			wantSeqs: [][]string{{"1", "2", "3", "4"}, {"1", "2", "3", "4"}},
		},
		{
			name:     "empty list is a complete success",
			types:    []string{"ReportBatchItemFailures"},
			respond:  fail(`{"batchItemFailures":[]}`),
			wantSeqs: [][]string{{"1", "2", "3", "4"}},
		},
		{
			name:     "ignored without ReportBatchItemFailures",
			respond:  fail(`{"batchItemFailures":[{"itemIdentifier":"3"}]}`),
			wantSeqs: [][]string{{"1", "2", "3", "4"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &CreateEventSourceMappingInput{
				FunctionResponseTypes: tt.types, BisectBatchOnFunctionError: tt.bisect,
			}
			h := newStreamHarness(t, in, [][]KinesisRecord{keyedRecords(base, "k", "k", "k", "k")}, base, tt.respond)

			for range 3 {
				h.p.poll(t.Context())
			}

			var got [][]string
			for _, c := range h.snapshot() {
				got = append(got, c.seqs(t))
			}

			assert.Equal(t, tt.wantSeqs, got)
		})
	}
}

func TestStreamMaximumBatchingWindow(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		polls     [][]KinesisRecord
		wantSizes []int
		batchSize int
		advance   time.Duration
	}{
		{
			name:      "partial batch waits for the window",
			polls:     [][]KinesisRecord{keyedRecords(base, "k", "k", "k")},
			batchSize: 10, advance: 0, wantSizes: nil,
		},
		{
			name:      "partial batch flushes when the window elapses",
			polls:     [][]KinesisRecord{keyedRecords(base, "k", "k", "k")},
			batchSize: 10, advance: 30 * time.Second, wantSizes: []int{3},
		},
		{
			name:      "full batch flushes immediately",
			polls:     [][]KinesisRecord{keyedRecords(base, "k", "k", "k", "k")},
			batchSize: 2, advance: 0, wantSizes: []int{2, 2},
		},
		{
			name:      "records accumulate across polls",
			polls:     [][]KinesisRecord{keyedRecords(base, "k", "k"), keyedRecords(base, "k", "k")},
			batchSize: 4, advance: 0, wantSizes: []int{4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &CreateEventSourceMappingInput{MaximumBatchingWindowInSeconds: 30, BatchSize: tt.batchSize}
			h := newStreamHarness(t, in, tt.polls, base,
				func(*streamHarness, streamCall) invokeOutcome { return invokeOutcome{} })

			for range tt.polls {
				h.p.poll(t.Context())
			}

			h.advance(tt.advance)
			h.p.poll(t.Context())

			var sizes []int
			for _, c := range h.snapshot() {
				sizes = append(sizes, len(c.Records))
			}

			assert.Equal(t, tt.wantSizes, sizes)
		})
	}
}

func TestStreamTuningValidation(t *testing.T) {
	t.Parallel()

	const sqsARN = "arn:aws:sqs:us-east-1:000000000000:q"

	const ddbARN = "arn:aws:dynamodb:us-east-1:000000000000:table/t/stream/2026-01-01T00:00:00.000"

	tests := []struct {
		name    string
		arn     string
		pf      int
		tumble  int
		wantErr bool
	}{
		{name: "unset", arn: testKinesisARN},
		{name: "factor 1", arn: testKinesisARN, pf: 1},
		{name: "factor 10", arn: testKinesisARN, pf: 10},
		{name: "factor 11", arn: testKinesisARN, pf: 11, wantErr: true},
		{name: "factor negative", arn: testKinesisARN, pf: -1, wantErr: true},
		{name: "ddb factor 10", arn: ddbARN, pf: 10},
		{name: "tumbling 900", arn: ddbARN, tumble: 900},
		{name: "tumbling 901", arn: ddbARN, tumble: 901, wantErr: true},
		{name: "tumbling negative", arn: testKinesisARN, tumble: -1, wantErr: true},
		{name: "factor on sqs", arn: sqsARN, pf: 2, wantErr: true},
		{name: "tumbling on sqs", arn: sqsARN, tumble: 60, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)
			require.NoError(t, b.CreateFunction(&FunctionConfiguration{FunctionName: "f"}))

			m, err := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
				FunctionName: "f", EventSourceARN: tt.arn, StartingPosition: "LATEST",
				ParallelizationFactor: tt.pf, TumblingWindowInSeconds: tt.tumble,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidParameterValue)

				return
			}

			require.NoError(t, err)

			if isStreamSourceARN(tt.arn) {
				assert.Equal(t, max(tt.pf, 1), m.ParallelizationFactor)
			}
		})
	}
}

func TestStreamTuningUpdateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tumble  *int32
		name    string
		pf      int32
		wantErr bool
	}{
		{name: "factor 5", pf: 5},
		{name: "factor 0", pf: 0, wantErr: true},
		{name: "factor 11", pf: 11, wantErr: true},
		{name: "tumbling 901", pf: 1, tumble: new(int32(901)), wantErr: true},
		{name: "tumbling 900", pf: 1, tumble: new(int32(900))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newKafkaTestBackend(t)
			require.NoError(t, b.CreateFunction(&FunctionConfiguration{FunctionName: "f"}))

			m, err := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
				FunctionName: "f", EventSourceARN: testKinesisARN, StartingPosition: "LATEST",
			})
			require.NoError(t, err)

			_, err = b.UpdateEventSourceMapping(m.UUID, &UpdateEventSourceMappingInput{
				ParallelizationFactor: &tt.pf, TumblingWindowInSeconds: tt.tumble,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidParameterValue)

				return
			}

			require.NoError(t, err)
		})
	}
}

type keyedDDBReader struct {
	recs   []DynamoDBStreamRecord
	mu     sync.Mutex
	served bool
}

func (*keyedDDBReader) DescribeStreamShards(string) ([]string, error) {
	return []string{"shardId-1"}, nil
}

func (*keyedDDBReader) GetStreamShardIterator(_, _, _ string) (string, error) { return "it", nil }

func (r *keyedDDBReader) GetStreamRecords(string, int) ([]DynamoDBStreamRecord, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.served {
		return nil, "it", nil
	}

	r.served = true

	return r.recs, "it", nil
}

func TestStreamDynamoDBParallelizationKeepsItemOrder(t *testing.T) {
	t.Parallel()

	const ddbARN = "arn:aws:dynamodb:us-east-1:000000000000:table/t/stream/2026-01-01T00:00:00.000"

	ids := []string{"a", "b", "c", "d", "a", "b", "c", "d", "a", "b"}
	recs := make([]DynamoDBStreamRecord, len(ids))

	for i, id := range ids {
		recs[i] = DynamoDBStreamRecord{
			EventID: strconv.Itoa(i), EventName: "MODIFY", SequenceNumber: strconv.Itoa(100 + i),
			Keys: map[string]any{"Id": map[string]any{"S": id}},
		}
	}

	b := newKafkaTestBackend(t)
	require.NoError(t, b.CreateFunction(&FunctionConfiguration{FunctionName: "f"}))
	_, err := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
		FunctionName: "f", EventSourceARN: ddbARN, StartingPosition: "TRIM_HORIZON", Enabled: true,
		ParallelizationFactor: 4, BatchSize: 100,
	})
	require.NoError(t, err)

	p := NewEventSourcePoller(b, nil)
	p.SetDynamoDBStreamsReader(&keyedDDBReader{recs: recs})

	var (
		mu    sync.Mutex
		calls [][]string
	)

	p.streamInvoker = func(_ context.Context, _ string, payload []byte) invokeOutcome {
		var ev struct {
			Records []struct {
				Dynamodb struct {
					Keys map[string]map[string]string `json:"Keys"`
					Seq  string                       `json:"SequenceNumber"`
				} `json:"dynamodb"`
			} `json:"Records"`
		}

		require.NoError(t, json.Unmarshal(payload, &ev))

		got := make([]string, 0, len(ev.Records))
		for _, r := range ev.Records {
			got = append(got, r.Dynamodb.Keys["Id"]["S"]+":"+r.Dynamodb.Seq)
		}

		mu.Lock()
		calls = append(calls, got)
		mu.Unlock()

		return invokeOutcome{}
	}

	p.poll(t.Context())

	lanes := map[string]int{}
	last := map[string]int{}
	total := 0

	for _, c := range calls {
		lane := -1

		for _, e := range c {
			id, seqStr, _ := strings.Cut(e, ":")
			seq, _ := strconv.Atoi(seqStr)
			assert.Greater(t, seq, last[id], "item %s out of order", id)
			last[id] = seq

			l := laneIndex(`{"Id":{"S":"`+id+`"}}`, 4)
			if lane >= 0 {
				assert.Equal(t, lane, l)
			}

			lane = l
			lanes[id] = l
			total++
		}
	}

	assert.Equal(t, len(ids), total)
	assert.Greater(t, len(calls), 1, "records were not split across lanes")
}

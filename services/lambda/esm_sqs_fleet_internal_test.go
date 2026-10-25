package lambda

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fleetTestQueue = "arn:aws:sqs:us-east-1:000000000000:q"

type fleetQueue struct {
	deleted []string
	pending []*SQSMessage
	mu      sync.Mutex
}

func newFleetQueue(n int) *fleetQueue {
	q := &fleetQueue{}
	q.add(n)

	return q
}

func (q *fleetQueue) add(n int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	base := len(q.pending)
	for i := range n {
		id := fmt.Sprintf("m%d-%d", base, i)
		q.pending = append(q.pending, &SQSMessage{MessageID: id, ReceiptHandle: id, Body: "{}"})
	}
}

func (q *fleetQueue) ReceiveMessagesLocal(_ string, maxMessages int) ([]*SQSMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	n := min(maxMessages, len(q.pending))
	out := q.pending[:n]
	q.pending = q.pending[n:]

	return out, nil
}

func (q *fleetQueue) DeleteMessagesLocal(_ string, handles []string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.deleted = append(q.deleted, handles...)

	return nil
}

func (q *fleetQueue) deletedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.deleted)
}

type fleetInvoker struct {
	gate     chan struct{}
	inFlight atomic.Int32
	peak     atomic.Int32
	total    atomic.Int32
}

func (f *fleetInvoker) invoke(_ context.Context, _ string) ([]byte, error) {
	cur := f.inFlight.Add(1)

	for {
		old := f.peak.Load()
		if cur <= old || f.peak.CompareAndSwap(old, cur) {
			break
		}
	}

	<-f.gate
	f.inFlight.Add(-1)
	f.total.Add(1)

	return nil, nil
}

func newFleetPoller(t *testing.T, q SQSReader, inv *fleetInvoker) *EventSourcePoller {
	t.Helper()

	b := NewInMemoryBackend(nil, nil, DefaultSettings(), "000000000000", "us-east-1")
	t.Cleanup(func() { b.Close(context.Background()) })

	p := NewEventSourcePoller(b, nil)
	p.SetSQSReader(q)
	p.sqsInvoker = inv.invoke

	return p
}

func fleetMapping(sc *ESMScalingConfig, ppc *ESMProvisionedPollerConfig) *EventSourceMapping {
	return &EventSourceMapping{
		UUID: "u1", EventSourceARN: fleetTestQueue, BatchSize: 1,
		FunctionARN:             "arn:aws:lambda:us-east-1:000000000000:function:f",
		ScalingConfig:           sc,
		ProvisionedPollerConfig: ppc,
		State:                   ESMStateEnabled,
	}
}

// runTick runs one poll tick against a queue of n messages with the invokers gated,
// returns the peak concurrency observed while all were blocked.
func runTick(t *testing.T, p *EventSourcePoller, m *EventSourceMapping, inv *fleetInvoker, q *fleetQueue) int32 {
	t.Helper()

	inv.gate = make(chan struct{})
	inv.peak.Store(0)

	done := make(chan struct{})

	go func() {
		defer close(done)

		p.processSQSMapping(t.Context(), m, q)
	}()

	synctest.Wait()

	blocked := inv.inFlight.Load()
	close(inv.gate)
	<-done

	return blocked
}

func TestSQSMaximumConcurrencyCapsInFlightInvokes(t *testing.T) {
	t.Parallel()

	two, five := int32(2), int32(5)

	tests := []struct {
		maxConc  *int32
		name     string
		messages int
		want     int32
	}{
		{name: "cap_two", maxConc: &two, messages: 20, want: 2},
		{name: "cap_five", maxConc: &five, messages: 20, want: 5},
		{name: "fewer_messages_than_cap", maxConc: &five, messages: 3, want: 3},
		{name: "unset_stays_serial", maxConc: nil, messages: 20, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				q := newFleetQueue(tt.messages)
				inv := &fleetInvoker{}
				p := newFleetPoller(t, q, inv)

				var sc *ESMScalingConfig
				if tt.maxConc != nil {
					sc = &ESMScalingConfig{MaximumConcurrency: tt.maxConc}
				}

				blocked := runTick(t, p, fleetMapping(sc, nil), inv, q)

				assert.Equal(t, tt.want, blocked)
				assert.LessOrEqual(t, inv.peak.Load(), tt.want)

				if tt.maxConc != nil {
					assert.EqualValues(t, tt.messages, inv.total.Load())
					assert.Equal(t, tt.messages, q.deletedCount())
				}
			})
		})
	}
}

func TestSQSProvisionedPollersBoundFleet(t *testing.T) {
	t.Parallel()

	i := func(v int32) *int32 { return &v }

	tests := []struct {
		ppc          *ESMProvisionedPollerConfig
		maxConc      *int32
		name         string
		wantFirstCap int32
		wantFinalCap int32
		wantPollers  int
	}{
		{
			name: "grows_to_max", ppc: &ESMProvisionedPollerConfig{MinimumPollers: i(1), MaximumPollers: i(2)},
			wantFirstCap: 10, wantFinalCap: 20, wantPollers: 2,
		},
		{
			name: "min_sets_start", ppc: &ESMProvisionedPollerConfig{MinimumPollers: i(3), MaximumPollers: i(3)},
			wantFirstCap: 30, wantFinalCap: 30, wantPollers: 3,
		},
		{
			name:    "max_concurrency_caps_pollers",
			ppc:     &ESMProvisionedPollerConfig{MinimumPollers: i(2), MaximumPollers: i(4)},
			maxConc: i(7), wantFirstCap: 7, wantFinalCap: 7, wantPollers: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				q := newFleetQueue(200)
				inv := &fleetInvoker{}
				p := newFleetPoller(t, q, inv)

				var sc *ESMScalingConfig
				if tt.maxConc != nil {
					sc = &ESMScalingConfig{MaximumConcurrency: tt.maxConc}
				}

				m := fleetMapping(sc, tt.ppc)

				assert.Equal(t, tt.wantFirstCap, runTick(t, p, m, inv, q))

				q.add(500)
				assert.Equal(t, tt.wantFinalCap, runTick(t, p, m, inv, q))
				assert.Equal(t, tt.wantPollers, p.SQSPollerCount(m.UUID))

				q.add(500)
				runTick(t, p, m, inv, q)
				assert.Equal(t, tt.wantPollers, p.SQSPollerCount(m.UUID))
			})
		})
	}
}

func TestSQSThrottledInvokeLeavesMessagesInQueue(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend(nil, nil, DefaultSettings(), "000000000000", "us-east-1")
	t.Cleanup(func() { b.Close(context.Background()) })

	require.NoError(t, b.CreateFunction(&FunctionConfiguration{
		FunctionName: "f", Runtime: "python3.12", Handler: "index.handler",
		Role: "arn:aws:iam::000000000000:role/r",
	}))
	_, err := b.PutFunctionConcurrency("f", 0)
	require.NoError(t, err)

	q := newFleetQueue(3)
	p := NewEventSourcePoller(b, nil)
	p.SetSQSReader(q)

	two := int32(2)
	m := fleetMapping(&ESMScalingConfig{MaximumConcurrency: &two}, nil)
	m.BatchSize = 3

	p.processSQSMapping(t.Context(), m, q)

	assert.Zero(t, q.deletedCount())
}

func TestValidateMaximumConcurrencyRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   int32
		wantErr bool
	}{
		{"below_min", 1, true},
		{"min", 2, false},
		{"max", 1000, false},
		{"above_max", 1001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateESMTuning(fleetTestQueue, false, &ESMScalingConfig{MaximumConcurrency: &tt.value}, nil, nil)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidParameterValue)

				return
			}

			require.NoError(t, err)
		})
	}
}

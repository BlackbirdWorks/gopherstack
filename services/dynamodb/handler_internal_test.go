package dynamodb

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestHandler_ClassifyError_Mapping(t *testing.T) {
	t.Parallel()
	h := NewHandler(NewInMemoryDB())

	tests := []struct {
		err            error
		name           string
		wantType       string
		wantStatusCode int
	}{
		{
			name:           "InternalServerError",
			err:            NewInternalServerError("boom"),
			wantStatusCode: http.StatusInternalServerError,
			wantType:       "InternalServerError",
		},
		{
			name:           "ValidationException",
			err:            NewValidationException("bad"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ValidationException",
		},
		{
			name:           "ResourceNotFoundException",
			err:            NewResourceNotFoundException("missing"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ResourceNotFoundException",
		},
		{
			name:           "ConditionalCheckFailedException",
			err:            NewConditionalCheckFailedException("fail"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ConditionalCheckFailedException",
		},
		{
			name:           "TransactionCanceledException",
			err:            NewTransactionCanceledException("cancel", nil),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "TransactionCanceledException",
		},
		{
			name:           "ItemCollectionSizeLimitExceededException",
			err:            NewItemCollectionSizeLimitExceededException("too big"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ItemCollectionSizeLimitExceededException",
		},
		{
			name:           "LimitExceededException",
			err:            NewLimitExceededException("limit"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "LimitExceededException",
		},
		{
			name:           "ResourceInUseException",
			err:            NewResourceInUseException("in use"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ResourceInUseException",
		},
		{
			name:           "ProvisionedThroughputExceededException",
			err:            NewProvisionedThroughputExceededException("throughput"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ProvisionedThroughputExceededException",
		},
		{
			name:           "ThrottlingException",
			err:            NewThrottlingException("throttle"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ThrottlingException",
		},
		{
			name:           "RequestLimitExceeded",
			err:            NewRequestLimitExceeded("request limit"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "RequestLimitExceeded",
		},
		{
			name:           "TransactionConflictException",
			err:            NewTransactionConflictException("conflict"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "TransactionConflictException",
		},
		{
			name:           "ReplicatedWriteConflictException",
			err:            NewReplicatedWriteConflictException("replicated conflict"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ReplicatedWriteConflictException",
		},
		{
			name:           "PolicyNotFoundException",
			err:            NewPolicyNotFoundException("missing policy"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "PolicyNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			status, wireErr := h.classifyError(tt.err)
			assert.Equal(t, tt.wantStatusCode, status)
			assert.Contains(t, wireErr.Type, tt.wantType)
		})
	}
}

type countingMarshaler struct {
	counter *atomic.Int64
}

func (m countingMarshaler) MarshalJSON() ([]byte, error) {
	m.counter.Add(1)

	return []byte(`"counted"`), nil
}

func TestHandler_DebugMarshallingConditional(t *testing.T) {
	t.Parallel()

	type wireOut struct {
		Value countingMarshaler `json:"Value"`
	}

	tests := []struct {
		invoke func(context.Context, *atomic.Int64) error
		name   string
		level  slog.Level
		want   int64
	}{
		{
			name:  "handleOp skips marshalling when debug disabled",
			level: slog.LevelInfo,
			want:  0,
			invoke: func(ctx context.Context, counter *atomic.Int64) error {
				_, err := handleOp[struct{}, struct{}, struct{}, wireOut](
					ctx, "TestAction", nil,
					func(*struct{}) *struct{} { return &struct{}{} },
					func(context.Context, *struct{}) (*struct{}, error) { return &struct{}{}, nil },
					func(*struct{}) *wireOut { return &wireOut{Value: countingMarshaler{counter: counter}} },
				)

				return err
			},
		},
		{
			name:  "handleOp marshals when debug enabled",
			level: slog.LevelDebug,
			want:  1,
			invoke: func(ctx context.Context, counter *atomic.Int64) error {
				_, err := handleOp[struct{}, struct{}, struct{}, wireOut](
					ctx, "TestAction", nil,
					func(*struct{}) *struct{} { return &struct{}{} },
					func(context.Context, *struct{}) (*struct{}, error) { return &struct{}{}, nil },
					func(*struct{}) *wireOut { return &wireOut{Value: countingMarshaler{counter: counter}} },
				)

				return err
			},
		},
		{
			name:  "handleOpErr skips marshalling when debug disabled",
			level: slog.LevelInfo,
			want:  0,
			invoke: func(ctx context.Context, counter *atomic.Int64) error {
				_, err := handleOpErr[struct{}, struct{}, struct{}, wireOut](
					ctx, "TestAction", nil,
					func(*struct{}) (*struct{}, error) { return &struct{}{}, nil },
					func(context.Context, *struct{}) (*struct{}, error) { return &struct{}{}, nil },
					func(*struct{}) *wireOut { return &wireOut{Value: countingMarshaler{counter: counter}} },
				)

				return err
			},
		},
		{
			name:  "handleOpErr marshals when debug enabled",
			level: slog.LevelDebug,
			want:  1,
			invoke: func(ctx context.Context, counter *atomic.Int64) error {
				_, err := handleOpErr[struct{}, struct{}, struct{}, wireOut](
					ctx, "TestAction", nil,
					func(*struct{}) (*struct{}, error) { return &struct{}{}, nil },
					func(context.Context, *struct{}) (*struct{}, error) { return &struct{}{}, nil },
					func(*struct{}) *wireOut { return &wireOut{Value: countingMarshaler{counter: counter}} },
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var counter atomic.Int64

			ctx := logger.Save(t.Context(), logger.NewLogger(tt.level))
			err := tt.invoke(ctx, &counter)
			require.NoError(t, err)
			assert.Equal(t, tt.want, counter.Load())
		})
	}
}

func TestHandler_JanitorLifecycle(t *testing.T) {
	t.Parallel()

	const testJanitorInterval = 50 * time.Millisecond
	const janitorShutdownTimeout = 2 * time.Second

	tests := []struct {
		name             string
		cancelBeforeStop bool
		startWorker      bool
		startWorkerTwice bool
		wantDoneChan     bool
	}{
		{
			name:         "shutdown without started worker is no-op",
			startWorker:  false,
			wantDoneChan: false,
		},
		{
			name:         "start then shutdown drains janitor goroutine",
			startWorker:  true,
			wantDoneChan: true,
		},
		{
			name:             "second StartWorker call is idempotent",
			startWorker:      true,
			startWorkerTwice: true,
			wantDoneChan:     true,
		},
		{
			name:             "cancelled parent context still drains janitor",
			startWorker:      true,
			cancelBeforeStop: true,
			wantDoneChan:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandler(NewInMemoryDB()).WithJanitor(Settings{JanitorInterval: testJanitorInterval})

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			var firstDone chan struct{}

			if tt.startWorker {
				require.NoError(t, h.StartWorker(ctx))

				h.janitorMu.Lock()
				firstDone = h.janitorDone
				h.janitorMu.Unlock()
			}

			if tt.startWorkerTwice {
				require.NoError(t, h.StartWorker(ctx))

				h.janitorMu.Lock()
				secondDone := h.janitorDone
				h.janitorMu.Unlock()
				assert.Equal(t, firstDone, secondDone)
			}

			if tt.cancelBeforeStop {
				cancel()
			}

			h.Shutdown(t.Context())

			h.janitorMu.Lock()
			done := h.janitorDone
			h.janitorMu.Unlock()

			if !tt.wantDoneChan {
				assert.Nil(t, done)

				return
			}

			require.NotNil(t, firstDone)

			exited := false
			select {
			case <-firstDone:
				exited = true
			case <-time.After(janitorShutdownTimeout):
			}

			assert.True(t, exited)
			assert.Nil(t, done)
		})
	}
}

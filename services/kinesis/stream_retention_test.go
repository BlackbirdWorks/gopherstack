package kinesis_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIncreaseDecreaseRetentionPeriod covers happy paths and validation for retention period changes.
func TestIncreaseDecreaseRetentionPeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(bk *kinesis.InMemoryBackend)
		action  func(bk *kinesis.InMemoryBackend) error
		name    string
		wantErr bool
	}{
		{
			name: "increase_from_24_to_48",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
			},
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.IncreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.IncreaseStreamRetentionPeriodInput{
						StreamName:           "s",
						RetentionPeriodHours: 48,
					},
				)
			},
		},
		{
			name: "decrease_from_48_to_24",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
				_ = bk.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
					StreamName: "s", RetentionPeriodHours: 48,
				})
			},
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.DecreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.DecreaseStreamRetentionPeriodInput{
						StreamName:           "s",
						RetentionPeriodHours: 24,
					},
				)
			},
		},
		{
			name:    "increase_stream_not_found",
			setup:   func(_ *kinesis.InMemoryBackend) {},
			wantErr: true,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.IncreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.IncreaseStreamRetentionPeriodInput{
						StreamName: "missing", RetentionPeriodHours: 48,
					},
				)
			},
		},
		{
			name: "increase_same_value_noop",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
			},
			// Real AWS accepts an increase whose target equals the current
			// retention period as an idempotent no-op (HTTP 200). The Terraform
			// provider calls IncreaseStreamRetentionPeriod on create for any
			// retention_period > 0, so a default-24h stream receives
			// IncreaseStreamRetentionPeriod(24) and must not be rejected.
			wantErr: false,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.IncreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.IncreaseStreamRetentionPeriodInput{
						StreamName: "s", RetentionPeriodHours: 24, // same as default
					},
				)
			},
		},
		{
			name: "increase_above_max_rejected",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
			},
			wantErr: true,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.IncreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.IncreaseStreamRetentionPeriodInput{
						StreamName: "s", RetentionPeriodHours: 9999,
					},
				)
			},
		},
		{
			name:    "decrease_stream_not_found",
			setup:   func(_ *kinesis.InMemoryBackend) {},
			wantErr: true,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.DecreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.DecreaseStreamRetentionPeriodInput{
						StreamName: "missing", RetentionPeriodHours: 24,
					},
				)
			},
		},
		{
			name: "decrease_below_min_rejected",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
				_ = bk.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
					StreamName: "s", RetentionPeriodHours: 48,
				})
			},
			wantErr: true,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.DecreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.DecreaseStreamRetentionPeriodInput{
						StreamName: "s", RetentionPeriodHours: 10, // below 24h minimum
					},
				)
			},
		},
		{
			name: "decrease_same_value_noop",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
				_ = bk.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
					StreamName: "s", RetentionPeriodHours: 48,
				})
			},
			// Mirrors the increase case: a decrease whose target equals the current
			// retention period is an idempotent no-op (HTTP 200), not a rejection.
			wantErr: false,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.DecreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.DecreaseStreamRetentionPeriodInput{
						StreamName: "s", RetentionPeriodHours: 48, // same as current
					},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := kinesis.NewInMemoryBackend()
			tt.setup(bk)
			err := tt.action(bk)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestHandleIncreaseStreamRetentionPeriod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with default retention (24 h).
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "retention-stream", "ShardCount": 1})

	// Increase retention to 48 h.
	rec := doRequest(t, h, "IncreaseStreamRetentionPeriod", map[string]any{
		"StreamName":           "retention-stream",
		"RetentionPeriodHours": 48,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify the new retention via DescribeStream.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "retention-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			RetentionPeriodHours int `json:"RetentionPeriodHours"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, 48, descResp.StreamDescription.RetentionPeriodHours)
}

func TestHandleDecreaseStreamRetentionPeriod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream and first increase retention to 48 h so there is room to decrease.
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "retention-stream", "ShardCount": 1})
	doRequest(t, h, "IncreaseStreamRetentionPeriod", map[string]any{
		"StreamName":           "retention-stream",
		"RetentionPeriodHours": 48,
	})

	// Decrease retention back to 24 h.
	rec := doRequest(t, h, "DecreaseStreamRetentionPeriod", map[string]any{
		"StreamName":           "retention-stream",
		"RetentionPeriodHours": 24,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify the new retention via DescribeStream.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "retention-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			RetentionPeriodHours int `json:"RetentionPeriodHours"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, 24, descResp.StreamDescription.RetentionPeriodHours)
}

func TestIncreaseRetention_BelowMinRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "retention-min-stream",
		ShardCount: 1,
	}))

	// Attempting to set retention to 0 (below minimum 24h) should fail.
	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "retention-min-stream",
		RetentionPeriodHours: 0,
	})
	require.Error(t, err)
}

func TestIncreaseRetention_AboveMaxRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "retention-max-stream",
		ShardCount: 1,
	}))

	// 8761 hours > maxRetentionHours (8760) should fail.
	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "retention-max-stream",
		RetentionPeriodHours: 8761,
	})
	require.Error(t, err)
}

func TestIncreaseRetention_ValidRangeAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "retention-valid-stream",
		ShardCount: 1,
	}))

	// 168 h (7 days) is within valid range [24, 8760].
	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "retention-valid-stream",
		RetentionPeriodHours: 168,
	})
	require.NoError(t, err)

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "retention-valid-stream"},
	)
	require.NoError(t, err)
	assert.Equal(t, 168, out.RetentionPeriodHours)
}

func TestIncreaseRetention_MaxBoundaryAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "retention-boundary-stream",
		ShardCount: 1,
	}))

	// Exactly maxRetentionHours (8760) should succeed.
	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "retention-boundary-stream",
		RetentionPeriodHours: 8760,
	})
	require.NoError(t, err)
}

func TestRetentionPeriod_IncreaseToSameValueIsNoOp(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "idempotent-retention",
		ShardCount: 1,
	}))

	// Set retention to 48 hours.
	require.NoError(
		t,
		b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
			StreamName:           "idempotent-retention",
			RetentionPeriodHours: 48,
		}),
	)

	// Real AWS accepts an increase whose target equals the current retention
	// period as an idempotent no-op returning HTTP 200 (not InvalidArgumentException).
	// The Terraform AWS provider issues IncreaseStreamRetentionPeriod on stream
	// create for any retention_period > 0 (guard `v.(int) > 0`), so a stream
	// whose configured retention already equals its current value must succeed;
	// rejecting it breaks `aws_kinesis_stream` apply.
	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "idempotent-retention",
		RetentionPeriodHours: 48,
	})
	require.NoError(t, err)

	out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "idempotent-retention"})
	require.NoError(t, err)
	assert.Equal(t, 48, out.RetentionPeriodHours)
}

// TestRetentionPeriod_IncreaseFromDefaultEqualsDefault reproduces the
// exact Terraform apply flow that regressed: a stream created with the default
// 24h retention receives IncreaseStreamRetentionPeriod(24) (the provider calls it
// unconditionally for any retention_period > 0). It must return success.
func TestRetentionPeriod_IncreaseFromDefaultEqualsDefault(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "default-retention",
		ShardCount: 1,
	}))

	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "default-retention",
		RetentionPeriodHours: 24,
	})
	require.NoError(t, err)

	out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "default-retention"})
	require.NoError(t, err)
	assert.Equal(t, 24, out.RetentionPeriodHours)
}

func TestRetentionPeriod_DecreaseStillWorks(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "decrease-retention",
		ShardCount: 1,
	}))

	require.NoError(
		t,
		b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
			StreamName:           "decrease-retention",
			RetentionPeriodHours: 168,
		}),
	)

	require.NoError(
		t,
		b.DecreaseStreamRetentionPeriod(context.Background(), &kinesis.DecreaseStreamRetentionPeriodInput{
			StreamName:           "decrease-retention",
			RetentionPeriodHours: 48,
		}),
	)

	out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "decrease-retention"})
	require.NoError(t, err)
	assert.Equal(t, 48, out.RetentionPeriodHours)
}

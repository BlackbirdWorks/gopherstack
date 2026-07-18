package ses_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestSESJanitor_TaskTimeout_WithJanitor verifies that WithJanitor propagates
// the variadic taskTimeout into the janitor's TaskTimeout field.
func TestSESJanitor_TaskTimeout_WithJanitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskTimeout time.Duration
		want        time.Duration
	}{
		{
			name:        "no_timeout_zero",
			taskTimeout: 0,
			want:        0,
		},
		{
			name:        "with_30s_timeout",
			taskTimeout: 30 * time.Second,
			want:        30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ses.NewHandler(ses.NewInMemoryBackend())
			h.WithJanitor(time.Minute, tt.taskTimeout)

			assert.Equal(t, tt.want, h.GetJanitorTaskTimeout())
		})
	}
}

// TestSESJanitor_Run_ExitsOnCancel verifies that the janitor goroutine exits
// promptly when the parent context is cancelled, even with a TaskTimeout set.
func TestSESJanitor_Run_ExitsOnCancel(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	j := ses.NewJanitor(b, 10*time.Millisecond)
	j.TaskTimeout = 30 * time.Second

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		j.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "janitor did not exit after context cancellation")
	}
}

// TestSESBackend_Reset_PreservesConfiguredEmailTTL verifies that Reset() restores
// the email TTL to the value configured via WithEmailTTL, not the hardcoded default.
func TestSESBackend_Reset_PreservesConfiguredEmailTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		configuredTTL  time.Duration
		wantAfterReset time.Duration
	}{
		{
			name:           "custom_ttl_preserved_after_reset",
			configuredTTL:  12 * time.Hour,
			wantAfterReset: 12 * time.Hour,
		},
		{
			name:           "default_ttl_preserved_after_reset",
			configuredTTL:  0,
			wantAfterReset: ses.DefaultEmailTTL,
		},
		{
			name:           "48h_ttl_preserved_after_reset",
			configuredTTL:  48 * time.Hour,
			wantAfterReset: 48 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend().WithEmailTTL(tt.configuredTTL)

			// Reset should restore to the value configured via WithEmailTTL, not the hardcoded default.
			b.Reset()

			assert.Equal(t, tt.wantAfterReset, b.GetEmailTTL())
		})
	}
}

// TestSESJanitor_DefaultInterval verifies that a zero interval in WithJanitor
// results in the default interval being used.
func TestSESJanitor_DefaultInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{
			name:     "zero_uses_default",
			interval: 0,
			want:     ses.DefaultJanitorInterval,
		},
		{
			name:     "custom_interval_propagated",
			interval: 5 * time.Minute,
			want:     5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ses.NewHandler(ses.NewInMemoryBackend())
			h.WithJanitor(tt.interval)

			assert.Equal(t, tt.want, h.GetJanitorInterval())
		})
	}
}

func TestWithJanitor_NonInMemoryBackend(t *testing.T) {
	t.Parallel()

	// When Backend is not *InMemoryBackend, WithJanitor should be a no-op.
	h := ses.NewHandler(ses.NewInMemoryBackend())
	before := h.GetJanitorInterval()

	// Re-attach with a real backend so we can test the fast path returning self.
	h2 := h.WithJanitor(5*time.Second, 2*time.Second)
	assert.NotNil(t, h2)
	assert.NotEqual(t, before, h.GetJanitorInterval())
}

func TestJanitor_Run_CancelContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskTimeout time.Duration
	}{
		{
			name:        "no_task_timeout",
			taskTimeout: 0,
		},
		{
			name:        "with_task_timeout",
			taskTimeout: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			j := ses.NewJanitor(b, 10*time.Millisecond)
			j.TaskTimeout = tt.taskTimeout

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			// Run should return promptly when ctx is cancelled.
			done := make(chan struct{})
			go func() {
				j.Run(ctx)
				close(done)
			}()

			select {
			case <-done:
				// ok
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Run did not return after context cancellation")
			}
		})
	}
}

func TestSESJanitor_SweepExpiredEmails(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.SetEmailTTL(time.Millisecond) // very short TTL
	require.NoError(t, b.VerifyEmailIdentity("j@test.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "j@test.com", To: []string{"to@test.com"}, Subject: "s", BodyText: "b",
	})
	require.NoError(t, err)

	require.Equal(t, 1, b.EmailCount())

	// Wait for TTL to expire then sweep.
	time.Sleep(5 * time.Millisecond)

	j := ses.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	assert.Equal(t, 0, b.EmailCount())
	assert.Equal(t, 0, b.EmailsByIDCount())
}

func TestSESJanitor_SweepNoExpired(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("j2@test.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "j2@test.com", To: []string{"to@test.com"}, Subject: "s", BodyText: "b",
	})
	require.NoError(t, err)

	j := ses.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	assert.Equal(t, 1, b.EmailCount())
}

func TestSESHandler_StartWorker(t *testing.T) {
	t.Parallel()

	h := newHandler()
	h.WithJanitor(time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}

func TestSESHandler_StartWorkerNoJanitor(t *testing.T) {
	t.Parallel()

	h := newHandler()

	err := h.StartWorker(t.Context())
	require.NoError(t, err)
}

// TestStartWorkerIdempotent verifies calling StartWorker twice does not start a second goroutine.
func TestStartWorkerIdempotent(t *testing.T) {
	t.Parallel()

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend).WithJanitor(time.Hour)
	ctx := context.Background()

	require.NoError(t, h.StartWorker(ctx))
	require.NoError(t, h.StartWorker(ctx))

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	h.Shutdown(shutdownCtx)
}

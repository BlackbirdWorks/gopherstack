package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// TestDeregisterJobDefinition_MarksInactive verifies that DeregisterJobDefinition
// marks the definition INACTIVE (matching AWS behavior) and it remains visible.
func TestDeregisterJobDefinition_MarksInactive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		deregisterWith func(jd *batch.JobDefinition) string
		name           string
	}{
		{
			name:           "by_arn",
			deregisterWith: func(jd *batch.JobDefinition) string { return jd.JobDefinitionArn },
		},
		{
			name:           "by_name_revision",
			deregisterWith: func(jd *batch.JobDefinition) string { return jd.JobDefinitionName + ":1" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := batch.NewInMemoryBackend("123456789012", "us-east-1")

			jd, err := backend.RegisterJobDefinition("my-job", "container", nil)
			require.NoError(t, err)
			assert.Equal(t, "ACTIVE", jd.Status)

			err = backend.DeregisterJobDefinition(tt.deregisterWith(jd))
			require.NoError(t, err)

			// Definition should still exist (AWS behavior) but be INACTIVE.
			assert.Equal(t, 1, backend.JobDefinitionCount(), "definition should remain visible after deregister")

			defs := backend.DescribeJobDefinitions([]string{jd.JobDefinitionName})
			require.Len(t, defs, 1)
			assert.Equal(t, "INACTIVE", defs[0].Status)
		})
	}
}

// TestDeregisterJobDefinition_RevisionCounterPreserved verifies that the revision
// counter is preserved across deregister/re-register cycles.
func TestDeregisterJobDefinition_RevisionCounterPreserved(t *testing.T) {
	t.Parallel()

	backend := batch.NewInMemoryBackend("123456789012", "us-east-1")

	jd1, err := backend.RegisterJobDefinition("my-job", "container", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(1), jd1.Revision)

	err = backend.DeregisterJobDefinition(jd1.JobDefinitionArn)
	require.NoError(t, err)

	// Re-register: should get revision 2.
	jd2, err := backend.RegisterJobDefinition("my-job", "container", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(2), jd2.Revision, "re-registration should yield revision 2")

	// Both definitions exist now: rev 1 (INACTIVE) + rev 2 (ACTIVE).
	assert.Equal(t, 2, backend.JobDefinitionCount())
}

// TestDeregisterJobDefinition_NotFound verifies that deregistering a non-existent
// job definition returns an error.
func TestDeregisterJobDefinition_NotFound(t *testing.T) {
	t.Parallel()

	backend := batch.NewInMemoryBackend("123456789012", "us-east-1")

	err := backend.DeregisterJobDefinition("arn:aws:batch:us-east-1:123456789012:job-definition/missing:1")
	assert.ErrorIs(t, err, batch.ErrNotFound)
}

// TestBatchJanitor_SweepInactiveJobDefinitions verifies that INACTIVE definitions
// older than the TTL are evicted by the janitor.
func TestBatchJanitor_SweepInactiveJobDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		deregisteredDelay time.Duration // negative = in the past
		ttl               time.Duration
		wantEvicted       bool
	}{
		{
			name:              "evict_past_ttl",
			deregisteredDelay: -25 * time.Hour,
			ttl:               24 * time.Hour,
			wantEvicted:       true,
		},
		{
			name:              "keep_within_ttl",
			deregisteredDelay: -1 * time.Hour,
			ttl:               24 * time.Hour,
			wantEvicted:       false,
		},
		{
			name:              "keep_active_definition",
			deregisteredDelay: 0,
			ttl:               24 * time.Hour,
			wantEvicted:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := batch.NewInMemoryBackend("123456789012", "us-east-1")

			jd, err := backend.RegisterJobDefinition("sweep-job", "container", nil)
			require.NoError(t, err)

			if tt.deregisteredDelay != 0 {
				// Deregister the definition.
				err = backend.DeregisterJobDefinition(jd.JobDefinitionArn)
				require.NoError(t, err)

				// Override DeregisteredAt for TTL testing.
				backend.SetJobDefinitionDeregisteredAt(jd.JobDefinitionArn, time.Now().Add(tt.deregisteredDelay))
			}
			// keep_active_definition: leave the definition as ACTIVE (no deregister).

			janitor := batch.NewJanitor(backend, time.Hour, tt.ttl)
			janitor.SweepOnce(t.Context())

			defs := backend.DescribeJobDefinitions([]string{jd.JobDefinitionName})

			if tt.wantEvicted {
				assert.Empty(t, defs, "definition should be evicted after TTL")
			} else {
				assert.NotEmpty(t, defs, "definition should be preserved")
			}
		})
	}
}

// TestBatchJanitor_RunContext verifies that the janitor stops when context is cancelled.
func TestBatchJanitor_RunContext(t *testing.T) {
	t.Parallel()

	backend := batch.NewInMemoryBackend("123456789012", "us-east-1")
	janitor := batch.NewJanitor(backend, 10*time.Millisecond, time.Hour)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		janitor.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "janitor did not stop after context cancellation")
	}
}

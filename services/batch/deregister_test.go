package batch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// TestDeregisterJobDefinition_DeletesEntry verifies that DeregisterJobDefinition
// actually removes entries from the backend rather than just marking them INACTIVE.
func TestDeregisterJobDefinition_DeletesEntry(t *testing.T) {
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

			assert.Equal(t, 1, backend.JobDefinitionCount(), "should have 1 job definition before deregister")

			err = backend.DeregisterJobDefinition(tt.deregisterWith(jd))
			require.NoError(t, err)

			assert.Equal(t, 0, backend.JobDefinitionCount(), "job definition should be removed after deregister")
		})
	}
}

// TestDeregisterJobDefinition_RevisionsIncrementAfterRereregister verifies that
// re-registering after deregister yields a higher revision number, confirming
// that jobDefRevisions is preserved.
func TestDeregisterJobDefinition_RevisionCounterPreserved(t *testing.T) {
	t.Parallel()

	backend := batch.NewInMemoryBackend("123456789012", "us-east-1")

	jd1, err := backend.RegisterJobDefinition("my-job", "container", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(1), jd1.Revision)

	err = backend.DeregisterJobDefinition(jd1.JobDefinitionArn)
	require.NoError(t, err)

	assert.Equal(t, 0, backend.JobDefinitionCount(), "map should be empty after deregister")

	// Re-register: should get revision 2 because the counter was preserved.
	jd2, err := backend.RegisterJobDefinition("my-job", "container", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(2), jd2.Revision, "re-registration should yield revision 2")
}

// TestDeregisterJobDefinition_NotFound verifies that deregistering a non-existent
// job definition returns an error.
func TestDeregisterJobDefinition_NotFound(t *testing.T) {
	t.Parallel()

	backend := batch.NewInMemoryBackend("123456789012", "us-east-1")

	err := backend.DeregisterJobDefinition("arn:aws:batch:us-east-1:123456789012:job-definition/missing:1")
	assert.ErrorIs(t, err, batch.ErrNotFound)
}

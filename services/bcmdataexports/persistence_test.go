package bcmdataexports_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bcmdataexports"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	arn := f.create(t, export("e1", curQuery))
	f.settle(t)

	snap := f.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := bcmdataexports.NewInMemoryBackend("999999999999", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	e, st, err := restored.GetExport(arn)
	require.NoError(t, err)
	assert.Equal(t, "e1", e.Name)
	assert.Equal(t, "HEALTHY", st.StatusCode)

	p, err := restored.ListExecutions(arn, "", nil)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "DELIVERY_SUCCESS", p.Data[0].ExecutionStatus.StatusCode)
}

package cleanrooms_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

func TestSchemasBackend(t *testing.T) {
	t.Parallel()
	b := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	collab, err := b.CreateCollaboration("seed-collab", "", "creator", nil, nil, "", nil)
	require.NoError(t, err)

	_, _, err = b.BatchGetSchema(collab.CollaborationIdentifier, []string{"test-schema"})
	require.NoError(t, err)

	_, _, err = b.BatchGetSchemaAnalysisRule(collab.CollaborationIdentifier, []string{"test-schema"}, "AGGREGATION")
	require.NoError(t, err)

	// Inject a fake schema using Restore
	snapJSON := `{"version":1,"tables":{` +
		`"collaborations":[{"CollaborationIdentifier":"` + collab.CollaborationIdentifier + `"}],` +
		`"schemas":[{"CollaborationIdentifier":"` + collab.CollaborationIdentifier +
		`","Name":"test-schema","Type":"TABLE","AnalysisMethod":"DIRECT_QUERY"}],` +
		`"schemaAnalysisRules":[{"CollaborationIdentifier":"` + collab.CollaborationIdentifier +
		`","Name":"test-schema","Type":"AGGREGATION"}]}}`
	err = b.Restore(t.Context(), []byte(snapJSON))
	require.NoError(t, err)

	// ListSchemas
	summaries, _, err := b.ListSchemas(collab.CollaborationIdentifier, "", "", "")
	require.NoError(t, err)
	require.Len(t, summaries, 1)

	// BatchGetSchema again
	schemas, _, err := b.BatchGetSchema(collab.CollaborationIdentifier, []string{"test-schema"})
	require.NoError(t, err)
	require.Len(t, schemas, 1)

	rules, _, err := b.BatchGetSchemaAnalysisRule(
		collab.CollaborationIdentifier,
		[]string{"test-schema"},
		"AGGREGATION",
	)
	require.NoError(t, err)
	require.Len(t, rules, 1)
}

func TestStoreBackend(t *testing.T) {
	t.Parallel()
	b := cleanrooms.NewInMemoryBackendWithContext(t.Context(), config.DefaultAccountID, config.DefaultRegion)
	require.NotNil(t, b)

	b.Reset()
}

func TestProvider(t *testing.T) {
	t.Parallel()
	p := cleanrooms.Provider{}
	require.Equal(t, "CleanRooms", p.Name())
	b, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	require.NotNil(t, b)

	_, err = p.Init(nil)
	require.ErrorIs(t, err, cleanrooms.ErrNilAppContext)
}

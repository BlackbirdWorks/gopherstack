package cleanrooms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

func TestSchemas_Paths(t *testing.T) {
	t.Parallel()
	b := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	// NotFound on GetSchema
	_, err := b.GetSchema("invalid", "invalid")
	require.Error(t, err)

	// NotFound on ListSchemas
	_, _, err = b.ListSchemas("invalid", "", "", "")
	require.Error(t, err)

	// NotFound on BatchGetSchema
	_, _, err = b.BatchGetSchema("invalid", []string{"a"})
	require.Error(t, err)

	// BatchGetSchema with some missing
	seed := seedFullState(t, b)
	schemas, errors, err := b.BatchGetSchema(seed.collab.CollaborationIdentifier, []string{"invalid"})
	require.NoError(t, err)
	assert.Empty(t, schemas)
	assert.Len(t, errors, 1)

	// NotFound on GetSchemaAnalysisRule
	_, err = b.GetSchemaAnalysisRule("invalid", "invalid", "invalid")
	require.Error(t, err)

	// NotFound on BatchGetSchemaAnalysisRule
	_, _, err = b.BatchGetSchemaAnalysisRule("invalid", []string{"a"}, "b")
	require.Error(t, err)

	rules, errors2, err := b.BatchGetSchemaAnalysisRule(
		seed.collab.CollaborationIdentifier,
		[]string{"invalid"},
		"AGGREGATION",
	)
	require.NoError(t, err)
	assert.Empty(t, rules)
	assert.Len(t, errors2, 1)
}

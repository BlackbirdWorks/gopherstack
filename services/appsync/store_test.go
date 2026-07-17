package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestInMemoryBackend_SetDynamoDBBackend(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	b.SetDynamoDBBackend(nil) // just ensure it's callable
}

func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI(
		"TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, map[string]string{"k": "v"}, nil,
	)
	require.NoError(t, err)

	// Create a data source with tags so Reset() must close them too.
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "ds",
		Type: "NONE",
		Tags: nil,
	})
	require.NoError(t, err)

	// Reset must not panic and must clear all resources.
	b.Reset()

	apis, err := b.ListGraphqlAPIs("")
	require.NoError(t, err)
	assert.Empty(t, apis)

	// Second Reset on empty backend must also not panic.
	b.Reset()
}

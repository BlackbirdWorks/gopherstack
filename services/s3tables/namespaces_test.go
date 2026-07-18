package s3tables_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_ListNamespaces_PaginationAndPrefix(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tb, err := b.CreateTableBucket("ns-page-bucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)

	for _, ns := range []string{"alpha", "beta", "gamma"} {
		_, err = b.CreateNamespace(tb.ARN, []string{ns})
		require.NoError(t, err)
	}

	pg, err := b.ListNamespaces(tb.ARN, s3tables.ListNamespacesParams{MaxNamespaces: 1})
	require.NoError(t, err)
	require.Len(t, pg.Data, 1)
	require.NotEmpty(t, pg.Next)

	pg, err = b.ListNamespaces(tb.ARN, s3tables.ListNamespacesParams{Prefix: "al"})
	require.NoError(t, err)
	require.Len(t, pg.Data, 1)
	assert.Equal(t, []string{"alpha"}, pg.Data[0].Namespace)
}

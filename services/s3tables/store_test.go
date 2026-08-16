package s3tables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

const (
	backendTestAccountID = "000000000000"
	backendTestRegion    = "us-east-1"
)

// newTestBackend creates a bare InMemoryBackend for tests in this file and
// its sibling store-level test files (table_buckets_test.go,
// namespaces_test.go, tables_test.go), which all share package
// s3tables_test.
func newTestBackend(t *testing.T) *s3tables.InMemoryBackend {
	t.Helper()

	return s3tables.NewInMemoryBackend(backendTestAccountID, backendTestRegion)
}

func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateTableBucket("mybucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)

	require.Equal(t, 1, s3tables.BucketCount(b))

	b.Reset()

	assert.Equal(t, 0, s3tables.BucketCount(b))
	assert.Equal(t, 0, s3tables.NamespaceCount(b))
	assert.Equal(t, 0, s3tables.TableCount(b))
	assert.Equal(t, 0, s3tables.BucketReplicationCount(b))
	assert.Equal(t, 0, s3tables.TableReplicationCount(b))
	assert.Equal(t, 0, s3tables.TableRecordExpiryCount(b))
}

func TestInMemoryBackend_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("123456789012", "us-west-2")
	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "us-west-2", b.Region())
}

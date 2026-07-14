package s3tables_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

const (
	backendTestAccountID = "000000000000"
	backendTestRegion    = "us-east-1"
)

func newTestBackend(t *testing.T) *s3tables.InMemoryBackend {
	t.Helper()

	return s3tables.NewInMemoryBackend(backendTestAccountID, backendTestRegion)
}

// ----------------------------------------
// CreateTableBucket / CreateTable options (encryption, storage class, tags)
// ----------------------------------------

func TestBackend_CreateTableBucket_AppliesOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		opts             s3tables.CreateTableBucketOptions
		name             string
		wantStorageClass string
		wantEncrypted    bool
	}{
		{
			name:             "no_options_defaults_to_standard",
			opts:             s3tables.CreateTableBucketOptions{},
			wantStorageClass: "STANDARD",
			wantEncrypted:    false,
		},
		{
			name: "encryption_and_storage_class_and_tags_applied",
			opts: s3tables.CreateTableBucketOptions{
				Encryption: map[string]any{
					"sseAlgorithm": "aws:kms",
					"kmsKeyArn":    "arn:aws:kms:us-east-1:000000000000:key/test",
				},
				StorageClass: "INTELLIGENT_TIERING",
				Tags:         map[string]string{"env": "prod"},
			},
			wantStorageClass: "INTELLIGENT_TIERING",
			wantEncrypted:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			tb, err := b.CreateTableBucket("opts-bucket-"+tt.name, tt.opts)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStorageClass, tb.StorageClass)

			if tt.wantEncrypted {
				require.NotNil(t, tb.Encryption)
				assert.Equal(t, "aws:kms", tb.Encryption["sseAlgorithm"])
			} else {
				assert.Nil(t, tb.Encryption)
			}

			if len(tt.opts.Tags) > 0 {
				tags, tagErr := b.ListTagsForResource(tb.ARN)
				require.NoError(t, tagErr)
				assert.Equal(t, tt.opts.Tags, tags)
			}
		})
	}
}

func TestBackend_CreateTable_AppliesOptions(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tb, err := b.CreateTableBucket("table-opts-bucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)

	_, err = b.CreateNamespace(tb.ARN, []string{"ns1"})
	require.NoError(t, err)

	table, err := b.CreateTable(tb.ARN, []string{"ns1"}, "t1", "ICEBERG", s3tables.CreateTableOptions{
		Encryption: map[string]any{
			"sseAlgorithm": "aws:kms",
			"kmsKeyArn":    "arn:aws:kms:us-east-1:000000000000:key/tbl",
		},
		StorageClass: "INTELLIGENT_TIERING",
		Tags:         map[string]string{"team": "data"},
	})
	require.NoError(t, err)

	assert.Equal(t, "INTELLIGENT_TIERING", table.StorageClass)
	require.NotNil(t, table.Encryption)
	assert.Equal(t, "aws:kms", table.Encryption["sseAlgorithm"])

	tags, err := b.ListTagsForResource(table.ARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "data"}, tags)
}

// ----------------------------------------
// GetTableByARN
// ----------------------------------------

func TestBackend_GetTableByARN(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tb, err := b.CreateTableBucket("by-arn-bucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)

	_, err = b.CreateNamespace(tb.ARN, []string{"ns1"})
	require.NoError(t, err)

	created, err := b.CreateTable(tb.ARN, []string{"ns1"}, "t1", "ICEBERG", s3tables.CreateTableOptions{})
	require.NoError(t, err)

	got, err := b.GetTableByARN(created.ARN)
	require.NoError(t, err)
	assert.Equal(t, created.Name, got.Name)
	assert.Equal(t, created.ARN, got.ARN)

	_, err = b.GetTableByARN("arn:aws:s3tables:us-east-1:000000000000:bucket/nope/table/ns1/nope")
	require.Error(t, err)
	assert.ErrorIs(t, err, s3tables.ErrTableNotFound)
}

// ----------------------------------------
// GetTableEncryption inheritance: table override > bucket default > AES256
// ----------------------------------------

func TestBackend_GetTableEncryption_Inheritance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bucketEnc map[string]any
		tableEnc  map[string]any
		wantSSE   string
	}{
		{
			name:    "no_config_anywhere_defaults_to_AES256",
			wantSSE: "AES256",
		},
		{
			name: "bucket_default_inherited_when_table_has_no_override",
			bucketEnc: map[string]any{
				"sseAlgorithm": "aws:kms",
				"kmsKeyArn":    "arn:aws:kms:us-east-1:000000000000:key/b",
			},
			wantSSE: "aws:kms",
		},
		{
			name: "table_override_wins_over_bucket_default",
			bucketEnc: map[string]any{
				"sseAlgorithm": "aws:kms",
				"kmsKeyArn":    "arn:aws:kms:us-east-1:000000000000:key/b",
			},
			tableEnc: map[string]any{"sseAlgorithm": "AES256"},
			wantSSE:  "AES256",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			bucketOpts := s3tables.CreateTableBucketOptions{}
			if tt.bucketEnc != nil {
				bucketOpts.Encryption = tt.bucketEnc
			}

			tb, err := b.CreateTableBucket("enc-inherit-"+tt.name, bucketOpts)
			require.NoError(t, err)

			_, err = b.CreateNamespace(tb.ARN, []string{"ns1"})
			require.NoError(t, err)

			tableOpts := s3tables.CreateTableOptions{}
			if tt.tableEnc != nil {
				tableOpts.Encryption = tt.tableEnc
			}

			_, err = b.CreateTable(tb.ARN, []string{"ns1"}, "t1", "ICEBERG", tableOpts)
			require.NoError(t, err)

			cfg, err := b.GetTableEncryption(tb.ARN, []string{"ns1"}, "t1")
			require.NoError(t, err)
			assert.Equal(t, tt.wantSSE, cfg["sseAlgorithm"])
		})
	}
}

func TestBackend_GetTableEncryption_TableNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tb, err := b.CreateTableBucket("enc-notfound-bucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)

	_, err = b.GetTableEncryption(tb.ARN, []string{"ns1"}, "missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, s3tables.ErrTableNotFound)
}

// ----------------------------------------
// List pagination (ListTableBuckets, ListNamespaces, ListTables)
// ----------------------------------------

func TestBackend_ListTableBuckets_Pagination(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	const total = 5
	for i := range total {
		_, err := b.CreateTableBucket(fmt.Sprintf("page-bucket-%d", i), s3tables.CreateTableBucketOptions{})
		require.NoError(t, err)
	}

	pg, err := b.ListTableBuckets(s3tables.ListTableBucketsParams{MaxBuckets: 2})
	require.NoError(t, err)
	assert.Len(t, pg.Data, 2)
	require.NotEmpty(t, pg.Next, "expected a continuation token when more results remain")

	seen := len(pg.Data)
	token := pg.Next

	for token != "" {
		pg, err = b.ListTableBuckets(s3tables.ListTableBucketsParams{MaxBuckets: 2, ContinuationToken: token})
		require.NoError(t, err)
		seen += len(pg.Data)
		token = pg.Next
	}

	assert.Equal(t, total, seen, "pagination must eventually enumerate every bucket exactly once")
}

func TestBackend_ListTableBuckets_PrefixAndTypeFilter(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.CreateTableBucket("alpha-bucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)
	_, err = b.CreateTableBucket("beta-bucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)

	pg, err := b.ListTableBuckets(s3tables.ListTableBucketsParams{Prefix: "alpha"})
	require.NoError(t, err)
	require.Len(t, pg.Data, 1)
	assert.Equal(t, "alpha-bucket", pg.Data[0].Name)

	// Every bucket this backend creates is "customer" type; filtering by
	// "aws" must match nothing, matching real AWS semantics.
	pg, err = b.ListTableBuckets(s3tables.ListTableBucketsParams{Type: "aws"})
	require.NoError(t, err)
	assert.Empty(t, pg.Data)

	pg, err = b.ListTableBuckets(s3tables.ListTableBucketsParams{Type: "customer"})
	require.NoError(t, err)
	assert.Len(t, pg.Data, 2)
}

func TestBackend_ListTableBuckets_InvalidToken(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.ListTableBuckets(s3tables.ListTableBucketsParams{ContinuationToken: "not-a-valid-token!!"})
	require.Error(t, err)
	assert.ErrorIs(t, err, s3tables.ErrInvalidContinuationToken)
}

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

func TestBackend_ListTables_PaginationAndPrefix(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tb, err := b.CreateTableBucket("tbl-page-bucket", s3tables.CreateTableBucketOptions{})
	require.NoError(t, err)

	_, err = b.CreateNamespace(tb.ARN, []string{"ns1"})
	require.NoError(t, err)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		_, err = b.CreateTable(tb.ARN, []string{"ns1"}, name, "ICEBERG", s3tables.CreateTableOptions{})
		require.NoError(t, err)
	}

	pg, err := b.ListTables(tb.ARN, "", s3tables.ListTablesParams{MaxTables: 1})
	require.NoError(t, err)
	require.Len(t, pg.Data, 1)
	require.NotEmpty(t, pg.Next)

	pg, err = b.ListTables(tb.ARN, "", s3tables.ListTablesParams{Prefix: "be"})
	require.NoError(t, err)
	require.Len(t, pg.Data, 1)
	assert.Equal(t, "beta", pg.Data[0].Name)
}

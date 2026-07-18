package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestGlueResourceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceARN  string
		resourceType string
		want         string
	}{
		{
			name:         "valid_database_arn_with_account",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:database/my-db",
			resourceType: "database",
			want:         "my-db",
		},
		{
			name:         "valid_database_arn_empty_account",
			resourceARN:  "arn:aws:glue:us-east-1::database/my-db",
			resourceType: "database",
			want:         "my-db",
		},
		{
			name:         "valid_crawler_arn",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:crawler/my-crawler",
			resourceType: "crawler",
			want:         "my-crawler",
		},
		{
			name:         "valid_job_arn",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:job/my-job",
			resourceType: "job",
			want:         "my-job",
		},
		{
			name:         "wrong_resource_type",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:database/my-db",
			resourceType: "crawler",
			want:         "",
		},
		{
			name:         "malformed_arn_too_few_parts",
			resourceARN:  "arn:aws:glue:us-east-1",
			resourceType: "database",
			want:         "",
		},
		{
			name:         "empty_arn",
			resourceARN:  "",
			resourceType: "database",
			want:         "",
		},
		{
			name:         "resource_type_not_in_arn",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:table/my-table",
			resourceType: "database",
			want:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := glue.GlueResourceNameForTest(tt.resourceARN, tt.resourceType)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &glue.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, glue.ErrNilAppContext)
}

func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend("000000000000", "us-east-1")
	h := glue.NewHandler(backend)

	assert.Positive(t, h.HandlerOpsLen())
}

func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend("000000000000", "us-east-1")
	h := glue.NewHandler(backend)

	ops := h.GetSupportedOperations()
	assert.Len(t, ops, h.HandlerOpsLen(), "supported ops count should match dispatch table size")

	expectedBatch := []string{
		"BatchCreatePartition",
		"BatchDeleteConnection",
		"BatchDeletePartition",
		"BatchDeleteTable",
		"BatchDeleteTableVersion",
		"BatchGetBlueprints",
		"BatchGetCrawlers",
		"BatchGetCustomEntityTypes",
		"BatchGetDataQualityResult",
		"BatchGetDevEndpoints",
	}

	for _, op := range expectedBatch {
		assert.Contains(t, ops, op, "expected batch op %s in supported operations", op)
	}
}

func TestErrValidation(t *testing.T) {
	t.Parallel()

	require.Error(t, glue.ErrValidation)
	assert.Error(t, glue.ErrValidation)
}

func TestProviderInit_ValidCtx(t *testing.T) {
	t.Parallel()

	p := &glue.Provider{}
	ctx := &service.AppContext{}
	h, err := p.Init(ctx)

	require.NoError(t, err)
	assert.NotNil(t, h)
}

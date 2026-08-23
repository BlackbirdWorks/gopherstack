package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestDataQualityStatsSeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *glue.InMemoryBackend)
		checkFunc func(t *testing.T, b *glue.InMemoryBackend)
		name      string
	}{
		{
			name: "AddConnectionInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddConnectionInternal(&glue.Connection{Name: "conn1"})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.ConnectionCount(b))
			},
		},
		{
			name: "AddBlueprintInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddBlueprintInternal(&glue.Blueprint{Name: "bp1", Status: "ACTIVE"})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.BlueprintCount(b))
			},
		},
		{
			name: "AddCustomEntityTypeInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddCustomEntityTypeInternal(&glue.CustomEntityType{Name: "cet1", RegexString: `\d+`})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.CustomEntityTypeCount(b))
			},
		},
		{
			name: "AddDataQualityResultInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "r1", Score: 0.9})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.DataQualityResultCount(b))
			},
		},
		{
			name: "AddDevEndpointInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddDevEndpointInternal(&glue.DevEndpoint{EndpointName: "ep1", Status: "READY"})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.DevEndpointCount(b))
			},
		},
		{
			name: "AddTableVersionInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddTableVersionInternal("db1", "tbl1", &glue.TableVersion{VersionID: "1"})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.TableVersionCount(b))
			},
		},
		{
			name: "AddPartitionInternal",
			setup: func(b *glue.InMemoryBackend) {
				b.AddPartitionInternal("db1", "tbl1", &glue.Partition{Values: []string{"2024", "01"}})
			},
			checkFunc: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.PartitionCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)
			tt.checkFunc(t, b)
		})
	}
}

func TestBatchGetDataQualityResult_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "r1", Score: 0.95})

	found, notFound := b.BatchGetDataQualityResult([]string{"r1", "r2"})

	assert.Len(t, found, 1)
	assert.InDelta(t, 0.95, found[0].Score, 0.001)
	assert.Equal(t, []string{"r2"}, notFound)
}

func TestNonNilSlices_BatchOutputs(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")

	tests := []struct {
		validate func(t *testing.T)
		name     string
	}{
		{
			name: "BatchGetBlueprints_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, missing := b.BatchGetBlueprints([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, missing)
			},
		},
		{
			name: "BatchGetCrawlers_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, missing := b.BatchGetCrawlers([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, missing)
			},
		},
		{
			name: "BatchGetCustomEntityTypes_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, missing := b.BatchGetCustomEntityTypes([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, missing)
			},
		},
		{
			name: "BatchGetDataQualityResult_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, errs := b.BatchGetDataQualityResult([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, errs)
			},
		},
		{
			name: "BatchGetDevEndpoints_empty",
			validate: func(t *testing.T) {
				t.Helper()
				found, missing := b.BatchGetDevEndpoints([]string{})
				assert.NotNil(t, found)
				assert.NotNil(t, missing)
			},
		},
		{
			name: "BatchDeleteConnection_empty",
			validate: func(t *testing.T) {
				t.Helper()
				succeeded, errs := b.BatchDeleteConnection([]string{})
				assert.NotNil(t, succeeded)
				assert.NotNil(t, errs)
			},
		},
		{
			name: "BatchDeletePartition_empty",
			validate: func(t *testing.T) {
				t.Helper()
				errs := b.BatchDeletePartition("db", "tbl", []glue.PartitionValueList{})
				assert.NotNil(t, errs)
			},
		},
		{
			name: "BatchDeleteTable_empty",
			validate: func(t *testing.T) {
				t.Helper()
				errs := b.BatchDeleteTable("db", []string{})
				assert.NotNil(t, errs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.validate(t)
		})
	}
}

package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestBatchCreatePartition_RoundTrip(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)
	_, err = b.CreateTable("db", glue.TableInput{Name: "tbl"})
	require.NoError(t, err)

	inputs := []glue.PartitionInput{
		{Values: []string{"2024", "01"}},
		{Values: []string{"2024", "02"}},
	}

	created, errs := b.BatchCreatePartition("db", "tbl", inputs)

	assert.Len(t, created, 2)
	assert.Empty(t, errs)
	assert.Equal(t, 2, glue.PartitionCount(b))

	// Duplicate should produce an error.
	created2, errs2 := b.BatchCreatePartition("db", "tbl", inputs)
	assert.Empty(t, created2)
	assert.Len(t, errs2, 2)
}

func TestBatchDeletePartition_RemovesPartition(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddPartitionInternal("db", "tbl", &glue.Partition{Values: []string{"2024", "01"}})
	b.AddPartitionInternal("db", "tbl", &glue.Partition{Values: []string{"2024", "02"}})

	errs := b.BatchDeletePartition("db", "tbl", []glue.PartitionValueList{
		{Values: []string{"2024", "01"}},
	})

	assert.Empty(t, errs)
	assert.Equal(t, 1, glue.PartitionCount(b))

	// Delete a non-existent partition.
	errs2 := b.BatchDeletePartition("db", "tbl", []glue.PartitionValueList{
		{Values: []string{"9999", "99"}},
	})

	assert.Len(t, errs2, 1)
	assert.Equal(t, "EntityNotFoundException", errs2[0].ErrorDetail.ErrorCode)
}

func TestBatchDeleteTable_CascadesPartitions(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)
	_, err = b.CreateTable("db", glue.TableInput{Name: "tbl"})
	require.NoError(t, err)

	b.AddPartitionInternal("db", "tbl", &glue.Partition{Values: []string{"2024"}})
	b.AddTableVersionInternal("db", "tbl", &glue.TableVersion{VersionID: "1"})

	require.Equal(t, 1, glue.PartitionCount(b))
	require.Equal(t, 1, glue.TableVersionCount(b))

	tableErrs := b.BatchDeleteTable("db", []string{"tbl"})

	assert.Empty(t, tableErrs)
	assert.Equal(t, 0, glue.TableCount(b))
	assert.Equal(t, 0, glue.PartitionCount(b))
	assert.Equal(t, 0, glue.TableVersionCount(b))
}

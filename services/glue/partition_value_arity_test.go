package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestBatchCreatePartition_ValueArity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantErrCode string
		keys        []glue.Column
		values      []string
	}{
		{name: "matching", keys: []glue.Column{{Name: "year"}}, values: []string{"2024"}},
		{
			name:        "too many",
			keys:        []glue.Column{{Name: "year"}},
			values:      []string{"2024", "x"},
			wantErrCode: "InvalidInputException",
		},
		{
			name:        "too few",
			keys:        []glue.Column{{Name: "y"}, {Name: "m"}},
			values:      []string{"2024"},
			wantErrCode: "InvalidInputException",
		},
		{name: "empty values", keys: []glue.Column{{Name: "year"}}, values: nil, wantErrCode: "InvalidInputException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
			require.NoError(t, err)
			_, err = b.CreateTable("db", glue.TableInput{Name: "tbl", PartitionKeys: tt.keys})
			require.NoError(t, err)

			created, errs := b.BatchCreatePartition("db", "tbl", []glue.PartitionInput{{Values: tt.values}})
			if tt.wantErrCode == "" {
				assert.Len(t, created, 1)
				assert.Empty(t, errs)

				return
			}

			assert.Empty(t, created)
			require.Len(t, errs, 1)
			assert.Equal(t, tt.wantErrCode, errs[0].ErrorDetail.ErrorCode)
			assert.Equal(t, 0, glue.PartitionCount(b))
		})
	}
}

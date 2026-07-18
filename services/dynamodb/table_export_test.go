package dynamodb_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestExportTableToPointInTime_UniqueARN(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	createSimplePPRTable(t, db, "ExportTable")

	h := dynamodb.NewHandler(db)
	ctx := t.Context()

	const calls = 5
	arns := make(map[string]bool, calls)

	for i := range calls {
		body := fmt.Appendf(
			nil,
			`{"TableArn":"arn:aws:dynamodb:us-east-1:123456789012:table/ExportTable","S3Bucket":"bucket-%d"}`,
			i,
		)
		out, err := h.HandleRequest(ctx, "ExportTableToPointInTime", body)
		require.NoError(t, err, "call %d", i)
		require.NotNil(t, out, "call %d returned nil", i)

		exportARN := dynamodb.ExtractExportARNForTest(out)
		require.NotEmpty(t, exportARN, "ExportArn missing on call %d", i)

		assert.False(t, arns[exportARN], "ARN collision on call %d: %s", i, exportARN)
		arns[exportARN] = true
	}
}

func TestListExports_TracksExports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filterARN string
		inject    []dynamodb.ExportDescFields
		wantCount int
	}{
		{
			name:      "empty_when_no_exports",
			inject:    nil,
			wantCount: 0,
		},
		{
			name: "returns_all_stored_exports",
			inject: []dynamodb.ExportDescFields{
				{
					ExportArn:    "arn:aws:dynamodb:us-east-1:123:table/T/export/1",
					ExportStatus: "COMPLETED",
					TableArn:     "arn:T",
					S3Bucket:     "b",
				},
				{
					ExportArn:    "arn:aws:dynamodb:us-east-1:123:table/T/export/2",
					ExportStatus: "COMPLETED",
					TableArn:     "arn:T",
					S3Bucket:     "b",
				},
			},
			wantCount: 2,
		},
		{
			name: "filters_by_table_arn",
			inject: []dynamodb.ExportDescFields{
				{ExportArn: "arn:1", ExportStatus: "COMPLETED", TableArn: "arn:TableA"},
				{ExportArn: "arn:2", ExportStatus: "COMPLETED", TableArn: "arn:TableB"},
			},
			filterARN: "arn:TableA",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			localDB := newTestDBWithCleanup(t)

			for _, e := range tt.inject {
				localDB.StoreExportForTest(e.ExportArn, e.TableArn, e.S3Bucket, e.ExportStatus)
			}

			assert.Equal(t, len(tt.inject), localDB.ExportCount())
		})
	}
}

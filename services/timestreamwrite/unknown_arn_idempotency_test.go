package timestreamwrite_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	timestreamwritesdk "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	twtypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

func TestTagOpsUnknownARN_RealClient(t *testing.T) {
	t.Parallel()

	const unknown = "arn:aws:timestream:us-east-1:000000000000:database/nope"

	tests := []struct {
		call func(t *testing.T, c *timestreamwritesdk.Client) error
		name string
	}{
		{
			name: "list",
			call: func(t *testing.T, c *timestreamwritesdk.Client) error {
				t.Helper()
				_, err := c.ListTagsForResource(t.Context(), &timestreamwritesdk.ListTagsForResourceInput{
					ResourceARN: aws.String(unknown),
				})

				return err
			},
		},
		{
			name: "untag",
			call: func(t *testing.T, c *timestreamwritesdk.Client) error {
				t.Helper()
				_, err := c.UntagResource(t.Context(), &timestreamwritesdk.UntagResourceInput{
					ResourceARN: aws.String(unknown),
					TagKeys:     []string{"k"},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestTimestreamWriteSDKClient(t, timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend()))
			err := tt.call(t, c)
			require.Error(t, err)

			var nf *twtypes.ResourceNotFoundException

			require.ErrorAs(t, err, &nf)
		})
	}
}

func TestUpdateDatabaseKmsKeyRequired_RawRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{name: "absent", body: map[string]any{"DatabaseName": "dbx"}, wantStatus: 400},
		{name: "empty_clears", body: map[string]any{"DatabaseName": "dbx", "KmsKeyId": ""}, wantStatus: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateDatabase("dbx", "", nil)
			require.NoError(t, err)

			rec := doRequest(t, h, "UpdateDatabase", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCreateBatchLoadTaskClientToken_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tokenB   string
		wantSame bool
	}{
		{name: "same_token_replays", tokenB: "tok-1", wantSame: true},
		{name: "different_token_new_task", tokenB: "tok-2", wantSame: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			c := newTestTimestreamWriteSDKClient(t, timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend()))

			_, err := c.CreateDatabase(ctx, &timestreamwritesdk.CreateDatabaseInput{DatabaseName: aws.String("dbx")})
			require.NoError(t, err)
			_, err = c.CreateTable(ctx, &timestreamwritesdk.CreateTableInput{
				DatabaseName: aws.String("dbx"), TableName: aws.String("tbx"),
			})
			require.NoError(t, err)

			create := func(token string) string {
				out, cerr := c.CreateBatchLoadTask(ctx, &timestreamwritesdk.CreateBatchLoadTaskInput{
					ClientToken:        aws.String(token),
					TargetDatabaseName: aws.String("dbx"),
					TargetTableName:    aws.String("tbx"),
					DataSourceConfiguration: &twtypes.DataSourceConfiguration{
						DataFormat:                twtypes.BatchLoadDataFormatCsv,
						DataSourceS3Configuration: &twtypes.DataSourceS3Configuration{BucketName: aws.String("b")},
					},
					ReportConfiguration: &twtypes.ReportConfiguration{},
				})
				require.NoError(t, cerr)

				return aws.ToString(out.TaskId)
			}

			first := create("tok-1")
			second := create(tt.tokenB)
			assert.Equal(t, tt.wantSame, first == second)
		})
	}
}

func TestCreateBatchLoadTaskReportConfigRequired_RawRequest(t *testing.T) {
	t.Parallel()

	ds := map[string]any{
		"DataFormat":                "CSV",
		"DataSourceS3Configuration": map[string]any{"BucketName": "b"},
	}

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "missing",
			body: map[string]any{
				"TargetDatabaseName":      "dbx",
				"TargetTableName":         "tbx",
				"DataSourceConfiguration": ds,
			},
			wantStatus: 400,
		},
		{
			name: "present",
			body: map[string]any{
				"TargetDatabaseName": "dbx", "TargetTableName": "tbx",
				"DataSourceConfiguration": ds, "ReportConfiguration": map[string]any{},
			},
			wantStatus: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateDatabase("dbx", "", nil)
			require.NoError(t, err)
			_, err = h.Backend.CreateTable("dbx", "tbx", nil, nil)
			require.NoError(t, err)

			rec := doRequest(t, h, "CreateBatchLoadTask", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

package stepfunctions_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

func parquetFixture(t *testing.T, compression string) []byte {
	t.Helper()

	cols := []parquetio.Column{
		{Name: "id", Type: "bigint"},
		{Name: "score", Type: "double"},
		{Name: "name", Type: "string"},
		{Name: "active", Type: "boolean"},
		{Name: "seen", Type: "timestamp"},
		{Name: "note", Type: "string"},
	}
	rows := [][]any{
		{int64(1), 1.5, "alpha", true, "2024-03-05 10:20:30", nil},
		{int64(2), 2.25, "beta", false, "2024-03-06 00:00:00", "x"},
		{int64(3), 0.0, "gamma", true, "2024-03-07 12:00:00", nil},
	}

	var buf bytes.Buffer
	require.NoError(t, parquetio.Write(&buf, cols, rows, compression))

	return buf.Bytes()
}

func TestItemReader_Parquet(t *testing.T) {
	t.Parallel()

	const wantAll = `[
		{"id":1,"score":1.5,"name":"alpha","active":true,"seen":"2024-03-05 10:20:30.000","note":null},
		{"id":2,"score":2.25,"name":"beta","active":false,"seen":"2024-03-06 00:00:00.000","note":"x"},
		{"id":3,"score":0,"name":"gamma","active":true,"seen":"2024-03-07 12:00:00.000","note":null}
	]`

	tests := []struct {
		name        string
		config      string
		data        func(t *testing.T) []byte
		want        string
		wantErrCode string
		wantCause   string
	}{
		{
			name:   "snappy_rows_become_objects",
			config: `{"InputType":"PARQUET"}`,
			data:   func(t *testing.T) []byte { t.Helper(); return parquetFixture(t, "SNAPPY") },
			want:   wantAll,
		},
		{
			name:   "zstd",
			config: `{"InputType":"PARQUET"}`,
			data:   func(t *testing.T) []byte { t.Helper(); return parquetFixture(t, "ZSTD") },
			want:   wantAll,
		},
		{
			name:   "gzip",
			config: `{"InputType":"parquet"}`,
			data:   func(t *testing.T) []byte { t.Helper(); return parquetFixture(t, "GZIP") },
			want:   wantAll,
		},
		{
			name:   "max_items",
			config: `{"InputType":"PARQUET","MaxItems":2}`,
			data:   func(t *testing.T) []byte { t.Helper(); return parquetFixture(t, "SNAPPY") },
			want: `[
				{"id":1,"score":1.5,"name":"alpha","active":true,"seen":"2024-03-05 10:20:30.000","note":null},
				{"id":2,"score":2.25,"name":"beta","active":false,"seen":"2024-03-06 00:00:00.000","note":"x"}
			]`,
		},
		{
			name:        "corrupt_file",
			config:      `{"InputType":"PARQUET"}`,
			data:        func(*testing.T) []byte { return []byte("this is not parquet") },
			wantErrCode: "States.ItemReaderFailed",
			wantCause:   "unable to read Parquet file",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const bucket = "parquet-bucket"

			s3Bk := newBucketBackedS3(t, bucket)
			putS3Object(t, s3Bk, bucket, "data/rows.parquet", tc.data(t))

			itemReader := `{
				"Resource": "arn:aws:states:::s3:getObject",
				"ReaderConfig": ` + tc.config + `,
				"Parameters": {"Bucket": "` + bucket + `", "Key": "data/rows.parquet"}
			}`

			status, output, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)

			if tc.wantErrCode != "" {
				assert.Equal(t, sfntypes.ExecutionStatusFailed, status)
				assert.Equal(t, tc.wantErrCode, errCode)
				assert.Contains(t, cause, tc.wantCause)

				return
			}

			require.Equal(t, sfntypes.ExecutionStatusSucceeded, status, "error=%s cause=%s", errCode, cause)
			assert.JSONEq(t, tc.want, output)
		})
	}
}

func TestItemReader_ParquetFlattenAndAthena(t *testing.T) {
	t.Parallel()

	const bucket = "parquet-multi-bucket"

	s3Bk := newBucketBackedS3(t, bucket)
	putS3Object(t, s3Bk, bucket, "p/a.parquet", parquetFixture(t, "SNAPPY"))
	putS3Object(t, s3Bk, bucket, "p/b.parquet", parquetFixture(t, "SNAPPY"))
	putS3Object(t, s3Bk, bucket, "m/manifest.csv", []byte("s3://"+bucket+"/p/a.parquet\ns3://"+bucket+"/p/b.parquet\n"))

	tests := []struct {
		name       string
		itemReader string
	}{
		{"load_and_flatten", `{
			"Resource": "arn:aws:states:::s3:listObjectsV2",
			"ReaderConfig": {"InputType": "PARQUET", "Transformation": "LOAD_AND_FLATTEN"},
			"Parameters": {"Bucket": "` + bucket + `", "Prefix": "p/"}
		}`},
		{"athena_manifest", `{
			"Resource": "arn:aws:states:::s3:getObject",
			"ReaderConfig": {"ManifestType": "ATHENA_DATA", "InputType": "PARQUET"},
			"Parameters": {"Bucket": "` + bucket + `", "Key": "m/manifest.csv"}
		}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			status, output, errCode, cause := runItemReaderExecution(t, s3Bk, tc.itemReader)
			require.Equal(t, sfntypes.ExecutionStatusSucceeded, status, "error=%s cause=%s", errCode, cause)

			var items []map[string]any
			require.NoError(t, json.Unmarshal([]byte(output), &items))
			assert.Len(t, items, 6)
		})
	}
}

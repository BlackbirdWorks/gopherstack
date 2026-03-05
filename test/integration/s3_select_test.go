package integration_test

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_S3_SelectObjectContent(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name       string
		setupCSV   string
		expression string
		headerInfo types.FileHeaderInfo
		wantResult string
		wantAbsent string
	}{
		{
			name:       "select with WHERE numeric comparison",
			setupCSV:   "name,age\nAlice,30\nBob,25\n",
			expression: "SELECT s.name FROM s3object s WHERE s.age > 26",
			headerInfo: types.FileHeaderInfoUse,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:       "select with LIKE",
			setupCSV:   "name,age\nAlice,30\nAlex,28\nBob,25\n",
			expression: "SELECT s.name FROM s3object s WHERE s.name LIKE 'Al%'",
			headerInfo: types.FileHeaderInfoUse,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:       "select with LIMIT",
			setupCSV:   "name,age\nAlice,30\nBob,25\nCharlie,20\n",
			expression: "SELECT s.name FROM s3object s LIMIT 1",
			headerInfo: types.FileHeaderInfoUse,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:       "select equals string",
			setupCSV:   "name,city\nAlice,NYC\nBob,LA\n",
			expression: "SELECT s.name FROM s3object s WHERE s.city = 'NYC'",
			headerInfo: types.FileHeaderInfoUse,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createS3Client(t)

			bucketName := "select-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(bucketName),
			})
			require.NoError(t, err)

			_, err = client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("data.csv"),
				Body:   strings.NewReader(tt.setupCSV),
			})
			require.NoError(t, err)

			out, err := client.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
				Bucket:         aws.String(bucketName),
				Key:            aws.String("data.csv"),
				Expression:     aws.String(tt.expression),
				ExpressionType: "SQL",
				InputSerialization: &types.InputSerialization{
					CSV: &types.CSVInput{
						FileHeaderInfo: tt.headerInfo,
					},
				},
				OutputSerialization: &types.OutputSerialization{
					CSV: &types.CSVOutput{},
				},
			})
			require.NoError(t, err)

			defer out.GetStream().Close()

			var records []byte

			for event := range out.GetStream().Events() {
				if v, ok := event.(*types.SelectObjectContentEventStreamMemberRecords); ok {
					records = append(records, v.Value.Payload...)
				}
			}

			require.NoError(t, out.GetStream().Err())

			if tt.wantResult != "" {
				assert.Contains(t, string(records), tt.wantResult)
			}

			if tt.wantAbsent != "" {
				assert.NotContains(t, string(records), tt.wantAbsent)
			}
		})
	}
}

func TestIntegration_S3_SelectObjectContent_JSONInput(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createS3Client(t)

	bucketName := "select-json-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	jsonLines := `{"name":"Alice","age":30}` + "\n" + `{"name":"Bob","age":25}` + "\n"

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("people.jsonl"),
		Body:   strings.NewReader(jsonLines),
	})
	require.NoError(t, err)

	out, err := client.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
		Bucket:         aws.String(bucketName),
		Key:            aws.String("people.jsonl"),
		Expression:     aws.String("SELECT s.name FROM s3object s WHERE s.age > 26"),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			JSON: &types.JSONInput{
				Type: "LINES",
			},
		},
		OutputSerialization: &types.OutputSerialization{
			JSON: &types.JSONOutput{},
		},
	})
	require.NoError(t, err)

	defer out.GetStream().Close()

	var records []byte

	for event := range out.GetStream().Events() {
		if v, ok := event.(*types.SelectObjectContentEventStreamMemberRecords); ok {
			records = append(records, v.Value.Payload...)
		}
	}

	require.NoError(t, out.GetStream().Err())
	assert.Contains(t, string(records), "Alice")
	assert.NotContains(t, string(records), "Bob")
}

func TestIntegration_S3_SelectObjectContent_NoResults(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createS3Client(t)

	bucketName := "select-empty-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("data.csv"),
		Body:   strings.NewReader("name,age\nAlice,30\n"),
	})
	require.NoError(t, err)

	out, err := client.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
		Bucket:         aws.String(bucketName),
		Key:            aws.String("data.csv"),
		Expression:     aws.String("SELECT s.name FROM s3object s WHERE s.age > 100"),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			CSV: &types.CSVInput{
				FileHeaderInfo: types.FileHeaderInfoUse,
			},
		},
		OutputSerialization: &types.OutputSerialization{
			CSV: &types.CSVOutput{},
		},
	})
	require.NoError(t, err)

	defer out.GetStream().Close()

	var records []byte

	for event := range out.GetStream().Events() {
		if v, ok := event.(*types.SelectObjectContentEventStreamMemberRecords); ok {
			records = append(records, v.Value.Payload...)
		}
	}

	require.NoError(t, out.GetStream().Err())

	// Drain reader to confirm no content.
	recordStr := string(records)
	assert.Empty(t, recordStr)
}

func TestIntegration_S3_SelectObjectContent_StatsEvent(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createS3Client(t)

	bucketName := "select-stats-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	csvData := "name,age\nAlice,30\n"

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("data.csv"),
		Body:   strings.NewReader(csvData),
	})
	require.NoError(t, err)

	out, err := client.SelectObjectContent(ctx, &s3.SelectObjectContentInput{
		Bucket:         aws.String(bucketName),
		Key:            aws.String("data.csv"),
		Expression:     aws.String("SELECT s.name FROM s3object s"),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			CSV: &types.CSVInput{
				FileHeaderInfo: types.FileHeaderInfoUse,
			},
		},
		OutputSerialization: &types.OutputSerialization{
			CSV: &types.CSVOutput{},
		},
	})
	require.NoError(t, err)

	defer out.GetStream().Close()

	var sawStats, sawEnd bool

	var records []byte

	for event := range out.GetStream().Events() {
		switch v := event.(type) {
		case *types.SelectObjectContentEventStreamMemberRecords:
			records = append(records, v.Value.Payload...)
		case *types.SelectObjectContentEventStreamMemberStats:
			sawStats = true
			assert.Positive(t, aws.ToInt64(v.Value.Details.BytesScanned))
		case *types.SelectObjectContentEventStreamMemberEnd:
			sawEnd = true
		}
	}

	require.NoError(t, out.GetStream().Err())
	assert.Contains(t, string(records), "Alice")
	assert.True(t, sawStats, "expected Stats event in response")
	assert.True(t, sawEnd, "expected End event in response")
}

func drainS3SelectStream(t *testing.T, out *s3.SelectObjectContentOutput) []byte {
	t.Helper()

	defer out.GetStream().Close()

	var records []byte

	for event := range out.GetStream().Events() {
		if v, ok := event.(*types.SelectObjectContentEventStreamMemberRecords); ok {
			records = append(records, v.Value.Payload...)
		}
	}

	require.NoError(t, out.GetStream().Err())

	return records
}

func mustSelectCSV(
	t *testing.T,
	client *s3.Client,
	bucket, key, expression string,
	headerInfo types.FileHeaderInfo,
) string {
	t.Helper()

	out, err := client.SelectObjectContent(t.Context(), &s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(key),
		Expression:     aws.String(expression),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			CSV: &types.CSVInput{
				FileHeaderInfo: headerInfo,
			},
		},
		OutputSerialization: &types.OutputSerialization{
			CSV: &types.CSVOutput{},
		},
	})
	require.NoError(t, err)

	return string(drainS3SelectStream(t, out))
}

func TestIntegration_S3_SelectObjectContent_Operators(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createS3Client(t)

	bucketName := "select-ops-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	csvData := "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,20,NYC\n"

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("data.csv"),
		Body:   io.NopCloser(strings.NewReader(csvData)),
	})
	require.NoError(t, err)

	tests := []struct {
		expression string
		name       string
		want       string
		absent     string
	}{
		{
			name:       "AND",
			expression: "SELECT s.name FROM s3object s WHERE s.age > 20 AND s.age < 30",
			want:       "Bob",
			absent:     "Alice",
		},
		{
			name:       "OR",
			expression: "SELECT s.name FROM s3object s WHERE s.age = 30 OR s.age = 20",
			want:       "Alice",
			absent:     "Bob",
		},
		{
			name:       "NOT",
			expression: "SELECT s.name FROM s3object s WHERE NOT s.name = 'Bob'",
			want:       "Alice",
			absent:     "Bob",
		},
		{
			name:       "LIKE",
			expression: "SELECT s.name FROM s3object s WHERE s.name LIKE 'Al%'",
			want:       "Alice",
			absent:     "Bob",
		},
		{
			name:       "IN",
			expression: "SELECT s.name FROM s3object s WHERE s.city IN ('NYC')",
			want:       "Alice",
			absent:     "Bob",
		},
		{
			name:       "LIMIT",
			expression: "SELECT s.name FROM s3object s LIMIT 1",
			want:       "Alice",
			absent:     "Bob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := mustSelectCSV(t, client, bucketName, "data.csv", tt.expression, types.FileHeaderInfoUse)

			assert.Contains(t, result, tt.want)
			assert.NotContains(t, result, tt.absent)
		})
	}
}

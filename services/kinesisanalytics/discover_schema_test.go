package kinesisanalytics_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestS3ObjectReader_SatisfiedByRealS3Backend locks the cross-service contract
// DiscoverInputSchema's S3 sampling path depends on: kinesisanalytics.S3ObjectReader is
// structurally satisfied by s3.InMemoryBackend.GetObject directly, with no adapter needed
// (mirrors services/cloudwatch's equivalent proof for firehose.InMemoryBackend). cli.go wiring
// (kaBk.SetS3ObjectReader(s3Bk)) itself is out of this pass's scope.
func TestS3ObjectReader_SatisfiedByRealS3Backend(t *testing.T) {
	t.Parallel()

	var _ kinesisanalytics.S3ObjectReader = (*s3.InMemoryBackend)(nil)
}

// fakeS3Reader is a minimal kinesisanalytics.S3ObjectReader test double: it proves
// DiscoverInputSchema's sampling+inference logic behaves correctly once ANY conforming backend
// is wired, without needing a full real s3.InMemoryBackend (bucket/region/compressor setup).
type fakeS3Reader struct {
	err  error
	body []byte
}

func (f *fakeS3Reader) GetObject(context.Context, *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error) {
	if f.err != nil {
		return nil, f.err
	}

	return &sdk_s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.body))}, nil
}

// fakeKinesisReader is a minimal kinesisanalytics.KinesisStreamReader test double, mirroring
// services/firehose's own mockKinesisReader (services/firehose/kinesis_source_test.go) for the
// same narrow interface shape.
type fakeKinesisReader struct {
	err     error
	records [][]byte
}

func (f *fakeKinesisReader) ListShards(string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}

	return []string{"shard-1"}, nil
}

func (f *fakeKinesisReader) GetShardIterator(string, string) (string, error) {
	return "iter", nil
}

func (f *fakeKinesisReader) GetRecords(_ string, limit int) ([][]byte, string, error) {
	if f.err != nil {
		return nil, "", f.err
	}

	if limit > len(f.records) {
		limit = len(f.records)
	}

	return f.records[:limit], "", nil
}

func discoverSchemaBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func TestDiscoverInputSchema_S3SamplesRealRecords(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.SetS3ObjectReader(&fakeS3Reader{
		body: []byte(
			"{\"ticker\":\"AMZN\",\"price\":42,\"active\":true}\n" +
				"{\"ticker\":\"GOOG\",\"price\":101.5,\"active\":false}\n",
		),
	})

	rec := doRequest(t, h, "DiscoverInputSchema", map[string]any{
		"S3Configuration": map[string]any{
			"BucketARN": "arn:aws:s3:::bucket",
			"FileKey":   "data.json",
			"RoleARN":   "arn:aws:iam::000000000000:role/role",
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	resp := discoverSchemaBody(t, rec)

	raw, ok := resp["RawInputRecords"].([]any)
	require.True(t, ok)
	assert.Len(t, raw, 2)
	assert.JSONEq(t, `{"ticker":"AMZN","price":42,"active":true}`, raw[0].(string))

	schema, ok := resp["InputSchema"].(map[string]any)
	require.True(t, ok)
	cols, ok := schema["RecordColumns"].([]any)
	require.True(t, ok)
	require.Len(t, cols, 3)

	// Alphabetical column order: active, price, ticker.
	assert.Equal(t, "active", cols[0].(map[string]any)["Name"])
	assert.Equal(t, "BOOLEAN", cols[0].(map[string]any)["SqlType"])
	assert.Equal(t, "price", cols[1].(map[string]any)["Name"])
	assert.Equal(t, "DOUBLE", cols[1].(map[string]any)["SqlType"])
	assert.Equal(t, "ticker", cols[2].(map[string]any)["Name"])
	assert.Equal(t, "VARCHAR(4)", cols[2].(map[string]any)["SqlType"])

	parsed, ok := resp["ParsedInputRecords"].([]any)
	require.True(t, ok)
	require.Len(t, parsed, 2)
	row0, ok := parsed[0].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"true", "42", "AMZN"}, row0)
}

func TestDiscoverInputSchema_S3EmptyObjectFails(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.SetS3ObjectReader(&fakeS3Reader{body: []byte("\n\n")})

	rec := doRequest(t, h, "DiscoverInputSchema", map[string]any{
		"S3Configuration": map[string]any{
			"BucketARN": "arn:aws:s3:::bucket",
			"FileKey":   "empty.json",
			"RoleARN":   "arn:aws:iam::000000000000:role/role",
		},
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := discoverSchemaBody(t, rec)
	assert.Equal(t, "UnableToDetectSchemaException", resp["__type"])
}

func TestDiscoverInputSchema_KinesisSamplesRealRecords(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.SetKinesisStreamReader(&fakeKinesisReader{
		records: [][]byte{
			[]byte(`{"id":1,"name":"a"}`),
			[]byte(`{"id":2,"name":"bb"}`),
		},
	})

	rec := doRequest(t, h, "DiscoverInputSchema", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/mystream",
		"RoleARN":     "arn:aws:iam::000000000000:role/role",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	resp := discoverSchemaBody(t, rec)

	schema, ok := resp["InputSchema"].(map[string]any)
	require.True(t, ok)
	cols, ok := schema["RecordColumns"].([]any)
	require.True(t, ok)
	require.Len(t, cols, 2)

	// Alphabetical column order: id, name.
	assert.Equal(t, "id", cols[0].(map[string]any)["Name"])
	assert.Equal(t, "INTEGER", cols[0].(map[string]any)["SqlType"])
	assert.Equal(t, "name", cols[1].(map[string]any)["Name"])
	assert.Equal(t, "VARCHAR(4)", cols[1].(map[string]any)["SqlType"])
}

func TestDiscoverInputSchema_KinesisNoRecordsFails(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.SetKinesisStreamReader(&fakeKinesisReader{})

	rec := doRequest(t, h, "DiscoverInputSchema", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/empty",
		"RoleARN":     "arn:aws:iam::000000000000:role/role",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := discoverSchemaBody(t, rec)
	assert.Equal(t, "UnableToDetectSchemaException", resp["__type"])
}

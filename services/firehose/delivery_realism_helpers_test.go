package firehose_test

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/snappy"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

type putObject struct {
	bucket   string
	key      string
	encoding string
	body     []byte
}

// s3Recorder is an in-process S3 that records every PutObject.
type s3Recorder struct {
	objs []putObject
	mu   sync.Mutex
}

func (s *s3Recorder) PutObject(_ context.Context, in *s3sdk.PutObjectInput) (*s3sdk.PutObjectOutput, error) {
	body, _ := io.ReadAll(in.Body)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.objs = append(s.objs, putObject{
		bucket: aws.ToString(
			in.Bucket,
		),
		key:      aws.ToString(in.Key),
		body:     body,
		encoding: aws.ToString(in.ContentEncoding),
	})

	return &s3sdk.PutObjectOutput{}, nil
}

func (s *s3Recorder) all() []putObject {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]putObject{}, s.objs...)
}

func (s *s3Recorder) withPrefix(prefix string) []putObject {
	var out []putObject

	for _, o := range s.all() {
		if strings.HasPrefix(o.key, prefix) {
			out = append(out, o)
		}
	}

	return out
}

// fakeLambda is an in-process Lambda invoker driven by respond.
type fakeLambda struct {
	respond  func(payload []byte, call int) ([]byte, error)
	payloads [][]byte
	mu       sync.Mutex
}

func (f *fakeLambda) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	f.mu.Lock()
	f.payloads = append(f.payloads, payload)
	call := len(f.payloads)
	f.mu.Unlock()

	out, err := f.respond(payload, call)

	return out, 200, err
}

func (f *fakeLambda) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.payloads)
}

type transformEvent struct {
	InvocationID      string `json:"invocationId"`
	DeliveryStreamARN string `json:"deliveryStreamArn"`
	Region            string `json:"region"`
	Records           []struct {
		RecordID                    string `json:"recordId"`
		Data                        string `json:"data"`
		ApproximateArrivalTimestamp int64  `json:"approximateArrivalTimestamp"`
	} `json:"records"`
}

// contentTransform drops "drop*", fails "fail*", upper-cases the rest and emits
// metadata.partitionKeys{"tenant": <text before the first dash>}.
func contentTransform(payload []byte, _ int) ([]byte, error) {
	var ev transformEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		return nil, err
	}

	recs := make([]map[string]any, 0, len(ev.Records))

	for _, r := range ev.Records {
		raw, _ := base64.StdEncoding.DecodeString(r.Data)
		text := string(raw)

		switch {
		case strings.HasPrefix(text, "drop"):
			recs = append(recs, map[string]any{"recordId": r.RecordID, "result": "Dropped", "data": r.Data})
		case strings.HasPrefix(text, "fail"):
			recs = append(recs, map[string]any{"recordId": r.RecordID, "result": "ProcessingFailed", "data": r.Data})
		default:
			tenant, _, _ := strings.Cut(text, "-")
			recs = append(recs, map[string]any{
				"recordId": r.RecordID, "result": "Ok",
				"data":     base64.StdEncoding.EncodeToString([]byte(strings.ToUpper(text))),
				"metadata": map[string]any{"partitionKeys": map[string]string{"tenant": tenant}},
			})
		}
	}

	return json.Marshal(map[string]any{"records": recs})
}

// denyAuthorizer denies the listed actions; role "untrusted" cannot be assumed.
type denyAuthorizer struct{ deny map[string]bool }

func (d denyAuthorizer) AuthorizeRole(principal, role, action, _ string) error {
	if principal != roleauth.PrincipalFirehose {
		return roleauth.ErrNotAssumable
	}

	if strings.HasSuffix(role, "/untrusted") {
		return roleauth.ErrNotAssumable
	}

	if d.deny[action] {
		return roleauth.ErrAccessDenied
	}

	return nil
}

type indexedDoc struct {
	doc    map[string]any
	domain string
	index  string
}

type fakeIndexer struct {
	docs []indexedDoc
	mu   sync.Mutex
}

func (f *fakeIndexer) IndexDocument(domain, index string, doc map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.docs = append(f.docs, indexedDoc{domain: domain, index: index, doc: doc})

	return nil
}

func (f *fakeIndexer) all() []indexedDoc {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]indexedDoc{}, f.docs...)
}

const (
	testRoleARN   = "arn:aws:iam::000000000000:role/firehose"
	testLambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:transform"
)

type realismEnv struct {
	backend *firehose.InMemoryBackend
	client  *firehosesdk.Client
	s3      *s3Recorder
}

func newRealismEnv(t *testing.T) *realismEnv {
	t.Helper()

	backend := firehose.NewInMemoryBackend("000000000000", rtTestRegion)
	t.Cleanup(backend.Reset)

	env := &realismEnv{backend: backend, s3: &s3Recorder{}}
	backend.SetS3Backend(env.s3)
	env.client = newRoundTripClient(t, firehose.NewHandler(backend))

	return env
}

func (e *realismEnv) putAndFlush(t *testing.T, stream string, records ...string) {
	t.Helper()

	entries := make([]types.Record, 0, len(records))
	for _, r := range records {
		entries = append(entries, types.Record{Data: []byte(r)})
	}

	out, err := e.client.PutRecordBatch(t.Context(), &firehosesdk.PutRecordBatchInput{
		DeliveryStreamName: aws.String(stream), Records: entries,
	})
	require.NoError(t, err)
	require.Zero(t, aws.ToInt32(out.FailedPutCount))

	e.backend.FlushAll(t.Context())
}

func lambdaProcessing() *types.ProcessingConfiguration {
	return &types.ProcessingConfiguration{
		Enabled: aws.Bool(true),
		Processors: []types.Processor{{
			Type: types.ProcessorTypeLambda,
			Parameters: []types.ProcessorParameter{
				{ParameterName: types.ProcessorParameterNameLambdaArn, ParameterValue: aws.String(testLambdaARN)},
			},
		}},
	}
}

func decodeObject(t *testing.T, format string, body []byte) string {
	t.Helper()

	switch format {
	case "GZIP":
		return string(gunzip(t, body))
	case "ZIP":
		zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		require.NoError(t, err)
		require.Len(t, zr.File, 1)

		rc, err := zr.File[0].Open()
		require.NoError(t, err)

		out, err := io.ReadAll(rc)
		require.NoError(t, err)

		return string(out)
	case "Snappy":
		out, err := snappy.Decode(nil, body)
		require.NoError(t, err)

		return string(out)
	case "HADOOP_SNAPPY":
		require.GreaterOrEqual(t, len(body), 8)
		rawLen := binary.BigEndian.Uint32(body[:4])
		compLen := binary.BigEndian.Uint32(body[4:8])
		require.Len(t, body, 8+int(compLen))

		out, err := snappy.Decode(nil, body[8:])
		require.NoError(t, err)
		require.Len(t, out, int(rawLen))

		return string(out)
	default:
		return string(body)
	}
}

func readAll(r *http.Request) []byte {
	b, _ := io.ReadAll(r.Body)

	return b
}

func gunzipReader(t *testing.T, r *http.Request) []byte {
	t.Helper()

	zr, err := gzip.NewReader(r.Body)
	require.NoError(t, err)

	b, err := io.ReadAll(zr)
	require.NoError(t, err)

	return b
}

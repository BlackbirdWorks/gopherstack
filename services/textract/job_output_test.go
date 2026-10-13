package textract_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

var errWrite = errors.New("write failed")

type fakeWriter struct {
	err     error
	objects map[string][]byte
	mu      sync.Mutex
}

func (f *fakeWriter) PutObject(_ context.Context, bucket, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}

	if f.objects == nil {
		f.objects = map[string][]byte{}
	}

	f.objects[bucket+"/"+key] = data

	return nil
}

func TestAsyncJobWritesOutputConfig(t *testing.T) {
	t.Parallel()

	cfg := &textract.OutputConfig{S3Bucket: "out", S3Prefix: "pre/"}

	tests := []struct {
		start   func(b *textract.InMemoryBackend, cfg *textract.OutputConfig) (string, string, error)
		name    string
		wantKey string
	}{
		{
			name: "detection", wantKey: "DetectDocumentTextModelVersion",
			start: func(b *textract.InMemoryBackend, c *textract.OutputConfig) (string, string, error) {
				j, err := b.StartDocumentTextDetectionWithOptions(t.Context(), "s3://in/a.png", c, nil, "", "")
				if err != nil {
					return "", "", err
				}

				return j.JobID, j.JobStatus, nil
			},
		},
		{
			name: "analysis", wantKey: "AnalyzeDocumentModelVersion",
			start: func(b *textract.InMemoryBackend, c *textract.OutputConfig) (string, string, error) {
				j, err := b.StartDocumentAnalysisWithOptions(
					t.Context(), "s3://in/a.png", []string{"TABLES"}, nil, c, "", "")
				if err != nil {
					return "", "", err
				}

				return j.JobID, j.JobStatus, nil
			},
		},
		{
			name: "expense", wantKey: "AnalyzeExpenseModelVersion",
			start: func(b *textract.InMemoryBackend, c *textract.OutputConfig) (string, string, error) {
				j, err := b.StartExpenseAnalysisWithOptions(t.Context(), "s3://in/a.png", c, nil, "", "")
				if err != nil {
					return "", "", err
				}

				return j.JobID, j.JobStatus, nil
			},
		},
		{
			name: "lending", wantKey: "AnalyzeLendingModelVersion",
			start: func(b *textract.InMemoryBackend, c *textract.OutputConfig) (string, string, error) {
				j, err := b.StartLendingAnalysisWithOptions(t.Context(), "s3://in/a.png", c, nil, "", "")
				if err != nil {
					return "", "", err
				}

				return j.JobID, j.JobStatus, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &fakeWriter{}
			b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
			b.SetS3Writer(w)

			id, status, err := tt.start(b, cfg)
			require.NoError(t, err)
			assert.Equal(t, "SUCCEEDED", status)
			assert.Contains(t, w.objects, "out/pre/"+id+"/.s3_access_check")

			var page map[string]any
			require.NoError(t, json.Unmarshal(w.objects["out/pre/"+id+"/1"], &page))
			assert.Contains(t, page, tt.wantKey)
			assert.Contains(t, page, "DocumentMetadata")

			w2 := &fakeWriter{err: errWrite}
			b2 := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
			b2.SetS3Writer(w2)

			_, status, err = tt.start(b2, cfg)
			require.NoError(t, err)
			assert.Equal(t, "FAILED", status)

			w3 := &fakeWriter{}
			b3 := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
			b3.SetS3Writer(w3)

			_, status, err = tt.start(b3, nil)
			require.NoError(t, err)
			assert.Equal(t, "SUCCEEDED", status)
			assert.Empty(t, w3.objects)
		})
	}
}

package comprehend_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

var errWrite = errors.New("write failed")

type fakeS3 struct {
	err     error
	objects map[string][]byte
	mu      sync.Mutex
}

func (f *fakeS3) PutObject(_ context.Context, bucket, key string, data []byte) error {
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

func untar(t *testing.T, data []byte) map[string]string {
	t.Helper()

	gz, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)

	out := map[string]string{}
	tr := tar.NewReader(gz)

	for {
		h, nextErr := tr.Next()
		if errors.Is(nextErr, io.EOF) {
			return out
		}

		require.NoError(t, nextErr)

		body, readErr := io.ReadAll(tr)
		require.NoError(t, readErr)

		out[h.Name] = string(body)
	}
}

func TestAsyncJobWritesOutputArchive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		writerErr error
		name      string
		jobType   string
		code      string
		wantState string
		wantFiles []string
	}{
		{
			name: "entities", jobType: "entities-detection-job", code: "NER",
			wantFiles: []string{"output"}, wantState: "COMPLETED",
		},
		{
			name: "sentiment", jobType: "sentiment-detection-job", code: "SENTIMENT",
			wantFiles: []string{"output"}, wantState: "COMPLETED",
		},
		{
			name: "topics", jobType: "topics-detection-job", code: "TOPICS",
			wantFiles: []string{"doc-topics.csv", "topics-terms.csv"}, wantState: "COMPLETED",
		},
		{
			name: "classification", jobType: "document-classification-job", code: "CLN",
			wantFiles: []string{"predictions.jsonl"}, wantState: "COMPLETED",
		},
		{name: "write error", jobType: "entities-detection-job", wantState: "FAILED", writerErr: errWrite},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &fakeS3{err: tt.writerErr}
			b := comprehend.NewInMemoryBackend("123456789012", "us-east-1")
			b.SetS3Writer(w)

			job, err := b.StartJob(tt.jobType, "job1", map[string]any{
				"InputDataConfig":  map[string]any{"S3Uri": "s3://in/docs"},
				"OutputDataConfig": map[string]any{"S3Uri": "s3://out/results/"},
			}, nil)
			require.NoError(t, err)

			var got *comprehend.Job

			for range 3 {
				got, err = b.DescribeJob(job.JobID, tt.jobType)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantState, got.JobStatus)

			if tt.wantState == "FAILED" {
				assert.Empty(t, w.objects)

				return
			}

			key := "out/results/123456789012-" + tt.code + "-" + job.JobID + "/output/output.tar.gz"
			require.Contains(t, w.objects, key)

			files := untar(t, w.objects[key])
			for _, f := range tt.wantFiles {
				assert.NotEmpty(t, files[f], f)
			}
		})
	}
}

func TestAsyncJobNoOutputConfig(t *testing.T) {
	t.Parallel()

	w := &fakeS3{}
	b := comprehend.NewInMemoryBackend("123456789012", "us-east-1")
	b.SetS3Writer(w)

	job, err := b.StartJob("entities-detection-job", "job2", map[string]any{}, nil)
	require.NoError(t, err)

	for range 3 {
		_, err = b.DescribeJob(job.JobID, "entities-detection-job")
		require.NoError(t, err)
	}

	assert.Empty(t, w.objects)
}

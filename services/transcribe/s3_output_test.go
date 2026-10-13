package transcribe_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
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

func TestTranscriptionJobWritesOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucket     string
		outputKey  string
		wantKey    string
		writerErr  error
		wantStatus string
	}{
		{name: "default key", bucket: "out", wantKey: "out/j.json", wantStatus: "COMPLETED"},
		{name: "folder", bucket: "out", outputKey: "a/b/", wantKey: "out/a/b/j.json", wantStatus: "COMPLETED"},
		{name: "file", bucket: "out", outputKey: "x/t.json", wantKey: "out/x/t.json", wantStatus: "COMPLETED"},
		{name: "managed", wantStatus: "COMPLETED"},
		{name: "write error", bucket: "out", writerErr: errWrite, wantStatus: "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &fakeS3{err: tt.writerErr}
			b := transcribe.NewInMemoryBackend()
			b.SetS3Writer(w)

			job, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
				JobName: "j", LanguageCode: "en-US", Media: transcribe.Media{MediaFileURI: "s3://in/a.wav"},
				OutputBucketName: tt.bucket, OutputKey: tt.outputKey,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, job.JobStatus)

			if tt.wantKey == "" {
				assert.Empty(t, w.objects)

				return
			}

			var doc struct {
				JobName string `json:"jobName"`
				Status  string `json:"status"`
				Results struct {
					Transcripts []struct {
						Transcript string `json:"transcript"`
					} `json:"transcripts"`
					Items []json.RawMessage `json:"items"`
				} `json:"results"`
			}
			require.NoError(t, json.Unmarshal(w.objects[tt.wantKey], &doc))
			assert.Equal(t, "j", doc.JobName)
			assert.Equal(t, "COMPLETED", doc.Status)
			assert.NotEmpty(t, doc.Results.Transcripts)
			assert.NotEmpty(t, doc.Results.Items)
		})
	}
}

func TestMedicalTranscriptionJobWritesOutput(t *testing.T) {
	t.Parallel()

	w := &fakeS3{}
	b := transcribe.NewInMemoryBackend()
	b.SetS3Writer(w)

	_, err := b.StartMedicalTranscriptionJob(&transcribe.MedicalTranscriptionJob{
		MedicalTranscriptionJobName: "m", LanguageCode: "en-US", Specialty: "PRIMARYCARE", Type: "CONVERSATION",
		Media: transcribe.Media{MediaFileURI: "s3://in/a.wav"}, OutputBucketName: "out", OutputKey: "med/",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, w.objects["out/med/m.json"])
}

func TestCallAnalyticsJobWritesOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		location string
		wantKey  string
	}{
		{name: "bucket only", location: "s3://out", wantKey: "out/c.json"},
		{name: "folder", location: "s3://out/ca/", wantKey: "out/ca/c.json"},
		{name: "file", location: "s3://out/ca/x.json", wantKey: "out/ca/x.json"},
		{name: "managed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &fakeS3{}
			b := transcribe.NewInMemoryBackend()
			b.SetS3Writer(w)

			_, err := b.StartCallAnalyticsJob(&transcribe.CallAnalyticsJob{
				CallAnalyticsJobName: "c", LanguageCode: "en-US",
				Media: transcribe.Media{MediaFileURI: "s3://in/a.wav"}, OutputLocation: tt.location,
			})
			require.NoError(t, err)

			if tt.wantKey == "" {
				assert.Empty(t, w.objects)

				return
			}

			var doc map[string]any
			require.NoError(t, json.Unmarshal(w.objects[tt.wantKey], &doc))
			assert.Equal(t, "COMPLETED", doc["JobStatus"])
			assert.NotEmpty(t, doc["Transcript"])
			assert.NotEmpty(t, doc["Participants"])
		})
	}
}

func TestDeferredTranscriptionJobWritesOnCompletion(t *testing.T) {
	t.Parallel()

	w := &fakeS3{}
	b := transcribe.NewInMemoryBackend()
	b.SetS3Writer(w)

	_, err := b.StartTranscriptionJob(&transcribe.TranscriptionJob{
		JobName: "d", LanguageCode: "en-US", Media: transcribe.Media{MediaFileURI: "s3://in/a.wav"},
		OutputBucketName: "out",
		JobExecutionSettings: &transcribe.JobExecutionSettings{
			AllowDeferredExecution: true, DataAccessRoleArn: "arn:aws:iam::123456789012:role/t",
		},
	})
	require.NoError(t, err)
	assert.Empty(t, w.objects)

	for range 2 {
		_, err = b.GetTranscriptionJob("d")
		require.NoError(t, err)
	}

	assert.NotEmpty(t, w.objects["out/d.json"])
}

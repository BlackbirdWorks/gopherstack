package codepipeline_test

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

type versionedS3 struct {
	body    []byte
	version int
	mu      sync.Mutex
}

func (v *versionedS3) PutObject(context.Context, *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func (v *versionedS3) GetObject(_ context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if aws.ToString(in.Key) == "art" || v.body == nil {
		return nil, awserr.ErrNotFound
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(v.body)), ETag: aws.String(`"e` + strconv.Itoa(v.version) + `"`),
	}, nil
}

func (v *versionedS3) bump() {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.version++
}

func TestPollSourceChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		poll      string
		wantExecs int
	}{
		{name: "default polls", poll: "", wantExecs: 1},
		{name: "explicit true", poll: "true", wantExecs: 1},
		{name: "disabled", poll: "false", wantExecs: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			s3fake := &versionedS3{body: []byte("zip"), version: 1}
			h.Backend.SetArtifactS3(s3fake)

			cfg := map[string]string{"S3Bucket": "src", "S3ObjectKey": "app.zip"}
			if tt.poll != "" {
				cfg["PollForSourceChanges"] = tt.poll
			}

			decl := builtinPipeline("p", codepipeline.Action{
				Name: "Source",
				ActionTypeID: codepipeline.ActionTypeID{
					Category: "Source", Owner: "AWS", Provider: "S3", Version: "1",
				},
				Configuration:   cfg,
				OutputArtifacts: []codepipeline.ArtifactRef{{Name: "out"}},
			})
			_, err := h.Backend.CreatePipeline(t.Context(), decl, nil)
			require.NoError(t, err)

			h.Backend.PollSourceChanges(t.Context())
			s3fake.bump()
			h.Backend.PollSourceChanges(t.Context())
			h.Backend.PollSourceChanges(t.Context())

			execs, err := h.Backend.ListPipelineExecutions(t.Context(), "p")
			require.NoError(t, err)
			require.Len(t, execs, tt.wantExecs)

			if tt.wantExecs > 0 {
				assert.Equal(t, "PollForSourceChanges", execs[0].Trigger)
			}
		})
	}
}

type blockingS3 struct{}

func (*blockingS3) PutObject(context.Context, *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func (*blockingS3) GetObject(ctx context.Context, _ *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	<-ctx.Done()

	return nil, ctx.Err()
}

func sourcePipeline(t *testing.T, h *codepipeline.Handler) {
	t.Helper()

	decl := builtinPipeline("polled", codepipeline.Action{
		Name: "Source",
		ActionTypeID: codepipeline.ActionTypeID{
			Category: "Source", Owner: "AWS", Provider: "S3", Version: "1",
		},
		Configuration:   map[string]string{"S3Bucket": "src", "S3ObjectKey": "app.zip"},
		OutputArtifacts: []codepipeline.ArtifactRef{{Name: "out"}},
	})
	_, err := h.Backend.CreatePipeline(t.Context(), decl, nil)
	require.NoError(t, err)
}

func TestPollSourceChangesHonorsContext(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newTestHandler(t)
		h.Backend.SetArtifactS3(&blockingS3{})
		sourcePipeline(t, h)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})

		go func() {
			defer close(done)

			h.Backend.PollSourceChanges(ctx)
		}()

		synctest.Wait()
		cancel()
		<-done
	})
}

func TestSourcePollingWorkerLifecycle(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newTestHandler(t)
		s3fake := &versionedS3{body: []byte("zip"), version: 1}
		h.Backend.SetArtifactS3(s3fake)
		sourcePipeline(t, h)

		ctx, cancel := context.WithCancel(t.Context())
		require.NoError(t, h.StartWorker(ctx))

		<-time.After(time.Minute)
		synctest.Wait()
		s3fake.bump()
		<-time.After(time.Minute)
		synctest.Wait()

		execs, err := h.Backend.ListPipelineExecutions(t.Context(), "polled")
		require.NoError(t, err)
		assert.Len(t, execs, 1)

		cancel()
	})
}

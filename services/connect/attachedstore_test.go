package connect_test

import (
	"context"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type presignCall struct {
	bound  map[string]string
	method string
	bucket string
	key    string
	ttl    time.Duration
}

// fakeAttachmentStore is a mutex-guarded in-memory AttachmentStore.
type fakeAttachmentStore struct {
	fakeResolvers
	objects map[string][]byte
	buckets map[string]bool
	calls   []presignCall
	mu      sync.Mutex
}

func newFakeAttachmentStore() *fakeAttachmentStore {
	return &fakeAttachmentStore{objects: map[string][]byte{}, buckets: map[string]bool{}}
}

func (s *fakeAttachmentStore) EnsureBucket(_ context.Context, bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buckets[bucket] = true

	return nil
}

func (s *fakeAttachmentStore) ObjectSize(_ context.Context, bucket, key string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.objects[bucket+"/"+key]

	return int64(len(d)), ok, nil
}

func (s *fakeAttachmentStore) PutObject(_ context.Context, bucket, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects[bucket+"/"+key] = append([]byte(nil), data...)

	return nil
}

func (s *fakeAttachmentStore) DeleteObject(_ context.Context, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.objects, bucket+"/"+key)

	return nil
}

func (s *fakeAttachmentStore) Presign(
	_ context.Context, method, baseURL, bucket, key string, ttl time.Duration, _ time.Time, bound map[string]string,
) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls = append(s.calls, presignCall{method: method, bucket: bucket, key: key, ttl: ttl, bound: bound})

	return baseURL + "/" + bucket + "/" + url.PathEscape(key) + "?m=" + method
}

func (s *fakeAttachmentStore) last() presignCall {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls[len(s.calls)-1]
}

func (s *fakeAttachmentStore) has(bucket, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.objects[bucket+"/"+key]

	return ok
}

const attachedText = "hello attachment"

const wantAttachBucket = "connect-attached-files-" + testAccount + "-us-east-1"

func attachedStartURLCase(t *testing.T, e *contactEnv, s *fakeAttachmentStore, resource string) {
	t.Helper()

	out, err := e.f.client.StartAttachedFileUpload(t.Context(), &sdk.StartAttachedFileUploadInput{
		InstanceId: aws.String(e.inst), AssociatedResourceArn: aws.String(resource),
		FileName: aws.String("a.txt"), FileSizeInBytes: aws.Int64(int64(len(attachedText))),
		FileUseCaseType: types.FileUseCaseTypeAttachment, UrlExpiryInSeconds: aws.Int32(60),
	})
	require.NoError(t, err)

	c := s.last()
	assert.Equal(t, "PUT", c.method)
	assert.Equal(t, wantAttachBucket, c.bucket)
	assert.Equal(t, "attached-files/"+e.inst+"/"+aws.ToString(out.FileId), c.key)
	assert.Equal(t, time.Minute, c.ttl)
	assert.Equal(t, map[string]string{"Content-Length": strconv.Itoa(len(attachedText))}, c.bound)
	assert.Equal(t, c.bound, out.UploadUrlMetadata.HeadersToInclude)
	assert.Contains(t, aws.ToString(out.UploadUrlMetadata.Url), "m=PUT")
	assert.True(t, s.buckets[wantAttachBucket])
}

func attachedCompleteApprovesCase(t *testing.T, e *contactEnv, s *fakeAttachmentStore, resource string) {
	t.Helper()

	start := e.startUpload(t, resource, "a.txt", int64(len(attachedText)))
	ref := &sdk.CompleteAttachedFileUploadInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	}

	_, err := e.f.client.CompleteAttachedFileUpload(t.Context(), ref)
	assert.Equal(t, "InvalidRequestException", apiCode(err), "nothing uploaded yet")

	c := s.last()
	require.NoError(t, s.PutObject(t.Context(), c.bucket, c.key, []byte(attachedText)))

	_, err = e.f.client.CompleteAttachedFileUpload(t.Context(), ref)
	require.NoError(t, err)

	got, err := e.f.client.GetAttachedFile(t.Context(), &sdk.GetAttachedFileInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	require.NoError(t, err)
	assert.Equal(t, types.FileStatusTypeApproved, got.FileStatus)
	require.NotNil(t, got.DownloadUrlMetadata)

	g := s.last()
	assert.Equal(t, "GET", g.method)
	assert.Equal(t, c.key, g.key)
	assert.Empty(t, g.bound)
	assert.Equal(t, 5*time.Minute, g.ttl)
	assert.Contains(t, aws.ToString(got.DownloadUrlMetadata.Url), "m=GET")
}

func attachedProcessingNoURLCase(t *testing.T, e *contactEnv, _ *fakeAttachmentStore, resource string) {
	t.Helper()

	start := e.startUpload(t, resource, "a.txt", 3)
	got, err := e.f.client.GetAttachedFile(t.Context(), &sdk.GetAttachedFileInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	require.NoError(t, err)
	assert.Nil(t, got.DownloadUrlMetadata)
}

func attachedWrongSizeCase(t *testing.T, e *contactEnv, s *fakeAttachmentStore, resource string) {
	t.Helper()

	start := e.startUpload(t, resource, "a.txt", int64(len(attachedText)))
	c := s.last()
	require.NoError(t, s.PutObject(t.Context(), c.bucket, c.key, []byte("short")))

	_, err := e.f.client.CompleteAttachedFileUpload(t.Context(), &sdk.CompleteAttachedFileUploadInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))
	assert.False(t, s.has(c.bucket, c.key))

	got, err := e.f.client.GetAttachedFile(t.Context(), &sdk.GetAttachedFileInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	require.NoError(t, err)
	assert.Equal(t, types.FileStatusTypeFailed, got.FileStatus)
	assert.Nil(t, got.DownloadUrlMetadata)
}

func attachedDeleteDropsCase(t *testing.T, e *contactEnv, s *fakeAttachmentStore, resource string) {
	t.Helper()

	start := e.startUpload(t, resource, "a.txt", int64(len(attachedText)))
	c := s.last()
	require.NoError(t, s.PutObject(t.Context(), c.bucket, c.key, []byte(attachedText)))

	_, err := e.f.client.DeleteAttachedFile(t.Context(), &sdk.DeleteAttachedFileInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	require.NoError(t, err)
	assert.False(t, s.has(c.bucket, c.key))
}

func attachedRestoreCase(t *testing.T, e *contactEnv, s *fakeAttachmentStore, resource string) {
	t.Helper()

	start := e.startUpload(t, resource, "a.txt", int64(len(attachedText)))
	c := s.last()
	require.NoError(t, s.PutObject(t.Context(), c.bucket, c.key, []byte(attachedText)))

	_, err := e.f.client.CompleteAttachedFileUpload(t.Context(), &sdk.CompleteAttachedFileUploadInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	require.NoError(t, err)

	r := e.f.restored(t)
	r.backend.SetResolvers(s)

	_, err = r.client.GetAttachedFile(t.Context(), &sdk.GetAttachedFileInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	require.NoError(t, err)
	assert.Equal(t, c.key, s.last().key)
	assert.Equal(t, "GET", s.last().method)
}

func TestAttachedFileS3Backed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e *contactEnv, s *fakeAttachmentStore, resource string)
		name string
	}{
		{name: "upload url is a put bound to the object, size and expiry", run: attachedStartURLCase},
		{name: "complete approves an exact-size upload and get presigns a get", run: attachedCompleteApprovesCase},
		{name: "get of a processing file has no download url", run: attachedProcessingNoURLCase},
		{name: "a wrong-size upload fails the file and drops the object", run: attachedWrongSizeCase},
		{name: "delete removes the object", run: attachedDeleteDropsCase},
		{name: "state survives a restore", run: attachedRestoreCase},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			s := newFakeAttachmentStore()
			e.f.backend.SetResolvers(s)
			tt.run(t, e, s, e.contactARN(t, e.task(t)))
		})
	}
}

func TestCreateAttachedFileStoresInS3(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	voice := e.outbound(t, "+442071230001", true)
	arn := e.contactARN(t, voice)
	e.stop(t, voice)

	s := newFakeAttachmentStore()
	s.fakeResolvers = fakeResolvers{
		objects: map[string]bool{"rec/call.wav": true},
		data:    map[string][]byte{"rec/call.wav": []byte("RIFF")},
	}
	e.f.backend.SetResolvers(s)

	out, err := e.f.client.CreateAttachedFile(t.Context(), &sdk.CreateAttachedFileInput{
		InstanceId: aws.String(e.inst), AssociatedResourceArn: aws.String(arn),
		FileSourceUri: aws.String("s3://rec/call.wav"), FileUseCaseType: types.FileUseCaseTypeVoiceRecording,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("RIFF"), s.objects[wantAttachBucket+"/attached-files/"+e.inst+"/"+aws.ToString(out.FileId)])
}

func TestAttachedFileCompletionIsSerialised(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	s := newFakeAttachmentStore()
	e.f.backend.SetResolvers(s)

	resource := e.contactARN(t, e.task(t))
	start := e.startUpload(t, resource, "a.txt", 3)
	c := s.last()
	require.NoError(t, s.PutObject(t.Context(), c.bucket, c.key, []byte("abc")))

	const workers = 8

	var (
		wg sync.WaitGroup
		ok int
		mu sync.Mutex
	)

	for range workers {
		wg.Go(func() {
			_, err := e.f.client.CompleteAttachedFileUpload(t.Context(), &sdk.CompleteAttachedFileUploadInput{
				InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
			})
			if err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		})
	}

	wg.Wait()
	assert.Equal(t, 1, ok, "exactly one completion wins; the rest see the file already approved")
}

func TestDeleteInstanceDropsAttachmentObjects(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	s := newFakeAttachmentStore()
	e.f.backend.SetResolvers(s)

	resource := e.contactARN(t, e.task(t))
	e.startUpload(t, resource, "a.txt", int64(len(attachedText)))
	c := s.last()
	require.NoError(t, s.PutObject(t.Context(), c.bucket, c.key, []byte(attachedText)))
	require.True(t, s.has(c.bucket, c.key))

	_, err := e.f.client.DeleteInstance(t.Context(), &sdk.DeleteInstanceInput{InstanceId: aws.String(e.inst)})
	require.NoError(t, err)
	assert.False(t, s.has(c.bucket, c.key), "the attachment object leaves S3 with its instance")
}

func TestAttachedUploadWindowExpires(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		useStore bool
	}{
		{name: "emulator content endpoint"},
		{name: "s3 backed", useStore: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			s := newFakeAttachmentStore()

			if tt.useStore {
				e.f.backend.SetResolvers(s)
			}

			resource := e.contactARN(t, e.task(t))
			start := e.startUpload(t, resource, "a.txt", int64(len(attachedText)))

			if tt.useStore {
				c := s.last()
				require.NoError(t, s.PutObject(t.Context(), c.bucket, c.key, []byte(attachedText)))
			}

			e.f.clock.advance(2 * time.Hour)

			ref := &sdk.GetAttachedFileInput{
				InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
			}
			got, err := e.f.client.GetAttachedFile(t.Context(), ref)
			require.NoError(t, err)
			assert.Equal(t, types.FileStatusTypeFailed, got.FileStatus, "a stale PROCESSING upload reads as failed")
			assert.Nil(t, got.DownloadUrlMetadata)

			_, err = e.f.client.CompleteAttachedFileUpload(t.Context(), &sdk.CompleteAttachedFileUploadInput{
				InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
			})
			assert.Equal(t, "InvalidRequestException", apiCode(err))
		})
	}
}

func TestStartAttachedFileUploadCreatesNoBucketOnFailure(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	s := newFakeAttachmentStore()
	e.f.backend.SetResolvers(s)

	_, err := e.f.client.StartAttachedFileUpload(t.Context(), &sdk.StartAttachedFileUploadInput{
		InstanceId: aws.String("00000000-0000-0000-0000-000000000000"), AssociatedResourceArn: aws.String("x"),
		FileName: aws.String("a.txt"), FileSizeInBytes: aws.Int64(1), FileUseCaseType: types.FileUseCaseTypeAttachment,
	})
	require.Error(t, err)

	_, err = e.f.client.StartAttachedFileUpload(t.Context(), &sdk.StartAttachedFileUploadInput{
		InstanceId: aws.String(e.inst), AssociatedResourceArn: aws.String("arn:aws:connect:us-east-1:" + testAccount +
			":instance/" + e.inst + "/contact/missing"),
		FileName: aws.String("a.txt"), FileSizeInBytes: aws.Int64(1), FileUseCaseType: types.FileUseCaseTypeAttachment,
	})
	require.Error(t, err)

	s.mu.Lock()
	defer s.mu.Unlock()

	assert.Empty(t, s.buckets, "a rejected request must not create the attachment bucket")
}

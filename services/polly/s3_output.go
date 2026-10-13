package polly

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// S3Writer stores a finished task's output object.
type S3Writer interface {
	PutObject(ctx context.Context, bucket, key string, data []byte) error
}

type siblingServices interface {
	GetS3Handler() service.Registerable
}

type backendS3Writer struct{ backend s3backend.StorageBackend }

func (w backendS3Writer) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	_, err := w.backend.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})

	return err
}

// SetS3Writer overrides the S3 destination for synthesis task output.
func (b *InMemoryBackend) SetS3Writer(w S3Writer) {
	b.mu.Lock("SetS3Writer")
	defer b.mu.Unlock()

	b.s3Writer = w
}

// SetAppConfig records the service.AppContext.Config for lazy S3 lookup.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.mu.Lock("SetAppConfig")
	defer b.mu.Unlock()

	b.appConfig = cfg
}

func (b *InMemoryBackend) appConfigValue() any {
	b.mu.RLock("appConfigValue")
	defer b.mu.RUnlock()

	return b.appConfig
}

// resolveS3Writer returns the explicit writer or the emulated S3; the caller holds b.mu.
func (b *InMemoryBackend) resolveS3Writer() (S3Writer, bool) {
	if b.s3Writer != nil {
		return b.s3Writer, true
	}

	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return backendS3Writer{backend: h.Backend}, true
}

// deliverTaskOutputLocked writes the finished task's audio to its OutputUri; the caller holds b.mu.
func (b *InMemoryBackend) deliverTaskOutputLocked(task *SpeechSynthesisTask) {
	w, ok := b.resolveS3Writer()
	if !ok {
		return
	}

	var data []byte
	if task.Options.OutputFormat == outputFormatJSON {
		data = speechMarks(task.Options)
	} else {
		data = syntheticAudioBytes(task.Options)
	}

	key := task.OutputS3KeyPrefix + task.TaskID + "." + taskExtension(task.Options.OutputFormat)
	if err := w.PutObject(context.Background(), task.OutputS3BucketName, key, data); err != nil {
		task.TaskStatus = taskStatusFailed
		task.TaskStatusReason = "Unable to write output to S3: " + err.Error()
	}
}

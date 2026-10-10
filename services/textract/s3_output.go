package textract

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

type documentMetadata struct {
	Pages int `json:"Pages"`
}

const keyDocumentMetadata = "DocumentMetadata"

const outputWriteFailure = "Unable to write job output to the specified S3 location."

// S3Writer stores a finished job's output objects.
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

// SetS3Writer overrides the S3 destination for OutputConfig job output.
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

// writeJobOutputLocked writes <S3Prefix>/<jobID>/{.s3_access_check,1}; false means a write failed.
func (b *InMemoryBackend) writeJobOutputLocked(cfg *OutputConfig, jobID string, page any) bool {
	if cfg == nil || cfg.S3Bucket == "" {
		return true
	}

	w, ok := b.resolveS3Writer()
	if !ok {
		return true
	}

	body, err := json.Marshal(page)
	if err != nil {
		return false
	}

	base := path.Join(strings.Trim(cfg.S3Prefix, "/"), jobID)
	ctx := context.Background()

	return w.PutObject(ctx, cfg.S3Bucket, base+"/.s3_access_check", nil) == nil &&
		w.PutObject(ctx, cfg.S3Bucket, base+"/1", body) == nil
}

func (b *InMemoryBackend) deliverDocumentJobLocked(j *DocumentJob) {
	version := "AnalyzeDocumentModelVersion"
	if j.JobType == jobTypeTextDetection {
		version = "DetectDocumentTextModelVersion"
	}

	page := map[string]any{
		keyDocumentMetadata: documentMetadata{Pages: 1},
		"Blocks":            j.Blocks,
		version:             modelVersion10,
	}
	if !b.writeJobOutputLocked(j.OutputConfig, j.JobID, page) {
		j.JobStatus = jobStatusFailed
		j.StatusMessage = outputWriteFailure
	}
}

func (b *InMemoryBackend) deliverExpenseJobLocked(j *ExpenseJob) {
	page := map[string]any{
		keyDocumentMetadata:          documentMetadata{Pages: 1},
		"ExpenseDocuments":           j.ExpenseDocuments,
		"AnalyzeExpenseModelVersion": modelVersion10,
	}
	if !b.writeJobOutputLocked(j.OutputConfig, j.JobID, page) {
		j.JobStatus = jobStatusFailed
		j.StatusMessage = outputWriteFailure
	}
}

func (b *InMemoryBackend) deliverLendingJobLocked(j *LendingJob) {
	page := map[string]any{
		keyDocumentMetadata:          documentMetadata{Pages: 1},
		"Results":                    j.Results,
		"AnalyzeLendingModelVersion": modelVersion10,
	}
	if !b.writeJobOutputLocked(j.OutputConfig, j.JobID, page) {
		j.JobStatus = jobStatusFailed
		j.StatusMessage = outputWriteFailure
	}
}

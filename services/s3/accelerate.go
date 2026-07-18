package s3

import (
	"context"
	"encoding/xml"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// accelerateStatusSuspended is the AWS default accelerate status when unset.
const accelerateStatusSuspended = "Suspended"

// PutBucketAccelerateConfiguration stores the accelerate status ("Enabled"/"Suspended") for a bucket.
func (b *InMemoryBackend) PutBucketAccelerateConfiguration(
	_ context.Context,
	bucketName, status string,
) error {
	b.mu.RLock("PutBucketAccelerateConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketAccelerateConfiguration")
	defer bucket.mu.Unlock()

	bucket.AccelerateStatus = status

	return nil
}

// GetBucketAccelerateConfiguration returns the bucket's accelerate status.
// When unset the AWS default of "Suspended" is returned.
func (b *InMemoryBackend) GetBucketAccelerateConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketAccelerateConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketAccelerateConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.AccelerateStatus == "" {
		return accelerateStatusSuspended, nil
	}

	return bucket.AccelerateStatus, nil
}

// s3AccelerateConfiguration is the XML response for Get/PutBucketAccelerateConfiguration.
type s3AccelerateConfiguration struct {
	XMLName xml.Name `xml:"AccelerateConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
	Status  string   `xml:"Status,omitempty"`
}

// handlePutBucketAccelerate handles PUT /{bucket}?accelerate.
// Reads the AccelerateConfiguration XML body to extract Status (Enabled/Suspended).
func (h *S3Handler) handlePutBucketAccelerate(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "PutBucketAccelerateConfiguration")

	bucket, _, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" {
		WriteError(ctx, w, r, ErrNoSuchBucket)

		return
	}

	var cfg s3AccelerateConfiguration

	body, _ := httputils.ReadBody(r)
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &cfg)
	}

	if cfg.Status != statusEnabled && cfg.Status != "Suspended" {
		cfg.Status = "Suspended"
	}

	if err := h.Backend.PutBucketAccelerateConfiguration(ctx, bucket, cfg.Status); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

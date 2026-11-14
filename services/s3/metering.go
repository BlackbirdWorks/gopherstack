package s3

import (
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/labstack/echo/v5"
)

// StorageHolding is the live, non-deleted storage of one bucket in one storage class.
type StorageHolding struct {
	Tags         map[string]string
	Bucket       string
	Region       string
	StorageClass string
	Bytes        int64
}

// RequestEvent describes one served request against a bucket, for usage metering.
type RequestEvent struct {
	Tags      map[string]string
	Bucket    string
	Region    string
	Method    string
	Operation string
	Status    int
	HasKey    bool
}

// RequestObserver receives one RequestEvent per request that names a bucket.
type RequestObserver func(RequestEvent)

type requestObserverHolder struct{ fn RequestObserver }

type bucketMetaSource interface {
	bucketMeta(name string) (string, map[string]string, bool)
}

// SetRequestObserver installs fn to observe every bucket request; nil removes it.
func (h *S3Handler) SetRequestObserver(fn RequestObserver) {
	if fn == nil {
		h.observer.Store(nil)

		return
	}

	h.observer.Store(&requestObserverHolder{fn: fn})
}

func (b *InMemoryBackend) bucketMeta(name string) (string, map[string]string, bool) {
	b.mu.RLock("bucketMeta")
	bucket, err := b.getBucket(name)
	b.mu.RUnlock()

	if err != nil {
		return "", nil, false
	}

	bucket.mu.RLock("bucketMeta")
	defer bucket.mu.RUnlock()

	return bucket.Region, bucketTagMap(bucket), true
}

func bucketTagMap(bucket *StoredBucket) map[string]string {
	if len(bucket.Tags) == 0 {
		return nil
	}

	out := make(map[string]string, len(bucket.Tags))
	for _, t := range bucket.Tags {
		out[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}

	return out
}

func (h *S3Handler) observeRequest(c *echo.Context) {
	holder := h.observer.Load()
	if holder == nil {
		return
	}

	m, ok := c.Request().Context().Value(s3Key).(*s3Metrics)
	if !ok || m.bucket == "" {
		return
	}

	src, ok := h.Backend.(bucketMetaSource)
	if !ok {
		return
	}

	region, tags, found := src.bucketMeta(m.bucket)
	if !found {
		return
	}

	key := strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(c.Request().URL.Path, "/"), m.bucket), "/")
	status := http.StatusOK
	if resp, err := echo.UnwrapResponse(c.Response()); err == nil && resp.Status != 0 {
		status = resp.Status
	}

	holder.fn(RequestEvent{
		Bucket: m.bucket, Region: region, Tags: tags, Method: c.Request().Method,
		Operation: m.operation, Status: status, HasKey: key != "",
	})
}

// StorageHoldings returns the live storage of every bucket by storage class.
func (b *InMemoryBackend) StorageHoldings() []StorageHolding {
	b.mu.RLock("StorageHoldings")
	buckets := b.buckets.All()
	b.mu.RUnlock()

	var out []StorageHolding

	for _, bucket := range buckets {
		if bucket.DeletePending {
			continue
		}

		bucket.mu.RLock("StorageHoldings")
		sizes := map[string]int64{}

		for _, obj := range bucket.Objects {
			for _, v := range obj.Versions {
				if !v.Deleted {
					class := v.StorageClass
					if class == "" {
						class = "STANDARD"
					}

					sizes[class] += v.Size
				}
			}
		}

		region, tags := bucket.Region, bucketTagMap(bucket)
		bucket.mu.RUnlock()

		for class, bytes := range sizes {
			out = append(out, StorageHolding{
				Bucket: bucket.Name, Region: region, StorageClass: class, Bytes: bytes, Tags: tags,
			})
		}
	}

	return out
}

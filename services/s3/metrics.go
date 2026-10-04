package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

// AWS/S3 metrics per docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html: daily
// storage metrics (BucketName, StorageType) and request metrics (BucketName, FilterId).
const (
	s3MetricNamespace = "AWS/S3"
	s3UnitCount       = "Count"
	s3UnitBytes       = "Bytes"
	s3UnitMillis      = "Milliseconds"
	storageMetricsTTL = 24 * time.Hour
	dimBucketName     = "BucketName"
)

// MetricsFilter is one bucket metrics configuration: its FilterId and optional key prefix.
type MetricsFilter struct {
	ID     string
	Prefix string
}

type metricsConfigXML struct {
	ID     string `xml:"Id"`
	Filter struct {
		Prefix string `xml:"Prefix"`
		And    struct {
			Prefix string `xml:"Prefix"`
		} `xml:"And"`
	} `xml:"Filter"`
}

// SetMetricEmitter sets the emitter used for the daily storage metrics.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

// SetMetricEmitter sets the emitter used for request metrics.
func (h *S3Handler) SetMetricEmitter(e cwmetric.Emitter) { h.metrics.Set(e) }

// RequestMetricFilters returns the bucket's region and its metrics configurations (none when absent).
func (b *InMemoryBackend) RequestMetricFilters(name string) (string, []MetricsFilter) {
	b.mu.RLock("RequestMetricFilters")
	bucket, err := b.getBucket(name)
	b.mu.RUnlock()

	if err != nil {
		return "", nil
	}

	bucket.mu.RLock("RequestMetricFilters")
	defer bucket.mu.RUnlock()

	if len(bucket.MetricsConfigs) == 0 {
		return bucket.Region, nil
	}

	filters := make([]MetricsFilter, 0, len(bucket.MetricsConfigs))

	for id, raw := range bucket.MetricsConfigs {
		var cfg metricsConfigXML
		if xml.Unmarshal([]byte(raw), &cfg) != nil {
			continue
		}

		prefix := cfg.Filter.Prefix
		if prefix == "" {
			prefix = cfg.Filter.And.Prefix
		}

		filters = append(filters, MetricsFilter{ID: id, Prefix: prefix})
	}

	return bucket.Region, filters
}

type requestMetricsSource interface {
	RequestMetricFilters(name string) (string, []MetricsFilter)
}

// emitRequestMetrics publishes request metrics for each metrics configuration matching the request.
func (h *S3Handler) emitRequestMetrics(c *echo.Context, start time.Time) {
	m, ok := c.Request().Context().Value(s3Key).(*s3Metrics)
	if !ok || m.bucket == "" {
		return
	}

	src, ok := h.Backend.(requestMetricsSource)
	if !ok {
		return
	}

	region, filters := src.RequestMetricFilters(m.bucket)
	if len(filters) == 0 {
		return
	}

	status, size := http.StatusOK, int64(0)
	if resp, err := echo.UnwrapResponse(c.Response()); err == nil {
		if resp.Status != 0 {
			status = resp.Status
		}

		size = resp.Size
	}

	key := strings.TrimPrefix(c.Request().URL.Path, "/")
	key = strings.TrimPrefix(key, m.bucket)
	key = strings.TrimPrefix(key, "/")

	for _, f := range filters {
		if f.Prefix != "" && !strings.HasPrefix(key, f.Prefix) {
			continue
		}

		h.putRequestMetrics(region, m, f.ID, c.Request(), status, size, time.Since(start))
	}
}

func (h *S3Handler) putRequestMetrics(
	region string, m *s3Metrics, filterID string, r *http.Request, status int, size int64, d time.Duration,
) {
	dims := []cwmetric.Dimension{{Name: dimBucketName, Value: m.bucket}, {Name: "FilterId", Value: filterID}}
	put := func(name, unit string, v float64) {
		h.metrics.Put(region, s3MetricNamespace, name, unit, v, dims...)
	}

	put("AllRequests", s3UnitCount, 1)
	put(requestClassMetric(r.Method, m.operation), s3UnitCount, 1)
	put("4xxErrors", s3UnitCount, boolFloat(status >= http.StatusBadRequest && status < http.StatusInternalServerError))
	put("5xxErrors", s3UnitCount, boolFloat(status >= http.StatusInternalServerError))
	put("TotalRequestLatency", s3UnitMillis, float64(d)/float64(time.Millisecond))

	switch {
	case r.Method == http.MethodGet && size > 0:
		put("BytesDownloaded", s3UnitBytes, float64(size))
	case (r.Method == http.MethodPut || r.Method == http.MethodPost) && r.ContentLength > 0:
		put("BytesUploaded", s3UnitBytes, float64(r.ContentLength))
	}
}

func requestClassMetric(method, operation string) string {
	switch method {
	case http.MethodGet:
		if strings.HasPrefix(operation, "List") {
			return "ListRequests"
		}

		return "GetRequests"
	case http.MethodPut:
		return "PutRequests"
	case http.MethodDelete:
		return "DeleteRequests"
	case http.MethodHead:
		return "HeadRequests"
	default:
		return "PostRequests"
	}
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

// EmitStorageMetrics publishes BucketSizeBytes (per StorageType) and NumberOfObjects for every bucket.
func (b *InMemoryBackend) EmitStorageMetrics() {
	if !b.metrics.Enabled() {
		return
	}

	b.mu.RLock("EmitStorageMetrics")
	buckets := b.buckets.All()
	b.mu.RUnlock()

	for _, bucket := range buckets {
		if !bucket.DeletePending {
			b.emitBucketStorage(bucket)
		}
	}
}

func (b *InMemoryBackend) emitBucketStorage(bucket *StoredBucket) {
	sizes := map[string]float64{"StandardStorage": 0}
	count := 0.0

	bucket.mu.RLock("EmitStorageMetrics")
	for _, obj := range bucket.Objects {
		for _, v := range obj.Versions {
			if !v.Deleted {
				count++
				sizes[storageTypeFor(v.StorageClass)] += float64(v.Size)
			}
		}
	}
	bucket.mu.RUnlock()

	name := cwmetric.Dimension{Name: dimBucketName, Value: bucket.Name}

	for storageType, size := range sizes {
		b.metrics.Put(bucket.Region, s3MetricNamespace, "BucketSizeBytes", s3UnitBytes, size,
			name, cwmetric.Dimension{Name: "StorageType", Value: storageType})
	}

	b.metrics.Put(bucket.Region, s3MetricNamespace, "NumberOfObjects", s3UnitCount, count,
		name, cwmetric.Dimension{Name: "StorageType", Value: "AllStorageTypes"})
}

func storageTypeFor(class string) string {
	switch class {
	case "STANDARD_IA":
		return "StandardIAStorage"
	case "ONEZONE_IA":
		return "OneZoneIAStorage"
	case "GLACIER":
		return "GlacierStorage"
	case "DEEP_ARCHIVE":
		return "DeepArchiveStorage"
	case "REDUCED_REDUNDANCY":
		return "ReducedRedundancyStorage"
	case "INTELLIGENT_TIERING":
		return "IntelligentTieringFAStorage"
	default:
		return "StandardStorage"
	}
}

func (j *Janitor) emitStorageMetrics(_ context.Context) { j.Backend.EmitStorageMetrics() }

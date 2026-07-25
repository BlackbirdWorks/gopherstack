package s3

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) deleteBucketAnalyticsConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketAnalyticsConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	if err := h.Backend.DeleteBucketAnalyticsConfiguration(ctx, bucket, id); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) deleteBucketIntelligentTieringConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketIntelligentTieringConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	if err := h.Backend.DeleteBucketIntelligentTieringConfiguration(ctx, bucket, id); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) deleteBucketInventoryConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketInventoryConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	if err := h.Backend.DeleteBucketInventoryConfiguration(ctx, bucket, id); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) deleteBucketMetricsConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketMetricsConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	if err := h.Backend.DeleteBucketMetricsConfiguration(ctx, bucket, id); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketAnalyticsConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketAnalyticsConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if err = h.Backend.PutBucketAnalyticsConfiguration(ctx, bucket, id, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketAnalyticsConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketAnalyticsConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	configXML, err := h.Backend.GetBucketAnalyticsConfiguration(ctx, bucket, id)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML)) // #nosec G705
}

func (h *S3Handler) listBucketAnalyticsConfigurations(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "ListBucketAnalyticsConfigurations")
	configs, err := h.Backend.ListBucketAnalyticsConfigurations(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	writeConfigListXML(w, "ListBucketAnalyticsConfigurationResult", configs)
}

func (h *S3Handler) putBucketIntelligentTieringConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketIntelligentTieringConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if err = h.Backend.PutBucketIntelligentTieringConfiguration(ctx, bucket, id, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketIntelligentTieringConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketIntelligentTieringConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	configXML, err := h.Backend.GetBucketIntelligentTieringConfiguration(ctx, bucket, id)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML)) // #nosec G705
}

func (h *S3Handler) listBucketIntelligentTieringConfigurations(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "ListBucketIntelligentTieringConfigurations")
	configs, err := h.Backend.ListBucketIntelligentTieringConfigurations(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	writeConfigListXML(w, "ListBucketIntelligentTieringConfigurationsResult", configs)
}

func (h *S3Handler) putBucketInventoryConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketInventoryConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if err = h.Backend.PutBucketInventoryConfiguration(ctx, bucket, id, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketInventoryConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketInventoryConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	configXML, err := h.Backend.GetBucketInventoryConfiguration(ctx, bucket, id)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML)) // #nosec G705
}

func (h *S3Handler) listBucketInventoryConfigurations(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "ListBucketInventoryConfigurations")
	configs, err := h.Backend.ListBucketInventoryConfigurations(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	writeConfigListXML(w, "ListInventoryConfigurationsResult", configs)
}

func (h *S3Handler) putBucketMetricsConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketMetricsConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if err = h.Backend.PutBucketMetricsConfiguration(ctx, bucket, id, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketMetricsConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketMetricsConfiguration")
	id := r.URL.Query().Get("id")
	if id == "" {
		WriteError(ctx, w, r, fmt.Errorf("%w: id query parameter is required", ErrInvalidArgument))

		return
	}
	configXML, err := h.Backend.GetBucketMetricsConfiguration(ctx, bucket, id)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML)) // #nosec G705
}

func (h *S3Handler) listBucketMetricsConfigurations(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "ListBucketMetricsConfigurations")
	configs, err := h.Backend.ListBucketMetricsConfigurations(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	writeConfigListXML(w, "ListMetricsConfigurationsResult", configs)
}

// writeConfigListXML writes a generic XML list response containing zero or
// more config elements.
//
// Each string in configs is the RAW request body that PutBucket*Configuration
// stored verbatim (see e.g. bucket_analytics.go's PutBucketAnalyticsConfiguration).
// Per the real SDK's serializer (awsRestxml_serializeOpPutBucketAnalyticsConfiguration
// and its Inventory/Metrics/IntelligentTiering siblings), that body's root
// element already is e.g. a complete
// `<AnalyticsConfiguration>...</AnalyticsConfiguration>` document, not just
// its inner fields. The real SDK's List deserializer likewise treats each
// top-level `<AnalyticsConfiguration>` (etc.) element directly under the list
// root as one unwrapped list entry (see
// awsRestxml_deserializeDocumentAnalyticsConfigurationListUnwrapped).
//
// So configs must be emitted AS-IS here, not re-wrapped in another element —
// doing so previously produced doubly-nested XML
// (<AnalyticsConfiguration><AnalyticsConfiguration>...) that no real SDK
// client could correctly parse back into its Id/Filter/etc fields.
func writeConfigListXML(w http.ResponseWriter, rootTag string, configs []string) {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	sb.WriteString(`<`)
	sb.WriteString(rootTag)
	sb.WriteString(` xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	sb.WriteString(`<IsTruncated>false</IsTruncated>`)
	for _, cfg := range configs {
		sb.WriteString(cfg)
	}
	sb.WriteString(`</`)
	sb.WriteString(rootTag)
	sb.WriteString(`>`)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(sb.String()))
}

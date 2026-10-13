package awsconfig

import (
	"context"
	"fmt"
	"strings"
)

// maxConformancePackTemplateBytes is PutConformancePack's documented TemplateS3Uri limit (300 KB).
const maxConformancePackTemplateBytes = 300 * 1024

// TemplateSource fetches conformance pack templates stored in S3 or as SSM documents.
type TemplateSource interface {
	S3Template(ctx context.Context, bucket, key string) ([]byte, error)
	SSMTemplate(ctx context.Context, documentName, documentVersion string) (string, error)
}

// SetTemplateSource wires the accessor used to resolve TemplateS3Uri and TemplateSSMDocumentDetails.
func (b *InMemoryBackend) SetTemplateSource(s TemplateSource) {
	b.mu.Lock("SetTemplateSource")
	defer b.mu.Unlock()

	b.templates = s
}

// SetTemplateSource wires s on the home backend and every region sibling built so far.
func (h *Handler) SetTemplateSource(s TemplateSource) {
	for _, b := range h.RegionBackends() {
		b.SetTemplateSource(s)
	}
}

// ResolveConformancePackTemplate returns the template body behind TemplateS3Uri or TemplateSSMDocumentDetails.
// It returns "" when neither is given or no source is wired.
func (b *InMemoryBackend) ResolveConformancePackTemplate(
	ctx context.Context, s3URI, ssmDocumentName, ssmDocumentVersion string,
) (string, error) {
	b.mu.RLock("ResolveConformancePackTemplate")
	src := b.templates
	b.mu.RUnlock()

	if src == nil {
		return "", nil
	}

	switch {
	case s3URI != "":
		bucket, key, ok := parseS3URI(s3URI)
		if !ok {
			return "", fmt.Errorf(
				"%w: TemplateS3Uri %q must be s3://bucket/key",
				ErrConformancePackTemplateValidation,
				s3URI,
			)
		}

		data, err := src.S3Template(ctx, bucket, key)
		if err != nil {
			return "", fmt.Errorf("%w: cannot read template %s: %w", ErrConformancePackTemplateValidation, s3URI, err)
		}

		if len(data) > maxConformancePackTemplateBytes {
			return "", fmt.Errorf(
				"%w: template %s exceeds %d bytes",
				ErrConformancePackTemplateValidation,
				s3URI,
				maxConformancePackTemplateBytes,
			)
		}

		return string(data), nil
	case ssmDocumentName != "":
		body, err := src.SSMTemplate(ctx, ssmDocumentNameFromARN(ssmDocumentName), ssmDocumentVersion)
		if err != nil {
			return "", fmt.Errorf(
				"%w: cannot read SSM document %s: %w", ErrConformancePackTemplateValidation, ssmDocumentName, err,
			)
		}

		return body, nil
	}

	return "", nil
}

func parseS3URI(uri string) (string, string, bool) {
	rest, found := strings.CutPrefix(uri, "s3://")
	if !found {
		return "", "", false
	}

	bucket, key, found := strings.Cut(rest, "/")

	return bucket, key, found && bucket != "" && key != ""
}

// ssmDocumentNameFromARN returns the document name of an SSM document ARN, or name unchanged.
func ssmDocumentNameFromARN(name string) string {
	if !strings.HasPrefix(name, "arn:") {
		return name
	}

	if _, doc, found := strings.Cut(name, ":document/"); found {
		return doc
	}

	return name
}

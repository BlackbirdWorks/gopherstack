package omics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

const (
	maxReadmeURIBytes     = 500 << 10
	maxBatchSettingsBytes = 6 << 30
	maxBatchRunSettings   = 100000
	maxRegistryMapBytes   = 1 << 20
)

// S3ObjectReader reads object bytes from S3.
type S3ObjectReader interface {
	GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error)
}

// SetS3Reader wires the S3 source for s3UriSettings, ReadmeUri and ContainerRegistryMapUri.
func (h *Handler) SetS3Reader(r S3ObjectReader) { h.s3 = r }

func parseS3URI(field, uri string) (string, string, error) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "s3" || u.Host == "" || strings.TrimPrefix(u.Path, "/") == "" {
		return "", "", fmt.Errorf("%w: %s must be an s3://bucket/key URI", ErrValidation, field)
	}

	return u.Host, strings.TrimPrefix(u.Path, "/"), nil
}

func (h *Handler) readS3URI(ctx context.Context, field, uri string, maxBytes int64) ([]byte, error) {
	bucket, key, err := parseS3URI(field, uri)
	if err != nil {
		return nil, err
	}

	if h.s3 == nil {
		return nil, fmt.Errorf("%w: %s requires an S3 source, which is not configured", ErrValidation, field)
	}

	data, err := h.s3.GetObjectBytes(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: unable to read s3://%s/%s", ErrValidation, field, bucket, key)
	}

	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: %s exceeds the %d byte limit", ErrValidation, field, maxBytes)
	}

	return data, nil
}

func (h *Handler) readmeFromURI(ctx context.Context, markdown, uri string) (string, error) {
	if markdown != "" || uri == "" {
		return markdown, nil
	}

	data, err := h.readS3URI(ctx, "readmeUri", uri, maxReadmeURIBytes)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (h *Handler) registryMapFromURI(ctx context.Context, m map[string]any, uri string) (map[string]any, error) {
	if m != nil || uri == "" {
		return m, nil
	}

	data, err := h.readS3URI(ctx, "containerRegistryMapUri", uri, maxRegistryMapBytes)
	if err != nil {
		return nil, err
	}

	var out map[string]any
	if err = json.Unmarshal(data, &out); err != nil || out == nil {
		return nil, fmt.Errorf("%w: containerRegistryMapUri must contain a JSON object", ErrValidation)
	}

	return out, nil
}

func (h *Handler) inlineSettingsFromURI(ctx context.Context, uri string) ([]inlineRunSettingWire, error) {
	data, err := h.readS3URI(ctx, "batchRunSettings.s3UriSettings", uri, maxBatchSettingsBytes)
	if err != nil {
		return nil, err
	}

	var out []inlineRunSettingWire
	if err = json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf(
			"%w: batchRunSettings.s3UriSettings must contain a JSON array of run settings",
			ErrValidation,
		)
	}

	if len(out) == 0 || len(out) > maxBatchRunSettings {
		return nil, fmt.Errorf(
			"%w: batchRunSettings.s3UriSettings must contain between 1 and %d run settings",
			ErrValidation, maxBatchRunSettings,
		)
	}

	return out, nil
}

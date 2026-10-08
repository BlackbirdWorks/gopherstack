package cloudfront

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	kvsImportSourceS3   = "S3"
	kvsImportS3ARNStart = "arn:aws:s3:::"
	maxKVSImportKeySize = 512
	maxKVSImportValSize = 1024
	maxKVSImportBytes   = 5 * 1024 * 1024
)

// S3ObjectReader reads an S3 object for a key value store import.
type S3ObjectReader interface {
	GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error)
}

// SetKVSImportReader wires the S3 reader used by CreateKeyValueStore's ImportSource.
func (h *Handler) SetKVSImportReader(r S3ObjectReader) { h.kvsImportReader = r }

type kvsImportSourceXML struct {
	SourceType string `xml:"SourceType"`
	SourceARN  string `xml:"SourceARN"`
}

// kvsImportDocument is the documented import file: {"data":[{"key":"k","value":"v"}]}.
type kvsImportDocument struct {
	Data []struct {
		Key   *string `json:"key"`
		Value *string `json:"value"`
	} `json:"data"`
}

func (h *Handler) readKVSImport(ctx context.Context, src *kvsImportSourceXML) ([]*KVSItem, error) {
	if src == nil {
		return nil, nil
	}

	if src.SourceType != kvsImportSourceS3 {
		return nil, fmt.Errorf("%w: ImportSource.SourceType must be %s", ErrValidation, kvsImportSourceS3)
	}

	bucket, key, ok := strings.Cut(strings.TrimPrefix(src.SourceARN, kvsImportS3ARNStart), "/")
	if !strings.HasPrefix(src.SourceARN, kvsImportS3ARNStart) || !ok || bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: ImportSource.SourceARN must be an S3 object ARN", ErrValidation)
	}

	if h.kvsImportReader == nil {
		return nil, nil
	}

	raw, err := h.kvsImportReader.GetObjectBytes(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read import source %s: %w", ErrValidation, src.SourceARN, err)
	}

	return parseKVSImport(raw)
}

func parseKVSImport(raw []byte) ([]*KVSItem, error) {
	var doc kvsImportDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("%w: import source is not valid JSON: %w", ErrValidation, err)
	}

	items := make([]*KVSItem, 0, len(doc.Data))
	total := 0

	for i, d := range doc.Data {
		if d.Key == nil || d.Value == nil || *d.Key == "" {
			return nil, fmt.Errorf("%w: import data[%d] needs a non-empty key and a value", ErrValidation, i)
		}

		if len(*d.Key) > maxKVSImportKeySize || len(*d.Value) > maxKVSImportValSize {
			return nil, fmt.Errorf("%w: import data[%d] key or value is too large", ErrValidation, i)
		}

		total += len(*d.Key) + len(*d.Value)
		items = append(items, &KVSItem{Key: *d.Key, Value: *d.Value})
	}

	if total > maxKVSImportBytes {
		return nil, fmt.Errorf(
			"%w: import exceeds the %d byte store limit", ErrEntitySizeLimitExceeded, maxKVSImportBytes,
		)
	}

	return items, nil
}

package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	icebergFormatVersion    = 2
	firstIcebergFieldID     = 1
	firstPartitionFieldID   = 1000
	defaultIcebergSortOrder = 1
	warehouseBucketSuffix   = "--table-s3"
	warehouseBucketIDChars  = 24
)

// MetadataWriter stores an object in the S3 bucket backing a table warehouse.
type MetadataWriter interface {
	WriteWarehouseObject(ctx context.Context, region, bucket, key string, body []byte) error
}

// SetMetadataWriter wires the S3 hook used to materialize a new table's initial Iceberg metadata.json.
func (h *Handler) SetMetadataWriter(w MetadataWriter) {
	h.metadataWriter = w
	h.Backend.setMetadataWriter(w)
}

func (b *InMemoryBackend) setMetadataWriter(w MetadataWriter) {
	b.metadataWriter.Store(&w)
}

func warehouseBucketName(bucketID string) string {
	id := strings.ReplaceAll(bucketID, "-", "")
	if len(id) > warehouseBucketIDChars {
		id = id[:warehouseBucketIDChars]
	}

	return id + warehouseBucketSuffix
}

// materializeMetadata writes the table's initial metadata.json and returns its s3:// location,
// or "" when no writer is wired or CreateTable carried no Metadata.
func (b *InMemoryBackend) materializeMetadata(t *Table, bucketID string) (string, error) {
	w := b.metadataWriter.Load()
	if w == nil || *w == nil || t.Metadata == nil {
		return "", nil
	}

	body, err := json.Marshal(buildIcebergTableMetadata(t, time.Now()))
	if err != nil {
		return "", fmt.Errorf("encode iceberg metadata: %w", err)
	}

	bucket := warehouseBucketName(bucketID)
	prefix := strings.TrimPrefix(t.WarehouseLocation, "s3://"+bucket+"/")
	key := prefix + "/metadata/00000-" + uuid.NewString() + metadataJSONSuffix

	if wErr := (*w).WriteWarehouseObject(context.Background(), b.region, bucket, key, body); wErr != nil {
		return "", fmt.Errorf("write iceberg metadata: %w", wErr)
	}

	return "s3://" + bucket + "/" + key, nil
}

func asMapSlice(v any) []map[string]any {
	raw, _ := v.([]any)
	out := make([]map[string]any, 0, len(raw))

	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}

	return out
}

func intField(m map[string]any, key string, def int) int {
	if f, ok := m[key].(float64); ok {
		return int(f)
	}

	if i, ok := m[key].(int); ok {
		return i
	}

	return def
}

// icebergSchema assigns sequential ids to fields that lack one, per the SchemaField.Id doc.
func icebergSchema(iceberg map[string]any) (map[string]any, int) {
	src, _ := iceberg["schemaV2"].(map[string]any)
	if src == nil {
		src, _ = iceberg["schema"].(map[string]any)
	}

	srcFields := asMapSlice(src[metaKeyFields])
	fields := make([]map[string]any, 0, len(srcFields))
	lastID := 0

	for _, f := range srcFields {
		out := map[string]any{"name": f["name"], "type": f["type"], "required": f["required"] == true}
		id := intField(f, "id", lastID+1)
		out["id"] = id
		lastID = max(lastID, id)

		if doc, ok := f["doc"]; ok {
			out["doc"] = doc
		}

		fields = append(fields, out)
	}

	return map[string]any{"type": "struct", "schema-id": 0, metaKeyFields: fields}, lastID
}

func icebergPartitionSpec(iceberg map[string]any) (map[string]any, int) {
	src, _ := iceberg["partitionSpec"].(map[string]any)
	srcFields := asMapSlice(src[metaKeyFields])
	fields := make([]map[string]any, 0, len(srcFields))
	lastID := firstPartitionFieldID - 1

	for i, f := range srcFields {
		id := intField(f, "field-id", firstPartitionFieldID+i)
		lastID = max(lastID, id)
		fields = append(fields, map[string]any{
			"name": f["name"], "transform": f["transform"], "source-id": f["source-id"], "field-id": id,
		})
	}

	return map[string]any{"spec-id": intField(src, "spec-id", 0), metaKeyFields: fields}, lastID
}

func icebergSortOrder(iceberg map[string]any) map[string]any {
	src, _ := iceberg["writeOrder"].(map[string]any)
	if src == nil {
		return map[string]any{"order-id": 0, metaKeyFields: []map[string]any{}}
	}

	srcFields := asMapSlice(src[metaKeyFields])
	fields := make([]map[string]any, 0, len(srcFields))

	for _, f := range srcFields {
		fields = append(fields, map[string]any{
			"transform": f["transform"], "source-id": f["source-id"],
			"direction": f["direction"], "null-order": f["null-order"],
		})
	}

	return map[string]any{"order-id": intField(src, "order-id", defaultIcebergSortOrder), metaKeyFields: fields}
}

// buildIcebergTableMetadata renders the Iceberg v2 table metadata document for a freshly created table.
func buildIcebergTableMetadata(t *Table, now time.Time) map[string]any {
	iceberg, _ := t.Metadata[metaKeyIceberg].(map[string]any)
	schema, lastColumnID := icebergSchema(iceberg)
	spec, lastPartitionID := icebergPartitionSpec(iceberg)
	order := icebergSortOrder(iceberg)

	props := map[string]string{}
	if p, ok := iceberg["properties"].(map[string]any); ok {
		for k, v := range p {
			if s, isStr := v.(string); isStr {
				props[k] = s
			}
		}
	}

	return map[string]any{
		"format-version":        icebergFormatVersion,
		"table-uuid":            uuid.NewString(),
		"location":              t.WarehouseLocation,
		"last-sequence-number":  0,
		"last-updated-ms":       now.UnixMilli(),
		"last-column-id":        lastColumnID,
		"current-schema-id":     0,
		"schemas":               []map[string]any{schema},
		"default-spec-id":       spec["spec-id"],
		"partition-specs":       []map[string]any{spec},
		"last-partition-id":     lastPartitionID,
		"default-sort-order-id": order["order-id"],
		"sort-orders":           []map[string]any{order},
		"properties":            maps.Clone(props),
		"current-snapshot-id":   -1,
		"snapshots":             []any{},
		"snapshot-log":          []any{},
		"metadata-log":          []any{},
	}
}

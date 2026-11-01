package iceberg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
)

// Manifest entry statuses and content types from the table spec.
const (
	StatusExisting = 0
	StatusAdded    = 1
	StatusDeleted  = 2

	ContentData            = 0
	ContentPositionDeletes = 1
	ContentEqualityDeletes = 2
)

// ErrInvalidManifest is returned for an Avro manifest or manifest list that cannot be decoded.
var ErrInvalidManifest = errors.New("invalid iceberg manifest")

// DataFile is one data or delete file of a manifest entry.
type DataFile struct {
	Partition   map[string]any
	ValueCounts map[int]int64
	NullCounts  map[int]int64
	Path        string
	Format      string
	EqualityIDs []int
	Content     int
	RecordCount int64
	FileSize    int64
}

// ManifestEntry is a manifest row.
type ManifestEntry struct {
	SnapshotID     *int64
	SequenceNumber *int64
	DataFile       DataFile
	Status         int
}

// ManifestFile is a manifest list row.
type ManifestFile struct {
	Path              string
	Length            int64
	SequenceNumber    int64
	MinSequenceNumber int64
	AddedSnapshotID   int64
	AddedRows         int64
	ExistingRows      int64
	DeletedRows       int64
	SpecID            int
	Content           int
	AddedFiles        int
	ExistingFiles     int
	DeletedFiles      int
}

const manifestListSchema = `{"type":"record","name":"manifest_file","fields":[
{"name":"manifest_path","type":"string","field-id":500},
{"name":"manifest_length","type":"long","field-id":501},
{"name":"partition_spec_id","type":"int","field-id":502},
{"name":"content","type":"int","field-id":517},
{"name":"sequence_number","type":"long","field-id":515},
{"name":"min_sequence_number","type":"long","field-id":516},
{"name":"added_snapshot_id","type":"long","field-id":503},
{"name":"added_files_count","type":"int","field-id":504},
{"name":"existing_files_count","type":"int","field-id":505},
{"name":"deleted_files_count","type":"int","field-id":506},
{"name":"added_rows_count","type":"long","field-id":512},
{"name":"existing_rows_count","type":"long","field-id":513},
{"name":"deleted_rows_count","type":"long","field-id":514}]}`

// EncodeManifestList writes a manifest list for a snapshot.
func EncodeManifestList(snapshotID int64, parent *int64, seq int64, files []ManifestFile) ([]byte, error) {
	meta := map[string][]byte{
		"snapshot-id":     []byte(strconv.FormatInt(snapshotID, 10)),
		"sequence-number": []byte(strconv.FormatInt(seq, 10)),
		keyFormatVersion:  []byte("2"),
	}
	if parent != nil {
		meta["parent-snapshot-id"] = []byte(strconv.FormatInt(*parent, 10))
	}

	var buf bytes.Buffer

	enc, err := ocf.NewEncoder(manifestListSchema, &buf, ocf.WithMetadata(meta))
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		row := map[string]any{
			"manifest_path": f.Path, "manifest_length": f.Length, "partition_spec_id": i32(f.SpecID),
			keyContent: i32(f.Content), "sequence_number": f.SequenceNumber,
			"min_sequence_number": f.MinSequenceNumber, "added_snapshot_id": f.AddedSnapshotID,
			"added_files_count": i32(f.AddedFiles), "existing_files_count": i32(f.ExistingFiles),
			"deleted_files_count": i32(f.DeletedFiles), "added_rows_count": f.AddedRows,
			"existing_rows_count": f.ExistingRows, "deleted_rows_count": f.DeletedRows,
		}
		if err = enc.Encode(row); err != nil {
			return nil, err
		}
	}

	if err = enc.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// ReadManifestList decodes a manifest list (v1 and v2 column names).
func ReadManifestList(data []byte) ([]ManifestFile, error) {
	rows, err := readOCF(data)
	if err != nil {
		return nil, err
	}

	out := make([]ManifestFile, 0, len(rows))

	for _, r := range rows {
		path, _ := r["manifest_path"].(string)
		if path == "" {
			return nil, fmt.Errorf("%w: manifest list entry without manifest_path", ErrInvalidManifest)
		}

		out = append(out, ManifestFile{
			Path:              path,
			Length:            asInt64(r["manifest_length"]),
			SpecID:            int(asInt64(r["partition_spec_id"])),
			Content:           int(asInt64(r["content"])),
			SequenceNumber:    asInt64(r["sequence_number"]),
			MinSequenceNumber: asInt64(r["min_sequence_number"]),
			AddedSnapshotID:   asInt64(r["added_snapshot_id"]),
			AddedFiles:        int(firstInt(r, "added_files_count", "added_data_files_count")),
			ExistingFiles:     int(firstInt(r, "existing_files_count", "existing_data_files_count")),
			DeletedFiles:      int(firstInt(r, "deleted_files_count", "deleted_data_files_count")),
			AddedRows:         asInt64(r["added_rows_count"]),
			ExistingRows:      asInt64(r["existing_rows_count"]),
			DeletedRows:       asInt64(r["deleted_rows_count"]),
		})
	}

	return out, nil
}

func firstInt(r map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := r[k]; ok && v != nil {
			return asInt64(v)
		}
	}

	return 0
}

// ReadManifest decodes a manifest.
func ReadManifest(data []byte) ([]ManifestEntry, error) {
	rows, err := readOCF(data)
	if err != nil {
		return nil, err
	}

	out := make([]ManifestEntry, 0, len(rows))

	for _, r := range rows {
		df, ok := unwrap(r["data_file"]).(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: manifest entry without data_file", ErrInvalidManifest)
		}

		e := ManifestEntry{Status: int(asInt64(r["status"]))}
		e.SnapshotID = optInt64(r["snapshot_id"])
		e.SequenceNumber = optInt64(r["sequence_number"])
		part, _ := unwrap(df["partition"]).(map[string]any)

		path, _ := df["file_path"].(string)
		format, _ := df["file_format"].(string)

		e.DataFile = DataFile{
			Content: int(asInt64(df["content"])), Path: path, Format: format,
			Partition: unwrapMap(part), RecordCount: asInt64(df["record_count"]),
			FileSize: asInt64(df["file_size_in_bytes"]), ValueCounts: intLongMap(df["value_counts"]),
			NullCounts: intLongMap(df["null_value_counts"]), EqualityIDs: intList(df["equality_ids"]),
		}
		out = append(out, e)
	}

	return out, nil
}

func readOCF(data []byte) ([]map[string]any, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidManifest, err)
	}

	var rows []map[string]any

	for dec.HasNext() {
		var row map[string]any
		if err = dec.Decode(&row); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidManifest, err)
		}

		rows = append(rows, row)
	}

	if err = dec.Error(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidManifest, err)
	}

	return rows, nil
}

// unwrap strips the single-key map hamba wraps around a decoded non-null union member.
func unwrap(v any) any {
	if m, ok := v.(map[string]any); ok && len(m) == 1 {
		for k, inner := range m {
			switch k {
			case typeInt, typeLong, typeString, typeBoolean, typeFloat, typeDouble, "bytes", "array", typeMap, "record":
				return inner
			}
		}
	}

	return v
}

func unwrapMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = unwrap(v)
	}

	return out
}

func asInt64(v any) int64 {
	switch n := unwrap(v).(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func optInt64(v any) *int64 {
	if unwrap(v) == nil {
		return nil
	}

	n := asInt64(v)

	return &n
}

func intLongMap(v any) map[int]int64 {
	arr, ok := unwrap(v).([]any)
	if !ok {
		return nil
	}

	out := make(map[int]int64, len(arr))

	for _, e := range arr {
		if m, isMap := e.(map[string]any); isMap {
			out[int(asInt64(m["key"]))] = asInt64(m["value"])
		}
	}

	return out
}

func intList(v any) []int {
	arr, ok := unwrap(v).([]any)
	if !ok {
		return nil
	}

	out := make([]int, 0, len(arr))
	for _, e := range arr {
		out = append(out, int(asInt64(e)))
	}

	return out
}

func avroPartitionField(meta *TableMetadata, pf PartitionField) (map[string]any, error) {
	var source string

	for _, s := range meta.Schemas {
		for _, f := range s.Fields {
			if f.ID == pf.SourceID {
				source = PrimitiveName(f.Type)
			}
		}
	}

	tr, err := ParseTransform(pf.Transform)
	if err != nil {
		return nil, err
	}

	at, err := AvroTypeOf(tr.ResultType(source))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyName: pf.Name, keyType: []any{"null", at}, keyDefault: nil, "field-id": pf.FieldID,
	}, nil
}

func avroField(name string, typ any, id int) map[string]any {
	return map[string]any{keyName: name, keyType: typ, "field-id": id}
}

func avroOptional(name string, typ any, id int) map[string]any {
	f := avroField(name, []any{"null", typ}, id)
	f[keyDefault] = nil

	return f
}

func kvArray(name string, keyID, valID int, valType string) map[string]any {
	return map[string]any{
		keyType: "array", "logicalType": typeMap,
		"items": map[string]any{
			keyType: avroRecord, keyName: name,
			keyFields: []any{
				avroField(keyKey, typeInt, keyID),
				avroField(keyValue, valType, valID),
			},
		},
	}
}

func manifestSchema(meta *TableMetadata, spec PartitionSpec) (string, error) {
	pfields := make([]any, 0, len(spec.Fields))

	for _, pf := range spec.Fields {
		f, err := avroPartitionField(meta, pf)
		if err != nil {
			return "", err
		}

		pfields = append(pfields, f)
	}

	dataFile := map[string]any{
		keyType: avroRecord, keyName: "r2",
		keyFields: []any{
			avroField(keyContent, typeInt, idContent),
			avroField("file_path", typeString, idFilePath),
			avroField("file_format", typeString, idFileFormat),
			avroField(
				"partition",
				map[string]any{keyType: avroRecord, keyName: "r102", keyFields: pfields},
				idPartition,
			),
			avroField("record_count", typeLong, idRecordCount),
			avroField("file_size_in_bytes", typeLong, idFileSize),
			avroOptional(
				"value_counts",
				kvArray("k119_v120", idValueCountsKey, idValueCountsValue, typeLong),
				idValueCounts,
			),
			avroOptional(
				"null_value_counts",
				kvArray("k121_v122", idNullCountsKey, idNullCountsValue, typeLong),
				idNullCounts,
			),
		},
	}

	schema := map[string]any{
		keyType: avroRecord, keyName: "manifest_entry",
		keyFields: []any{
			avroField("status", typeInt, idStatus),
			avroOptional("snapshot_id", typeLong, idSnapshotID),
			avroOptional("sequence_number", typeLong, idSequenceNumber),
			avroOptional("file_sequence_number", typeLong, idFileSequence),
			avroField("data_file", dataFile, idDataFile),
		},
	}

	b, err := json.Marshal(schema)

	return string(b), err
}

// EncodeManifest writes a manifest of entries written under spec.
func EncodeManifest(meta *TableMetadata, spec PartitionSpec, content int, entries []ManifestEntry) ([]byte, error) {
	schemaJSON, err := manifestSchema(meta, spec)
	if err != nil {
		return nil, err
	}

	if _, err = avro.Parse(schemaJSON); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidManifest, err)
	}

	cur, _ := json.Marshal(meta.CurrentSchema())
	specJSON, _ := json.Marshal(spec.Fields)
	contentName := "data"

	if content != ContentData {
		contentName = "deletes"
	}

	var buf bytes.Buffer

	enc, err := ocf.NewEncoder(schemaJSON, &buf, ocf.WithMetadata(map[string][]byte{
		"schema":            cur,
		"schema-id":         []byte(strconv.Itoa(meta.CurrentSchemaID)),
		"partition-spec":    specJSON,
		"partition-spec-id": []byte(strconv.Itoa(spec.SpecID)),
		keyFormatVersion:    []byte("2"),
		keyContent:          []byte(contentName),
	}))
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		if err = enc.Encode(entryRow(e, spec)); err != nil {
			return nil, err
		}
	}

	if err = enc.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func entryRow(e ManifestEntry, spec PartitionSpec) map[string]any {
	part := map[string]any{}
	for _, pf := range spec.Fields {
		part[pf.Name] = e.DataFile.Partition[pf.Name]
	}

	df := map[string]any{
		keyContent: i32(e.DataFile.Content), "file_path": e.DataFile.Path, "file_format": e.DataFile.Format,
		"partition": part, "record_count": e.DataFile.RecordCount, "file_size_in_bytes": e.DataFile.FileSize,
		"value_counts": kvRows(e.DataFile.ValueCounts), "null_value_counts": kvRows(e.DataFile.NullCounts),
	}

	row := map[string]any{"status": i32(e.Status), "data_file": df}
	row["snapshot_id"] = e.SnapshotID
	row["sequence_number"] = e.SequenceNumber
	row["file_sequence_number"] = e.SequenceNumber

	return row
}

func kvRows(m map[int]int64) any {
	if m == nil {
		return nil
	}

	rows := make([]map[string]any, 0, len(m))
	for k, v := range m {
		rows = append(rows, map[string]any{keyKey: i32(k), keyValue: v})
	}

	return rows
}

// i32 narrows a small count or id for an Avro int; values here are bounded by file and field counts.
func i32(n int) int32 {
	return int32(n) //nolint:gosec // counts and ids are far below MaxInt32
}

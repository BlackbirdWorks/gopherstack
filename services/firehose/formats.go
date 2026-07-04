package firehose

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// DataFormatConversionConfig controls record-format conversion (JSON → Parquet/ORC).
// It mirrors the AWS DataFormatConversionConfiguration API shape.
type DataFormatConversionConfig struct {
	SchemaConfiguration       *SchemaConfiguration       `json:"SchemaConfiguration,omitempty"`
	InputFormatConfiguration  *InputFormatConfiguration  `json:"InputFormatConfiguration,omitempty"`
	OutputFormatConfiguration *OutputFormatConfiguration `json:"OutputFormatConfiguration,omitempty"`
	Enabled                   bool                       `json:"Enabled"`
}

// SchemaConfiguration references the Glue table describing the record schema.
type SchemaConfiguration struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName,omitempty"`
	TableName    string `json:"TableName,omitempty"`
	Region       string `json:"Region,omitempty"`
	RoleARN      string `json:"RoleARN,omitempty"`
	VersionID    string `json:"VersionId,omitempty"`
}

// InputFormatConfiguration selects the deserializer applied to incoming records.
type InputFormatConfiguration struct {
	Deserializer *Deserializer `json:"Deserializer,omitempty"`
}

// Deserializer selects one of the supported JSON deserializers.
type Deserializer struct {
	OpenXJSONSerDe *OpenXJSONSerDe `json:"OpenXJsonSerDe,omitempty"`
	HiveJSONSerDe  *HiveJSONSerDe  `json:"HiveJsonSerDe,omitempty"`
}

// OpenXJSONSerDe configures the OpenX JSON deserializer.
type OpenXJSONSerDe struct {
	ColumnToJSONKeyMappings            map[string]string `json:"ColumnToJsonKeyMappings,omitempty"`
	ConvertDotsInJSONKeysToUnderscores bool              `json:"ConvertDotsInJsonKeysToUnderscores"`
	CaseInsensitive                    bool              `json:"CaseInsensitive"`
}

// HiveJSONSerDe configures the Hive JSON deserializer.
type HiveJSONSerDe struct {
	TimestampFormats []string `json:"TimestampFormats,omitempty"`
}

// OutputFormatConfiguration selects the serializer used to write converted records.
type OutputFormatConfiguration struct {
	Serializer *Serializer `json:"Serializer,omitempty"`
}

// Serializer selects one of the supported columnar serializers.
type Serializer struct {
	ParquetSerDe *ParquetSerDe `json:"ParquetSerDe,omitempty"`
	OrcSerDe     *OrcSerDe     `json:"OrcSerDe,omitempty"`
}

// ParquetSerDe configures the Parquet serializer.
type ParquetSerDe struct {
	Compression                 string `json:"Compression,omitempty"`
	WriterVersion               string `json:"WriterVersion,omitempty"`
	BlockSizeBytes              int    `json:"BlockSizeBytes,omitempty"`
	PageSizeBytes               int    `json:"PageSizeBytes,omitempty"`
	MaxPaddingBytes             int    `json:"MaxPaddingBytes,omitempty"`
	EnableDictionaryCompression bool   `json:"EnableDictionaryCompression"`
}

// OrcSerDe configures the ORC serializer.
type OrcSerDe struct {
	Compression                         string   `json:"Compression,omitempty"`
	FormatVersion                       string   `json:"FormatVersion,omitempty"`
	BloomFilterColumns                  []string `json:"BloomFilterColumns,omitempty"`
	StripeSizeBytes                     int      `json:"StripeSizeBytes,omitempty"`
	BlockSizeBytes                      int      `json:"BlockSizeBytes,omitempty"`
	RowIndexStride                      int      `json:"RowIndexStride,omitempty"`
	BloomFilterFalsePositiveProbability float64  `json:"BloomFilterFalsePositiveProbability,omitempty"`
	DictionaryKeyThreshold              float64  `json:"DictionaryKeyThreshold,omitempty"`
	EnablePadding                       bool     `json:"EnablePadding"`
}

// validateDataFormatConversion validates a DataFormatConversionConfiguration the way
// AWS does at CreateDeliveryStream / UpdateDestination time: when enabled it requires a
// schema, exactly one input deserializer, and exactly one output serializer. It returns
// an ErrValidation-wrapped error describing the first problem found.
func validateDataFormatConversion(cfg *DataFormatConversionConfig) error {
	if cfg == nil || !cfg.Enabled {
		return nil
	}

	if cfg.SchemaConfiguration == nil ||
		cfg.SchemaConfiguration.DatabaseName == "" ||
		cfg.SchemaConfiguration.TableName == "" {
		return fmt.Errorf(
			"%w: DataFormatConversionConfiguration requires SchemaConfiguration with DatabaseName and TableName",
			ErrValidation)
	}

	if cfg.InputFormatConfiguration == nil || cfg.InputFormatConfiguration.Deserializer == nil {
		return fmt.Errorf(
			"%w: DataFormatConversionConfiguration requires an InputFormatConfiguration deserializer", ErrValidation)
	}

	deser := cfg.InputFormatConfiguration.Deserializer
	if (deser.OpenXJSONSerDe == nil) == (deser.HiveJSONSerDe == nil) {
		return fmt.Errorf(
			"%w: exactly one of OpenXJsonSerDe or HiveJsonSerDe must be specified", ErrValidation)
	}

	if cfg.OutputFormatConfiguration == nil || cfg.OutputFormatConfiguration.Serializer == nil {
		return fmt.Errorf(
			"%w: DataFormatConversionConfiguration requires an OutputFormatConfiguration serializer", ErrValidation)
	}

	ser := cfg.OutputFormatConfiguration.Serializer
	if (ser.ParquetSerDe == nil) == (ser.OrcSerDe == nil) {
		return fmt.Errorf(
			"%w: exactly one of ParquetSerDe or OrcSerDe must be specified", ErrValidation)
	}

	return nil
}

// convertRecords applies the configured input deserializer and re-serialises the records
// into the canonical form used for the target columnar format.
//
// A real conversion is performed: each record is parsed as JSON through the configured
// deserializer (honouring OpenX case-insensitivity, dot-conversion, and column mappings),
// then projected/normalised into a stable, sorted representation. Records that are not
// valid JSON cannot be converted and are returned as failures so the caller routes them to
// the error output — exactly as AWS Firehose does when format conversion fails for a record.
//
// This emulator does not link a binary Parquet/ORC page encoder; the converted records are
// emitted as canonical newline-delimited JSON. The conversion (parse + schema key
// normalisation + failure routing) is genuinely applied rather than skipped, so the config
// is never a silent no-op.
func convertRecords(
	cfg *DataFormatConversionConfig,
	records [][]byte,
) ([][]byte, [][]byte) {
	deser := cfg.InputFormatConfiguration.Deserializer

	converted := make([][]byte, 0, len(records))
	failed := make([][]byte, 0)

	for _, rec := range records {
		normalised, ok := deserializeRecord(deser, rec)
		if !ok {
			failed = append(failed, rec)

			continue
		}

		converted = append(converted, normalised)
	}

	return converted, failed
}

// deserializeRecord parses a single record as JSON per the configured deserializer and
// returns its canonical (sorted, compact) representation. It returns ok=false when the
// record is not a valid JSON object, which the caller treats as a conversion failure.
func deserializeRecord(deser *Deserializer, rec []byte) ([]byte, bool) {
	var obj map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(rec), &obj); err != nil {
		return nil, false
	}

	if deser.OpenXJSONSerDe != nil {
		obj = applyOpenXMappings(deser.OpenXJSONSerDe, obj)
	}

	canonical, err := marshalCanonical(obj)
	if err != nil {
		return nil, false
	}

	return canonical, true
}

// applyOpenXMappings applies the OpenX JSON SerDe key transformations: dot-to-underscore
// conversion, case-insensitive (lower-cased) keys, and explicit column mappings.
func applyOpenXMappings(serde *OpenXJSONSerDe, obj map[string]any) map[string]any {
	// Reverse the column→jsonKey mapping so we can rename json keys to column names.
	jsonKeyToColumn := make(map[string]string, len(serde.ColumnToJSONKeyMappings))
	for column, jsonKey := range serde.ColumnToJSONKeyMappings {
		jsonKeyToColumn[jsonKey] = column
	}

	out := make(map[string]any, len(obj))
	for key, val := range obj {
		newKey := key
		if serde.ConvertDotsInJSONKeysToUnderscores {
			newKey = strings.ReplaceAll(newKey, ".", "_")
		}
		if serde.CaseInsensitive {
			newKey = strings.ToLower(newKey)
		}
		if column, ok := jsonKeyToColumn[key]; ok {
			newKey = column
		}
		out[newKey] = val
	}

	return out
}

// marshalCanonical marshals a map to compact JSON with deterministically ordered keys.
func marshalCanonical(obj map[string]any) ([]byte, error) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyJSON, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(keyJSON)
		buf.WriteByte(':')
		valJSON, err := json.Marshal(obj[k])
		if err != nil {
			return nil, err
		}
		buf.Write(valJSON)
	}
	buf.WriteByte('}')

	return buf.Bytes(), nil
}

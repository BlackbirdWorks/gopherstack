package gcpbigquery

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/iskorotkov/avro/v2/ocf"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	avroLogical     = "logicalType"
	avroLong        = "long"
	avroBytes       = "bytes"
	bigNumericPrec  = 77
	bigNumericScale = 38
)

// avroOutType maps a column to its Avro type per
// https://docs.cloud.google.com/bigquery/docs/exporting-data#avro_export_details.
func avroOutType(f bqsql.Field) (any, error) {
	if f.Type == litRecord || f.Mode == bqsql.ModeRepeated {
		return nil, fmt.Errorf("%w: Avro export of nested field %s is not supported", errLoad, f.Name)
	}

	var t any

	switch f.Type {
	case litInteger:
		t = avroLong
	case litFloat:
		t = "double"
	case litBoolean:
		t = "boolean"
	case litBytes:
		t = avroBytes
	case litTimestamp:
		t = map[string]any{litType: avroLong, avroLogical: "timestamp-micros"}
	case litDate:
		t = map[string]any{litType: "int", avroLogical: "date"}
	case "TIME":
		t = map[string]any{litType: avroLong, avroLogical: "time-micros"}
	case "DATETIME":
		t = map[string]any{litType: litStringType, avroLogical: "datetime"}
	case litNumeric:
		t = map[string]any{
			litType:     avroBytes,
			avroLogical: "decimal",
			"precision": numericMaxPrec,
			"scale":     numericMaxScale,
		}
	case litBignumeric:
		t = map[string]any{
			litType:     avroBytes,
			avroLogical: "decimal",
			"precision": bigNumericPrec,
			"scale":     bigNumericScale,
		}
	default:
		t = litStringType
	}

	if f.Mode == bqsql.ModeRequired {
		return t, nil
	}

	return []any{"null", t}, nil
}

func avroOutCell(f bqsql.Field, v any) any {
	s, ok := v.(string)
	if !ok {
		return nil
	}

	switch f.Type {
	case litInteger:
		n, _ := strconv.ParseInt(s, 10, 64)

		return n
	case litFloat:
		x, _ := strconv.ParseFloat(s, 64)

		return x
	case litBoolean:
		return s == litTrue
	case litBytes:
		raw, _ := bqsqlDecodeBase64(s)

		return raw
	case litTimestamp:
		t, _ := time.Parse(bqsql.TimestampLayout, s)

		return t
	case litDate:
		t, _ := time.Parse(time.DateOnly, s)

		return t
	case "TIME":
		t, _ := time.Parse("15:04:05.999999", s)

		return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())
	case litNumeric, litBignumeric:
		r, _ := new(big.Rat).SetString(s)
		if r == nil {
			r = new(big.Rat)
		}

		return r
	}

	return s
}

func avroCodec(compression string) (ocf.CodecName, error) {
	switch strings.ToUpper(compression) {
	case "", "NONE":
		return ocf.Null, nil
	case "DEFLATE":
		return ocf.Deflate, nil
	case "SNAPPY":
		return ocf.Snappy, nil
	}

	return "", fmt.Errorf("%w: compression %s is not supported for AVRO export", errLoad, compression)
}

func encodeAvro(ec extractConfig, t *Table) ([]byte, error) {
	fields := make([]any, len(t.Schema))

	for i, f := range t.Schema {
		ty, err := avroOutType(f)
		if err != nil {
			return nil, err
		}

		fields[i] = map[string]any{litName: f.Name, litType: ty}
	}

	schema, err := json.Marshal(
		map[string]any{litType: "record", litName: "root", litClusterFields: fields},
		jsonx.V1(),
	)
	if err != nil {
		return nil, fmt.Errorf("avro schema: %w", err)
	}

	codec, err := avroCodec(ec.Compression)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	enc, err := ocf.NewEncoder(string(schema), &buf, ocf.WithCodec(codec))
	if err != nil {
		return nil, fmt.Errorf("avro export: %w", err)
	}

	for _, row := range t.Rows {
		rec := make(map[string]any, len(t.Schema))
		for i, f := range t.Schema {
			rec[f.Name] = avroOutCell(f, row[i])
		}

		if err = enc.Encode(rec); err != nil {
			return nil, fmt.Errorf("avro export: %w", err)
		}
	}

	if err = enc.Close(); err != nil {
		return nil, fmt.Errorf("avro export: %w", err)
	}

	return buf.Bytes(), nil
}

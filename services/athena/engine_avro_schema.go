package athena

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	paramAvroLiteral = "avro.schema.literal"
	paramAvroURL     = "avro.schema.url"
	avroRecordType   = "record"
	typeBinaryName   = "binary"
	avroBytes        = "bytes"
)

// avroSchemaText returns the table's Avro schema from avro.schema.literal or avro.schema.url (an S3 object).
func avroSchemaText(ctx context.Context, def *GlueTableDef, get S3Getter) string {
	for _, params := range []map[string]string{def.Parameters, def.SerdeParameters} {
		if lit := lowerParam(params, paramAvroLiteral); lit != "" {
			return lit
		}
	}

	for _, params := range []map[string]string{def.Parameters, def.SerdeParameters} {
		uri := lowerParam(params, paramAvroURL)

		bucket, key, ok := splitS3URI(uri)
		if !ok || get == nil {
			continue
		}

		out, err := get.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			continue
		}

		body, rerr := io.ReadAll(io.LimitReader(out.Body, maxLineBytes))
		_ = out.Body.Close()

		if rerr == nil {
			return string(body)
		}
	}

	return ""
}

// withAvroColumns derives the columns of an Avro table that has none from its schema, as Hive does.
func withAvroColumns(ctx context.Context, def *GlueTableDef, get S3Getter) *GlueTableDef {
	if len(def.Columns) > 0 || !strings.Contains(strings.ToLower(def.SerdeLibrary+def.InputFormat), "avro") {
		return def
	}

	text := avroSchemaText(ctx, def, get)
	if text == "" {
		return def
	}

	cols, err := avroColumns(text)
	if err != nil || len(cols) == 0 {
		return def
	}

	cp := *def
	cp.Columns = cols

	return &cp
}

// avroColumns maps the fields of a top-level Avro record schema to Hive columns.
func avroColumns(schema string) ([]Column, error) {
	var root any
	if err := json.Unmarshal([]byte(schema), &root, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: avro schema: %w", errUnsupportedFormat, err)
	}

	m, ok := root.(map[string]any)
	if !ok || m["type"] != avroRecordType {
		return nil, fmt.Errorf("%w: avro schema is not a record", errUnsupportedFormat)
	}

	names := map[string]string{}
	fields, _ := m["fields"].([]any)
	cols := make([]Column, 0, len(fields))

	for _, f := range fields {
		fm, isMap := f.(map[string]any)
		if !isMap {
			continue
		}

		name, _ := fm["name"].(string)
		cols = append(cols, Column{Name: strings.ToLower(name), Type: avroHiveType(fm["type"], names)})
	}

	return cols, nil
}

func avroHiveType(t any, names map[string]string) string {
	switch v := t.(type) {
	case string:
		return avroPrimitiveHive(v, names)
	case []any:
		for _, branch := range v {
			if s, isStr := branch.(string); isStr && s == "null" {
				continue
			}

			return avroHiveType(branch, names)
		}

		return columnTypeString
	case map[string]any:
		return avroComplexHive(v, names)
	default:
		return columnTypeString
	}
}

func avroPrimitiveHive(name string, names map[string]string) string {
	switch name {
	case typeInt:
		return typeInt
	case icebergLong:
		return typeBigint
	case typeFloat:
		return typeFloat
	case typeDouble:
		return typeDouble
	case typeBoolean:
		return typeBoolean
	case avroBytes, "fixed":
		return typeBinaryName
	}

	if h, ok := names[name]; ok {
		return h
	}

	return columnTypeString
}

func avroComplexHive(m map[string]any, names map[string]string) string {
	if h, ok := avroLogicalHive(m); ok {
		return h
	}

	kind, _ := m["type"].(string)

	var out string

	switch kind {
	case hiveArray:
		out = "array<" + avroHiveType(m["items"], names) + ">"
	case "map":
		out = "map<string," + avroHiveType(m["values"], names) + ">"
	case avroRecordType:
		parts := make([]string, 0)
		fields, _ := m["fields"].([]any)

		for _, f := range fields {
			if fm, ok := f.(map[string]any); ok {
				name, _ := fm["name"].(string)
				parts = append(parts, strings.ToLower(name)+":"+avroHiveType(fm["type"], names))
			}
		}

		out = "struct<" + strings.Join(parts, ",") + ">"
	case "enum":
		out = columnTypeString
	default:
		out = avroPrimitiveHive(kind, names)
	}

	if name, ok := m["name"].(string); ok {
		names[name] = out
	}

	return out
}

func avroLogicalHive(m map[string]any) (string, bool) {
	logical, _ := m["logicalType"].(string)

	switch logical {
	case typeDate:
		return typeDate, true
	case "timestamp-millis", "timestamp-micros":
		return typeTimestamp, true
	case typeDecimal:
		p, _ := m["precision"].(float64)
		s, _ := m["scale"].(float64)

		return fmt.Sprintf("decimal(%d,%d)", int(p), int(s)), true
	default:
		return "", false
	}
}

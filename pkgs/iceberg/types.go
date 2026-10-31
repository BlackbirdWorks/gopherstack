package iceberg

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrUnsupportedType is returned for a type that cannot be converted or written.
var ErrUnsupportedType = errors.New("unsupported iceberg type")

type nestedType struct {
	Element       json.RawMessage `json:"element"`
	Key           json.RawMessage `json:"key"`
	Value         json.RawMessage `json:"value"`
	Type          string          `json:"type"`
	Fields        []Field         `json:"fields"`
	ElementID     int             `json:"element-id"`
	KeyID         int             `json:"key-id"`
	ValueID       int             `json:"value-id"`
	ElementNeeded bool            `json:"element-required"`
	ValueNeeded   bool            `json:"value-required"`
}

// HiveType renders an Iceberg type as the Athena/Hive type name used in the
// Glue catalog (long -> bigint, list -> array<..>, struct -> struct<a:int,..>).
func HiveType(raw json.RawMessage) (string, error) {
	var prim string
	if err := json.Unmarshal(raw, &prim); err == nil {
		return hivePrimitive(prim)
	}

	var n nestedType
	if err := json.Unmarshal(raw, &n); err != nil {
		return "", fmt.Errorf("%w: %s", ErrUnsupportedType, raw)
	}

	switch n.Type {
	case typeStruct:
		parts := make([]string, 0, len(n.Fields))

		for _, f := range n.Fields {
			t, err := HiveType(f.Type)
			if err != nil {
				return "", err
			}

			parts = append(parts, f.Name+":"+t)
		}

		return "struct<" + strings.Join(parts, ",") + ">", nil
	case "list":
		t, err := HiveType(n.Element)
		if err != nil {
			return "", err
		}

		return "array<" + t + ">", nil
	case typeMap:
		k, err := HiveType(n.Key)
		if err != nil {
			return "", err
		}

		v, err := HiveType(n.Value)
		if err != nil {
			return "", err
		}

		return "map<" + k + "," + v + ">", nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedType, n.Type)
	}
}

func hivePrimitive(p string) (string, error) {
	switch {
	case p == typeBoolean,
		p == typeInt,
		p == typeFloat,
		p == typeDouble,
		p == typeDate,
		p == typeString,
		p == typeBinary:
		return p, nil
	case p == typeLong:
		return "bigint", nil
	case p == typeTime:
		return typeTime, nil
	case p == typeTimestamp:
		return typeTimestamp, nil
	case p == typeTimestamptz, p == "timestamp_ns", p == "timestamptz_ns":
		return "timestamp with time zone", nil
	case p == typeUUID:
		return typeString, nil
	case strings.HasPrefix(p, "decimal("):
		return strings.ReplaceAll(p, " ", ""), nil
	case strings.HasPrefix(p, "fixed["):
		return typeBinary, nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedType, p)
	}
}

// PrimitiveName returns the primitive type name of a field type ("" for nested types).
func PrimitiveName(raw json.RawMessage) string {
	var prim string
	if err := json.Unmarshal(raw, &prim); err != nil {
		return ""
	}

	return prim
}

// FromHive maps an Athena/Hive column type to an Iceberg primitive type name.
// Nested types are not writable and return ErrUnsupportedType.
func FromHive(hive string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(hive))
	base, _, _ := strings.Cut(t, "(")
	base = strings.TrimSpace(base)

	switch base {
	case typeBoolean, typeFloat, typeDouble, typeDate, typeBinary:
		return base, nil
	case "tinyint", "smallint", typeInt, "integer":
		return typeInt, nil
	case "bigint":
		return typeLong, nil
	case "real":
		return typeFloat, nil
	case typeString, "varchar", "char":
		return typeString, nil
	case typeTimestamp:
		if strings.Contains(t, "with time zone") {
			return typeTimestamptz, nil
		}

		return typeTimestamp, nil
	case "decimal", "numeric":
		if !strings.Contains(t, "(") {
			return "decimal(10, 0)", nil
		}

		_, rest, _ := strings.Cut(t, "(")
		p, s, _ := strings.Cut(strings.TrimSuffix(rest, ")"), ",")

		pi, err1 := strconv.Atoi(strings.TrimSpace(p))
		si, err2 := strconv.Atoi(strings.TrimSpace(s))

		if err1 != nil || (err2 != nil && s != "") {
			return "", fmt.Errorf("%w: %s", ErrUnsupportedType, hive)
		}

		return fmt.Sprintf("decimal(%d, %d)", pi, si), nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedType, hive)
	}
}

// ColumnSpec is a column of a table about to be created.
type ColumnSpec struct {
	Name    string
	Type    string
	Comment string
}

// SchemaFromColumns builds a schema with sequential field ids; columns are optional (nullable).
func SchemaFromColumns(cols []ColumnSpec) (Schema, error) {
	fields := make([]Field, 0, len(cols))

	for i, c := range cols {
		t, err := FromHive(c.Type)
		if err != nil {
			return Schema{}, err
		}

		raw, _ := json.Marshal(t)
		fields = append(fields, Field{ID: i + 1, Name: c.Name, Type: raw, Doc: c.Comment})
	}

	return Schema{Type: typeStruct, Fields: fields}, nil
}

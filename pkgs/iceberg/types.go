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
	case "struct":
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
	case "map":
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
	case p == "boolean", p == "int", p == "float", p == "double", p == "date", p == "string", p == "binary":
		return p, nil
	case p == "long":
		return "bigint", nil
	case p == "time":
		return "time", nil
	case p == "timestamp":
		return "timestamp", nil
	case p == "timestamptz", p == "timestamp_ns", p == "timestamptz_ns":
		return "timestamp with time zone", nil
	case p == "uuid":
		return "string", nil
	case strings.HasPrefix(p, "decimal("):
		return strings.ReplaceAll(p, " ", ""), nil
	case strings.HasPrefix(p, "fixed["):
		return "binary", nil
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

// IcebergType maps an Athena/Hive column type to an Iceberg primitive type name.
// Nested types are not writable and return ErrUnsupportedType.
func IcebergType(hive string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(hive))
	base, _, _ := strings.Cut(t, "(")
	base = strings.TrimSpace(base)

	switch base {
	case "boolean", "float", "double", "date", "binary":
		return base, nil
	case "tinyint", "smallint", "int", "integer":
		return "int", nil
	case "bigint":
		return "long", nil
	case "real":
		return "float", nil
	case "string", "varchar", "char":
		return "string", nil
	case "timestamp":
		if strings.Contains(t, "with time zone") {
			return "timestamptz", nil
		}

		return "timestamp", nil
	case "decimal", "numeric":
		if !strings.Contains(t, "(") {
			return "decimal(10, 0)", nil
		}

		args := strings.TrimSuffix(strings.TrimPrefix(t[strings.Index(t, "("):], "("), ")")
		p, s, _ := strings.Cut(args, ",")

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
		t, err := IcebergType(c.Type)
		if err != nil {
			return Schema{}, err
		}

		raw, _ := json.Marshal(t)
		fields = append(fields, Field{ID: i + 1, Name: c.Name, Type: raw, Doc: c.Comment})
	}

	return Schema{Type: "struct", Fields: fields}, nil
}

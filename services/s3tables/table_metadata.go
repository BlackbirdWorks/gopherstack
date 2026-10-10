package s3tables

import (
	"fmt"
	"slices"
)

const (
	metaKeyIceberg = "iceberg"
	metaKeyFields  = "fields"
)

func metaObject(parent map[string]any, key string) (map[string]any, bool, error) {
	raw, ok := parent[key]
	if !ok || raw == nil {
		return nil, false, nil
	}

	obj, isObj := raw.(map[string]any)
	if !isObj {
		return nil, false, fmt.Errorf("%w: metadata %s must be an object", errInvalidRequest, key)
	}

	return obj, true, nil
}

func metaFields(parent map[string]any, path string) ([]map[string]any, error) {
	raw, ok := parent[metaKeyFields].([]any)
	if !ok {
		return nil, fmt.Errorf("%w: %s.fields is required", errInvalidRequest, path)
	}

	out := make([]map[string]any, 0, len(raw))

	for i, item := range raw {
		obj, isObj := item.(map[string]any)
		if !isObj {
			return nil, fmt.Errorf("%w: %s.fields[%d] must be an object", errInvalidRequest, path, i)
		}

		out = append(out, obj)
	}

	return out, nil
}

func requireKeys(obj map[string]any, path string, keys ...string) error {
	for _, k := range keys {
		if v, ok := obj[k]; !ok || v == nil {
			return fmt.Errorf("%w: %s.%s is required", errInvalidRequest, path, k)
		}
	}

	return nil
}

func validateFieldList(parent map[string]any, path string, required ...string) error {
	fields, err := metaFields(parent, path)
	if err != nil {
		return err
	}

	for i, f := range fields {
		if kErr := requireKeys(f, fmt.Sprintf("%s.fields[%d]", path, i), required...); kErr != nil {
			return kErr
		}
	}

	return nil
}

func validateEnumField(path string, fields []map[string]any, key string, allowed ...string) error {
	for i, f := range fields {
		val, _ := f[key].(string)
		if !slices.Contains(allowed, val) {
			return fmt.Errorf("%w: %s.fields[%d].%s must be one of %v, got %q",
				errInvalidRequest, path, i, key, allowed, val)
		}
	}

	return nil
}

func validateIcebergSchemaV2(schema map[string]any) error {
	if err := validateFieldList(schema, "schemaV2", "id", "name", "required", "type"); err != nil {
		return err
	}

	if t, _ := schema["type"].(string); t != "struct" {
		return fmt.Errorf("%w: schemaV2.type must be struct", errInvalidRequest)
	}

	return nil
}

func validateIcebergWriteOrder(order map[string]any) error {
	if err := requireKeys(order, "writeOrder", "order-id"); err != nil {
		return err
	}

	if err := validateFieldList(order, "writeOrder", "direction", "null-order", "source-id", "transform"); err != nil {
		return err
	}

	fields, _ := metaFields(order, "writeOrder")

	if err := validateEnumField("writeOrder", fields, "direction", "asc", "desc"); err != nil {
		return err
	}

	return validateEnumField("writeOrder", fields, "null-order", "nulls-first", "nulls-last")
}

// validateTableMetadata checks CreateTable's Metadata union against the required members and
// enums of types.TableMetadataMemberIceberg (s3tables SDK types.go IcebergMetadata and children).
func validateTableMetadata(meta map[string]any) error {
	if meta == nil {
		return nil
	}

	iceberg, ok, err := metaObject(meta, metaKeyIceberg)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("%w: metadata must carry the iceberg member", errInvalidRequest)
	}

	if schema, present, sErr := metaObject(iceberg, "schema"); sErr != nil {
		return sErr
	} else if present {
		if fErr := validateFieldList(schema, "schema", "name", "type"); fErr != nil {
			return fErr
		}
	}

	if schema, present, sErr := metaObject(iceberg, "schemaV2"); sErr != nil {
		return sErr
	} else if present {
		if fErr := validateIcebergSchemaV2(schema); fErr != nil {
			return fErr
		}
	}

	if spec, present, sErr := metaObject(iceberg, "partitionSpec"); sErr != nil {
		return sErr
	} else if present {
		if fErr := validateFieldList(spec, "partitionSpec", "name", "source-id", "transform"); fErr != nil {
			return fErr
		}
	}

	if order, present, sErr := metaObject(iceberg, "writeOrder"); sErr != nil {
		return sErr
	} else if present {
		return validateIcebergWriteOrder(order)
	}

	return nil
}

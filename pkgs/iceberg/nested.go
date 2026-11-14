package iceberg

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/hivetype"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const keyElement = "element"

// TypeFromHive converts a Hive/Athena column type, nested ones included, to an Iceberg type,
// assigning fresh field ids to nested fields from *next onward.
func TypeFromHive(hive string, next *int) (jsontext.Value, error) {
	t, err := hivetype.Parse(hive)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedType, hive)
	}

	v, err := typeOf(t, hive, next)
	if err != nil {
		return nil, err
	}

	return json.Marshal(v, jsonx.V1())
}

func typeOf(t *hivetype.Type, raw string, next *int) (any, error) {
	switch t.Kind {
	case hivetype.Array:
		id := *next
		*next++

		elem, err := typeOf(t.Elem, raw, next)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			keyType: "list", "element-id": id, keyElement: elem, "element-required": false,
		}, nil
	case hivetype.Map:
		kid, vid := *next, *next+1
		*next += 2

		key, err := typeOf(t.Key, raw, next)
		if err != nil {
			return nil, err
		}

		val, err := typeOf(t.Value, raw, next)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			keyType: typeMap, "key-id": kid, "key": key, "value-id": vid, keyValue: val, "value-required": false,
		}, nil
	case hivetype.Struct:
		ids := make([]int, len(t.Fields))
		for i := range ids {
			ids[i] = *next
			*next++
		}

		fields := make([]map[string]any, len(t.Fields))

		for i, f := range t.Fields {
			ft, err := typeOf(f.Type, raw, next)
			if err != nil {
				return nil, err
			}

			fields[i] = map[string]any{"id": ids[i], keyName: f.Name, "required": false, keyType: ft}
		}

		return map[string]any{keyType: typeStruct, keyFields: fields}, nil
	default:
		prim, err := FromHive(scalarText(t, raw))
		if err != nil {
			return nil, err
		}

		return prim, nil
	}
}

func scalarText(t *hivetype.Type, _ string) string {
	switch t.Kind {
	case hivetype.Decimal:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision, t.Scale)
	case hivetype.Boolean:
		return typeBoolean
	case hivetype.Tinyint:
		return "tinyint"
	case hivetype.Smallint:
		return "smallint"
	case hivetype.Int:
		return typeInt
	case hivetype.Bigint:
		return typeBigint
	case hivetype.Float:
		return typeFloat
	case hivetype.Double:
		return typeDouble
	case hivetype.Binary:
		return typeBinary
	case hivetype.Date:
		return typeDate
	case hivetype.Timestamp:
		return typeTimestamp
	default:
		return typeString
	}
}

// MaxFieldID is the largest field id in s, nested fields included.
func (s Schema) MaxFieldID() int {
	most := 0

	for _, f := range s.Fields {
		most = max(most, f.ID, maxTypeID(f.Type))
	}

	return most
}

func maxTypeID(raw jsontext.Value) int {
	var n nestedType
	if err := json.Unmarshal(raw, &n, jsonx.V1()); err != nil {
		return 0
	}

	most := max(n.ElementID, n.KeyID, n.ValueID)

	for _, f := range n.Fields {
		most = max(most, f.ID, maxTypeID(f.Type))
	}

	for _, sub := range []jsontext.Value{n.Element, n.Key, n.Value} {
		if len(sub) > 0 {
			most = max(most, maxTypeID(sub))
		}
	}

	return most
}

// NestedIDs maps the logical path of every nested field below a column to its field id
// (struct members by name, "element" for list items, "key" and "value" for maps), e.g. "tags.element".
func NestedIDs(f Field) map[string]int {
	out := map[string]int{}
	collectIDs(f.Name, f.Type, out)

	return out
}

func collectIDs(path string, raw jsontext.Value, out map[string]int) {
	var n nestedType
	if err := json.Unmarshal(raw, &n, jsonx.V1()); err != nil {
		return
	}

	for _, f := range n.Fields {
		out[path+"."+f.Name] = f.ID
		collectIDs(path+"."+f.Name, f.Type, out)
	}

	if n.ElementID > 0 {
		out[path+"."+keyElement] = n.ElementID
		collectIDs(path+"."+keyElement, n.Element, out)
	}

	if n.KeyID > 0 {
		out[path+".key"] = n.KeyID
		collectIDs(path+".key", n.Key, out)
		out[path+".value"] = n.ValueID
		collectIDs(path+".value", n.Value, out)
	}
}

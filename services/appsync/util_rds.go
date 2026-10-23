package appsync

import (
	"encoding/json"
	"fmt"
)

const rdsLongValue = "longValue"

func rdsFieldKeys() []string {
	return []string{"stringValue", rdsLongValue, "doubleValue", "booleanValue", "arrayValue", "blobValue"}
}

// utilRDS is $util.rds: converters from the AppSync RDS result string.
func utilRDS() map[string]utilFn {
	return map[string]utilFn{
		"rds.toJsonObject": func(_ *utilEnv, a []any) (any, error) { return rdsToObjects(argAt(a, 0)) },
		"rds.toJsonString": func(_ *utilEnv, a []any) (any, error) {
			objs, err := rdsToObjects(argAt(a, 0))
			if err != nil {
				return nil, err
			}

			raw, err := json.Marshal(objs)

			return string(raw), err
		},
	}
}

// rdsToObjects turns {"sqlStatementResults":[...]} (string or decoded) into
// one list of column-keyed records per statement.
func rdsToObjects(v any) ([]any, error) {
	root, err := rdsResultRoot(v)
	if err != nil {
		return nil, err
	}

	results, _ := root["sqlStatementResults"].([]any)
	out := make([]any, 0, len(results))

	for _, r := range results {
		out = append(out, rdsStatementRecords(r))
	}

	return out, nil
}

func rdsResultRoot(v any) (map[string]any, error) {
	switch t := v.(type) {
	case map[string]any:
		return t, nil
	case string:
		var m map[string]any
		if err := json.Unmarshal([]byte(t), &m); err != nil {
			return nil, fmt.Errorf("%w: invalid RDS result JSON", ErrValidation)
		}

		return m, nil
	}

	return nil, fmt.Errorf("%w: RDS result must be a JSON string", ErrValidation)
}

func rdsStatementRecords(stmt any) []any {
	m, _ := stmt.(map[string]any)
	cols, _ := m["columnMetadata"].([]any)
	records, _ := m["records"].([]any)
	out := make([]any, 0, len(records))

	for _, rec := range records {
		row := map[string]any{}
		fields, _ := rec.([]any)

		for i, f := range fields {
			row[rdsColumnName(cols, i)] = rdsFieldValue(f)
		}

		out = append(out, row)
	}

	return out
}

func rdsColumnName(cols []any, i int) string {
	if i < len(cols) {
		c, _ := cols[i].(map[string]any)
		if n := firstNonEmpty(strVal(c["label"]), strVal(c["name"])); n != "" {
			return n
		}
	}

	return fmt.Sprintf("column%d", i)
}

func rdsFieldValue(f any) any {
	m, _ := f.(map[string]any)
	if null, _ := m["isNull"].(bool); null {
		return nil
	}

	for _, k := range rdsFieldKeys() {
		v, ok := m[k]
		if !ok {
			continue
		}

		if n, isNum := v.(float64); isNum && k == rdsLongValue {
			return int64(n)
		}

		if k == "arrayValue" {
			return rdsArrayValue(v)
		}

		return v
	}

	return nil
}

func rdsArrayValue(v any) any {
	m, _ := v.(map[string]any)

	for _, vals := range m {
		list, ok := vals.([]any)
		if !ok {
			continue
		}

		out := make([]any, len(list))
		for i, e := range list {
			out[i] = e
			if nested, isMap := e.(map[string]any); isMap {
				out[i] = rdsArrayValue(nested)
			}
		}

		return out
	}

	return []any{}
}

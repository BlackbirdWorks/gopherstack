package gcpbigquery

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

type paramType struct {
	ArrayType   *paramType   `json:"arrayType,omitempty"`
	Type        string       `json:"type"`
	StructTypes []structType `json:"structTypes,omitempty"`
}

type structType struct {
	Name string    `json:"name"`
	Type paramType `json:"type"`
}

type paramValue struct {
	Value        *string               `json:"value,omitempty"`
	StructValues map[string]paramValue `json:"structValues,omitempty"`
	ArrayValues  []paramValue          `json:"arrayValues,omitempty"`
}

type queryParam struct {
	Name           string     `json:"name"`
	ParameterType  paramType  `json:"parameterType"`
	ParameterValue paramValue `json:"parameterValue"`
}

// field renders the parameter type as a schema field so values can reuse the table value parser.
func (t paramType) field(name string) bqsql.Field {
	switch strings.ToUpper(t.Type) {
	case "ARRAY":
		if t.ArrayType == nil {
			return bqsql.Field{Name: name, Type: "STRING", Mode: bqsql.ModeRepeated}
		}

		f := t.ArrayType.field(name)
		f.Mode = bqsql.ModeRepeated

		return f
	case "STRUCT", litRecord:
		f := bqsql.Field{Name: name, Type: litRecord, Mode: bqsql.ModeNullable}
		for _, st := range t.StructTypes {
			f.Fields = append(f.Fields, st.Type.field(st.Name))
		}

		return f
	}

	return bqsql.Field{Name: name, Type: t.Type, Mode: bqsql.ModeNullable}
}

// tree converts a parameter value into the decoded-JSON shape bqsql.ParseValue expects.
func (v paramValue) tree(t paramType) any {
	switch strings.ToUpper(t.Type) {
	case "ARRAY":
		out := make([]any, len(v.ArrayValues))

		for i, e := range v.ArrayValues {
			if t.ArrayType != nil {
				out[i] = e.tree(*t.ArrayType)
			}
		}

		return out
	case "STRUCT", litRecord:
		out := map[string]any{}

		for _, st := range t.StructTypes {
			if sv, ok := v.StructValues[st.Name]; ok {
				out[st.Name] = sv.tree(st.Type)
			}
		}

		return out
	}

	if v.Value == nil {
		return nil
	}

	return *v.Value
}

func engineParams(in []queryParam, mode string) ([]bqsql.Param, error) {
	out := make([]bqsql.Param, 0, len(in))

	for _, p := range in {
		name := p.Name
		if strings.EqualFold(mode, "POSITIONAL") {
			name = ""
		}

		f := p.ParameterType.field(name)
		if _, ok := bqsql.KindOf(f.Type); !ok {
			return nil, errInvalidQuery("Invalid query parameter type " + p.ParameterType.Type)
		}

		v, err := bqsql.ParseValue(f, p.ParameterValue.tree(p.ParameterType), bqsql.ParseOptions{})
		if err != nil {
			return nil, errInvalidQuery("Invalid value for query parameter " + p.Name + ": " + err.Error())
		}

		out = append(out, bqsql.Param{Name: name, Field: f, Value: v})
	}

	return out, nil
}

package ssm

import (
	"encoding/json"
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// DocumentParameter is one declared document parameter (types.DocumentParameter).
type DocumentParameter struct {
	Name         string `json:"Name"`
	Type         string `json:"Type,omitempty"`
	Description  string `json:"Description,omitempty"`
	DefaultValue string `json:"DefaultValue,omitempty"`
}

const documentParameterTypeStringList = "StringList"

// parseDocumentParameters derives DocumentDescription.Parameters from the top-level "parameters"
// block; DocumentParameterType has only String and StringList, so non-list types report String.
func parseDocumentParameters(content, format string) []DocumentParameter {
	if content == "" {
		return nil
	}

	var doc struct {
		Parameters map[string]struct {
			Default     any    `json:"default" yaml:"default"`
			Type        string `json:"type" yaml:"type"`
			Description string `json:"description" yaml:"description"`
		} `json:"parameters" yaml:"parameters"`
	}

	var err error
	if format == "YAML" {
		err = yaml.Unmarshal([]byte(content), &doc)
	} else {
		err = json.Unmarshal([]byte(content), &doc)
	}

	if err != nil || len(doc.Parameters) == 0 {
		return nil
	}

	out := make([]DocumentParameter, 0, len(doc.Parameters))

	for name, p := range doc.Parameters {
		typ := "String"
		if p.Type == documentParameterTypeStringList {
			typ = documentParameterTypeStringList
		}

		out = append(out, DocumentParameter{
			Name: name, Type: typ, Description: p.Description, DefaultValue: documentDefaultString(p.Default),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func documentDefaultString(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	default:
		if b, err := json.Marshal(val); err == nil {
			return string(b)
		}

		return fmt.Sprint(val)
	}
}

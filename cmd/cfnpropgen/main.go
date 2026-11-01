// Command cfnpropgen builds services/cloudformation/cfn_property_schemas.json
// from the official CloudFormation resource specification (PrimitiveType/Type
// per property). Only types listed in -types (the provisionable set) are kept.
//
//	go run ./cmd/cfnpropgen -spec <full-spec.json> \
//	  -types services/cloudformation/cfn_provisionable_types.json \
//	  -out services/cloudformation/cfn_property_schemas.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
)

type propertySpec struct {
	PrimitiveType string `json:"PrimitiveType"`
	Type          string `json:"Type"`
	Required      bool   `json:"Required"`
}

type resourceTypeSpec struct {
	Properties map[string]propertySpec `json:"Properties"`
}

type resourceSpec struct {
	ResourceTypes map[string]resourceTypeSpec `json:"ResourceTypes"`
}

func main() {
	specPath := flag.String("spec", "", "full CloudFormation resource specification JSON")
	typesPath := flag.String("types", "", "JSON array of resource types to include")
	outPath := flag.String("out", "", "output schema JSON")
	reqPath := flag.String("required-out", "", "output required-properties JSON (optional)")
	flag.Parse()

	if *specPath == "" || *typesPath == "" || *outPath == "" {
		fmt.Fprintln(os.Stderr, "usage: cfnpropgen -spec <spec.json> -types <types.json> -out <out.json>")
		os.Exit(1)
	}

	if err := run(*specPath, *typesPath, *outPath, *reqPath); err != nil {
		fmt.Fprintln(os.Stderr, "cfnpropgen:", err)
		os.Exit(1)
	}
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, v)
}

func run(specPath, typesPath, outPath, reqPath string) error {
	var spec resourceSpec
	if err := readJSON(specPath, &spec); err != nil {
		return fmt.Errorf("load spec: %w", err)
	}

	var types []string
	if err := readJSON(typesPath, &types); err != nil {
		return fmt.Errorf("load types: %w", err)
	}

	out, err := json.MarshalIndent(buildSchemas(&spec, types), "", "  ")
	if err != nil {
		return fmt.Errorf("render: %w", err)
	}

	if err = os.WriteFile(outPath, append(out, '\n'), 0o600); err != nil {
		return err
	}

	if reqPath == "" {
		return nil
	}

	req, err := json.MarshalIndent(buildRequired(&spec, types), "", "  ")
	if err != nil {
		return fmt.Errorf("render required: %w", err)
	}

	return os.WriteFile(reqPath, append(req, '\n'), 0o600)
}

// registryRequired are required keys the registry schema enforces but the
// legacy spec leaves optional (metric-math alarms in the spec).
var registryRequired = map[string][]string{ //nolint:gochecknoglobals // static table
	"AWS::CloudWatch::Alarm": {"ComparisonOperator", "EvaluationPeriods"},
}

// registryOptional are spec-required keys the registry schema does not require.
var registryOptional = map[string][]string{ //nolint:gochecknoglobals // static table
	"AWS::SageMaker::Workteam": {"Description"},
}

// buildRequired lists each type's spec-required properties, sorted.
func buildRequired(spec *resourceSpec, types []string) map[string][]string {
	out := map[string][]string{}

	for _, name := range types {
		req := slices.Clone(registryRequired[name])

		for prop, ps := range spec.ResourceTypes[name].Properties {
			if ps.Required && !slices.Contains(registryOptional[name], prop) {
				req = append(req, prop)
			}
		}

		if len(req) > 0 {
			slices.Sort(req)

			req = slices.Compact(req)

			out[name] = req
		}
	}

	return out
}

// buildSchemas keeps a type only when the spec documents its Properties.
func buildSchemas(spec *resourceSpec, types []string) map[string]map[string]string {
	out := make(map[string]map[string]string, len(types))

	for _, name := range types {
		rt, ok := spec.ResourceTypes[name]
		if !ok || len(rt.Properties) == 0 {
			continue
		}

		props := make(map[string]string, len(rt.Properties))
		for prop, ps := range rt.Properties {
			props[prop] = kindOf(ps)
		}

		out[name] = props
	}

	return out
}

// kindOf maps a spec property to the kind vocabulary the validator reads.
func kindOf(ps propertySpec) string {
	switch ps.PrimitiveType {
	case "String", "Timestamp":
		return "string"
	case "Integer", "Long", "Double":
		return "number"
	case "Boolean":
		return "boolean"
	case "Json":
		return "any"
	}

	switch ps.Type {
	case "List":
		return "list"
	case "Map":
		return "map"
	case "":
		return "any"
	}

	return "map"
}

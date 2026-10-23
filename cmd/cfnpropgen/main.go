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
)

type propertySpec struct {
	PrimitiveType string `json:"PrimitiveType"`
	Type          string `json:"Type"`
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
	flag.Parse()

	if *specPath == "" || *typesPath == "" || *outPath == "" {
		fmt.Fprintln(os.Stderr, "usage: cfnpropgen -spec <spec.json> -types <types.json> -out <out.json>")
		os.Exit(1)
	}

	if err := run(*specPath, *typesPath, *outPath); err != nil {
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

func run(specPath, typesPath, outPath string) error {
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

	return os.WriteFile(outPath, append(out, '\n'), 0o600)
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

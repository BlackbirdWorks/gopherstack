package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKindOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		in   propertySpec
	}{
		{name: "string", in: propertySpec{PrimitiveType: "String"}, want: "string"},
		{name: "timestamp", in: propertySpec{PrimitiveType: "Timestamp"}, want: "string"},
		{name: "long", in: propertySpec{PrimitiveType: "Long"}, want: "number"},
		{name: "bool", in: propertySpec{PrimitiveType: "Boolean"}, want: "boolean"},
		{name: "json", in: propertySpec{PrimitiveType: "Json"}, want: "any"},
		{name: "list", in: propertySpec{Type: "List"}, want: "list"},
		{name: "map", in: propertySpec{Type: "Map"}, want: "map"},
		{name: "complex", in: propertySpec{Type: "Encryption"}, want: "map"},
		{name: "empty", in: propertySpec{}, want: "any"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, kindOf(tt.in))
		})
	}
}

func TestBuildSchemas(t *testing.T) {
	t.Parallel()

	spec := &resourceSpec{ResourceTypes: map[string]resourceTypeSpec{
		"AWS::A::B": {Properties: map[string]propertySpec{"N": {PrimitiveType: "String"}}},
		"AWS::C::D": {},
	}}

	got := buildSchemas(spec, []string{"AWS::A::B", "AWS::C::D", "AWS::E::F"})
	assert.Equal(t, map[string]map[string]string{"AWS::A::B": {"N": "string"}}, got)
}

func TestBuildRequired(t *testing.T) {
	t.Parallel()

	spec := &resourceSpec{ResourceTypes: map[string]resourceTypeSpec{
		"AWS::A::B": {Properties: map[string]propertySpec{
			"Z": {PrimitiveType: "String", Required: true},
			"A": {PrimitiveType: "String", Required: true},
			"O": {PrimitiveType: "String"},
		}},
		"AWS::SageMaker::Workteam": {Properties: map[string]propertySpec{"Description": {Required: true}}},
		"AWS::CloudWatch::Alarm":   {Properties: map[string]propertySpec{"Namespace": {}}},
		"AWS::C::D":                {Properties: map[string]propertySpec{"N": {}}},
	}}

	got := buildRequired(spec, []string{
		"AWS::A::B", "AWS::C::D", "AWS::SageMaker::Workteam", "AWS::CloudWatch::Alarm", "AWS::E::F",
	})
	assert.Equal(t, map[string][]string{
		"AWS::A::B":              {"A", "Z"},
		"AWS::CloudWatch::Alarm": {"ComparisonOperator", "EvaluationPeriods"},
	}, got)
}

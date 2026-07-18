package cloudformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		wantDesc     string
		wantResource string
		wantErr      bool
	}{
		{
			name:         "json_valid",
			input:        simpleTemplate,
			wantResource: "MyBucket",
		},
		{
			name:         "yaml_valid",
			input:        yamlTemplate,
			wantDesc:     "YAML template",
			wantResource: "MyQueue",
		},
		{
			name:    "empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid_json",
			input:   "{invalid json}",
			wantErr: true,
		},
		{
			name:    "invalid_yaml",
			input:   ":\n  bad: [unclosed",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := cloudformation.ParseTemplate(tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, tmpl)

			if tt.wantDesc != "" {
				assert.Equal(t, tt.wantDesc, tmpl.Description)
			}

			if tt.wantResource != "" {
				assert.Contains(t, tmpl.Resources, tt.wantResource)
			}
		})
	}
}

func TestResolveParameters(t *testing.T) {
	t.Parallel()

	tmpl, err := cloudformation.ParseTemplate(templateWithParams)
	require.NoError(t, err)

	tests := []struct {
		name    string
		wantVal string
		params  []cloudformation.Parameter
	}{
		{
			name:    "defaults_only",
			params:  nil,
			wantVal: "default-bucket",
		},
		{
			name: "override",
			params: []cloudformation.Parameter{
				{ParameterKey: "BucketName", ParameterValue: "my-bucket"},
			},
			wantVal: "my-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resolved := cloudformation.ResolveParameters(tmpl, tt.params)
			assert.Equal(t, tt.wantVal, resolved["BucketName"])
		})
	}
}

func TestResolveValue(t *testing.T) {
	t.Parallel()

	params := map[string]string{"Env": "prod"}
	ids := map[string]string{"MyQueue": "https://queue-url"}

	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"string", "hello", "hello"},
		{"bool_true", true, "true"},
		{"bool_false", false, "false"},
		{"int", 42, "42"},
		{"float", 3.14, "3.14"},
		{"nil", nil, ""},
		{"ref_param", map[string]any{"Ref": "Env"}, "prod"},
		{"ref_physical", map[string]any{"Ref": "MyQueue"}, "https://queue-url"},
		{"ref_missing", map[string]any{"Ref": "Unknown"}, "Unknown"},
		{"fn_sub", map[string]any{"Fn::Sub": "env-${Env}"}, "env-prod"},
		{"fn_join", map[string]any{"Fn::Join": []any{"-", []any{"a", "b", "c"}}}, "a-b-c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.ResolveValue(tt.input, params, ids)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestTopoSortResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		resources map[string]cloudformation.TemplateResource
		// wantBefore asserts pairs [before, after] that must hold in the result.
		wantBefore [][2]string
	}{
		{
			name: "no_dependencies_alphabetical",
			resources: map[string]cloudformation.TemplateResource{
				"Zebra": {Type: "AWS::S3::Bucket"},
				"Apple": {Type: "AWS::S3::Bucket"},
				"Mango": {Type: "AWS::S3::Bucket"},
			},
			wantBefore: [][2]string{
				{"Apple", "Mango"},
				{"Apple", "Zebra"},
				{"Mango", "Zebra"},
			},
		},
		{
			name: "single_dependency",
			resources: map[string]cloudformation.TemplateResource{
				"Service": {Type: "AWS::ECS::Service", DependsOn: []string{"Cluster"}},
				"Cluster": {Type: "AWS::ECS::Cluster"},
			},
			wantBefore: [][2]string{
				{"Cluster", "Service"},
			},
		},
		{
			name: "chain_dependency",
			resources: map[string]cloudformation.TemplateResource{
				"C": {Type: "AWS::S3::Bucket", DependsOn: []string{"B"}},
				"B": {Type: "AWS::S3::Bucket", DependsOn: []string{"A"}},
				"A": {Type: "AWS::S3::Bucket"},
			},
			wantBefore: [][2]string{
				{"A", "B"},
				{"B", "C"},
				{"A", "C"},
			},
		},
		{
			name: "multiple_dependencies",
			resources: map[string]cloudformation.TemplateResource{
				"Service": {Type: "AWS::ECS::Service", DependsOn: []string{"Cluster", "TaskDef"}},
				"Cluster": {Type: "AWS::ECS::Cluster"},
				"TaskDef": {Type: "AWS::ECS::TaskDefinition"},
			},
			wantBefore: [][2]string{
				{"Cluster", "Service"},
				{"TaskDef", "Service"},
			},
		},
		{
			name:      "empty",
			resources: map[string]cloudformation.TemplateResource{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := cloudformation.TopoSortResources(tt.resources)

			assert.Len(t, result, len(tt.resources))

			pos := make(map[string]int, len(result))
			for i, id := range result {
				pos[id] = i
			}

			for _, pair := range tt.wantBefore {
				before, after := pair[0], pair[1]
				assert.Less(t, pos[before], pos[after],
					"expected %s (pos %d) before %s (pos %d)", before, pos[before], after, pos[after])
			}
		})
	}
}

func TestParseDependsOn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input any
		want  []string
	}{
		{
			name:  "nil",
			input: nil,
			want:  nil,
		},
		{
			name:  "single_string",
			input: "MyCluster",
			want:  []string{"MyCluster"},
		},
		{
			name:  "slice_of_strings",
			input: []any{"A", "B"},
			want:  []string{"A", "B"},
		},
		{
			name:  "empty_slice",
			input: []any{},
			want:  []string{},
		},
		{
			name:  "unknown_type",
			input: 42,
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.ParseDependsOn(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseTemplate_DependsOn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDependsOn map[string][]string
		name          string
		template      string
	}{
		{
			name: "string_depends_on",
			template: `{
"AWSTemplateFormatVersion":"2010-09-09",
"Resources":{
"Svc":{"Type":"AWS::ECS::Service","DependsOn":"Cluster","Properties":{}},
"Cluster":{"Type":"AWS::ECS::Cluster","Properties":{}}
}}`,
			wantDependsOn: map[string][]string{
				"Svc":     {"Cluster"},
				"Cluster": nil,
			},
		},
		{
			name: "array_depends_on",
			template: `{
"AWSTemplateFormatVersion":"2010-09-09",
"Resources":{
"Svc":{"Type":"AWS::ECS::Service","DependsOn":["Cluster","TaskDef"],"Properties":{}},
"Cluster":{"Type":"AWS::ECS::Cluster","Properties":{}},
"TaskDef":{"Type":"AWS::ECS::TaskDefinition","Properties":{}}
}}`,
			wantDependsOn: map[string][]string{
				"Svc":     {"Cluster", "TaskDef"},
				"Cluster": nil,
				"TaskDef": nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := cloudformation.ParseTemplate(tt.template)
			require.NoError(t, err)

			for logicalID, wantDeps := range tt.wantDependsOn {
				res, ok := tmpl.Resources[logicalID]
				require.True(t, ok, "resource %s should exist", logicalID)
				assert.Equal(t, wantDeps, res.DependsOn, "DependsOn for %s", logicalID)
			}
		})
	}
}

func TestParseTemplate_FnForEach(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		template     string
		wantResource []string
		wantAbsent   []string
	}{
		{
			name: "list literal expansion",
			template: `{"Resources":{
				"Fn::ForEach::Topics":[
					"Name",
					["Alpha","Beta","Gamma"],
					{"Topic${Name}":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"${Name}"}}}
				]
			}}`,
			wantResource: []string{"TopicAlpha", "TopicBeta", "TopicGamma"},
		},
		{
			name: "loop coexists with static resource",
			template: `{"Resources":{
				"MainBucket":{"Type":"AWS::S3::Bucket","Properties":{}},
				"Fn::ForEach::Queues":[
					"Q",
					["one","two"],
					{"Queue${Q}":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"${Q}"}}}
				]
			}}`,
			wantResource: []string{"MainBucket", "Queueone", "Queuetwo"},
		},
		{
			name: "comma-delimited string collection",
			template: `{"Resources":{
				"Fn::ForEach::Items":[
					"I",
					"x,y",
					{"Res${I}":{"Type":"AWS::SNS::Topic","Properties":{}}}
				]
			}}`,
			wantResource: []string{"Resx", "Resy"},
			wantAbsent:   []string{"Fn::ForEach::Items"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := cloudformation.ParseTemplate(tc.template)
			require.NoError(t, err)

			for _, id := range tc.wantResource {
				_, ok := tmpl.Resources[id]
				assert.True(t, ok, "expected expanded resource %q", id)
			}
			for _, id := range tc.wantAbsent {
				_, ok := tmpl.Resources[id]
				assert.False(t, ok, "loop key %q must not survive expansion", id)
			}
		})
	}
}

func TestParseTemplate_FnForEach_Substitutes(t *testing.T) {
	t.Parallel()

	const tpl = `{"Resources":{
		"Fn::ForEach::Topics":[
			"Name",
			["Alpha"],
			{"Topic${Name}":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"prefix-${Name}"}}}
		]
	}}`

	tmpl, err := cloudformation.ParseTemplate(tpl)
	require.NoError(t, err)

	res, ok := tmpl.Resources["TopicAlpha"]
	require.True(t, ok)
	assert.Equal(t, "AWS::SNS::Topic", res.Type)
	assert.Equal(t, "prefix-Alpha", res.Properties["TopicName"])
}

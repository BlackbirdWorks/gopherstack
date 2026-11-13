package cloudformation_test

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func expandedResources(t *testing.T, template, region string) map[string]map[string]any {
	t.Helper()

	out, err := cloudformation.ProcessedTemplateBody(template, region)
	require.NoError(t, err)
	var doc struct {
		Resources map[string]map[string]any `json:"Resources"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &doc, jsonx.V1()))

	return doc.Resources
}

func TestSAMTransform_GlobalsArchitecturesReplace(t *testing.T) {
	t.Parallel()

	const template = samHeader + `
Globals:
  Function:
    Runtime: python3.12
    Handler: app.handler
    Architectures:
      - x86_64
Resources:
  Inherits:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: s3://bucket/code.zip
  Overrides:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: s3://bucket/code.zip
      Architectures:
        - arm64
`

	tests := []struct {
		name string
		id   string
		want []any
	}{
		{name: "inherits", id: "Inherits", want: []any{"x86_64"}},
		{name: "overrides", id: "Overrides", want: []any{"arm64"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := expandedResources(t, template, "")
			props, _ := res[tt.id]["Properties"].(map[string]any)
			assert.Equal(t, tt.want, props["Architectures"])
		})
	}
}

func TestSAMTransform_RegionInParameterResolvedProperties(t *testing.T) {
	t.Parallel()

	const template = samHeader + `
Resources:
  Layer:
    Type: AWS::Serverless::LayerVersion
    Properties:
      ContentUri: s3://bucket/layer.zip
      LayerName: !Ref AWS::Region
`

	tests := []struct {
		want   any
		name   string
		region string
	}{
		{name: "resolved", region: "eu-west-1", want: "eu-west-1"},
		{name: "kept", region: "", want: map[string]any{"Ref": "AWS::Region"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := expandedResources(t, template, tt.region)
			for _, r := range res {
				props, _ := r["Properties"].(map[string]any)
				if r["Type"] == "AWS::Lambda::LayerVersion" {
					assert.Equal(t, tt.want, props["LayerName"])

					return
				}
			}
			t.Fatal("no layer generated")
		})
	}
}

package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestValidatePropertySchemas(t *testing.T) {
	t.Parallel()

	schemas := map[string]map[string]string{
		"AWS::Test::Thing": {
			"Name": "string", "Count": "number", "On": "boolean",
			"Items": "list", "Cfg": "map", "Doc": "any",
		},
	}

	tests := []struct {
		props   map[string]any
		name    string
		wantErr string
	}{
		{name: "valid", props: map[string]any{"Name": "a", "Count": 3.0, "On": true}},
		{name: "coerced", props: map[string]any{"Name": 5.0, "Count": "7", "On": "false"}},
		{name: "intrinsic", props: map[string]any{"Count": map[string]any{"Ref": "P"}}},
		{name: "any", props: map[string]any{"Doc": []any{1.0}}},
		{name: "unknown", props: map[string]any{"Bogus": "x"}, wantErr: "Encountered unsupported property Bogus"},
		{
			name: "string-list", props: map[string]any{"Name": []any{"a"}},
			wantErr: "#/Name: expected type: string, found: JSONArray",
		},
		{
			name: "number-text", props: map[string]any{"Count": "abc"},
			wantErr: "#/Count: expected type: number, found: String",
		},
		{name: "bool-num", props: map[string]any{"On": 1.0}, wantErr: "#/On: expected type: boolean"},
		{name: "list-string", props: map[string]any{"Items": "x"}, wantErr: "#/Items: expected type: list"},
		{name: "map-list", props: map[string]any{"Cfg": []any{}}, wantErr: "#/Cfg: expected type: map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpl := &cloudformation.Template{Resources: map[string]cloudformation.TemplateResource{
				"R": {Type: "AWS::Test::Thing", Properties: tt.props},
			}}
			tmpl.Resources["U"] = cloudformation.TemplateResource{
				Type:       "AWS::Other::Thing",
				Properties: map[string]any{"Any": "x"},
			}

			err := cloudformation.ValidatePropertySchemasForTest(tmpl, schemas)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestEmbeddedPropertySchemasParse(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, cloudformation.EmbeddedPropertySchemasForTest())
}

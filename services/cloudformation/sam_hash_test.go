package cloudformation_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// The expected ids are SHA-1 digests of Python json.dumps(separators=(",", ":"), sort_keys=True),
// the stringification SAM's LogicalIdGenerator applies, computed with hashlib.
func TestSAMTransform_LogicalIDHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
		wantID   string
	}{
		{
			name: "layer_hash_covers_non_ascii_and_escapes",
			template: samHeader + `
Resources:
  UniLayer:
    Type: AWS::Serverless::LayerVersion
    Properties:
      ContentUri: s3://bucket/layer.zip
      Description: "café ☃ 😀 \"quoted\"\ttab"
`,
			wantID: "UniLayer92b1076ce6",
		},
		{
			name: "function_version_hash_covers_code_dict",
			template: samHeader + `
Resources:
  Fn:
    Type: AWS::Serverless::Function
    Properties:
      Handler: hello.handler
      Runtime: python3.12
      CodeUri: s3://sam-demo-bucket/hello.zip
      AutoPublishAlias: live
`,
			wantID: "FnVersion640128d35d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			processed, err := cloudformation.ProcessedTemplateBody(tt.template, "")
			require.NoError(t, err)

			var doc struct {
				Resources map[string]json.RawMessage `json:"Resources"`
			}
			require.NoError(t, json.Unmarshal([]byte(processed), &doc))
			assert.Contains(t, doc.Resources, tt.wantID)
		})
	}
}

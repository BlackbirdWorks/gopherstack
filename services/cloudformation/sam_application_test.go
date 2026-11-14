package cloudformation_test

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestSAMTransform_Application(t *testing.T) {
	t.Parallel()

	const head = "Transform: AWS::Serverless-2016-10-31\nConditions:\n  C:\n    Fn::Equals: [a, a]\nResources:\n  App:\n"
	tests := []struct {
		want    map[string]any
		name    string
		body    string
		wantErr bool
	}{
		{
			name: "url_location",
			body: head + "    Type: AWS::Serverless::Application\n    Condition: C\n    Properties:\n" +
				"      Location: https://s3.amazonaws.com/b/t.yaml\n      Parameters:\n        P: v\n" +
				"      NotificationARNs: [arn:aws:sns:us-east-1:123456789012:t]\n      TimeoutInMinutes: 15\n" +
				"      Tags:\n        K: V\n",
			want: map[string]any{
				"Type":      "AWS::CloudFormation::Stack",
				"Condition": "C",
				"Properties": map[string]any{
					"TemplateURL":      "https://s3.amazonaws.com/b/t.yaml",
					"Parameters":       map[string]any{"P": "v"},
					"NotificationARNs": []any{"arn:aws:sns:us-east-1:123456789012:t"},
					"TimeoutInMinutes": float64(15),
					"Tags": []any{
						map[string]any{"Key": "lambda:createdBy", "Value": "SAM"},
						map[string]any{"Key": "K", "Value": "V"},
					},
				},
			},
		},
		{
			name: "repository_location",
			body: head + "    Type: AWS::Serverless::Application\n    Properties:\n      Location:\n" +
				"        ApplicationId: arn:aws:serverlessrepo:us-east-1:123456789012:applications/a\n" +
				"        SemanticVersion: 1.0.0\n",
			wantErr: true,
		},
		{
			name:    "missing_location",
			body:    head + "    Type: AWS::Serverless::Application\n    Properties:\n      TimeoutInMinutes: 10\n",
			wantErr: true,
		},
		{
			name: "unsupported_property",
			body: head + "    Type: AWS::Serverless::Application\n    Properties:\n      Location: https://x/t.yaml\n" +
				"      TemplateUrl: u\n",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := cloudformation.ProcessedTemplateBody(tc.body, "")
			if tc.wantErr {
				require.ErrorIs(t, err, cloudformation.ErrSAMTransform)

				return
			}
			require.NoError(t, err)
			var doc map[string]map[string]any
			require.NoError(t, json.Unmarshal([]byte(out), &doc, jsonx.V1()))
			assert.Equal(t, tc.want, doc["Resources"]["App"])
		})
	}
}

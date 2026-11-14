package cloudformation_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSAMTransform_StateMachineShape(t *testing.T) {
	t.Parallel()

	const fn = "  Fn:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader
	tests := []struct {
		check func(t *testing.T, res map[string]map[string]any)
		name  string
		body  string
	}{
		{
			name: "substitutions_and_tags",
			body: fn + "  SM:\n    Type: AWS::Serverless::StateMachine\n    Properties:\n      Tags:\n        A: b\n" +
				"      Tracing:\n        Enabled: true\n      Policies:\n        - AmazonDynamoDBFullAccess\n" +
				"      Definition:\n        StartAt: T\n        States:\n          T:\n            Type: Task\n" +
				"            Resource: !GetAtt Fn.Arn\n            End: true\n",
			check: func(t *testing.T, res map[string]map[string]any) {
				t.Helper()
				props := dig(t, res["SM"], "Properties").(map[string]any)
				assert.Equal(
					t,
					map[string]any{"definition_substitution_1": map[string]any{"Fn::GetAtt": []any{"Fn", "Arn"}}},
					props["DefinitionSubstitutions"],
				)
				join := dig(t, props, "DefinitionString", "Fn::Join").([]any)
				assert.Equal(t, "\n", join[0])
				assert.Contains(t, join[1], `        "T": {`)
				assert.Contains(t, join[1], `            "Resource": "${definition_substitution_1}",`)
				assert.Equal(t, []any{
					map[string]any{"Key": "stateMachine:createdBy", "Value": "SAM"},
					map[string]any{"Key": "A", "Value": "b"},
				}, props["Tags"])
				assert.Equal(t, map[string]any{"Enabled": true}, props["TracingConfiguration"])
				managed := dig(t, res["SMRole"], "Properties", "ManagedPolicyArns").([]any)
				assert.Len(t, managed, 2)
			},
		},
		{
			name: "alias_version",
			body: "  SM:\n    Type: AWS::Serverless::StateMachine\n    Properties:\n      AutoPublishAlias: live\n" +
				"      Role: arn:aws:iam::000000000000:role/r\n      Definition:\n        StartAt: P\n" +
				"        States:\n          P:\n            Type: Pass\n            End: true\n",
			check: func(t *testing.T, res map[string]map[string]any) {
				t.Helper()
				assert.Equal(t, "Retain", res["SMVersion"]["DeletionPolicy"])
				assert.Equal(t, "ALL_AT_ONCE", dig(t, res["SMAliaslive"], "Properties", "DeploymentPreference", "Type"))
				assert.Equal(t, map[string]any{"Ref": "SMVersion"},
					dig(t, res["SMAliaslive"], "Properties", "DeploymentPreference", "StateMachineVersionArn"))
			},
		},
		{
			name: "layer_retention",
			body: "  L:\n    Type: AWS::Serverless::LayerVersion\n    Properties:\n      ContentUri: s3://b/k.zip\n" +
				"      RetentionPolicy: DeLeTe\n",
			check: func(t *testing.T, res map[string]map[string]any) {
				t.Helper()
				for id, r := range res {
					if strings.HasPrefix(id, "L") {
						assert.Equal(t, "Delete", r["DeletionPolicy"])
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			deploySAM(t, env.client, samHeader+"Resources:\n"+tc.body)
			res := processedResources(t, env)
			require.NotEmpty(t, res)
			tc.check(t, res)
		})
	}
}

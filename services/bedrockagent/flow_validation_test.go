package bedrockagent_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func flowNode(name string, typ types.FlowNodeType) types.FlowNode {
	n := types.FlowNode{Name: aws.String(name), Type: typ}
	switch typ {
	case types.FlowNodeTypeInput:
		n.Configuration = &types.FlowNodeConfigurationMemberInput{Value: types.InputFlowNodeConfiguration{}}
	case types.FlowNodeTypeOutput:
		n.Configuration = &types.FlowNodeConfigurationMemberOutput{Value: types.OutputFlowNodeConfiguration{}}
	default:
	}

	return n
}

func dataConn(name, source, target string) types.FlowConnection {
	return types.FlowConnection{
		Name: aws.String(name), Source: aws.String(source), Target: aws.String(target),
		Type: types.FlowConnectionTypeData,
		Configuration: &types.FlowConnectionConfigurationMemberData{
			Value: types.FlowDataConnectionConfiguration{
				SourceOutput: aws.String("out"), TargetInput: aws.String("in"),
			},
		},
	}
}

func TestValidateFlowDefinition_RealClient(t *testing.T) {
	t.Parallel()

	in := flowNode("in", types.FlowNodeTypeInput)
	out := flowNode("out", types.FlowNodeTypeOutput)

	tests := []struct {
		check func(t *testing.T, v []types.FlowValidation)
		name  string
		want  []types.FlowValidationType
		def   types.FlowDefinition
	}{
		{
			name: "valid",
			def: types.FlowDefinition{
				Nodes:       []types.FlowNode{in, out},
				Connections: []types.FlowConnection{dataConn("c", "in", "out")},
			},
		},
		{
			name: "missing start and end",
			def:  types.FlowDefinition{Nodes: []types.FlowNode{flowNode("p", types.FlowNodeTypePrompt)}},
			want: []types.FlowValidationType{
				types.FlowValidationTypeMissingStartingNodes, types.FlowValidationTypeMissingEndingNodes,
			},
		},
		{
			name: "unknown source and target",
			def: types.FlowDefinition{
				Nodes:       []types.FlowNode{in, out},
				Connections: []types.FlowConnection{dataConn("bad", "ghost", "phantom")},
			},
			want: []types.FlowValidationType{
				types.FlowValidationTypeUnknownConnectionSource, types.FlowValidationTypeUnknownConnectionTarget,
			},
			check: func(t *testing.T, v []types.FlowValidation) {
				t.Helper()

				d, ok := v[0].Details.(*types.FlowValidationDetailsMemberUnknownConnectionSource)
				require.True(t, ok)
				assert.Equal(t, "bad", aws.ToString(d.Value.Connection))
			},
		},
		{
			name: "duplicate connections",
			def: types.FlowDefinition{
				Nodes:       []types.FlowNode{in, out},
				Connections: []types.FlowConnection{dataConn("a", "in", "out"), dataConn("b", "in", "out")},
			},
			want: []types.FlowValidationType{types.FlowValidationTypeDuplicateConnections},
			check: func(t *testing.T, v []types.FlowValidation) {
				t.Helper()

				d, ok := v[0].Details.(*types.FlowValidationDetailsMemberDuplicateConnections)
				require.True(t, ok)
				assert.Equal(t, "in", aws.ToString(d.Value.Source))
				assert.Equal(t, "out", aws.ToString(d.Value.Target))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			res, err := client.ValidateFlowDefinition(t.Context(), &bedrockagentsdk.ValidateFlowDefinitionInput{
				Definition: &tt.def,
			})
			require.NoError(t, err)
			require.Len(t, res.Validations, len(tt.want))

			for i, want := range tt.want {
				assert.Equal(t, want, res.Validations[i].Type)
				assert.Equal(t, types.FlowValidationSeverityError, res.Validations[i].Severity)
			}

			if tt.check != nil {
				tt.check(t, res.Validations)
			}
		})
	}
}

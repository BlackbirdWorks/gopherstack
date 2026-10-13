package bedrockagent_test

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strOut() types.FlowNodeOutput {
	return types.FlowNodeOutput{Name: aws.String("out"), Type: types.FlowNodeIODataTypeString}
}

func strIn(expr string) types.FlowNodeInput {
	return types.FlowNodeInput{
		Name:       aws.String("in"),
		Type:       types.FlowNodeIODataTypeString,
		Expression: aws.String(expr),
	}
}

func flowNode(name string, typ types.FlowNodeType) types.FlowNode {
	n := types.FlowNode{Name: aws.String(name), Type: typ}

	switch typ {
	case types.FlowNodeTypeInput:
		n.Configuration = &types.FlowNodeConfigurationMemberInput{Value: types.InputFlowNodeConfiguration{}}
		n.Outputs = []types.FlowNodeOutput{strOut()}
	case types.FlowNodeTypeOutput:
		n.Configuration = &types.FlowNodeConfigurationMemberOutput{Value: types.OutputFlowNodeConfiguration{}}
		n.Inputs = []types.FlowNodeInput{strIn("$.data")}
	case types.FlowNodeTypeCondition:
		n.Inputs = []types.FlowNodeInput{strIn("$.data")}
		n.Configuration = &types.FlowNodeConfigurationMemberCondition{Value: types.ConditionFlowNodeConfiguration{
			Conditions: []types.FlowCondition{
				{Name: aws.String("big"), Expression: aws.String("(in > 3)")},
				{Name: aws.String("default")},
			},
		}}
	case types.FlowNodeTypeLoopInput:
		n.Configuration = &types.FlowNodeConfigurationMemberLoopInput{Value: types.LoopInputFlowNodeConfiguration{}}
		n.Outputs = []types.FlowNodeOutput{strOut()}
	case types.FlowNodeTypeLoopController:
		n.Configuration = &types.FlowNodeConfigurationMemberLoopController{
			Value: types.LoopControllerFlowNodeConfiguration{
				ContinueCondition: &types.FlowCondition{Name: aws.String("go"), Expression: aws.String("(in > 3)")},
			},
		}
		n.Inputs = []types.FlowNodeInput{strIn("$.data")}
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

func condConn(name, source, target, cond string) types.FlowConnection {
	return types.FlowConnection{
		Name: aws.String(name), Source: aws.String(source), Target: aws.String(target),
		Type: types.FlowConnectionTypeConditional,
		Configuration: &types.FlowConnectionConfigurationMemberConditional{
			Value: types.FlowConditionalConnectionConfiguration{Condition: aws.String(cond)},
		},
	}
}

func withInputs(n types.FlowNode, ins ...types.FlowNodeInput) types.FlowNode {
	n.Inputs = ins

	return n
}

func withOutputs(n types.FlowNode, outs ...types.FlowNodeOutput) types.FlowNode {
	n.Outputs = outs

	return n
}

func relay(name string) types.FlowNode {
	n := flowNode(name, types.FlowNodeTypeCondition)
	n.Configuration = &types.FlowNodeConfigurationMemberCondition{Value: types.ConditionFlowNodeConfiguration{
		Conditions: []types.FlowCondition{{Name: aws.String("default")}},
	}}
	n.Inputs = nil
	n.Outputs = []types.FlowNodeOutput{strOut()}

	return n
}

func TestValidateFlowDefinition_RealClient(t *testing.T) {
	t.Parallel()

	in := flowNode("in", types.FlowNodeTypeInput)
	out := flowNode("out", types.FlowNodeTypeOutput)

	loopOf := func(body types.FlowDefinition) types.FlowNode {
		return types.FlowNode{
			Name: aws.String("loop"), Type: types.FlowNodeTypeLoop,
			Configuration: &types.FlowNodeConfigurationMemberLoop{
				Value: types.LoopFlowNodeConfiguration{Definition: &body},
			},
		}
	}

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
			def:  types.FlowDefinition{Nodes: []types.FlowNode{relay("p")}},
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
				types.FlowValidationTypeUnfulfilledNodeInput, types.FlowValidationTypeUnreachableNode,
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
			want: []types.FlowValidationType{
				types.FlowValidationTypeDuplicateConnections, types.FlowValidationTypeMultipleNodeInputConnections,
			},
			check: func(t *testing.T, v []types.FlowValidation) {
				t.Helper()

				d, ok := v[0].Details.(*types.FlowValidationDetailsMemberDuplicateConnections)
				require.True(t, ok)
				assert.Equal(t, "in", aws.ToString(d.Value.Source))
				assert.Equal(t, "out", aws.ToString(d.Value.Target))
			},
		},
		{
			name: "unknown output and input",
			def: types.FlowDefinition{
				Nodes: []types.FlowNode{in, out},
				Connections: []types.FlowConnection{{
					Name: aws.String("c"), Source: aws.String("in"), Target: aws.String("out"),
					Type: types.FlowConnectionTypeData,
					Configuration: &types.FlowConnectionConfigurationMemberData{
						Value: types.FlowDataConnectionConfiguration{
							SourceOutput: aws.String("nope"), TargetInput: aws.String("nada"),
						},
					},
				}},
			},
			want: []types.FlowValidationType{
				types.FlowValidationTypeUnknownConnectionSourceOutput,
				types.FlowValidationTypeUnknownConnectionTargetInput,
				types.FlowValidationTypeUnfulfilledNodeInput,
			},
		},
		{
			name: "missing connection configuration",
			def: types.FlowDefinition{
				Nodes: []types.FlowNode{in, out},
				Connections: []types.FlowConnection{{
					Name: aws.String("c"), Source: aws.String("in"), Target: aws.String("out"),
					Type: types.FlowConnectionTypeData,
				}},
			},
			want: []types.FlowValidationType{
				types.FlowValidationTypeMissingConnectionConfiguration,
				types.FlowValidationTypeUnfulfilledNodeInput,
			},
		},
		{
			name: "missing node configuration",
			def: types.FlowDefinition{
				Nodes: []types.FlowNode{
					in, out, {Name: aws.String("p"), Type: types.FlowNodeTypePrompt},
				},
				Connections: []types.FlowConnection{dataConn("c", "in", "out")},
			},
			want: []types.FlowValidationType{
				types.FlowValidationTypeMissingNodeConfiguration, types.FlowValidationTypeUnreachableNode,
			},
		},
		{
			name: "cycle",
			def: types.FlowDefinition{
				Nodes: []types.FlowNode{
					in, out,
					withOutputs(withInputs(relay("a"), strIn("$.data")), strOut()),
					withOutputs(withInputs(relay("b"), strIn("$.data")), strOut()),
				},
				Connections: []types.FlowConnection{
					dataConn("c1", "in", "a"), dataConn("c2", "a", "b"), dataConn("c3", "b", "a"),
					dataConn("c4", "b", "out"),
				},
			},
			want: []types.FlowValidationType{
				types.FlowValidationTypeMultipleNodeInputConnections, types.FlowValidationTypeCyclicConnection,
			},
			check: func(t *testing.T, v []types.FlowValidation) {
				t.Helper()

				d, ok := v[1].Details.(*types.FlowValidationDetailsMemberCyclicConnection)
				require.True(t, ok)
				assert.Equal(t, "c3", aws.ToString(d.Value.Connection))
			},
		},
		{
			name: "unreachable node",
			def: types.FlowDefinition{
				Nodes:       []types.FlowNode{in, out, withInputs(relay("island"))},
				Connections: []types.FlowConnection{dataConn("c", "in", "out")},
			},
			want: []types.FlowValidationType{types.FlowValidationTypeUnreachableNode},
			check: func(t *testing.T, v []types.FlowValidation) {
				t.Helper()

				d, ok := v[0].Details.(*types.FlowValidationDetailsMemberUnreachableNode)
				require.True(t, ok)
				assert.Equal(t, "island", aws.ToString(d.Value.Node))
			},
		},
		{
			name: "malformed input expression",
			def: types.FlowDefinition{
				Nodes:       []types.FlowNode{in, withInputs(out, strIn("data.x"))},
				Connections: []types.FlowConnection{dataConn("c", "in", "out")},
			},
			want: []types.FlowValidationType{types.FlowValidationTypeMalformedNodeInputExpression},
		},
		{
			name: "condition node problems",
			def: types.FlowDefinition{
				Nodes: []types.FlowNode{
					in, out,
					{
						Name: aws.String("cond"), Type: types.FlowNodeTypeCondition,
						Inputs: []types.FlowNodeInput{strIn("$.data")},
						Configuration: &types.FlowNodeConfigurationMemberCondition{
							Value: types.ConditionFlowNodeConfiguration{Conditions: []types.FlowCondition{
								{Name: aws.String("a"), Expression: aws.String("(in > 3")},
								{Name: aws.String("b"), Expression: aws.String("(in > 3")},
							}},
						},
					},
				},
				Connections: []types.FlowConnection{
					dataConn("c", "in", "out"), dataConn("c2", "in", "cond"), condConn("c3", "cond", "out", "ghost"),
				},
			},
			want: []types.FlowValidationType{
				types.FlowValidationTypeMalformedConditionExpression,
				types.FlowValidationTypeMalformedConditionExpression,
				types.FlowValidationTypeDuplicateConditionExpression,
				types.FlowValidationTypeMissingDefaultCondition,
				types.FlowValidationTypeUnknownConnectionCondition,
			},
		},
		{
			name: "loop shape",
			def: types.FlowDefinition{
				Nodes: []types.FlowNode{
					in, out,
					loopOf(types.FlowDefinition{Nodes: []types.FlowNode{
						flowNode("li", types.FlowNodeTypeLoopInput),
						flowNode("li2", types.FlowNodeTypeLoopInput),
						flowNode("bad", types.FlowNodeTypeInput),
					}}),
				},
				Connections: []types.FlowConnection{dataConn("c", "in", "out")},
			},
			want: []types.FlowValidationType{
				types.FlowValidationTypeLoopIncompatibleNodeType,
				types.FlowValidationTypeMultipleLoopInputNodes,
				types.FlowValidationTypeMissingLoopControllerNode,
				types.FlowValidationTypeUnreachableNode,
				types.FlowValidationTypeUnreachableNode,
			},
			check: func(t *testing.T, v []types.FlowValidation) {
				t.Helper()

				idx := slices.IndexFunc(v, func(x types.FlowValidation) bool {
					return x.Type == types.FlowValidationTypeLoopIncompatibleNodeType
				})
				require.GreaterOrEqual(t, idx, 0)

				d, ok := v[idx].Details.(*types.FlowValidationDetailsMemberLoopIncompatibleNodeType)
				require.True(t, ok)
				assert.Equal(t, "loop", aws.ToString(d.Value.Node))
				assert.Equal(t, "bad", aws.ToString(d.Value.IncompatibleNodeName))
				assert.Equal(t, types.IncompatibleLoopNodeTypeInput, d.Value.IncompatibleNodeType)
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

			got := make([]types.FlowValidationType, 0, len(res.Validations))
			for _, v := range res.Validations {
				got = append(got, v.Type)
				assert.Equal(t, types.FlowValidationSeverityError, v.Severity)
			}

			require.ElementsMatch(t, tt.want, got)

			if tt.check != nil {
				tt.check(t, res.Validations)
			}
		})
	}
}

func TestPrepareFlow_ValidationOutcome(t *testing.T) {
	t.Parallel()

	in := flowNode("in", types.FlowNodeTypeInput)
	out := flowNode("out", types.FlowNodeTypeOutput)

	tests := []struct {
		name            string
		def             *types.FlowDefinition
		wantStatus      types.FlowStatus
		wantValidations []types.FlowValidationType
	}{
		{
			name: "valid definition", wantStatus: types.FlowStatusPrepared,
			def: &types.FlowDefinition{
				Nodes:       []types.FlowNode{in, out},
				Connections: []types.FlowConnection{dataConn("c", "in", "out")},
			},
		},
		{
			name: "invalid definition", wantStatus: types.FlowStatusFailed,
			def:             &types.FlowDefinition{Nodes: []types.FlowNode{in}},
			wantValidations: []types.FlowValidationType{types.FlowValidationTypeMissingEndingNodes},
		},
		{name: "no definition", wantStatus: types.FlowStatusPrepared},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			created, err := client.CreateFlow(t.Context(), &bedrockagentsdk.CreateFlowInput{
				Name:             aws.String("f"),
				ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
				Definition:       tt.def,
			})
			require.NoError(t, err)

			prep, err := client.PrepareFlow(t.Context(), &bedrockagentsdk.PrepareFlowInput{
				FlowIdentifier: created.Id,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, prep.Status)

			got, err := client.GetFlow(t.Context(), &bedrockagentsdk.GetFlowInput{FlowIdentifier: created.Id})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)

			gotTypes := make([]types.FlowValidationType, 0, len(got.Validations))
			for _, v := range got.Validations {
				gotTypes = append(gotTypes, v.Type)
			}

			assert.ElementsMatch(t, tt.wantValidations, gotTypes)

			updated, err := client.UpdateFlow(t.Context(), &bedrockagentsdk.UpdateFlowInput{
				FlowIdentifier:   created.Id,
				Name:             aws.String("f"),
				ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
			})
			require.NoError(t, err)
			assert.Equal(t, "NotPrepared", string(updated.Status))
		})
	}
}

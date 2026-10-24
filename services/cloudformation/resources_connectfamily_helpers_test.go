package cloudformation_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// familyStackEnv is a CloudFormation client plus the config and endpoint of the service the stack provisions.
type familyStackEnv struct {
	cfn *cfnsdk.Client
	cfg aws.Config
	url string
}

func newFamilyStackEnv(
	t *testing.T, handler service.Registerable, wire func(*cloudformation.ServiceBackends),
) familyStackEnv {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	wire(backends)

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("000000000000", "test", "")),
	)
	require.NoError(t, err)

	return familyStackEnv{cfn: newTestClientForBackend(t, backend), cfg: cfg, url: sdktest.Serve(t, handler)}
}

func (e familyStackEnv) updateStack(t *testing.T, name, template string) {
	t.Helper()

	_, err := e.cfn.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String(name), TemplateBody: aws.String(template),
	})
	require.NoError(t, err)

	desc, err := e.cfn.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(name)})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	require.Equal(t, cfntypes.StackStatusUpdateComplete, desc.Stacks[0].StackStatus,
		aws.ToString(desc.Stacks[0].StackStatusReason))
}

func (e familyStackEnv) deleteStack(t *testing.T, name string) {
	t.Helper()

	_, err := e.cfn.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(name)})
	require.NoError(t, err)
}

func cfnTags(kv map[string]string) []map[string]string {
	out := make([]map[string]string, 0, len(kv))
	for k, v := range kv {
		out = append(out, map[string]string{"Key": k, "Value": v})
	}

	return out
}

// stackTemplate renders resources plus Ref/GetAtt outputs given as "Output": value maps.
func stackTemplate(t *testing.T, resources map[string]any, outputs map[string]any) string {
	t.Helper()

	outs := make(map[string]any, len(outputs))
	for k, v := range outputs {
		outs[k] = map[string]any{"Value": v}
	}

	doc, err := json.Marshal(map[string]any{"Resources": resources, "Outputs": outs})
	require.NoError(t, err)

	return string(doc)
}

func cfnRes(resourceType string, props map[string]any) map[string]any {
	return map[string]any{"Type": resourceType, "Properties": props}
}

func cfnRef(id string) map[string]any { return map[string]any{"Ref": id} }

func cfnGetAtt(id, attr string) map[string]any {
	return map[string]any{"Fn::GetAtt": []string{id, attr}}
}

// createStack creates a stack and returns its outputs, failing with the stack's status reason on rollback.
func (e familyStackEnv) createStack(t *testing.T, name, template string) map[string]string {
	t.Helper()

	_, err := e.cfn.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String(name), TemplateBody: aws.String(template),
	})
	require.NoError(t, err)

	desc, err := e.cfn.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(name)})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	require.Equal(t, cfntypes.StackStatusCreateComplete, desc.Stacks[0].StackStatus,
		aws.ToString(desc.Stacks[0].StackStatusReason))

	out := make(map[string]string, len(desc.Stacks[0].Outputs))
	for _, o := range desc.Stacks[0].Outputs {
		out[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}

	return out
}

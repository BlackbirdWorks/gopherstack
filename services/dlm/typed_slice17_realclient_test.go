package dlm_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dlmsdk "github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// newTestDLMClient stands up the real aws-sdk-go-v2 dlm client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production. (test/integration's
// tag_routing_test.go already drives dlm's tag family against a live
// container; this local helper covers the remaining ops without that
// heavier harness.)
func newTestDLMClient(t *testing.T, h *dlm.Handler) *dlmsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return dlmsdk.NewFromConfig(cfg, func(o *dlmsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSlice17_DLM_RealClient covers dlm's last three typed-client-uncovered
// ops (gopherstack-n3zi slice 17): GetLifecyclePolicies, GetLifecyclePolicy,
// UpdateLifecyclePolicy.
func TestSlice17_DLM_RealClient(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	h := dlm.NewHandler(dlm.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestDLMClient(t, h)

	created, err := client.CreateLifecyclePolicy(ctx, &dlmsdk.CreateLifecyclePolicyInput{
		Description:      aws.String("slice17 policy"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/AWSDataLifecycleManagerRole"),
		State:            dlmtypes.SettablePolicyStateValuesEnabled,
		Tags:             map[string]string{"env": "test"},
	})
	require.NoError(t, err)
	policyID := aws.ToString(created.PolicyId)
	require.NotEmpty(t, policyID)

	listed, err := client.GetLifecyclePolicies(ctx, &dlmsdk.GetLifecyclePoliciesInput{
		PolicyIds: []string{policyID},
	})
	require.NoError(t, err)
	require.Len(t, listed.Policies, 1)
	assert.Equal(t, policyID, aws.ToString(listed.Policies[0].PolicyId))
	assert.Equal(t, "slice17 policy", aws.ToString(listed.Policies[0].Description))
	assert.Equal(t, dlmtypes.GettablePolicyStateValuesEnabled, listed.Policies[0].State)

	got, err := client.GetLifecyclePolicy(ctx, &dlmsdk.GetLifecyclePolicyInput{
		PolicyId: aws.String(policyID),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Policy)
	assert.Equal(t, policyID, aws.ToString(got.Policy.PolicyId))
	assert.Equal(
		t,
		"arn:aws:iam::000000000000:role/AWSDataLifecycleManagerRole",
		aws.ToString(got.Policy.ExecutionRoleArn),
	)
	assert.Equal(t, map[string]string{"env": "test"}, got.Policy.Tags)

	_, err = client.UpdateLifecyclePolicy(ctx, &dlmsdk.UpdateLifecyclePolicyInput{
		PolicyId:    aws.String(policyID),
		Description: aws.String("slice17 policy updated"),
		State:       dlmtypes.SettablePolicyStateValuesDisabled,
	})
	require.NoError(t, err)

	afterUpdate, err := client.GetLifecyclePolicy(ctx, &dlmsdk.GetLifecyclePolicyInput{
		PolicyId: aws.String(policyID),
	})
	require.NoError(t, err)
	require.NotNil(t, afterUpdate.Policy)
	assert.Equal(t, "slice17 policy updated", aws.ToString(afterUpdate.Policy.Description))
	assert.Equal(t, dlmtypes.GettablePolicyStateValuesDisabled, afterUpdate.Policy.State)
}

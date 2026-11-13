package gcpfunctions_test

import (
	"context"
	"testing"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy/iamtest"
)

func TestIAMPolicySemantics(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "iam-sem", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	iamtest.Run(t, iamtest.Endpoint{
		Get: func(ctx context.Context, r *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
			return e.client.GetIamPolicy(ctx, r)
		},
		Set: func(ctx context.Context, r *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
			return e.client.SetIamPolicy(ctx, r)
		},
		Test: func(ctx context.Context, r *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
			return e.client.TestIamPermissions(ctx, r)
		},
		Resource: fn.GetName(), Permission: "cloudfunctions.functions.get",
	})
}

func TestIAMPolicyLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "iam-life", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))
	binding := &iampb.Binding{Role: "roles/cloudfunctions.invoker", Members: []string{"allUsers"}}

	_, err := e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: parent + "/functions/missing", Policy: &iampb.Policy{Bindings: []*iampb.Binding{binding}},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: fn.GetName(), Policy: &iampb.Policy{Bindings: []*iampb.Binding{binding}},
	})
	require.NoError(t, err)

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), e.handler.Snapshot(t.Context())))

	got, err := dst.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: fn.GetName()})
	require.NoError(t, err)
	require.Len(t, got.GetBindings(), 1)
	assert.Equal(t, "allUsers", got.GetBindings()[0].GetMembers()[0])

	op, err := dst.client.DeleteFunction(t.Context(), &functionspb.DeleteFunctionRequest{Name: fn.GetName()})
	require.NoError(t, err)
	err = op.Wait(t.Context())
	require.NoError(t, err)

	_, err = dst.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: fn.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

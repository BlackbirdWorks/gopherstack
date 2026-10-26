package gcpspanner_test

import (
	"context"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy/iamtest"
)

func TestIAMPolicySemantics(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.createInstance(t, "iam-sem")
	ep := iamtest.Endpoint{
		Get: func(ctx context.Context, r *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
			return e.instances.GetIamPolicy(ctx, r)
		},
		Set: func(ctx context.Context, r *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
			return e.instances.SetIamPolicy(ctx, r)
		},
		Test: func(ctx context.Context, r *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
			return e.instances.TestIamPermissions(ctx, r)
		},
		Resource: inst.GetName(), Permission: "spanner.instances.get",
	}

	iamtest.Run(t, ep)
}

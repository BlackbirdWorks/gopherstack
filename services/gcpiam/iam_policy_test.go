package gcpiam_test

import (
	"context"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy/iamtest"
)

func TestIAMPolicySemantics(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "iam-sem")
	ep := iamtest.Endpoint{
		Get: func(ctx context.Context, r *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
			return e.raw.GetIamPolicy(ctx, r)
		},
		Set: func(ctx context.Context, r *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
			return e.raw.SetIamPolicy(ctx, r)
		},
		Test: func(ctx context.Context, r *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
			return e.raw.TestIamPermissions(ctx, r)
		},
		Resource: sa.GetName(), Permission: "iam.serviceAccounts.get",
	}

	iamtest.Run(t, ep)
}

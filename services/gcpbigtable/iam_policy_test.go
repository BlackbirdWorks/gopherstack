package gcpbigtable_test

import (
	"context"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy/iamtest"
)

func TestIAMPolicySemantics(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)

	b := e.handler.Backend

	iamtest.Run(t, iamtest.Endpoint{
		Get: func(_ context.Context, r *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
			return b.GetIamPolicy(r.GetResource(), r.GetOptions().GetRequestedPolicyVersion())
		},
		Set: func(_ context.Context, r *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
			return b.SetIamPolicy(r)
		},
		Test: func(_ context.Context, r *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
			return b.TestIamPermissions(r)
		},
		Resource: "projects/" + project + "/instances/" + instance, Permission: "bigtable.instances.get",
	})
}

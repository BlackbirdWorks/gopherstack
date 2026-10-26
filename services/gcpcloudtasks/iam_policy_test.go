package gcpcloudtasks_test

import (
	"context"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy/iamtest"
)

func TestIAMPolicySemantics(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "iam-sem")
	ep := iamtest.Endpoint{
		Get: func(ctx context.Context, r *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
			return e.client.GetIamPolicy(ctx, r)
		},
		Set: func(ctx context.Context, r *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
			return e.client.SetIamPolicy(ctx, r)
		},
		Test: func(ctx context.Context, r *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
			return e.client.TestIamPermissions(ctx, r)
		},
		Resource: q.GetName(), Permission: "cloudtasks.queues.get",
	}

	iamtest.Run(t, ep)
}

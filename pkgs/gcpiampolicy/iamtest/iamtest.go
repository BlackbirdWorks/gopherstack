// Package iamtest runs the shared google.iam.v1 conformance cases against a service's IAM endpoints.
package iamtest

import (
	"context"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

// Endpoint is one resource's IAM surface.
type Endpoint struct {
	Get      func(context.Context, *iampb.GetIamPolicyRequest) (*iampb.Policy, error)
	Set      func(context.Context, *iampb.SetIamPolicyRequest) (*iampb.Policy, error)
	Test     func(context.Context, *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error)
	Resource string
	// Permission is a permission valid for the service; the foreign one is derived by the helper.
	Permission string
}

func cond() *iampb.Binding {
	return &iampb.Binding{
		Role: "roles/viewer", Members: []string{"user:a@example.com"},
		Condition: &expr.Expr{Expression: "request.time < timestamp('2099-01-01T00:00:00Z')", Title: "expires"},
	}
}

// Run exercises etag, policy-version, condition and permission-filtering semantics.
func Run(t *testing.T, ep Endpoint) {
	t.Helper()

	ctx := t.Context()

	set := func(p *iampb.Policy) (*iampb.Policy, error) {
		return ep.Set(ctx, &iampb.SetIamPolicyRequest{Resource: ep.Resource, Policy: p})
	}

	get := func(v int32) *iampb.Policy {
		got, err := ep.Get(ctx, &iampb.GetIamPolicyRequest{
			Resource: ep.Resource, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: v},
		})
		require.NoError(t, err)

		return got
	}

	_, err := set(&iampb.Policy{Bindings: []*iampb.Binding{cond()}, Version: 1})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "conditions require policy version 3")

	plain := &iampb.Binding{Role: "roles/viewer", Members: []string{"user:a@example.com"}}
	_, err = set(&iampb.Policy{Bindings: []*iampb.Binding{plain}})
	require.NoError(t, err)

	first := get(1)
	assert.Equal(t, int32(1), first.GetVersion())

	conditional, err := set(
		&iampb.Policy{
			Bindings: []*iampb.Binding{cond()},
			Version:  gcpiampolicy.ConditionalVersion,
			Etag:     first.GetEtag(),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(gcpiampolicy.ConditionalVersion), conditional.GetVersion())
	assert.NotEqual(t, first.GetEtag(), conditional.GetEtag())

	_, err = set(&iampb.Policy{Etag: first.GetEtag()})
	assert.Equal(t, codes.Aborted, status.Code(err), "a stale etag aborts")

	v3 := get(gcpiampolicy.ConditionalVersion)
	require.Len(t, v3.GetBindings(), 1)
	assert.Equal(t, "expires", v3.GetBindings()[0].GetCondition().GetTitle())

	v1 := get(1)
	require.Len(t, v1.GetBindings(), 1)
	assert.Nil(t, v1.GetBindings()[0].GetCondition())
	assert.Contains(t, v1.GetBindings()[0].GetRole(), "_withcond_")
	assert.Equal(t, int32(1), v1.GetVersion())

	perms, err := ep.Test(ctx, &iampb.TestIamPermissionsRequest{
		Resource: ep.Resource, Permissions: []string{ep.Permission, "other.things.get"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{ep.Permission}, perms.GetPermissions(), "foreign permissions are dropped")
}

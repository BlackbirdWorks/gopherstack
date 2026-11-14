package gcpiam_test

import (
	"context"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

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

func TestSetIamPolicyUpdateMask(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sa := e.account(t, "iam-mask")
	ctx := t.Context()

	first, err := e.raw.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: sa.GetName(), Policy: &iampb.Policy{
		Bindings: []*iampb.Binding{{Role: "roles/iam.serviceAccountUser", Members: []string{"user:a@example.com"}}},
	}})
	require.NoError(t, err)
	require.Len(t, first.GetBindings(), 1)

	audit := []*iampb.AuditConfig{{Service: "allServices", AuditLogConfigs: []*iampb.AuditLogConfig{
		{LogType: iampb.AuditLogConfig_DATA_READ},
	}}}
	other := []*iampb.Binding{{Role: "roles/iam.serviceAccountViewer", Members: []string{"user:b@example.com"}}}

	masked, err := e.raw.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: sa.GetName(), UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"audit_configs"}},
		Policy: &iampb.Policy{Bindings: other, AuditConfigs: audit},
	})
	require.NoError(t, err)
	assert.Equal(t, "roles/iam.serviceAccountUser", masked.GetBindings()[0].GetRole())
	assert.Len(t, masked.GetAuditConfigs(), 1)

	def, err := e.raw.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: sa.GetName(), Policy: &iampb.Policy{Bindings: other, AuditConfigs: nil},
	})
	require.NoError(t, err)
	assert.Equal(t, "roles/iam.serviceAccountViewer", def.GetBindings()[0].GetRole())
	assert.Len(t, def.GetAuditConfigs(), 1, "the default mask leaves audit_configs alone")

	_, err = e.raw.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: sa.GetName(), UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"nope"}}, Policy: &iampb.Policy{},
	})
	requireCode(t, codes.InvalidArgument, err)
}

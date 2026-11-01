package gcpresourcemanager_test

import (
	"context"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
)

type iamAPI interface {
	GetIamPolicy(context.Context, *iampb.GetIamPolicyRequest, ...gax.CallOption) (*iampb.Policy, error)
	SetIamPolicy(context.Context, *iampb.SetIamPolicyRequest, ...gax.CallOption) (*iampb.Policy, error)
	TestIamPermissions(
		context.Context, *iampb.TestIamPermissionsRequest, ...gax.CallOption,
	) (*iampb.TestIamPermissionsResponse, error)
}

type iamTarget struct {
	api  iamAPI
	name string
	res  string
}

func iamTargets(t *testing.T, c *clients) []iamTarget {
	t.Helper()

	folder := c.createFolder(t, orgName, "IAM folder")
	proj := c.createProject(t, "iam-proj", folder.GetName())
	_ = c.createProject(t, "iam-proj-two", folder.GetName())

	tk := mustTagKey(t, c, "iamkey")

	return []iamTarget{
		{c.orgs, "organization", orgName},
		{c.folders, "folder", folder.GetName()},
		{c.projects, "project number", proj.GetName()},
		{c.projects, "project id", "projects/iam-proj-two"},
		{c.tagKeys, "tag key", tk.GetName()},
	}
}

func TestIamPolicy(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		for _, tg := range iamTargets(t, c) {
			t.Run(tg.name, func(t *testing.T) {
				t.Parallel()

				ctx := t.Context()
				empty, err := tg.api.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: tg.res})
				require.NoError(t, err)
				assert.Empty(t, empty.GetBindings())
				assert.NotEmpty(t, empty.GetEtag())

				viewer := &iampb.Binding{Role: "roles/viewer", Members: []string{"user:a@example.com"}}
				first, err := tg.api.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
					Resource: tg.res, Policy: &iampb.Policy{Bindings: []*iampb.Binding{viewer}, Etag: empty.GetEtag()},
				})
				require.NoError(t, err)
				assert.Len(t, first.GetBindings(), 1)
				assert.NotEqual(t, empty.GetEtag(), first.GetEtag())

				_, err = tg.api.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
					Resource: tg.res, Policy: &iampb.Policy{Etag: empty.GetEtag()},
				})
				assert.Equal(t, codes.Aborted, codeOf(err), "the etag from before the first write is stale")

				got, err := tg.api.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: tg.res})
				require.NoError(t, err)
				assert.Equal(t, first.GetEtag(), got.GetEtag())
				assert.Equal(t, "roles/viewer", got.GetBindings()[0].GetRole())

				_, err = tg.api.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
					Resource: tg.res, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{
						Role: "roles/viewer", Members: []string{"user:a@example.com"},
						Condition: &expr.Expr{Expression: "true", Title: "t"},
					}}, Version: 1},
				})
				assert.Equal(t, codes.InvalidArgument, codeOf(err), "conditions need policy version 3")

				perms, err := tg.api.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
					Resource: tg.res, Permissions: []string{"resourcemanager.projects.get", "compute.instances.list"},
				})
				require.NoError(t, err)
				assert.ElementsMatch(
					t,
					[]string{"resourcemanager.projects.get", "compute.instances.list"},
					perms.GetPermissions(),
				)
			})
		}
	})
}

func TestIamPolicyErrors(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		tests := []struct {
			policy   *iampb.Policy
			name     string
			resource string
			code     codes.Code
		}{
			{&iampb.Policy{}, "unknown project", "projects/nope-nope", codes.PermissionDenied},
			{&iampb.Policy{}, "unknown folder", "folders/1", codes.PermissionDenied},
			{&iampb.Policy{}, "unknown org", "organizations/1", codes.PermissionDenied},
			{&iampb.Policy{Bindings: []*iampb.Binding{{
				Role: "roles/viewer", Members: []string{"allUsers"},
			}}}, "all users on project", "projects/" + seedID, codes.InvalidArgument},
			{
				&iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/viewer"}}},
				"empty binding",
				"projects/" + seedID,
				codes.InvalidArgument,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				_, err := c.projects.SetIamPolicy(
					t.Context(),
					&iampb.SetIamPolicyRequest{Resource: tt.resource, Policy: tt.policy},
				)
				assert.Equal(t, tt.code, codeOf(err), "%v", err)
			})
		}
	})
}

func TestIamPolicyOfDeletedProject(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		p := c.createProject(t, "iam-deleted", "")

		op, err := c.projects.DeleteProject(t.Context(), &resourcemanagerpb.DeleteProjectRequest{Name: p.GetName()})
		require.NoError(t, err)

		_, err = op.Wait(t.Context())
		require.NoError(t, err)

		_, err = c.projects.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: p.GetName()})
		require.NoError(t, err, "a DELETE_REQUESTED project can still be read")

		_, err = c.projects.SetIamPolicy(
			t.Context(),
			&iampb.SetIamPolicyRequest{Resource: p.GetName(), Policy: &iampb.Policy{}},
		)
		assert.Equal(t, codes.FailedPrecondition, codeOf(err))
	})
}

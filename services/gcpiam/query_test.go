package gcpiam_test

import (
	"testing"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
)

func TestQueryGrantableRoles(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	createRole(t, e, project, "grantMe", "iam.roles.get")
	createRole(t, e, "projects/other", "notMine", "iam.roles.get")

	names := func(resource string) []string {
		resp, err := e.raw.QueryGrantableRoles(ctx, &adminpb.QueryGrantableRolesRequest{FullResourceName: resource})
		require.NoError(t, err)

		out := make([]string, 0, len(resp.GetRoles()))
		for _, r := range resp.GetRoles() {
			out = append(out, r.GetName())
		}

		return out
	}

	proj := names("//cloudresourcemanager.googleapis.com/projects/test-project")
	assert.Contains(t, proj, "roles/storage.admin")
	assert.Contains(t, proj, project+"/roles/grantMe")
	assert.NotContains(t, proj, "projects/other/roles/notMine")

	sa := names("//iam.googleapis.com/projects/test-project/serviceAccounts/x@test-project.iam.gserviceaccount.com")
	assert.Contains(t, sa, "roles/iam.serviceAccountUser")
	assert.Contains(t, sa, "roles/owner")
	assert.NotContains(t, sa, "roles/storage.admin")

	_, err := e.raw.QueryGrantableRoles(
		ctx,
		&adminpb.QueryGrantableRolesRequest{FullResourceName: "projects/test-project"},
	)
	requireCode(t, codes.InvalidArgument, err)
}

func TestQueryTestablePermissionsAndServices(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	perms, err := e.raw.QueryTestablePermissions(ctx, &adminpb.QueryTestablePermissionsRequest{
		FullResourceName: "//cloudresourcemanager.googleapis.com/projects/test-project", PageSize: 5,
	})
	require.NoError(t, err)
	assert.Len(t, perms.GetPermissions(), 5)
	assert.NotEmpty(t, perms.GetNextPageToken())

	rest, err := e.raw.QueryTestablePermissions(ctx, &adminpb.QueryTestablePermissionsRequest{
		FullResourceName: "//cloudresourcemanager.googleapis.com/projects/test-project",
		PageToken:        perms.GetNextPageToken(),
		PageSize:         1000,
	})
	require.NoError(t, err)
	assert.Empty(t, rest.GetNextPageToken())
	assert.Greater(t, len(rest.GetPermissions()), 50)

	saPerms, err := e.raw.QueryTestablePermissions(ctx, &adminpb.QueryTestablePermissionsRequest{
		FullResourceName: "//iam.googleapis.com/projects/test-project/serviceAccounts/x",
	})
	require.NoError(t, err)

	for _, p := range saPerms.GetPermissions() {
		assert.Contains(t, p.GetName(), "iam.serviceAccounts.")
	}

	svcs, err := e.client.QueryAuditableServices(ctx, &adminpb.QueryAuditableServicesRequest{
		FullResourceName: "//cloudresourcemanager.googleapis.com/projects/test-project",
	})
	require.NoError(t, err)

	got := make([]string, 0, len(svcs.GetServices()))
	for _, s := range svcs.GetServices() {
		got = append(got, s.GetName())
	}

	assert.Contains(t, got, "iam.googleapis.com")

	_, err = e.client.QueryAuditableServices(ctx, &adminpb.QueryAuditableServicesRequest{FullResourceName: "nope"})
	requireCode(t, codes.InvalidArgument, err)
}

func TestLintPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct {
		name     string
		expr     string
		wantErrs int
	}{
		{"valid", `request.time < timestamp("2030-01-01T00:00:00Z")`, 0},
		{"empty", "  ", 1},
		{"unbalanced paren", `(a && b`, 1},
		{"stray closer", `a)`, 1},
		{"unterminated string", `resource.name == "x`, 1},
		{"too many operators", `a || b || c || d || e || f || g || h || i || j || k || l || m || n`, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.client.LintPolicy(t.Context(), &adminpb.LintPolicyRequest{
				FullResourceName: "//cloudresourcemanager.googleapis.com/projects/test-project",
				LintObject:       &adminpb.LintPolicyRequest_Condition{Condition: &expr.Expr{Expression: tt.expr}},
			})
			require.NoError(t, err)
			assert.Len(t, resp.GetLintResults(), tt.wantErrs)

			for _, r := range resp.GetLintResults() {
				assert.Equal(t, adminpb.LintResult_ERROR, r.GetSeverity())
				assert.Equal(t, adminpb.LintResult_CONDITION, r.GetLevel())
				assert.Equal(t, "condition.expression", r.GetFieldName())
			}
		})
	}

	_, err := e.client.LintPolicy(t.Context(), &adminpb.LintPolicyRequest{FullResourceName: "x"})
	requireCode(t, codes.InvalidArgument, err)
}

func TestServiceAccountIamPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	sa := e.account(t, "policy-sa")
	custom := createRole(t, e, project, "polRole", "iam.roles.get")

	empty, err := e.raw.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: sa.GetName()})
	require.NoError(t, err)
	assert.Empty(t, empty.GetBindings())

	bindings := []*iampb.Binding{
		{
			Role:    "roles/iam.serviceAccountUser",
			Members: []string{"user:a@example.com", "serviceAccount:" + sa.GetEmail()},
		},
		{Role: custom.GetName(), Members: []string{"group:g@example.com"}},
	}

	first, err := e.raw.SetIamPolicy(
		ctx,
		&iampb.SetIamPolicyRequest{
			Resource: sa.GetName(),
			Policy:   &iampb.Policy{Bindings: bindings, Etag: empty.GetEtag()},
		},
	)
	require.NoError(t, err)
	assert.NotEqual(t, empty.GetEtag(), first.GetEtag())

	_, err = e.raw.SetIamPolicy(
		ctx,
		&iampb.SetIamPolicyRequest{
			Resource: sa.GetName(),
			Policy:   &iampb.Policy{Bindings: bindings, Etag: empty.GetEtag()},
		},
	)
	requireCode(t, codes.Aborted, err)

	second, err := e.raw.SetIamPolicy(
		ctx,
		&iampb.SetIamPolicyRequest{
			Resource: sa.GetName(),
			Policy:   &iampb.Policy{Bindings: bindings[:1], Etag: first.GetEtag()},
		},
	)
	require.NoError(t, err)

	got, err := e.raw.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: sa.GetName()})
	require.NoError(t, err)
	assert.Len(t, got.GetBindings(), 1)
	assert.Equal(t, second.GetEtag(), got.GetEtag())

	perms, err := e.client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: sa.GetName(), Permissions: []string{"iam.serviceAccounts.actAs"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"iam.serviceAccounts.actAs"}, perms.GetPermissions())

	bad := []*iampb.Binding{
		{Role: "roles/viewer", Members: []string{"nobody"}},
		{Role: "roles/viewer"},
		{Role: "viewer", Members: []string{"user:a@example.com"}},
		{Role: project + "/roles/missing", Members: []string{"user:a@example.com"}},
	}
	for _, bd := range bad {
		_, err = e.raw.SetIamPolicy(
			ctx,
			&iampb.SetIamPolicyRequest{Resource: sa.GetName(), Policy: &iampb.Policy{Bindings: []*iampb.Binding{bd}}},
		)
		requireCode(t, codes.InvalidArgument, err)
	}

	_, err = e.raw.GetIamPolicy(
		ctx,
		&iampb.GetIamPolicyRequest{Resource: project + "/serviceAccounts/ghost@test-project.iam.gserviceaccount.com"},
	)
	requireCode(t, codes.NotFound, err)

	_, err = e.raw.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: sa.GetName()})
	requireCode(t, codes.InvalidArgument, err)
}

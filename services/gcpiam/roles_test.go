package gcpiam_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func createRole(t *testing.T, e *env, parent, id string, perms ...string) *adminpb.Role {
	t.Helper()

	role, err := e.client.CreateRole(t.Context(), &adminpb.CreateRoleRequest{
		Parent: parent, RoleId: id,
		Role: &adminpb.Role{Title: "Title " + id, Description: "d", IncludedPermissions: perms, Stage: adminpb.Role_GA},
	})
	require.NoError(t, err)

	return role
}

func TestCustomRoleLifecycle(t *testing.T) {
	t.Parallel()

	for _, parent := range []string{project, "organizations/1234"} {
		t.Run(parent, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ctx := t.Context()
			role := createRole(
				t,
				e,
				parent,
				"myRole.v1",
				"storage.objects.get",
				"pubsub.topics.get",
				"storage.objects.get",
			)

			assert.Equal(t, parent+"/roles/myRole.v1", role.GetName())
			assert.Equal(t, []string{"pubsub.topics.get", "storage.objects.get"}, role.GetIncludedPermissions())
			assert.NotEmpty(t, role.GetEtag())
			assert.False(t, role.GetDeleted())

			_, err := e.client.CreateRole(
				ctx,
				&adminpb.CreateRoleRequest{Parent: parent, RoleId: "myRole.v1", Role: &adminpb.Role{}},
			)
			requireCode(t, codes.AlreadyExists, err)

			updated, err := e.client.UpdateRole(ctx, &adminpb.UpdateRoleRequest{
				Name: role.GetName(),
				Role: &adminpb.Role{
					Title:               "Renamed",
					IncludedPermissions: []string{"iam.roles.get"},
					Etag:                role.GetEtag(),
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title", "included_permissions"}},
			})
			require.NoError(t, err)
			assert.Equal(t, "Renamed", updated.GetTitle())
			assert.Equal(t, "d", updated.GetDescription())
			assert.Equal(t, []string{"iam.roles.get"}, updated.GetIncludedPermissions())
			assert.NotEqual(t, role.GetEtag(), updated.GetEtag())

			deleted, err := e.client.DeleteRole(
				ctx,
				&adminpb.DeleteRoleRequest{Name: role.GetName(), Etag: updated.GetEtag()},
			)
			require.NoError(t, err)
			assert.True(t, deleted.GetDeleted())

			got, err := e.client.GetRole(ctx, &adminpb.GetRoleRequest{Name: role.GetName()})
			require.NoError(t, err)
			assert.True(t, got.GetDeleted())

			_, err = e.client.UpdateRole(
				ctx,
				&adminpb.UpdateRoleRequest{Name: role.GetName(), Role: &adminpb.Role{Title: "x"}},
			)
			requireCode(t, codes.FailedPrecondition, err)

			_, err = e.client.DeleteRole(ctx, &adminpb.DeleteRoleRequest{Name: role.GetName()})
			requireCode(t, codes.FailedPrecondition, err)

			restored, err := e.client.UndeleteRole(ctx, &adminpb.UndeleteRoleRequest{Name: role.GetName()})
			require.NoError(t, err)
			assert.False(t, restored.GetDeleted())
			assert.Equal(t, "Renamed", restored.GetTitle())

			_, err = e.client.UndeleteRole(ctx, &adminpb.UndeleteRoleRequest{Name: role.GetName()})
			requireCode(t, codes.FailedPrecondition, err)
		})
	}
}

func TestRoleEtagConflictAndUndeleteWindow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	role := createRole(t, e, project, "etagRole", "iam.roles.get")

	_, err := e.client.UpdateRole(ctx, &adminpb.UpdateRoleRequest{
		Name: role.GetName(), Role: &adminpb.Role{Title: "A", Etag: role.GetEtag()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
	})
	require.NoError(t, err)

	_, err = e.client.UpdateRole(ctx, &adminpb.UpdateRoleRequest{
		Name: role.GetName(), Role: &adminpb.Role{Title: "B", Etag: role.GetEtag()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
	})
	requireCode(t, codes.Aborted, err)

	_, err = e.client.DeleteRole(ctx, &adminpb.DeleteRoleRequest{Name: role.GetName(), Etag: role.GetEtag()})
	requireCode(t, codes.Aborted, err)

	_, err = e.client.DeleteRole(ctx, &adminpb.DeleteRoleRequest{Name: role.GetName()})
	require.NoError(t, err)

	_, err = e.client.UndeleteRole(ctx, &adminpb.UndeleteRoleRequest{Name: role.GetName(), Etag: role.GetEtag()})
	requireCode(t, codes.Aborted, err)

	e.clk.advance(8 * 24 * time.Hour)

	_, err = e.client.UndeleteRole(ctx, &adminpb.UndeleteRoleRequest{Name: role.GetName()})
	requireCode(t, codes.FailedPrecondition, err)

	_, err = e.client.CreateRole(
		ctx,
		&adminpb.CreateRoleRequest{Parent: project, RoleId: "etagRole", Role: &adminpb.Role{}},
	)
	requireCode(t, codes.AlreadyExists, err)

	e.clk.advance(40 * 24 * time.Hour)

	_, err = e.client.CreateRole(
		ctx,
		&adminpb.CreateRoleRequest{Parent: project, RoleId: "etagRole", Role: &adminpb.Role{}},
	)
	require.NoError(t, err, "the id is reusable once the deleted role is permanently purged")
}

func TestCreateRoleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		parent string
		id     string
		perms  []string
	}{
		{"short id", project, "ab", nil},
		{"bad chars", project, "my-role", nil},
		{"long id", project, strings.Repeat("a", 65), nil},
		{"bad parent", "folders/1", "myRole", nil},
		{"wildcard parent", "projects/*", "myRole", nil},
		{"bad permission", project, "myRole", []string{"nodots"}},
		{"two part permission", project, "myRole", []string{"a.b"}},
	}

	e := newEnv(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.CreateRole(t.Context(), &adminpb.CreateRoleRequest{
				Parent: tt.parent, RoleId: tt.id, Role: &adminpb.Role{IncludedPermissions: tt.perms},
			})
			requireCode(t, codes.InvalidArgument, err)
		})
	}

	_, err := e.client.CreateRole(t.Context(), &adminpb.CreateRoleRequest{Parent: project, RoleId: "myRole"})
	requireCode(t, codes.InvalidArgument, err)
}

func TestPredefinedRoles(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	got, err := e.client.GetRole(ctx, &adminpb.GetRoleRequest{Name: "roles/iam.serviceAccountTokenCreator"})
	require.NoError(t, err)
	assert.Contains(t, got.GetIncludedPermissions(), "iam.serviceAccounts.signBlob")
	assert.Equal(t, adminpb.Role_GA, got.GetStage())

	_, err = e.client.GetRole(ctx, &adminpb.GetRoleRequest{Name: "roles/does.notExist"})
	requireCode(t, codes.NotFound, err)

	it := e.client.ListRolesIter(ctx, &adminpb.ListRolesRequest{PageSize: 7})

	var names []string

	for {
		r, ierr := it.Next()
		if errors.Is(ierr, iterator.Done) {
			break
		}

		require.NoError(t, ierr)
		assert.Empty(t, r.GetIncludedPermissions(), "BASIC view omits permissions")

		names = append(names, r.GetName())
	}

	assert.Greater(t, len(names), 30)
	assert.IsIncreasing(t, names)
	assert.Contains(t, names, "roles/owner")

	full, err := e.raw.ListRoles(ctx, &adminpb.ListRolesRequest{View: adminpb.RoleView_FULL, PageSize: 1000})
	require.NoError(t, err)

	for _, r := range full.GetRoles() {
		assert.NotEmpty(t, r.GetIncludedPermissions(), r.GetName())
	}
}

func TestListCustomRolesShowDeleted(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	keep := createRole(t, e, project, "keepRole", "iam.roles.get")
	gone := createRole(t, e, project, "goneRole", "iam.roles.get")
	createRole(t, e, "organizations/9", "orgRole", "iam.roles.get")

	_, err := e.client.DeleteRole(ctx, &adminpb.DeleteRoleRequest{Name: gone.GetName()})
	require.NoError(t, err)

	list := func(parent string, deleted bool, view adminpb.RoleView) []*adminpb.Role {
		resp, lerr := e.raw.ListRoles(ctx, &adminpb.ListRolesRequest{Parent: parent, ShowDeleted: deleted, View: view})
		require.NoError(t, lerr)

		return resp.GetRoles()
	}

	roles := list(project, false, adminpb.RoleView_BASIC)
	require.Len(t, roles, 1)
	assert.Equal(t, keep.GetName(), roles[0].GetName())
	assert.Empty(t, roles[0].GetIncludedPermissions())

	assert.Len(t, list(project, true, adminpb.RoleView_FULL), 2)
	assert.Len(t, list("organizations/9", false, adminpb.RoleView_BASIC), 1)

	_, err = e.raw.ListRoles(ctx, &adminpb.ListRolesRequest{Parent: "bogus"})
	requireCode(t, codes.InvalidArgument, err)
}

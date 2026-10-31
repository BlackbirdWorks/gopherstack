package gcpresourcemanager_test

import (
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestFolderNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		code codes.Code
	}{
		{"plain", "Engineering", codes.OK},
		{"with space hyphen underscore", "Team a_b-c", codes.OK},
		{"three chars", "abc", codes.OK},
		{"two chars", "ab", codes.InvalidArgument},
		{"empty", "", codes.InvalidArgument},
		{"thirty chars", strings.Repeat("a", 30), codes.OK},
		{"thirty one chars", strings.Repeat("a", 31), codes.InvalidArgument},
		{"leading space", " abc", codes.InvalidArgument},
		{"trailing hyphen", "abc-", codes.InvalidArgument},
		{"punctuation", "abc!def", codes.InvalidArgument},
		{"unicode letters", "Équipe 1", codes.OK},
	}

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				op, err := c.folders.CreateFolder(t.Context(), &resourcemanagerpb.CreateFolderRequest{
					Folder: &resourcemanagerpb.Folder{Parent: orgName, DisplayName: tt.in},
				})
				if err == nil {
					_, err = op.Wait(t.Context())
				}

				assert.Equal(t, tt.code, codeOf(err), "%v", err)
			})
		}
	})
}

func TestFolderLifecycle(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		f := c.createFolder(t, orgName, "Platform")
		assert.Regexp(t, `^folders/\d{12}$`, f.GetName())
		assert.Equal(t, resourcemanagerpb.Folder_ACTIVE, f.GetState())

		got, err := c.folders.GetFolder(t.Context(), &resourcemanagerpb.GetFolderRequest{Name: f.GetName()})
		require.NoError(t, err)
		assert.Equal(t, "Platform", got.GetDisplayName())

		_ = c.createFolder(t, orgName, "Other")

		_, err = c.folders.CreateFolder(t.Context(), &resourcemanagerpb.CreateFolderRequest{
			Folder: &resourcemanagerpb.Folder{Parent: orgName, DisplayName: "platform"},
		})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "display names are unique among siblings, ignoring case")

		up, err := c.folders.UpdateFolder(t.Context(), &resourcemanagerpb.UpdateFolderRequest{
			Folder:     &resourcemanagerpb.Folder{Name: f.GetName(), DisplayName: "Platform 2"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		})
		require.NoError(t, err)

		renamed, err := up.Wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "Platform 2", renamed.GetDisplayName())

		_, err = c.folders.UpdateFolder(t.Context(), &resourcemanagerpb.UpdateFolderRequest{
			Folder:     &resourcemanagerpb.Folder{Name: f.GetName(), DisplayName: "Other"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "rename to a sibling's name")

		_, err = c.folders.UpdateFolder(t.Context(), &resourcemanagerpb.UpdateFolderRequest{
			Folder:     &resourcemanagerpb.Folder{Name: f.GetName(), DisplayName: "x1", Etag: "stale"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		})
		assert.Equal(t, codes.Aborted, codeOf(err))

		list := c.folders.ListFolders(t.Context(), &resourcemanagerpb.ListFoldersRequest{Parent: orgName})

		var names []string

		for fo, lerr := list.Next(); lerr == nil; fo, lerr = list.Next() {
			names = append(names, fo.GetDisplayName())
		}

		assert.Equal(t, []string{"Other", "Platform 2"}, names)

		search := c.folders.SearchFolders(
			t.Context(),
			&resourcemanagerpb.SearchFoldersRequest{Query: "displayName=Plat*"},
		)
		hit, err := search.Next()
		require.NoError(t, err)
		assert.Equal(t, f.GetName(), hit.GetName())
	})
}

func TestFolderDeleteUndelete(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		parent := c.createFolder(t, orgName, "Parent")
		child := c.createFolder(t, parent.GetName(), "Child")
		proj := c.createProject(t, "in-folder", child.GetName())

		del := func(name string) error {
			op, err := c.folders.DeleteFolder(t.Context(), &resourcemanagerpb.DeleteFolderRequest{Name: name})
			if err != nil {
				return err
			}

			_, err = op.Wait(t.Context())

			return err
		}

		assert.Equal(t, codes.FailedPrecondition, codeOf(del(parent.GetName())), "folder holds a folder")
		assert.Equal(t, codes.FailedPrecondition, codeOf(del(child.GetName())), "folder holds a project")

		dp, err := c.projects.DeleteProject(t.Context(), &resourcemanagerpb.DeleteProjectRequest{Name: proj.GetName()})
		require.NoError(t, err)

		_, err = dp.Wait(t.Context())
		require.NoError(t, err)

		require.NoError(t, del(child.GetName()))
		require.NoError(t, del(child.GetName()), "deleting a DELETE_REQUESTED folder is a no-op")

		got, err := c.folders.GetFolder(t.Context(), &resourcemanagerpb.GetFolderRequest{Name: child.GetName()})
		require.NoError(t, err)
		assert.Equal(t, resourcemanagerpb.Folder_DELETE_REQUESTED, got.GetState())

		_, err = c.folders.CreateFolder(t.Context(), &resourcemanagerpb.CreateFolderRequest{
			Folder: &resourcemanagerpb.Folder{Parent: child.GetName(), DisplayName: "Under deleted"},
		})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "parent must be ACTIVE")

		_ = c.createFolder(t, parent.GetName(), "Child")

		un := func(name string) error {
			op, uerr := c.folders.UndeleteFolder(t.Context(), &resourcemanagerpb.UndeleteFolderRequest{Name: name})
			if uerr != nil {
				return uerr
			}

			_, uerr = op.Wait(t.Context())

			return uerr
		}

		assert.Equal(t, codes.FailedPrecondition, codeOf(un(child.GetName())), "name now taken by a sibling")
		assert.NoError(t, un(parent.GetName()), "undelete of an ACTIVE folder is a no-op")
	})
}

func TestFolderDepthAndMove(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		parent := orgName
		chain := make([]string, 0, 10)

		for i := range 10 {
			f := c.createFolder(t, parent, fmt.Sprintf("Level %d", i+1))
			chain = append(chain, f.GetName())
			parent = f.GetName()
		}

		_, err := c.folders.CreateFolder(t.Context(), &resourcemanagerpb.CreateFolderRequest{
			Folder: &resourcemanagerpb.Folder{Parent: parent, DisplayName: "Level 11"},
		})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "height limit 10")

		side := c.createFolder(t, orgName, "Side")
		mv := func(name, dest string) error {
			op, merr := c.folders.MoveFolder(
				t.Context(),
				&resourcemanagerpb.MoveFolderRequest{Name: name, DestinationParent: dest},
			)
			if merr != nil {
				return merr
			}

			_, merr = op.Wait(t.Context())

			return merr
		}

		assert.Equal(t, codes.FailedPrecondition, codeOf(mv(side.GetName(), chain[9])), "move would exceed height")
		assert.Equal(t, codes.FailedPrecondition, codeOf(mv(chain[0], chain[3])), "move into a descendant")
		assert.Equal(t, codes.FailedPrecondition, codeOf(mv(chain[0], chain[0])), "move into itself")
		require.NoError(t, mv(side.GetName(), chain[7]))

		moved, err := c.folders.GetFolder(t.Context(), &resourcemanagerpb.GetFolderRequest{Name: side.GetName()})
		require.NoError(t, err)
		assert.Equal(t, chain[7], moved.GetParent())

		assert.Equal(
			t,
			codes.FailedPrecondition,
			codeOf(mv(chain[1], chain[7])),
			"subtree of 9 under depth 8 is too tall",
		)
		assert.Equal(t, codes.InvalidArgument, codeOf(mv(side.GetName(), "projects/1")))
		assert.Equal(t, codes.PermissionDenied, codeOf(mv(side.GetName(), "folders/1")))
	})
}

func TestFolderFanoutLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	parent := e.grpc.createFolder(t, orgName, "Wide")

	for i := range 300 {
		op, err := e.grpc.folders.CreateFolder(t.Context(), &resourcemanagerpb.CreateFolderRequest{
			Folder: &resourcemanagerpb.Folder{Parent: parent.GetName(), DisplayName: fmt.Sprintf("Child %03d", i)},
		})
		require.NoError(t, err)

		_, err = op.Wait(t.Context())
		require.NoError(t, err)
	}

	_, err := e.grpc.folders.CreateFolder(t.Context(), &resourcemanagerpb.CreateFolderRequest{
		Folder: &resourcemanagerpb.Folder{Parent: parent.GetName(), DisplayName: "One too many"},
	})
	assert.Equal(t, codes.FailedPrecondition, codeOf(err))
}

func TestOperationsAcrossTransports(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		op, err := c.folders.CreateFolder(t.Context(), &resourcemanagerpb.CreateFolderRequest{
			Folder: &resourcemanagerpb.Folder{Parent: orgName, DisplayName: "Op folder"},
		})
		require.NoError(t, err)

		got, err := c.folders.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.Name()})
		require.NoError(t, err)
		assert.True(t, got.GetDone())
		assert.Equal(t, op.Name(), got.GetName())

		_, err = c.folders.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: "operations/nope"})
		assert.Equal(t, codes.NotFound, codeOf(err))
	})
}

func TestOrganization(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		org, err := c.orgs.GetOrganization(t.Context(), &resourcemanagerpb.GetOrganizationRequest{Name: orgName})
		require.NoError(t, err)
		assert.Equal(t, "gopherstack.local", org.GetDisplayName())
		assert.Equal(t, resourcemanagerpb.Organization_ACTIVE, org.GetState())
		assert.NotEmpty(t, org.GetDirectoryCustomerId())

		_, err = c.orgs.GetOrganization(t.Context(), &resourcemanagerpb.GetOrganizationRequest{Name: "organizations/1"})
		assert.Equal(t, codes.PermissionDenied, codeOf(err))

		tests := []struct {
			query string
			want  int
		}{
			{"", 1},
			{"domain:gopherstack.local", 1},
			{"domain:GOPHERSTACK.LOCAL", 1},
			{"domain:other.example", 0},
			{"directorycustomerid:" + org.GetDirectoryCustomerId(), 1},
		}

		for _, tt := range tests {
			it := c.orgs.SearchOrganizations(
				t.Context(),
				&resourcemanagerpb.SearchOrganizationsRequest{Query: tt.query},
			)
			n := 0

			for _, ierr := it.Next(); ierr == nil; _, ierr = it.Next() {
				n++
			}

			assert.Equal(t, tt.want, n, tt.query)
		}
	})
}

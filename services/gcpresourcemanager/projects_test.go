package gcpresourcemanager_test

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestCreateProjectValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		project *resourcemanagerpb.Project
		code    codes.Code
	}{
		{"valid", &resourcemanagerpb.Project{ProjectId: "tokyo-rain-123"}, codes.OK},
		{"valid with display name", &resourcemanagerpb.Project{ProjectId: "abcdef", DisplayName: "My Project!"}, codes.OK},
		{"min length", &resourcemanagerpb.Project{ProjectId: "abcde"}, codes.InvalidArgument},
		{"max length", &resourcemanagerpb.Project{ProjectId: "a" + strings.Repeat("b", 30)}, codes.InvalidArgument},
		{"thirty chars", &resourcemanagerpb.Project{ProjectId: "a" + strings.Repeat("b", 29)}, codes.OK},
		{"uppercase", &resourcemanagerpb.Project{ProjectId: "Tokyo-rain"}, codes.InvalidArgument},
		{"digit start", &resourcemanagerpb.Project{ProjectId: "1tokyo-rain"}, codes.InvalidArgument},
		{"trailing hyphen", &resourcemanagerpb.Project{ProjectId: "tokyo-rain-"}, codes.InvalidArgument},
		{"underscore", &resourcemanagerpb.Project{ProjectId: "tokyo_rain"}, codes.InvalidArgument},
		{"restricted google", &resourcemanagerpb.Project{ProjectId: "my-google-app"}, codes.InvalidArgument},
		{"restricted ssl", &resourcemanagerpb.Project{ProjectId: "my-ssl-app"}, codes.InvalidArgument},
		{"empty", &resourcemanagerpb.Project{}, codes.InvalidArgument},
		{"short display name", &resourcemanagerpb.Project{ProjectId: "proj-aaa", DisplayName: "abc"}, codes.InvalidArgument},
		{"bad display char", &resourcemanagerpb.Project{ProjectId: "proj-bbb", DisplayName: "bad_name"}, codes.InvalidArgument},
		{"bad label key", &resourcemanagerpb.Project{ProjectId: "proj-ccc", Labels: map[string]string{"Env": "x"}}, codes.InvalidArgument},
		{"bad label value", &resourcemanagerpb.Project{ProjectId: "proj-ddd", Labels: map[string]string{"env": "A"}}, codes.InvalidArgument},
		{"good labels", &resourcemanagerpb.Project{ProjectId: "proj-eee", Labels: map[string]string{"env": "prod", "k": ""}}, codes.OK},
		{"unknown org", &resourcemanagerpb.Project{ProjectId: "proj-fff", Parent: "organizations/1"}, codes.PermissionDenied},
		{"unknown folder", &resourcemanagerpb.Project{ProjectId: "proj-ggg", Parent: "folders/1"}, codes.PermissionDenied},
		{"bad parent", &resourcemanagerpb.Project{ProjectId: "proj-hhh", Parent: "projects/1"}, codes.InvalidArgument},
		{"duplicate seed", &resourcemanagerpb.Project{ProjectId: seedID}, codes.AlreadyExists},
	}

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				op, err := c.projects.CreateProject(t.Context(), &resourcemanagerpb.CreateProjectRequest{Project: tt.project})
				if err == nil {
					_, err = op.Wait(t.Context())
				}

				assert.Equal(t, tt.code, codeOf(err), "%v", err)
			})
		}
	})
}

func TestProjectLifecycle(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
	id := "life-x"
	p := c.createProject(t, id, orgName)
	assert.Regexp(t, `^projects/\d{12}$`, p.GetName())
	assert.Equal(t, resourcemanagerpb.Project_ACTIVE, p.GetState())
	assert.Equal(t, orgName, p.GetParent())
	assert.NotNil(t, p.GetCreateTime())
	assert.NotEmpty(t, p.GetEtag())

	byID, err := c.projects.GetProject(t.Context(), &resourcemanagerpb.GetProjectRequest{Name: "projects/" + id})
	require.NoError(t, err)
	assert.Equal(t, p.GetName(), byID.GetName())

	byNum, err := c.projects.GetProject(t.Context(), &resourcemanagerpb.GetProjectRequest{Name: p.GetName()})
	require.NoError(t, err)
	assert.Equal(t, id, byNum.GetProjectId())

	up, err := c.projects.UpdateProject(t.Context(), &resourcemanagerpb.UpdateProjectRequest{
		Project:    &resourcemanagerpb.Project{Name: p.GetName(), DisplayName: "Renamed", Labels: map[string]string{"team": "a"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "labels"}},
	})
	require.NoError(t, err)

	updated, err := up.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "Renamed", updated.GetDisplayName())
	assert.Equal(t, map[string]string{"team": "a"}, updated.GetLabels())
	assert.NotEqual(t, p.GetEtag(), updated.GetEtag())

	del, err := c.projects.DeleteProject(t.Context(), &resourcemanagerpb.DeleteProjectRequest{Name: p.GetName()})
	require.NoError(t, err)

	deleted, err := del.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, resourcemanagerpb.Project_DELETE_REQUESTED, deleted.GetState())
	assert.NotNil(t, deleted.GetDeleteTime())

	again, err := c.projects.DeleteProject(t.Context(), &resourcemanagerpb.DeleteProjectRequest{Name: p.GetName()})
	require.NoError(t, err, "deleting a DELETE_REQUESTED project is idempotent")

	_, err = again.Wait(t.Context())
	require.NoError(t, err)

	_, err = c.projects.UpdateProject(t.Context(), &resourcemanagerpb.UpdateProjectRequest{
		Project: &resourcemanagerpb.Project{Name: p.GetName(), DisplayName: "Nope"},
	})
	assert.Equal(t, codes.FailedPrecondition, codeOf(err))

	dup, err := c.projects.CreateProject(t.Context(), &resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: id},
	})
	if err == nil {
		_, err = dup.Wait(t.Context())
	}

	assert.Equal(t, codes.AlreadyExists, codeOf(err), "a deleted project keeps its id")

	un, err := c.projects.UndeleteProject(t.Context(), &resourcemanagerpb.UndeleteProjectRequest{Name: p.GetName()})
	require.NoError(t, err)

	restored, err := un.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, resourcemanagerpb.Project_ACTIVE, restored.GetState())
	assert.Nil(t, restored.GetDeleteTime())

	_, err = c.projects.UndeleteProject(t.Context(), &resourcemanagerpb.UndeleteProjectRequest{Name: p.GetName()})
	assert.Equal(t, codes.FailedPrecondition, codeOf(err), "undelete of an ACTIVE project")
	})
}

func TestProjectNotFound(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
	_, err := c.projects.GetProject(t.Context(), &resourcemanagerpb.GetProjectRequest{Name: "projects/does-not-exist"})
	assert.Equal(t, codes.PermissionDenied, codeOf(err))
	assert.Contains(t, err.Error(), "or it may not exist")
	})
}

func TestProjectRetentionPurge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		retention time.Duration
		advance   time.Duration
		code      codes.Code
		reuse     bool
	}{
		{"within retention", 24 * time.Hour, time.Hour, codes.OK, false},
		{"past retention", 24 * time.Hour, 25 * time.Hour, codes.PermissionDenied, true},
		{"zero purges at once", 0, 0, codes.PermissionDenied, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, withRetention(tt.retention))
			p := e.grpc.createProject(t, "purge-me", "")

			op, err := e.grpc.projects.DeleteProject(t.Context(), &resourcemanagerpb.DeleteProjectRequest{Name: p.GetName()})
			require.NoError(t, err)

			_, err = op.Wait(t.Context())
			require.NoError(t, err)

			e.clock.Advance(tt.advance)

			_, err = e.grpc.projects.GetProject(t.Context(), &resourcemanagerpb.GetProjectRequest{Name: p.GetName()})
			assert.Equal(t, tt.code, codeOf(err))

			if tt.reuse {
				assert.NotEmpty(t, e.grpc.createProject(t, "purge-me", "").GetName(), "a purged id can be created again")
			}
		})
	}
}

func TestProjectEtagConflict(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
	p := c.createProject(t, "etag-x", "")

	_, err := c.projects.UpdateProject(t.Context(), &resourcemanagerpb.UpdateProjectRequest{
		Project:    &resourcemanagerpb.Project{Name: p.GetName(), DisplayName: "Stale etag", Etag: "stale"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	assert.Equal(t, codes.Aborted, codeOf(err))

	op, err := c.projects.UpdateProject(t.Context(), &resourcemanagerpb.UpdateProjectRequest{
		Project:    &resourcemanagerpb.Project{Name: p.GetName(), DisplayName: "Fresh etag", Etag: p.GetEtag()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)
	})
}

func TestProjectUpdateMask(t *testing.T) {
	t.Parallel()

	c := newEnv(t).grpc
	p := c.createProject(t, "mask-proj", "")

	_, err := c.projects.UpdateProject(t.Context(), &resourcemanagerpb.UpdateProjectRequest{
		Project:    &resourcemanagerpb.Project{Name: p.GetName(), Parent: orgName},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"parent"}},
	})
	assert.Equal(t, codes.InvalidArgument, codeOf(err))

	op, err := c.projects.UpdateProject(t.Context(), &resourcemanagerpb.UpdateProjectRequest{
		Project:    &resourcemanagerpb.Project{Name: p.GetName(), Labels: map[string]string{"a": "b"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "Project mask-proj", got.GetDisplayName(), "display name is outside the mask")

	op, err = c.projects.UpdateProject(t.Context(), &resourcemanagerpb.UpdateProjectRequest{
		Project:    &resourcemanagerpb.Project{Name: p.GetName()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	require.NoError(t, err)

	got, err = op.Wait(t.Context())
	require.NoError(t, err)
	assert.Empty(t, got.GetLabels(), "an empty labels field under the mask clears labels")
}

func TestProjectListSearchMove(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
	folder := c.createFolder(t, orgName, "Team x")
	a := c.createProject(t, "list-a-x", folder.GetName())
	b := c.createProject(t, "list-b-x", folder.GetName())
	_ = c.createProject(t, "list-c-x", orgName)

	it := c.projects.ListProjects(t.Context(), &resourcemanagerpb.ListProjectsRequest{Parent: folder.GetName()})

	var got []string

	for {
		p, err := it.Next()
		if err != nil {
			break
		}

		got = append(got, p.GetProjectId())
	}

	assert.Equal(t, []string{a.GetProjectId(), b.GetProjectId()}, got, "ordered by display name")

	search := func(q string) []string {
		var ids []string

		s := c.projects.SearchProjects(t.Context(), &resourcemanagerpb.SearchProjectsRequest{Query: q})

		for {
			p, err := s.Next()
			if err != nil {
				return ids
			}

			ids = append(ids, p.GetProjectId())
		}
	}

	assert.Equal(t, []string{"list-a-x"}, search("id:list-a-x"))
	assert.ElementsMatch(t, []string{a.GetProjectId(), b.GetProjectId()}, search("parent:"+folder.GetName()))
	assert.ElementsMatch(t, []string{"list-a-x", "list-c-x"}, search("id:list-a-x id:list-c-x"), "terms match any")
	assert.Equal(t, []string{"list-b-x"}, search(`displayName:"Project list-b*"`))

	mv, err := c.projects.MoveProject(t.Context(), &resourcemanagerpb.MoveProjectRequest{
		Name: a.GetName(), DestinationParent: orgName,
	})
	require.NoError(t, err)

	moved, err := mv.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, orgName, moved.GetParent())

	_, err = c.projects.MoveProject(t.Context(), &resourcemanagerpb.MoveProjectRequest{
		Name: a.GetName(), DestinationParent: "folders/1",
	})
	assert.Equal(t, codes.PermissionDenied, codeOf(err))

	_, err = c.projects.ListProjects(t.Context(), &resourcemanagerpb.ListProjectsRequest{}).Next()
	assert.Equal(t, codes.InvalidArgument, codeOf(err))

	_, err = c.projects.SearchProjects(t.Context(), &resourcemanagerpb.SearchProjectsRequest{Query: "bogus:x"}).Next()
	assert.Equal(t, codes.InvalidArgument, codeOf(err))
	})
}

func TestSearchProjectsStateAndDeleted(t *testing.T) {
	t.Parallel()

	c := newEnv(t).grpc
	p := c.createProject(t, "gone-soon", orgName)

	op, err := c.projects.DeleteProject(t.Context(), &resourcemanagerpb.DeleteProjectRequest{Name: p.GetName()})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	count := func(parent, q string, show bool) int {
		n := 0

		if q != "" {
			it := c.projects.SearchProjects(t.Context(), &resourcemanagerpb.SearchProjectsRequest{Query: q})
			for _, err := it.Next(); err == nil; _, err = it.Next() {
				n++
			}

			return n
		}

		it := c.projects.ListProjects(t.Context(), &resourcemanagerpb.ListProjectsRequest{Parent: parent, ShowDeleted: show})
		for _, err := it.Next(); err == nil; _, err = it.Next() {
			n++
		}

		return n
	}

	assert.Equal(t, 0, count(orgName, "", false), "list hides deleted by default")
	assert.Equal(t, 1, count(orgName, "", true), "show_deleted lists it")
	assert.Equal(t, 0, count("", "id:gone-soon", false), "search hides deleted by default")
	assert.Equal(t, 1, count("", "id:gone-soon state:DELETE_REQUESTED", false), "a state term widens the search")
}

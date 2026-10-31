package gcpresourcemanager_test

import (
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/services/gcpresourcemanager"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	c := src.grpc

	folder := c.createFolder(t, orgName, "Persisted")
	proj := c.createProject(t, "persisted-proj", folder.GetName())
	key := mustTagKey(t, c, "persist")
	val := mustTagValue(t, c, key.GetName(), "yes")

	bind, err := c.bindings.CreateTagBinding(t.Context(), &resourcemanagerpb.CreateTagBindingRequest{
		TagBinding: &resourcemanagerpb.TagBinding{
			Parent: "//cloudresourcemanager.googleapis.com/" + proj.GetName(), TagValue: val.GetName(),
		},
	})
	require.NoError(t, err)

	_, err = bind.Wait(t.Context())
	require.NoError(t, err)

	pol, err := c.projects.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: proj.GetName(),
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: "roles/viewer", Members: []string{"user:a@example.com"}}},
		},
	})
	require.NoError(t, err)

	en, err := c.usage.EnableService(t.Context(), &serviceusagepb.EnableServiceRequest{
		Name: svcName(proj.GetProjectId(), "run.googleapis.com"),
	})
	require.NoError(t, err)

	_, err = en.Wait(t.Context())
	require.NoError(t, err)

	del, err := c.projects.DeleteProject(t.Context(), &resourcemanagerpb.DeleteProjectRequest{Name: proj.GetName()})
	require.NoError(t, err)

	_, err = del.Wait(t.Context())
	require.NoError(t, err)

	want, err := c.projects.GetProject(t.Context(), &resourcemanagerpb.GetProjectRequest{Name: proj.GetName()})
	require.NoError(t, err)

	snap := src.backend.Snapshot(t.Context())

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	d := dst.grpc

	got, err := d.projects.GetProject(t.Context(), &resourcemanagerpb.GetProjectRequest{Name: proj.GetName()})
	require.NoError(t, err)
	assert.True(t, proto.Equal(want, got), "project survives with state, parent, times and etag")
	assert.Equal(t, resourcemanagerpb.Project_DELETE_REQUESTED, got.GetState())

	gotFolder, err := d.folders.GetFolder(t.Context(), &resourcemanagerpb.GetFolderRequest{Name: folder.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "Persisted", gotFolder.GetDisplayName())

	policy, err := d.projects.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: proj.GetName()})
	require.NoError(t, err)
	assert.Equal(t, pol.GetEtag(), policy.GetEtag())

	_, err = d.projects.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: proj.GetName(), Policy: &iampb.Policy{Etag: pol.GetEtag()},
	})
	assert.Equal(t, codes.FailedPrecondition, codeOf(err), "restored project is DELETE_REQUESTED")

	nsKey, err := d.tagKeys.GetNamespacedTagKey(
		t.Context(),
		&resourcemanagerpb.GetNamespacedTagKeyRequest{Name: "123456789012/persist"},
	)
	require.NoError(t, err)
	assert.Equal(t, key.GetName(), nsKey.GetName())

	_, err = d.bindings.ListTagBindings(t.Context(), &resourcemanagerpb.ListTagBindingsRequest{
		Parent: "//cloudresourcemanager.googleapis.com/" + proj.GetName(),
	}).Next()
	require.NoError(t, err)

	opGot, err := d.folders.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: bind.Name()})
	require.NoError(t, err)
	assert.True(t, opGot.GetDone(), "operations survive")

	un, err := d.projects.UndeleteProject(t.Context(), &resourcemanagerpb.UndeleteProjectRequest{Name: proj.GetName()})
	require.NoError(t, err)

	_, err = un.Wait(t.Context())
	require.NoError(t, err)

	svc, err := d.usage.GetService(
		t.Context(),
		&serviceusagepb.GetServiceRequest{Name: svcName(proj.GetProjectId(), "run.googleapis.com")},
	)
	require.NoError(t, err)
	assert.Equal(t, serviceusagepb.State_ENABLED, svc.GetState(), "enabled services survive")
}

func TestRestoreRejectsOtherVersion(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.grpc.createProject(t, "before-restore", "")

	require.NoError(t, e.backend.Restore(t.Context(), []byte(`{"version":999}`)))
	assert.Equal(t, []string{seedID}, e.backend.ProjectIDs(), "an incompatible snapshot resets to the seed")
}

func TestSeedAndReset(t *testing.T) {
	t.Parallel()

	b := gcpresourcemanager.NewBackend(gcpresourcemanager.Settings{
		OrganizationID: "999", Domain: "corp.example", SeedProjects: "first-proj, second-proj,,",
		DeleteRetention: time.Hour,
	})
	assert.Equal(t, []string{"first-proj", "second-proj"}, b.ProjectIDs())
	assert.Equal(t, "organizations/999", b.OrganizationName())

	h := gcpresourcemanager.NewHandler(b)
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	op, err := h.ProjectsAPI().CreateProject(t.Context(), &resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: "extra-proj", Parent: "organizations/999"},
	})
	require.NoError(t, err)
	assert.True(t, op.GetDone())
	assert.Len(t, b.ProjectIDs(), 3)

	h.Reset()
	assert.Equal(t, []string{"first-proj", "second-proj"}, b.ProjectIDs(), "reset restores the seed")
	assert.Equal(t, "organizations/999", b.OrganizationName())
}

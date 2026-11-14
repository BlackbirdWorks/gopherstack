package gcpresourcemanager_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	crm "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

func TestLiens(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	proj := e.grpc.createProject(t, "lien-proj", "")

	svc, err := crm.NewService(ctx, option.WithEndpoint("http://"+e.addr), option.WithoutAuthentication())
	require.NoError(t, err)

	tests := []struct {
		lien *crm.Lien
		name string
		code int
	}{
		{&crm.Lien{Parent: proj.GetName()}, "no restrictions", http.StatusBadRequest},
		{
			&crm.Lien{Parent: proj.GetName(), Restrictions: []string{"compute.instances.delete"}},
			"unsupported restriction",
			400,
		},
		{&crm.Lien{Parent: "folders/1", Restrictions: []string{"resourcemanager.projects.delete"}}, "bad parent", 400},
		{&crm.Lien{Parent: "projects/nope", Restrictions: []string{"resourcemanager.projects.delete"}}, "unknown", 403},
	}

	for _, tt := range tests {
		_, cerr := svc.Liens.Create(tt.lien).Context(ctx).Do()

		var gerr *googleapi.Error

		require.ErrorAs(t, cerr, &gerr, tt.name)
		assert.Equal(t, tt.code, gerr.Code, tt.name)
	}

	lien, err := svc.Liens.Create(&crm.Lien{
		Parent: "projects/lien-proj", Restrictions: []string{"resourcemanager.projects.delete"},
		Origin: "test", Reason: "protect",
	}).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, proj.GetName(), lien.Parent)
	assert.Regexp(t, `^liens/p\d+-l`, lien.Name)

	got, err := svc.Liens.Get(lien.Name).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "protect", got.Reason)

	list, err := svc.Liens.List().Parent(proj.GetName()).Context(ctx).Do()
	require.NoError(t, err)
	assert.Len(t, list.Liens, 1)

	_, err = e.grpc.projects.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: proj.GetName()})
	assert.Equal(t, codes.FailedPrecondition, codeOf(err), "a lien blocks project deletion")

	_, err = svc.Liens.Delete(lien.Name).Context(ctx).Do()
	require.NoError(t, err)

	_, err = svc.Liens.Get(lien.Name).Context(ctx).Do()
	require.Error(t, err)

	op, err := e.grpc.projects.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: proj.GetName()})
	require.NoError(t, err)

	_, err = op.Wait(ctx)
	require.NoError(t, err)
}

func TestLiensPersist(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	ctx := t.Context()
	proj := src.grpc.createProject(t, "lien-persist", "")

	svc, err := crm.NewService(ctx, option.WithEndpoint("http://"+src.addr), option.WithoutAuthentication())
	require.NoError(t, err)

	lien, err := svc.Liens.Create(&crm.Lien{
		Parent: proj.GetName(), Restrictions: []string{"resourcemanager.projects.delete"},
	}).Context(ctx).Do()
	require.NoError(t, err)

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(ctx, src.backend.Snapshot(ctx)))

	svc2, err := crm.NewService(ctx, option.WithEndpoint("http://"+dst.addr), option.WithoutAuthentication())
	require.NoError(t, err)

	got, err := svc2.Liens.Get(lien.Name).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, lien.Parent, got.Parent)
}

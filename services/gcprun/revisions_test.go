package gcprun_test

import (
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRevisions_GetListDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	v1 := e.createService(t, "app", "example/app:1")

	rev, err := e.revs.GetRevision(t.Context(), &runpb.GetRevisionRequest{Name: v1.GetLatestReadyRevision()})
	require.NoError(t, err)
	assert.Equal(t, v1.GetName(), rev.GetService())
	assert.Equal(t, "example/app:1", rev.GetContainers()[0].GetImage())
	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, rev.GetConditions()[0].GetState())

	_, err = e.revs.GetRevision(t.Context(), &runpb.GetRevisionRequest{Name: v1.GetName() + "/revisions/none"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.revs.DeleteRevision(t.Context(), &runpb.DeleteRevisionRequest{Name: rev.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "serving revision cannot be deleted")

	upd := svcSpec("example/app:2")
	upd.Name = v1.GetName()

	op, err := e.services.UpdateService(t.Context(), &runpb.UpdateServiceRequest{Service: upd})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	del, err := e.revs.DeleteRevision(t.Context(), &runpb.DeleteRevisionRequest{Name: rev.GetName()})
	require.NoError(t, err)

	deleted, err := del.Wait(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, deleted.GetDeleteTime())

	_, err = e.revs.GetRevision(t.Context(), &runpb.GetRevisionRequest{Name: rev.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.revs.ListRevisions(t.Context(), &runpb.ListRevisionsRequest{Parent: parent + "/services/none"}).Next()
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestRevisions_ExplicitName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rev  string
		code codes.Code
	}{
		{"prefixed", "named-v1", codes.OK},
		{"unprefixed", "other-v1", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			s := svcSpec("i")
			s.Template.Revision = tt.rev

			op, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
				Parent: parent, ServiceId: "named", Service: s,
			})
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)

			svc, err := op.Wait(t.Context())
			require.NoError(t, err)
			assert.Equal(t, parent+"/services/named/revisions/"+tt.rev, svc.GetLatestReadyRevision())
		})
	}
}

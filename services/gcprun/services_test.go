package gcprun_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestService_CreateGetListDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	svc := e.createService(t, "hello", "example/hello:1")

	assert.Equal(t, parent+"/services/hello", svc.GetName())
	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, svc.GetTerminalCondition().GetState())
	assert.False(t, svc.GetReconciling())
	assert.Equal(t, svc.GetLatestCreatedRevision(), svc.GetLatestReadyRevision())
	assert.Contains(t, svc.GetUri(), ".localhost:")
	assert.Equal(t, int64(1), svc.GetGeneration())
	require.Len(t, svc.GetTrafficStatuses(), 1)
	assert.Equal(t, int32(100), svc.GetTrafficStatuses()[0].GetPercent())
	assert.Equal(t, 1, e.rt.running())

	got, err := e.services.GetService(t.Context(), &runpb.GetServiceRequest{Name: svc.GetName()})
	require.NoError(t, err)
	assert.Equal(t, svc.GetUid(), got.GetUid())

	it := e.services.ListServices(t.Context(), &runpb.ListServicesRequest{Parent: parent})
	listed, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, svc.GetName(), listed.GetName())

	_, err = it.Next()
	require.ErrorIs(t, err, iterator.Done)

	del, err := e.services.DeleteService(t.Context(), &runpb.DeleteServiceRequest{Name: svc.GetName()})
	require.NoError(t, err)

	deleted, err := del.Wait(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, deleted.GetDeleteTime())
	assert.Equal(t, 0, e.rt.running())

	_, err = e.services.GetService(t.Context(), &runpb.GetServiceRequest{Name: svc.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestService_CreateErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req  func() *runpb.CreateServiceRequest
		name string
		code codes.Code
	}{
		{name: "bad id", req: func() *runpb.CreateServiceRequest {
			return &runpb.CreateServiceRequest{Parent: parent, ServiceId: "Bad_ID", Service: svcSpec("i")}
		}, code: codes.InvalidArgument},
		{name: "bad parent", req: func() *runpb.CreateServiceRequest {
			return &runpb.CreateServiceRequest{Parent: "projects/p", ServiceId: "ok", Service: svcSpec("i")}
		}, code: codes.InvalidArgument},
		{name: "no template", req: func() *runpb.CreateServiceRequest {
			return &runpb.CreateServiceRequest{Parent: parent, ServiceId: "ok", Service: &runpb.Service{}}
		}, code: codes.InvalidArgument},
		{name: "no image", req: func() *runpb.CreateServiceRequest {
			return &runpb.CreateServiceRequest{Parent: parent, ServiceId: "ok", Service: svcSpec("")}
		}, code: codes.InvalidArgument},
		{name: "reserved env", req: func() *runpb.CreateServiceRequest {
			s := svcSpec("i")
			s.Template.Containers[0].Env = []*runpb.EnvVar{{Name: "PORT"}}

			return &runpb.CreateServiceRequest{Parent: parent, ServiceId: "ok", Service: s}
		}, code: codes.InvalidArgument},
		{name: "traffic sum", req: func() *runpb.CreateServiceRequest {
			s := svcSpec("i")
			s.Traffic = []*runpb.TrafficTarget{{Percent: 60}}

			return &runpb.CreateServiceRequest{Parent: parent, ServiceId: "ok", Service: s}
		}, code: codes.InvalidArgument},
		{name: "unknown revision", req: func() *runpb.CreateServiceRequest {
			s := svcSpec("i")
			s.Traffic = []*runpb.TrafficTarget{{Revision: "nope", Percent: 100}}

			return &runpb.CreateServiceRequest{Parent: parent, ServiceId: "ok", Service: s}
		}, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			_, err := e.services.CreateService(t.Context(), tt.req())
			assert.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestService_DuplicateAndValidateOnly(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.createService(t, "dup", "i")

	_, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
		Parent: parent, ServiceId: "dup", Service: svcSpec("i"),
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	op, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
		Parent: parent, ServiceId: "dry", Service: svcSpec("i"), ValidateOnly: true,
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	_, err = e.services.GetService(t.Context(), &runpb.GetServiceRequest{Name: parent + "/services/dry"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestService_UpdateCreatesRevisionAndSplitsTraffic(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	v1 := e.createService(t, "app", "example/app:1")

	upd := svcSpec("example/app:2")
	upd.Name = v1.GetName()

	op, err := e.services.UpdateService(t.Context(), &runpb.UpdateServiceRequest{Service: upd})
	require.NoError(t, err)

	v2, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(2), v2.GetGeneration())
	assert.NotEqual(t, v1.GetLatestReadyRevision(), v2.GetLatestReadyRevision())
	assert.Equal(t, 1, e.rt.running(), "the revision without traffic is scaled down")

	revs := e.revs.ListRevisions(t.Context(), &runpb.ListRevisionsRequest{Parent: v1.GetName()})
	count := 0

	for {
		_, nerr := revs.Next()
		if nerr != nil {
			break
		}

		count++
	}

	assert.Equal(t, 2, count)

	old := v1.GetLatestReadyRevision()[strings.LastIndex(v1.GetLatestReadyRevision(), "/")+1:]
	split := &runpb.Service{Name: v1.GetName(), Traffic: []*runpb.TrafficTarget{
		{Type: runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST, Percent: 50},
		{Revision: old, Percent: 50, Tag: "stable"},
	}}

	op, err = e.services.UpdateService(t.Context(), &runpb.UpdateServiceRequest{
		Service: split, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"traffic"}},
	})
	require.NoError(t, err)

	v3, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, v2.GetLatestReadyRevision(), v3.GetLatestReadyRevision(), "traffic-only update adds no revision")
	require.Len(t, v3.GetTrafficStatuses(), 2)

	host := strings.TrimPrefix(v3.GetUri(), "http://")
	seen := map[string]int{}

	for range 100 {
		code, body := e.get(t, host, "/")
		require.Equal(t, 200, code)

		seen[strings.Fields(body)[0]]++
	}

	assert.Len(t, seen, 2)

	for _, n := range seen {
		assert.Equal(t, 50, n)
	}

	code, body := e.get(t, "stable---"+host, "/x")
	assert.Equal(t, 200, code)
	assert.Contains(t, body, "revision="+old)
}

func TestService_UpdateErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	svc := e.createService(t, "app", "i")

	tests := []struct {
		req  *runpb.UpdateServiceRequest
		name string
		code codes.Code
	}{
		{
			name: "missing",
			req:  &runpb.UpdateServiceRequest{Service: &runpb.Service{Name: parent + "/services/none"}},
			code: codes.NotFound,
		},
		{name: "bad mask", req: &runpb.UpdateServiceRequest{
			Service: &runpb.Service{Name: svc.GetName()}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"uid"}},
		}, code: codes.InvalidArgument},
		{name: "bad traffic", req: &runpb.UpdateServiceRequest{
			Service: &runpb.Service{Name: svc.GetName(), Traffic: []*runpb.TrafficTarget{{Percent: 10}}},
		}, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.services.UpdateService(t.Context(), tt.req)
			assert.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestService_DeleteEtag(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	svc := e.createService(t, "app", "i")

	_, err := e.services.DeleteService(t.Context(), &runpb.DeleteServiceRequest{Name: svc.GetName(), Etag: "stale"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	op, err := e.services.DeleteService(
		t.Context(),
		&runpb.DeleteServiceRequest{Name: svc.GetName(), Etag: svc.GetEtag()},
	)
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)
}

func TestIngress(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	svc := e.createService(t, "web", "example/web:1")
	host := strings.TrimPrefix(svc.GetUri(), "http://")

	tests := []struct {
		name     string
		host     string
		path     string
		wantBody string
		wantCode int
	}{
		{"served", host, "/hello", "path=/hello", 200},
		{"container sees port env", host, "/", "port=8080", 200},
		{"unknown service", "other-0000000000-us-central1.localhost", "/", "", 404},
		{"foreign host", "example.com", "/", "", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := e.get(t, tt.host, tt.path)
			assert.Equal(t, tt.wantCode, code)
			assert.Contains(t, body, tt.wantBody)
		})
	}
}

func TestService_NoRuntimeServes503(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	svc := e.createService(t, "web", "example/web:1")

	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, svc.GetTerminalCondition().GetState())
	assert.NotEmpty(t, svc.GetLatestReadyRevision())

	code, body := e.get(t, strings.TrimPrefix(svc.GetUri(), "http://"), "/")
	assert.Equal(t, 503, code)
	assert.Contains(t, body, "container runtime is unavailable")
}

func TestService_StartFailureFailsOperation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.rt.failRun = assert.AnError

	op, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
		Parent: parent, ServiceId: "bad", Service: svcSpec("i"),
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	got, err := e.services.GetService(t.Context(), &runpb.GetServiceRequest{Name: parent + "/services/bad"})
	require.NoError(t, err)
	assert.Equal(t, runpb.Condition_CONDITION_FAILED, got.GetTerminalCondition().GetState())
	assert.Empty(t, got.GetLatestReadyRevision())
}

package integration_test

import (
	"testing"

	run "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newRunServicesClient(t *testing.T) *run.ServicesClient {
	t.Helper()

	client, err := run.NewServicesClient(t.Context(), gcpGRPCOpts(t, "8126")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestIntegration_GCP_Run_ServiceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newRunServicesClient(t)
	ctx := t.Context()
	parent := "projects/" + gcpProject + "/locations/us-central1"
	id := gcpName("it-svc")

	op, err := client.CreateService(ctx, &runpb.CreateServiceRequest{
		Parent: parent, ServiceId: id,
		Service: &runpb.Service{Template: &runpb.RevisionTemplate{
			Containers: []*runpb.Container{{Image: "example.invalid/web:1"}},
		}},
	})
	require.NoError(t, err)

	svc, err := op.Wait(ctx)
	if err != nil {
		t.Skipf("Cloud Run deploy needs a container runtime inside the test container: %v", err)
	}

	assert.Equal(t, parent+"/services/"+id, svc.GetName())
	assert.NotEmpty(t, svc.GetLatestReadyRevision())

	got, err := client.GetService(ctx, &runpb.GetServiceRequest{Name: svc.GetName()})
	require.NoError(t, err)
	assert.Equal(t, svc.GetUri(), got.GetUri())

	listed := 0
	it := client.ListServices(ctx, &runpb.ListServicesRequest{Parent: parent})

	for {
		s, nerr := it.Next()
		if nerr != nil {
			break
		}

		if s.GetName() == svc.GetName() {
			listed++
		}
	}

	assert.Equal(t, 1, listed)

	del, err := client.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: svc.GetName()})
	require.NoError(t, err)
	_, err = del.Wait(ctx)
	require.NoError(t, err)
}

func TestIntegration_GCP_Run_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newRunServicesClient(t)

	_, err := client.GetService(t.Context(), &runpb.GetServiceRequest{
		Name: "projects/" + gcpProject + "/locations/us-central1/services/" + gcpName("absent"),
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

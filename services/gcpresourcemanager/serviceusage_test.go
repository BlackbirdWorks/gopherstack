package gcpresourcemanager_test

import (
	"testing"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpresourcemanager"
)

func svcName(project, svc string) string { return "projects/" + project + "/services/" + svc }

func listServices(t *testing.T, c *clients, parent, filter string) []*serviceusagepb.Service {
	t.Helper()

	var out []*serviceusagepb.Service

	it := c.usage.ListServices(t.Context(), &serviceusagepb.ListServicesRequest{Parent: parent, Filter: filter})
	for s, err := it.Next(); err == nil; s, err = it.Next() {
		out = append(out, s)
	}

	return out
}

func stateOf(t *testing.T, c *clients, project, svc string) serviceusagepb.State {
	t.Helper()

	s, err := c.usage.GetService(t.Context(), &serviceusagepb.GetServiceRequest{Name: svcName(project, svc)})
	require.NoError(t, err)

	return s.GetState()
}

func TestServiceUsageListAndGet(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		all := listServices(t, c, "projects/"+seedID, "")
		assert.Len(t, all, len(gcpresourcemanager.CatalogServices()))

		enabled := listServices(t, c, "projects/"+seedID, "state:ENABLED")
		disabled := listServices(t, c, "projects/"+seedID, "state:DISABLED")
		assert.Len(t, enabled, len(all)-len(disabled))
		assert.NotEmpty(t, enabled, "API-created projects start with default services")

		for _, s := range enabled {
			assert.Equal(t, serviceusagepb.State_ENABLED, s.GetState())
		}

		names := map[string]bool{}
		for _, s := range disabled {
			names[s.GetConfig().GetName()] = true
		}

		for _, want := range []string{"compute.googleapis.com", "pubsub.googleapis.com", "run.googleapis.com",
			"secretmanager.googleapis.com", "container.googleapis.com", "sqladmin.googleapis.com"} {
			assert.True(t, names[want], want)
		}

		got, err := c.usage.GetService(t.Context(), &serviceusagepb.GetServiceRequest{
			Name: svcName(seedID, "storage.googleapis.com"),
		})
		require.NoError(t, err)
		assert.Equal(t, serviceusagepb.State_ENABLED, got.GetState())
		assert.Equal(t, "Cloud Storage API", got.GetConfig().GetTitle())
		assert.Regexp(t, `^projects/\d{12}/services/storage\.googleapis\.com$`, got.GetName(), "name carries the project number")
		assert.Regexp(t, `^projects/\d{12}$`, got.GetParent())

		_, err = c.usage.GetService(t.Context(), &serviceusagepb.GetServiceRequest{Name: svcName(seedID, "bogus.googleapis.com")})
		assert.Equal(t, codes.PermissionDenied, codeOf(err))

		_, err = c.usage.GetService(t.Context(), &serviceusagepb.GetServiceRequest{Name: svcName("nope-project", "storage.googleapis.com")})
		assert.Equal(t, codes.PermissionDenied, codeOf(err))

		it := c.usage.ListServices(t.Context(), &serviceusagepb.ListServicesRequest{Parent: "projects/" + seedID, Filter: "state:BOGUS"})
		_, err = it.Next()
		assert.Equal(t, codes.InvalidArgument, codeOf(err))
	})
}

func TestServiceUsageEnableDisable(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		const svc = "pubsub.googleapis.com"

		assert.Equal(t, serviceusagepb.State_DISABLED, stateOf(t, c, seedID, svc))

		op, err := c.usage.EnableService(t.Context(), &serviceusagepb.EnableServiceRequest{Name: svcName(seedID, svc)})
		require.NoError(t, err)

		resp, err := op.Wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, serviceusagepb.State_ENABLED, resp.GetService().GetState())
		assert.Equal(t, serviceusagepb.State_ENABLED, stateOf(t, c, seedID, svc))

		again, err := c.usage.EnableService(t.Context(), &serviceusagepb.EnableServiceRequest{Name: svcName(seedID, svc)})
		require.NoError(t, err, "enabling an enabled service succeeds")

		_, err = again.Wait(t.Context())
		require.NoError(t, err)

		off, err := c.usage.DisableService(t.Context(), &serviceusagepb.DisableServiceRequest{Name: svcName(seedID, svc)})
		require.NoError(t, err)

		dis, err := off.Wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, serviceusagepb.State_DISABLED, dis.GetService().GetState())

		_, err = c.usage.DisableService(t.Context(), &serviceusagepb.DisableServiceRequest{Name: svcName(seedID, svc)})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "disabling a disabled service")

		_, err = c.usage.EnableService(t.Context(), &serviceusagepb.EnableServiceRequest{Name: svcName(seedID, "bogus.googleapis.com")})
		assert.Equal(t, codes.PermissionDenied, codeOf(err))

		_, err = c.usage.EnableService(t.Context(), &serviceusagepb.EnableServiceRequest{Name: "folders/1/services/x"})
		assert.Equal(t, codes.InvalidArgument, codeOf(err), "only projects are supported")
	})
}

func TestServiceUsageDependencies(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		en, err := c.usage.EnableService(t.Context(), &serviceusagepb.EnableServiceRequest{
			Name: svcName(seedID, "container.googleapis.com"),
		})
		require.NoError(t, err)

		_, err = en.Wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, serviceusagepb.State_ENABLED, stateOf(t, c, seedID, "compute.googleapis.com"), "dependency enabled with it")

		_, err = c.usage.DisableService(t.Context(), &serviceusagepb.DisableServiceRequest{Name: svcName(seedID, "compute.googleapis.com")})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "an enabled service depends on it")
		assert.Equal(t, serviceusagepb.State_ENABLED, stateOf(t, c, seedID, "compute.googleapis.com"))

		off, err := c.usage.DisableService(t.Context(), &serviceusagepb.DisableServiceRequest{
			Name: svcName(seedID, "compute.googleapis.com"), DisableDependentServices: true,
			CheckIfServiceHasUsage: serviceusagepb.DisableServiceRequest_CHECK,
		})
		require.NoError(t, err)

		_, err = off.Wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, serviceusagepb.State_DISABLED, stateOf(t, c, seedID, "compute.googleapis.com"))
		assert.Equal(t, serviceusagepb.State_DISABLED, stateOf(t, c, seedID, "container.googleapis.com"), "dependent disabled too")
	})
}

func TestServiceUsageBatch(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		parent := "projects/" + seedID

		op, err := c.usage.BatchEnableServices(t.Context(), &serviceusagepb.BatchEnableServicesRequest{
			Parent: parent, ServiceIds: []string{"run.googleapis.com", "secretmanager.googleapis.com"},
		})
		require.NoError(t, err)

		resp, err := op.Wait(t.Context())
		require.NoError(t, err)
		assert.Len(t, resp.GetServices(), 2)

		_, err = c.usage.BatchEnableServices(t.Context(), &serviceusagepb.BatchEnableServicesRequest{
			Parent: parent, ServiceIds: []string{"dns.googleapis.com", "bogus.googleapis.com"},
		})
		assert.Equal(t, codes.PermissionDenied, codeOf(err))
		assert.Equal(t, serviceusagepb.State_DISABLED, stateOf(t, c, seedID, "dns.googleapis.com"), "the batch is atomic")

		tooMany := make([]string, 21)
		for i := range tooMany {
			tooMany[i] = "dns.googleapis.com"
		}

		_, err = c.usage.BatchEnableServices(t.Context(), &serviceusagepb.BatchEnableServicesRequest{Parent: parent, ServiceIds: tooMany})
		assert.Equal(t, codes.InvalidArgument, codeOf(err))

		_, err = c.usage.BatchEnableServices(t.Context(), &serviceusagepb.BatchEnableServicesRequest{Parent: parent})
		assert.Equal(t, codes.InvalidArgument, codeOf(err))

		got, err := c.usage.BatchGetServices(t.Context(), &serviceusagepb.BatchGetServicesRequest{
			Parent: parent, Names: []string{svcName(seedID, "run.googleapis.com"), svcName(seedID, "dns.googleapis.com")},
		})
		require.NoError(t, err)
		require.Len(t, got.GetServices(), 2)
		assert.Equal(t, serviceusagepb.State_ENABLED, got.GetServices()[0].GetState())
		assert.Equal(t, serviceusagepb.State_DISABLED, got.GetServices()[1].GetState())

		_, err = c.usage.BatchGetServices(t.Context(), &serviceusagepb.BatchGetServicesRequest{
			Parent: "projects/other", Names: []string{svcName(seedID, "run.googleapis.com")},
		})
		assert.Equal(t, codes.InvalidArgument, codeOf(err), "names must belong to the parent")
	})
}

func TestServiceUsageDeletedProject(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		p := c.createProject(t, "su-deleted", "")
		name := p.GetName() + "/services/storage.googleapis.com"

		_, err := c.usage.GetService(t.Context(), &serviceusagepb.GetServiceRequest{Name: name})
		require.NoError(t, err)

		op, err := c.projects.DeleteProject(t.Context(), &resourcemanagerpb.DeleteProjectRequest{Name: p.GetName()})
		require.NoError(t, err)

		_, err = op.Wait(t.Context())
		require.NoError(t, err)

		_, err = c.usage.GetService(t.Context(), &serviceusagepb.GetServiceRequest{Name: name})
		assert.Equal(t, codes.PermissionDenied, codeOf(err), "a DELETE_REQUESTED project is not a valid consumer")
	})
}

func TestCheckServiceEnabled(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct {
		name    string
		project string
		service string
		reason  string
		code    codes.Code
	}{
		{"enabled", seedID, "storage.googleapis.com", "", codes.OK},
		{"unknown service passes", seedID, "not-in-catalog.googleapis.com", "", codes.OK},
		{"disabled", seedID, "compute.googleapis.com", "SERVICE_DISABLED", codes.PermissionDenied},
		{"unknown project", "nope-nope", "storage.googleapis.com", "CONSUMER_INVALID", codes.PermissionDenied},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := e.backend.CheckServiceEnabled(tt.project, tt.service)
			assert.Equal(t, tt.code, status.Code(err))

			if tt.reason == "" {
				return
			}

			var info *errdetails.ErrorInfo

			for _, d := range status.Convert(err).Details() {
				if i, ok := d.(*errdetails.ErrorInfo); ok {
					info = i
				}
			}

			require.NotNil(t, info)
			assert.Equal(t, tt.reason, info.GetReason())
			assert.Equal(t, "googleapis.com", info.GetDomain())

			if tt.reason == "SERVICE_DISABLED" {
				assert.Equal(t, tt.service, info.GetMetadata()["service"])
				assert.Regexp(t, `^\d{12}$`, info.GetMetadata()["containerInfo"])
				assert.Contains(t, status.Convert(err).Message(), "Compute Engine API has not been used in project")
			}
		})
	}
}

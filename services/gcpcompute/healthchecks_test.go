package gcpcompute_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestHealthCheckSpecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hc       *computepb.HealthCheck
		check    func(t *testing.T, h *computepb.HealthCheck)
		name     string
		wantType string
	}{
		{
			name: "http defaults", wantType: "HTTP",
			hc: &computepb.HealthCheck{HttpHealthCheck: &computepb.HTTPHealthCheck{}},
			check: func(t *testing.T, h *computepb.HealthCheck) {
				t.Helper()
				assert.Equal(t, int32(80), h.GetHttpHealthCheck().GetPort())
				assert.Equal(t, "/", h.GetHttpHealthCheck().GetRequestPath())
				assert.Equal(t, "USE_FIXED_PORT", h.GetHttpHealthCheck().GetPortSpecification())
				assert.Equal(t, "NONE", h.GetHttpHealthCheck().GetProxyHeader())
				assert.Equal(t, int32(5), h.GetCheckIntervalSec())
				assert.Equal(t, int32(5), h.GetTimeoutSec())
				assert.Equal(t, int32(2), h.GetHealthyThreshold())
				assert.Equal(t, int32(2), h.GetUnhealthyThreshold())
			},
		},
		{
			name: "https", wantType: "HTTPS",
			hc: &computepb.HealthCheck{HttpsHealthCheck: &computepb.HTTPSHealthCheck{RequestPath: s("/healthz")}},
			check: func(t *testing.T, h *computepb.HealthCheck) {
				t.Helper()
				assert.Equal(t, int32(443), h.GetHttpsHealthCheck().GetPort())
				assert.Equal(t, "/healthz", h.GetHttpsHealthCheck().GetRequestPath())
			},
		},
		{
			name: "http2", wantType: "HTTP2",
			hc: &computepb.HealthCheck{Http2HealthCheck: &computepb.HTTP2HealthCheck{}},
			check: func(t *testing.T, h *computepb.HealthCheck) {
				t.Helper()
				assert.Equal(t, int32(443), h.GetHttp2HealthCheck().GetPort())
			},
		},
		{
			name: "tcp", wantType: "TCP",
			hc: &computepb.HealthCheck{
				TcpHealthCheck: &computepb.TCPHealthCheck{Port: proto.Int32(3306), ProxyHeader: s("PROXY_V1")},
			},
			check: func(t *testing.T, h *computepb.HealthCheck) {
				t.Helper()
				assert.Equal(t, int32(3306), h.GetTcpHealthCheck().GetPort())
				assert.Equal(t, "PROXY_V1", h.GetTcpHealthCheck().GetProxyHeader())
			},
		},
		{
			name: "ssl", wantType: "SSL",
			hc: &computepb.HealthCheck{SslHealthCheck: &computepb.SSLHealthCheck{}},
			check: func(t *testing.T, h *computepb.HealthCheck) {
				t.Helper()
				assert.Equal(t, int32(443), h.GetSslHealthCheck().GetPort())
			},
		},
		{
			name: "grpc named port", wantType: "GRPC",
			hc: &computepb.HealthCheck{GrpcHealthCheck: &computepb.GRPCHealthCheck{PortName: s("grpc"), GrpcServiceName: s("svc")}},
			check: func(t *testing.T, h *computepb.HealthCheck) {
				t.Helper()
				assert.Equal(t, "USE_NAMED_PORT", h.GetGrpcHealthCheck().GetPortSpecification())
				assert.Equal(t, "svc", h.GetGrpcHealthCheck().GetGrpcServiceName())
			},
		},
	}

	for _, tt := range tests {
		for _, regional := range []bool{false, true} {
			name := tt.name
			if regional {
				name += " regional"
			}

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				e := newEnv(t)
				hc := proto.Clone(tt.hc).(*computepb.HealthCheck)
				hc.Name = s("hc")

				var got *computepb.HealthCheck

				if regional {
					op, err := e.regionHealthChecks(t).Insert(t.Context(),
						&computepb.InsertRegionHealthCheckRequest{Project: project, Region: region, HealthCheckResource: hc})
					e.done(t, op, err)

					got, err = e.regionHealthChecks(t).Get(t.Context(),
						&computepb.GetRegionHealthCheckRequest{Project: project, Region: region, HealthCheck: "hc"})
					require.NoError(t, err)
					assert.Contains(t, got.GetSelfLink(), "/regions/"+region+"/healthChecks/hc")
				} else {
					op, err := e.healthChecks(t).Insert(t.Context(),
						&computepb.InsertHealthCheckRequest{Project: project, HealthCheckResource: hc})
					e.done(t, op, err)

					got, err = e.healthChecks(t).Get(t.Context(),
						&computepb.GetHealthCheckRequest{Project: project, HealthCheck: "hc"})
					require.NoError(t, err)
				}

				assert.Equal(t, tt.wantType, got.GetType())
				tt.check(t, got)
			})
		}
	}
}

func TestHealthCheckValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hc     *computepb.HealthCheck
		name   string
		status int
	}{
		{name: "no spec", hc: &computepb.HealthCheck{Type: s("TCP")}, status: http.StatusBadRequest},
		{
			name: "two specs", status: http.StatusBadRequest,
			hc: &computepb.HealthCheck{
				TcpHealthCheck: &computepb.TCPHealthCheck{}, HttpHealthCheck: &computepb.HTTPHealthCheck{},
			},
		},
		{
			name: "type mismatch", status: http.StatusBadRequest,
			hc: &computepb.HealthCheck{Type: s("HTTP"), TcpHealthCheck: &computepb.TCPHealthCheck{}},
		},
		{
			name: "timeout over interval", status: http.StatusBadRequest,
			hc: &computepb.HealthCheck{
				TcpHealthCheck: &computepb.TCPHealthCheck{}, CheckIntervalSec: proto.Int32(5), TimeoutSec: proto.Int32(10),
			},
		},
		{
			name: "bad threshold", status: http.StatusBadRequest,
			hc: &computepb.HealthCheck{TcpHealthCheck: &computepb.TCPHealthCheck{}, HealthyThreshold: proto.Int32(11)},
		},
		{
			name: "port out of range", status: http.StatusBadRequest,
			hc: &computepb.HealthCheck{TcpHealthCheck: &computepb.TCPHealthCheck{Port: proto.Int32(70000)}},
		},
		{
			name: "relative path", status: http.StatusBadRequest,
			hc: &computepb.HealthCheck{HttpHealthCheck: &computepb.HTTPHealthCheck{RequestPath: s("health")}},
		},
		{
			name: "named and fixed", status: http.StatusBadRequest,
			hc: &computepb.HealthCheck{
				TcpHealthCheck: &computepb.TCPHealthCheck{PortSpecification: s("USE_NAMED_PORT"), Port: proto.Int32(80)},
			},
		},
		{name: "grpc needs a port", hc: &computepb.HealthCheck{GrpcHealthCheck: &computepb.GRPCHealthCheck{}}, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tt.hc.Name = s("hc")

			_, err := e.healthChecks(t).Insert(t.Context(),
				&computepb.InsertHealthCheckRequest{Project: project, HealthCheckResource: tt.hc})
			assertAPIError(t, err, tt.status, "")
		})
	}
}

func TestHealthCheckPatchUpdateDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.healthCheck(t, "hc")

	op, err := e.healthChecks(t).Patch(t.Context(), &computepb.PatchHealthCheckRequest{
		Project: project, HealthCheck: "hc", HealthCheckResource: &computepb.HealthCheck{CheckIntervalSec: proto.Int32(30), TimeoutSec: proto.Int32(10)},
	})
	e.done(t, op, err)

	got, err := e.healthChecks(t).Get(t.Context(), &computepb.GetHealthCheckRequest{Project: project, HealthCheck: "hc"})
	require.NoError(t, err)
	assert.Equal(t, int32(30), got.GetCheckIntervalSec())
	assert.Equal(t, int32(80), got.GetTcpHealthCheck().GetPort())

	op, err = e.healthChecks(t).Patch(t.Context(), &computepb.PatchHealthCheckRequest{
		Project: project, HealthCheck: "hc",
		HealthCheckResource: &computepb.HealthCheck{HttpHealthCheck: &computepb.HTTPHealthCheck{Port: proto.Int32(8080)}},
	})
	e.done(t, op, err)

	got, err = e.healthChecks(t).Get(t.Context(), &computepb.GetHealthCheckRequest{Project: project, HealthCheck: "hc"})
	require.NoError(t, err)
	assert.Equal(t, "HTTP", got.GetType())
	assert.Nil(t, got.TcpHealthCheck)
	assert.Equal(t, int32(8080), got.GetHttpHealthCheck().GetPort())

	op, err = e.healthChecks(t).Update(t.Context(), &computepb.UpdateHealthCheckRequest{
		Project: project, HealthCheck: "hc",
		HealthCheckResource: &computepb.HealthCheck{Name: s("hc"), SslHealthCheck: &computepb.SSLHealthCheck{}},
	})
	e.done(t, op, err)

	got, err = e.healthChecks(t).Get(t.Context(), &computepb.GetHealthCheckRequest{Project: project, HealthCheck: "hc"})
	require.NoError(t, err)
	assert.Equal(t, "SSL", got.GetType())

	_, err = e.healthChecks(t).Patch(t.Context(), &computepb.PatchHealthCheckRequest{
		Project: project, HealthCheck: "hc",
		HealthCheckResource: &computepb.HealthCheck{TimeoutSec: proto.Int32(99)},
	})
	assertAPIError(t, err, http.StatusBadRequest, "")

	_, err = e.healthChecks(t).Delete(t.Context(), &computepb.DeleteHealthCheckRequest{Project: project, HealthCheck: "nope"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	op, err = e.healthChecks(t).Delete(t.Context(), &computepb.DeleteHealthCheckRequest{Project: project, HealthCheck: "hc"})
	e.done(t, op, err)

	_, err = e.healthChecks(t).Get(t.Context(), &computepb.GetHealthCheckRequest{Project: project, HealthCheck: "hc"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

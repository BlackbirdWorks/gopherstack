package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBackendServiceInsert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bs     *computepb.BackendService
		check  func(t *testing.T, got *computepb.BackendService)
		name   string
		reason string
		status int
	}{
		{
			name: "defaults",
			bs: &computepb.BackendService{
				HealthChecks: []string{hcRef}, Backends: []*computepb.Backend{{Group: s(igRef)}},
			},
			check: func(t *testing.T, g *computepb.BackendService) {
				t.Helper()
				assert.Equal(t, "EXTERNAL", g.GetLoadBalancingScheme())
				assert.Equal(t, "HTTP", g.GetProtocol())
				assert.Equal(t, int32(30), g.GetTimeoutSec())
				assert.Equal(t, "NONE", g.GetSessionAffinity())
				assert.Equal(t, int32(300), g.GetConnectionDraining().GetDrainingTimeoutSec())
				assert.NotEmpty(t, g.GetFingerprint())
				be := g.GetBackends()[0]
				assert.Equal(t, "UTILIZATION", be.GetBalancingMode())
				assert.InDelta(t, 1.0, be.GetCapacityScaler(), 0.001)
				assert.InDelta(t, 0.8, be.GetMaxUtilization(), 0.001)
				assert.Contains(t, be.GetGroup(), "/zones/"+zone+"/instanceGroups/ig")
				assert.Equal(t, []string{"https://www.googleapis.com/compute/v1/" + hcRef}, g.GetHealthChecks())
			},
		},
		{
			name: "tcp connection mode",
			bs: &computepb.BackendService{
				Protocol: s("TCP"), HealthChecks: []string{hcRef}, Backends: []*computepb.Backend{{Group: s(igRef)}},
			},
			check: func(t *testing.T, g *computepb.BackendService) {
				t.Helper()
				assert.Equal(t, "CONNECTION", g.GetBackends()[0].GetBalancingMode())
			},
		},
		{
			name: "drained backend keeps scaler zero",
			bs: &computepb.BackendService{
				HealthChecks: []string{hcRef},
				Backends:     []*computepb.Backend{{Group: s(igRef), CapacityScaler: proto.Float32(0)}},
			},
			check: func(t *testing.T, g *computepb.BackendService) {
				t.Helper()
				assert.InDelta(t, 0.0, g.GetBackends()[0].GetCapacityScaler(), 0.001)
				assert.NotNil(t, g.GetBackends()[0].CapacityScaler)
			},
		},
		{
			name: "global internal scheme", status: http.StatusBadRequest, reason: "invalid",
			bs: &computepb.BackendService{LoadBalancingScheme: s("INTERNAL"), HealthChecks: []string{hcRef}},
		},
		{
			name: "bad protocol", status: http.StatusBadRequest, reason: "invalid",
			bs: &computepb.BackendService{Protocol: s("QUIC")},
		},
		{
			name: "timeout too large", status: http.StatusBadRequest, reason: "invalid",
			bs: &computepb.BackendService{TimeoutSec: proto.Int32(100000)},
		},
		{
			name: "missing health check", status: http.StatusBadRequest, reason: "required",
			bs: &computepb.BackendService{Backends: []*computepb.Backend{{Group: s(igRef)}}},
		},
		{
			name: "unknown health check", status: http.StatusNotFound, reason: "notFound",
			bs: &computepb.BackendService{HealthChecks: []string{gl + "healthChecks/nope"}},
		},
		{
			name: "unknown group", status: http.StatusNotFound, reason: "notFound",
			bs: &computepb.BackendService{
				HealthChecks: []string{hcRef}, Backends: []*computepb.Backend{{Group: s("zones/" + zone + "/instanceGroups/x")}},
			},
		},
		{
			name: "duplicate backend", status: http.StatusBadRequest, reason: "invalid",
			bs: &computepb.BackendService{
				HealthChecks: []string{hcRef}, Backends: []*computepb.Backend{{Group: s(igRef)}, {Group: s(igRef)}},
			},
		},
		{
			name: "scaler out of range", status: http.StatusBadRequest, reason: "invalid",
			bs: &computepb.BackendService{
				HealthChecks: []string{hcRef},
				Backends:     []*computepb.Backend{{Group: s(igRef), CapacityScaler: proto.Float32(1.5)}},
			},
		},
		{
			name: "cdn on tcp", status: http.StatusBadRequest, reason: "invalid",
			bs: &computepb.BackendService{Protocol: s("TCP"), EnableCDN: new(true)},
		},
		{
			name: "bad affinity", status: http.StatusBadRequest, reason: "invalid",
			bs: &computepb.BackendService{SessionAffinity: s("BOGUS")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.lbHC(t, "hc")
			e.group(t, "ig")

			tt.bs.Name = s("bs")
			err := e.lbBS(t, tt.bs)

			if tt.status != 0 {
				assertAPIError(t, err, tt.status, tt.reason)

				return
			}

			require.NoError(t, err)

			got, gerr := client(t, e, compute.NewBackendServicesRESTClient).
				Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
			require.NoError(t, gerr)
			tt.check(t, got)
		})
	}
}

func TestBackendServiceLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.lbStack(t)

	bss := client(t, e, compute.NewBackendServicesRESTClient)

	t.Run("duplicate", func(t *testing.T) {
		assertAPIError(t, e.lbBS(t, &computepb.BackendService{Name: s("bs")}), http.StatusConflict, "alreadyExists")
	})

	t.Run("patch keeps backends and changes fingerprint", func(t *testing.T) {
		before, err := bss.Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
		require.NoError(t, err)

		op, err := bss.Patch(t.Context(), &computepb.PatchBackendServiceRequest{
			Project: project, BackendService: "bs",
			BackendServiceResource: &computepb.BackendService{TimeoutSec: proto.Int32(60)},
		})
		e.done(t, op, err)

		after, err := bss.Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
		require.NoError(t, err)
		assert.Equal(t, int32(60), after.GetTimeoutSec())
		assert.Len(t, after.GetBackends(), 1)
		assert.NotEqual(t, before.GetFingerprint(), after.GetFingerprint())
	})

	t.Run("update needs current fingerprint", func(t *testing.T) {
		_, err := bss.Update(t.Context(), &computepb.UpdateBackendServiceRequest{
			Project: project, BackendService: "bs",
			BackendServiceResource: &computepb.BackendService{Name: s("bs"), Fingerprint: s("stale")},
		})
		assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")
	})

	t.Run("list and aggregated", func(t *testing.T) {
		n := 0
		it := bss.List(t.Context(), &computepb.ListBackendServicesRequest{Project: project})

		for {
			_, err := it.Next()
			if err != nil {
				break
			}

			n++
		}

		assert.Equal(t, 1, n)

		agg := bss.AggregatedList(t.Context(), &computepb.AggregatedListBackendServicesRequest{Project: project})
		found := false

		for {
			kv, err := agg.Next()
			if err != nil {
				break
			}

			if kv.Key == "global" {
				found = len(kv.Value.GetBackendServices()) == 1
			}
		}

		assert.True(t, found)
	})
}

func TestRegionalBackendService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		scheme string
		status int
	}{
		{name: "internal managed", scheme: "INTERNAL_MANAGED"},
		{name: "internal", scheme: "INTERNAL"},
		{name: "self managed rejected", scheme: "INTERNAL_SELF_MANAGED", status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			rbs := client(t, e, compute.NewRegionBackendServicesRESTClient)
			rhc := client(t, e, compute.NewRegionHealthChecksRESTClient)

			op, err := rhc.Insert(t.Context(), &computepb.InsertRegionHealthCheckRequest{
				Project: project, Region: region,
				HealthCheckResource: &computepb.HealthCheck{Name: s("rhc"), TcpHealthCheck: &computepb.TCPHealthCheck{}},
			})
			e.done(t, op, err)

			op, err = rbs.Insert(t.Context(), &computepb.InsertRegionBackendServiceRequest{
				Project: project, Region: region, BackendServiceResource: &computepb.BackendService{
					Name: s("rbs"), LoadBalancingScheme: s(tt.scheme), HealthChecks: []string{regBase + "healthChecks/rhc"},
				},
			})

			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "invalid")

				return
			}

			e.done(t, op, err)

			got, gerr := rbs.Get(t.Context(), &computepb.GetRegionBackendServiceRequest{
				Project: project, Region: region, BackendService: "rbs",
			})
			require.NoError(t, gerr)
			assert.Contains(t, got.GetSelfLink(), "/regions/"+region+"/backendServices/rbs")
			assert.Contains(t, got.GetRegion(), region)

			_, werr := rbs.Insert(t.Context(), &computepb.InsertRegionBackendServiceRequest{
				Project: project, Region: region, BackendServiceResource: &computepb.BackendService{
					Name: s("bad"), HealthChecks: []string{hcRef},
				},
			})
			assertAPIError(t, werr, http.StatusBadRequest, "invalid")
		})
	}
}

func TestBackendServiceGetHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		hcs   []string
		stop  bool
		state string
	}{
		{name: "running with health check", hcs: []string{hcRef}, state: "HEALTHY"},
		{name: "stopped with health check", hcs: []string{hcRef}, stop: true, state: "UNHEALTHY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.lbHC(t, "hc")
			e.group(t, "ig")
			e.vm(t, vmSpec("vm1"))
			require.NoError(t, e.addToGroup(t, "ig", "vm1"))

			if tt.stop {
				op, err := e.instances(t).Stop(t.Context(), &computepb.StopInstanceRequest{
					Project: project, Zone: zone, Instance: "vm1",
				})
				e.done(t, op, err)
			}

			require.NoError(t, e.lbBS(t, &computepb.BackendService{
				Name: s("bs"), HealthChecks: tt.hcs, PortName: s("web"),
				Backends: []*computepb.Backend{{Group: s(igRef)}},
			}))

			bss := client(t, e, compute.NewBackendServicesRESTClient)
			h, err := bss.GetHealth(t.Context(), &computepb.GetHealthBackendServiceRequest{
				Project: project, BackendService: "bs",
				ResourceGroupReferenceResource: &computepb.ResourceGroupReference{Group: s(igRef)},
			})
			require.NoError(t, err)
			require.Len(t, h.GetHealthStatus(), 1)
			assert.Equal(t, tt.state, h.GetHealthStatus()[0].GetHealthState())
			assert.Contains(t, h.GetHealthStatus()[0].GetInstance(), "vm1")
			assert.Equal(t, int32(80), h.GetHealthStatus()[0].GetPort())

			_, err = bss.GetHealth(t.Context(), &computepb.GetHealthBackendServiceRequest{
				Project: project, BackendService: "bs",
				ResourceGroupReferenceResource: &computepb.ResourceGroupReference{Group: s("zones/" + zone + "/instanceGroups/other")},
			})
			assertAPIError(t, err, http.StatusNotFound, "notFound")
		})
	}
}

func TestBackendBucketAndSignedKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		bucket string
		status int
	}{
		{name: "valid", bucket: "my-bucket"},
		{name: "uppercase", bucket: "My_Bucket", status: http.StatusBadRequest},
		{name: "missing", bucket: "", status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			bb := client(t, e, compute.NewBackendBucketsRESTClient)
			res := &computepb.BackendBucket{Name: s("bb"), EnableCdn: new(true)}

			if tt.bucket != "" {
				res.BucketName = s(tt.bucket)
			}

			op, err := bb.Insert(t.Context(), &computepb.InsertBackendBucketRequest{Project: project, BackendBucketResource: res})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)

			key := &computepb.SignedUrlKey{KeyName: s("k1"), KeyValue: s("abcdefghijklmnopqrstuv==")}
			op, err = bb.AddSignedUrlKey(t.Context(), &computepb.AddSignedUrlKeyBackendBucketRequest{
				Project: project, BackendBucket: "bb", SignedUrlKeyResource: key,
			})
			e.done(t, op, err)

			got, gerr := bb.Get(t.Context(), &computepb.GetBackendBucketRequest{Project: project, BackendBucket: "bb"})
			require.NoError(t, gerr)
			assert.Equal(t, []string{"k1"}, got.GetCdnPolicy().GetSignedUrlKeyNames())

			_, err = bb.AddSignedUrlKey(t.Context(), &computepb.AddSignedUrlKeyBackendBucketRequest{
				Project: project, BackendBucket: "bb", SignedUrlKeyResource: key,
			})
			assertAPIError(t, err, http.StatusBadRequest, "invalid")

			op, err = bb.DeleteSignedUrlKey(t.Context(), &computepb.DeleteSignedUrlKeyBackendBucketRequest{
				Project: project, BackendBucket: "bb", KeyName: "k1",
			})
			e.done(t, op, err)
		})
	}
}

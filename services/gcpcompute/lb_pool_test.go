package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	computeapi "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
)

const (
	vm1Ref   = "zones/" + zone + "/instances/vm1"
	vm2Ref   = "zones/" + zone + "/instances/vm2"
	poolRef  = regBase + "targetPools/pool"
	pool2Ref = regBase + "targetPools/pool2"
	lhcRef   = gl + "httpHealthChecks/lhc"
)

func (e *env) legacy(t *testing.T) *computeapi.Service {
	t.Helper()

	svc, err := computeapi.NewService(t.Context(),
		option.WithEndpoint(e.url+"/compute/v1/"), option.WithoutAuthentication())
	require.NoError(t, err)

	return svc
}

func (e *env) legacyHC(t *testing.T, name string) {
	t.Helper()

	op, err := e.legacy(t).
		HttpHealthChecks.Insert(project, &computeapi.HttpHealthCheck{Name: name}).
		Context(t.Context()).
		Do()
	require.NoError(t, err)
	e.settle()
	require.Empty(t, op.Error)
}

func (e *env) pools(t *testing.T) *compute.TargetPoolsClient {
	t.Helper()

	return client(t, e, compute.NewTargetPoolsRESTClient)
}

func (e *env) poolInsert(t *testing.T, p *computepb.TargetPool) error {
	t.Helper()

	op, err := e.pools(t).Insert(t.Context(), &computepb.InsertTargetPoolRequest{
		Project: project, Region: region, TargetPoolResource: p,
	})

	return e.finish(t, op, err)
}

func (e *env) getPool(t *testing.T) *computepb.TargetPool {
	t.Helper()

	p, err := e.pools(t).Get(t.Context(), &computepb.GetTargetPoolRequest{
		Project: project, Region: region, TargetPool: "pool",
	})
	require.NoError(t, err)

	return p
}

func TestTargetPoolInsert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pool   *computepb.TargetPool
		check  func(t *testing.T, p *computepb.TargetPool)
		name   string
		status int
	}{
		{
			name: "defaults", pool: &computepb.TargetPool{Name: s("pool")},
			check: func(t *testing.T, p *computepb.TargetPool) {
				t.Helper()
				assert.Equal(t, "NONE", p.GetSessionAffinity())
				assert.Equal(t, "compute#targetPool", p.GetKind())
				assert.Contains(t, p.GetRegion(), "regions/"+region)
				assert.Contains(t, p.GetSelfLink(), "/regions/"+region+"/targetPools/pool")
			},
		},
		{
			name: "members and check", pool: &computepb.TargetPool{
				Name: s("pool"), Instances: []string{vm1Ref}, HealthChecks: []string{lhcRef},
				SessionAffinity: s("CLIENT_IP"),
			},
			check: func(t *testing.T, p *computepb.TargetPool) {
				t.Helper()
				assert.Contains(t, p.GetInstances()[0], "/zones/"+zone+"/instances/vm1")
				assert.Contains(t, p.GetHealthChecks()[0], "/httpHealthChecks/lhc")
				assert.Equal(t, "CLIENT_IP", p.GetSessionAffinity())
			},
		},
		{
			name:   "bad affinity",
			pool:   &computepb.TargetPool{Name: s("pool"), SessionAffinity: s("COOKIE")},
			status: http.StatusBadRequest,
		},
		{
			name: "two checks", status: http.StatusBadRequest,
			pool: &computepb.TargetPool{Name: s("pool"), HealthChecks: []string{lhcRef, lhcRef}},
		},
		{
			name: "missing instance", status: http.StatusNotFound,
			pool: &computepb.TargetPool{Name: s("pool"), Instances: []string{vm2Ref}},
		},
		{
			name: "missing check", status: http.StatusNotFound,
			pool: &computepb.TargetPool{Name: s("pool"), HealthChecks: []string{gl + "httpHealthChecks/none"}},
		},
		{
			name: "instance in other region", status: http.StatusBadRequest,
			pool: &computepb.TargetPool{Name: s("pool"), Instances: []string{"zones/" + otherZone + "/instances/vm1"}},
		},
		{
			name: "failover ratio without backup", status: http.StatusBadRequest,
			pool: &computepb.TargetPool{Name: s("pool"), FailoverRatio: new(float32(0.5))},
		},
		{
			name: "failover ratio out of range", status: http.StatusBadRequest,
			pool: &computepb.TargetPool{Name: s("pool"), FailoverRatio: new(float32(1.5)), BackupPool: s(pool2Ref)},
		},
		{
			name: "missing backup", status: http.StatusNotFound,
			pool: &computepb.TargetPool{Name: s("pool"), FailoverRatio: new(float32(0.5)), BackupPool: s(pool2Ref)},
		},
		{
			name: "duplicate instance", status: http.StatusBadRequest,
			pool: &computepb.TargetPool{Name: s("pool"), Instances: []string{vm1Ref, vm1Ref}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.runningVM(t)
			e.legacyHC(t, "lhc")

			err := e.poolInsert(t, tt.pool)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			require.NoError(t, err)
			tt.check(t, e.getPool(t))
		})
	}
}

func TestTargetPoolMembersAndBackup(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.runningVM(t)
	e.vm(t, vmSpec("vm2"))
	e.legacyHC(t, "lhc")
	e.legacyHC(t, "lhc2")
	require.NoError(t, e.poolInsert(t, &computepb.TargetPool{Name: s("pool2")}))
	require.NoError(t, e.poolInsert(t, &computepb.TargetPool{Name: s("pool"), Instances: []string{vm1Ref}}))

	op, err := e.pools(t).AddInstance(t.Context(), &computepb.AddInstanceTargetPoolRequest{
		Project: project, Region: region, TargetPool: "pool",
		TargetPoolsAddInstanceRequestResource: &computepb.TargetPoolsAddInstanceRequest{
			Instances: []*computepb.InstanceReference{{Instance: s(vm2Ref)}},
		},
	})
	e.done(t, op, err)
	assert.Len(t, e.getPool(t).GetInstances(), 2)

	_, err = e.pools(t).AddInstance(t.Context(), &computepb.AddInstanceTargetPoolRequest{
		Project: project, Region: region, TargetPool: "pool",
		TargetPoolsAddInstanceRequestResource: &computepb.TargetPoolsAddInstanceRequest{
			Instances: []*computepb.InstanceReference{{Instance: s(vm2Ref)}},
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "")

	op, err = e.pools(t).RemoveInstance(t.Context(), &computepb.RemoveInstanceTargetPoolRequest{
		Project: project, Region: region, TargetPool: "pool",
		TargetPoolsRemoveInstanceRequestResource: &computepb.TargetPoolsRemoveInstanceRequest{
			Instances: []*computepb.InstanceReference{{Instance: s(vm1Ref)}},
		},
	})
	e.done(t, op, err)
	assert.Len(t, e.getPool(t).GetInstances(), 1)

	addHC := func(name string) error {
		_, err := e.pools(t).AddHealthCheck(t.Context(), &computepb.AddHealthCheckTargetPoolRequest{
			Project: project, Region: region, TargetPool: "pool",
			TargetPoolsAddHealthCheckRequestResource: &computepb.TargetPoolsAddHealthCheckRequest{
				HealthChecks: []*computepb.HealthCheckReference{{HealthCheck: s(gl + "httpHealthChecks/" + name)}},
			},
		})

		return err
	}

	require.NoError(t, addHC("lhc"))
	assertAPIError(t, addHC("lhc2"), http.StatusBadRequest, "")
	assert.Len(t, e.getPool(t).GetHealthChecks(), 1)

	op, err = e.pools(t).RemoveHealthCheck(t.Context(), &computepb.RemoveHealthCheckTargetPoolRequest{
		Project: project, Region: region, TargetPool: "pool",
		TargetPoolsRemoveHealthCheckRequestResource: &computepb.TargetPoolsRemoveHealthCheckRequest{
			HealthChecks: []*computepb.HealthCheckReference{{HealthCheck: s(gl + "httpHealthChecks/lhc")}},
		},
	})
	e.done(t, op, err)
	assert.Empty(t, e.getPool(t).GetHealthChecks())

	op, err = e.pools(t).SetBackup(t.Context(), &computepb.SetBackupTargetPoolRequest{
		Project: project, Region: region, TargetPool: "pool", FailoverRatio: new(float32(0.25)),
		TargetReferenceResource: &computepb.TargetReference{Target: s(pool2Ref)},
	})
	e.done(t, op, err)

	p := e.getPool(t)
	assert.Contains(t, p.GetBackupPool(), "/targetPools/pool2")
	assert.InDelta(t, 0.25, p.GetFailoverRatio(), 0.001)
}

func TestTargetPoolHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		check bool
		stop  bool
	}{
		{name: "running with check", check: true, want: "HEALTHY"},
		{name: "stopped with check", check: true, stop: true, want: "UNHEALTHY"},
		{name: "no check", want: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.runningVM(t)
			e.legacyHC(t, "lhc")

			pool := &computepb.TargetPool{Name: s("pool"), Instances: []string{vm1Ref}}
			if tt.check {
				pool.HealthChecks = []string{lhcRef}
			}

			require.NoError(t, e.poolInsert(t, pool))

			if tt.stop {
				e.stopVM(t)
			}

			h, err := e.pools(t).GetHealth(t.Context(), &computepb.GetHealthTargetPoolRequest{
				Project: project, Region: region, TargetPool: "pool",
				InstanceReferenceResource: &computepb.InstanceReference{Instance: s(vm1Ref)},
			})
			require.NoError(t, err)
			require.Len(t, h.GetHealthStatus(), 1)
			assert.Equal(t, tt.want, h.GetHealthStatus()[0].GetHealthState())
			assert.Contains(t, h.GetHealthStatus()[0].GetInstance(), "/instances/vm1")
			assert.NotEmpty(t, h.GetHealthStatus()[0].GetIpAddress())
		})
	}
}

func TestTargetPoolGetHealthNonMember(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.runningVM(t)
	require.NoError(t, e.poolInsert(t, &computepb.TargetPool{Name: s("pool")}))

	_, err := e.pools(t).GetHealth(t.Context(), &computepb.GetHealthTargetPoolRequest{
		Project: project, Region: region, TargetPool: "pool",
		InstanceReferenceResource: &computepb.InstanceReference{Instance: s(vm1Ref)},
	})
	assertAPIError(t, err, http.StatusBadRequest, "")
}

func TestTargetPoolForwardingRuleAndDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.runningVM(t)
	e.legacyHC(t, "lhc")
	require.NoError(t, e.poolInsert(t, &computepb.TargetPool{
		Name: s("pool"), Instances: []string{vm1Ref}, HealthChecks: []string{lhcRef},
	}))

	_, err := e.legacy(t).HttpHealthChecks.Delete(project, "lhc").Context(t.Context()).Do()
	require.Error(t, err)

	rules := client(t, e, compute.NewForwardingRulesRESTClient)
	op, err := rules.Insert(t.Context(), &computepb.InsertForwardingRuleRequest{
		Project: project, Region: region, ForwardingRuleResource: &computepb.ForwardingRule{
			Name: s("fr"), Target: s(poolRef), PortRange: s("80-90"), IPProtocol: s("UDP"),
		},
	})
	e.done(t, op, err)

	r, err := rules.Get(t.Context(), &computepb.GetForwardingRuleRequest{
		Project: project, Region: region, ForwardingRule: "fr",
	})
	require.NoError(t, err)
	assert.Contains(t, r.GetTarget(), "/targetPools/pool")
	assert.Equal(t, "UDP", r.GetIPProtocol())

	_, err = e.pools(t).Delete(t.Context(), &computepb.DeleteTargetPoolRequest{
		Project: project, Region: region, TargetPool: "pool",
	})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	_, err = rules.Insert(t.Context(), &computepb.InsertForwardingRuleRequest{
		Project: project, Region: region, ForwardingRuleResource: &computepb.ForwardingRule{
			Name: s("fr2"), Target: s(poolRef), PortRange: s("80"), LoadBalancingScheme: s("INTERNAL"),
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "")

	op, err = rules.Delete(t.Context(), &computepb.DeleteForwardingRuleRequest{
		Project: project, Region: region, ForwardingRule: "fr",
	})
	e.done(t, op, err)

	op, err = e.pools(t).Delete(t.Context(), &computepb.DeleteTargetPoolRequest{
		Project: project, Region: region, TargetPool: "pool",
	})
	e.done(t, op, err)

	op2, err := e.legacy(t).HttpHealthChecks.Delete(project, "lhc").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "compute#operation", op2.Kind)
}

func TestTargetInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ti     *computepb.TargetInstance
		check  func(t *testing.T, ti *computepb.TargetInstance)
		name   string
		status int
	}{
		{
			name: "defaults", ti: &computepb.TargetInstance{Name: s("ti"), Instance: s(vm1Ref)},
			check: func(t *testing.T, ti *computepb.TargetInstance) {
				t.Helper()
				assert.Equal(t, "NO_NAT", ti.GetNatPolicy())
				assert.Contains(t, ti.GetInstance(), "/zones/"+zone+"/instances/vm1")
				assert.Contains(t, ti.GetNetwork(), "/global/networks/vpc")
				assert.Contains(t, ti.GetZone(), "/zones/"+zone)
			},
		},
		{
			name:   "missing instance",
			ti:     &computepb.TargetInstance{Name: s("ti"), Instance: s(vm2Ref)},
			status: http.StatusNotFound,
		},
		{name: "no instance", ti: &computepb.TargetInstance{Name: s("ti")}, status: http.StatusBadRequest},
		{
			name: "bad nat policy", status: http.StatusBadRequest,
			ti: &computepb.TargetInstance{Name: s("ti"), Instance: s(vm1Ref), NatPolicy: s("NAT")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.runningVM(t)

			c := client(t, e, compute.NewTargetInstancesRESTClient)
			op, err := c.Insert(t.Context(), &computepb.InsertTargetInstanceRequest{
				Project: project, Zone: zone, TargetInstanceResource: tt.ti,
			})

			if tt.status != 0 {
				assertAPIError(t, e.finish(t, op, err), tt.status, "")

				return
			}

			e.done(t, op, err)

			got, err := c.Get(t.Context(), &computepb.GetTargetInstanceRequest{
				Project: project, Zone: zone, TargetInstance: "ti",
			})
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}

func TestTargetInstanceForwardingRule(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.runningVM(t)

	c := client(t, e, compute.NewTargetInstancesRESTClient)
	op, err := c.Insert(t.Context(), &computepb.InsertTargetInstanceRequest{
		Project: project, Zone: zone, TargetInstanceResource: &computepb.TargetInstance{
			Name: s("ti"), Instance: s(vm1Ref),
		},
	})
	e.done(t, op, err)

	rules := client(t, e, compute.NewForwardingRulesRESTClient)
	op, err = rules.Insert(t.Context(), &computepb.InsertForwardingRuleRequest{
		Project: project, Region: region, ForwardingRuleResource: &computepb.ForwardingRule{
			Name: s("fr"), IPProtocol: s("ESP"),
			Target: s("zones/" + zone + "/targetInstances/ti"),
		},
	})
	e.done(t, op, err)

	_, err = c.Delete(t.Context(), &computepb.DeleteTargetInstanceRequest{
		Project: project, Zone: zone, TargetInstance: "ti",
	})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	it := c.AggregatedList(t.Context(), &computepb.AggregatedListTargetInstancesRequest{Project: project})
	found := 0

	for {
		kv, err := it.Next()
		if err != nil {
			break
		}

		found += len(kv.Value.GetTargetInstances())
	}

	assert.Equal(t, 1, found)
}

func TestLegacyHTTPHealthChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in     *computeapi.HttpHealthCheck
		check  func(t *testing.T, h *computeapi.HttpHealthCheck)
		name   string
		status int
	}{
		{
			name: "defaults", in: &computeapi.HttpHealthCheck{Name: "h"},
			check: func(t *testing.T, h *computeapi.HttpHealthCheck) {
				t.Helper()
				assert.EqualValues(t, 80, h.Port)
				assert.Equal(t, "/", h.RequestPath)
				assert.EqualValues(t, 5, h.CheckIntervalSec)
				assert.EqualValues(t, 5, h.TimeoutSec)
				assert.EqualValues(t, 2, h.HealthyThreshold)
				assert.EqualValues(t, 2, h.UnhealthyThreshold)
				assert.Equal(t, "compute#httpHealthCheck", h.Kind)
			},
		},
		{
			name: "explicit", in: &computeapi.HttpHealthCheck{
				Name: "h", Port: 8080, RequestPath: "/ping", CheckIntervalSec: 10, TimeoutSec: 3, Host: "example.com",
			},
			check: func(t *testing.T, h *computeapi.HttpHealthCheck) {
				t.Helper()
				assert.EqualValues(t, 8080, h.Port)
				assert.Equal(t, "/ping", h.RequestPath)
				assert.Equal(t, "example.com", h.Host)
			},
		},
		{
			name: "timeout above interval", status: http.StatusBadRequest,
			in: &computeapi.HttpHealthCheck{Name: "h", CheckIntervalSec: 3, TimeoutSec: 5},
		},
		{
			name:   "bad path",
			in:     &computeapi.HttpHealthCheck{Name: "h", RequestPath: "ping"},
			status: http.StatusBadRequest,
		},
		{name: "bad port", in: &computeapi.HttpHealthCheck{Name: "h", Port: 70000}, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			svc := e.legacy(t)

			_, err := svc.HttpHealthChecks.Insert(project, tt.in).Context(t.Context()).Do()
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			require.NoError(t, err)

			got, err := svc.HttpHealthChecks.Get(project, "h").Context(t.Context()).Do()
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}

func TestLegacyHTTPHealthCheckPatchAndHTTPS(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	svc := e.legacy(t)

	e.legacyHC(t, "h")

	_, err := svc.HttpHealthChecks.Patch(project, "h", &computeapi.HttpHealthCheck{
		Port: 9090, CheckIntervalSec: 20,
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	got, err := svc.HttpHealthChecks.Get(project, "h").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.EqualValues(t, 9090, got.Port)
	assert.EqualValues(t, 20, got.CheckIntervalSec)

	_, err = svc.HttpsHealthChecks.Insert(project, &computeapi.HttpsHealthCheck{Name: "hs"}).Context(t.Context()).Do()
	require.NoError(t, err)

	hs, err := svc.HttpsHealthChecks.Get(project, "hs").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.EqualValues(t, 443, hs.Port)

	list, err := svc.HttpHealthChecks.List(project).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)
}

func TestRegionalSSLPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := client(t, e, compute.NewRegionSslPoliciesRESTClient)

	op, err := c.Insert(t.Context(), &computepb.InsertRegionSslPolicyRequest{
		Project: project, Region: region, SslPolicyResource: &computepb.SslPolicy{
			Name: s("sp"), Profile: s("RESTRICTED"), MinTlsVersion: s("TLS_1_2"),
		},
	})
	e.done(t, op, err)

	got, err := c.Get(t.Context(), &computepb.GetRegionSslPolicyRequest{
		Project: project, Region: region, SslPolicy: "sp",
	})
	require.NoError(t, err)
	assert.Equal(t, "RESTRICTED", got.GetProfile())
	assert.Contains(t, got.GetRegion(), "regions/"+region)
	assert.NotEmpty(t, got.GetEnabledFeatures())

	feats, err := c.ListAvailableFeatures(t.Context(), &computepb.ListAvailableFeaturesRegionSslPoliciesRequest{
		Project: project, Region: region,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, feats.GetFeatures())

	op, err = c.Delete(t.Context(), &computepb.DeleteRegionSslPolicyRequest{
		Project: project, Region: region, SslPolicy: "sp",
	})
	e.done(t, op, err)
}

package ociloadbalancer_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prefix(path, set string) loadbalancer.PathRoute {
	return loadbalancer.PathRoute{
		Path: sp(path), BackendSetName: sp(set),
		PathMatchType: &loadbalancer.PathMatchType{MatchType: loadbalancer.PathMatchTypeMatchTypePrefixMatch},
	}
}

func TestBackendSetsAndBackends(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	id := e.mkLB(t, baseDetails("sets"))
	a, b := startBackend(t, "a"), startBackend(t, "b")
	ctx := t.Context()

	step := func() { e.settle() }

	step()

	_, err := e.lb.CreateBackendSet(ctx, loadbalancer.CreateBackendSetRequest{
		LoadBalancerId: &id,
		CreateBackendSetDetails: loadbalancer.CreateBackendSetDetails{
			Name: sp("web"), Policy: sp("LEAST_CONNECTIONS"), HealthChecker: hc(), Backends: []loadbalancer.BackendDetails{a.detail()},
		},
	})
	require.NoError(t, err)

	step()

	set, err := e.lb.GetBackendSet(ctx, loadbalancer.GetBackendSetRequest{LoadBalancerId: &id, BackendSetName: sp("web")})
	require.NoError(t, err)
	assert.Equal(t, "LEAST_CONNECTIONS", *set.Policy)
	require.Len(t, set.Backends, 1)
	assert.Equal(t, 1, *set.Backends[0].Weight)
	assert.False(t, *set.Backends[0].Backup)
	assert.Equal(t, 0, *set.HealthChecker.Retries)

	t.Run("duplicate set", func(t *testing.T) {
		_, derr := e.lb.CreateBackendSet(ctx, loadbalancer.CreateBackendSetRequest{
			LoadBalancerId:          &id,
			CreateBackendSetDetails: loadbalancer.CreateBackendSetDetails{Name: sp("web"), Policy: sp("ROUND_ROBIN"), HealthChecker: hc()},
		})
		assert.Equal(t, http.StatusConflict, svcErr(t, derr).GetHTTPStatusCode())
	})

	name := a.ip() + ":" + itoa(a.port())

	t.Run("backend lifecycle", func(t *testing.T) {
		step()

		_, cerr := e.lb.CreateBackend(ctx, loadbalancer.CreateBackendRequest{
			LoadBalancerId: &id, BackendSetName: sp("web"),
			CreateBackendDetails: loadbalancer.CreateBackendDetails{IpAddress: sp(b.ip()), Port: sp(b.port()), Weight: sp(3)},
		})
		require.NoError(t, cerr)
		step()

		_, cerr = e.lb.CreateBackend(ctx, loadbalancer.CreateBackendRequest{
			LoadBalancerId: &id, BackendSetName: sp("web"),
			CreateBackendDetails: loadbalancer.CreateBackendDetails{IpAddress: sp(b.ip()), Port: sp(b.port())},
		})
		assert.Equal(t, http.StatusConflict, svcErr(t, cerr).GetHTTPStatusCode())

		_, cerr = e.lb.CreateBackend(ctx, loadbalancer.CreateBackendRequest{
			LoadBalancerId: &id, BackendSetName: sp("web"),
			CreateBackendDetails: loadbalancer.CreateBackendDetails{IpAddress: sp("not-an-ip"), Port: sp(80)},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, cerr).GetHTTPStatusCode())

		list, cerr := e.lb.ListBackends(ctx, loadbalancer.ListBackendsRequest{LoadBalancerId: &id, BackendSetName: sp("web")})
		require.NoError(t, cerr)
		assert.Len(t, list.Items, 2)

		bname := b.ip() + ":" + itoa(b.port())
		_, cerr = e.lb.UpdateBackend(ctx, loadbalancer.UpdateBackendRequest{
			LoadBalancerId: &id, BackendSetName: sp("web"), BackendName: &bname,
			UpdateBackendDetails: loadbalancer.UpdateBackendDetails{Weight: sp(5), Backup: sp(true), Drain: sp(false), Offline: sp(false)},
		})
		require.NoError(t, cerr)
		step()

		got, cerr := e.lb.GetBackend(ctx, loadbalancer.GetBackendRequest{LoadBalancerId: &id, BackendSetName: sp("web"), BackendName: &bname})
		require.NoError(t, cerr)
		assert.Equal(t, 5, *got.Weight)
		assert.True(t, *got.Backup)

		_, cerr = e.lb.DeleteBackend(ctx, loadbalancer.DeleteBackendRequest{LoadBalancerId: &id, BackendSetName: sp("web"), BackendName: &bname})
		require.NoError(t, cerr)
		step()

		_, cerr = e.lb.GetBackend(ctx, loadbalancer.GetBackendRequest{LoadBalancerId: &id, BackendSetName: sp("web"), BackendName: &bname})
		assert.Equal(t, http.StatusNotFound, svcErr(t, cerr).GetHTTPStatusCode())
	})

	t.Run("health checker", func(t *testing.T) {
		step()

		_, herr := e.lb.UpdateHealthChecker(ctx, loadbalancer.UpdateHealthCheckerRequest{
			LoadBalancerId: &id, BackendSetName: sp("web"),
			UpdateHealthCheckerDetails: loadbalancer.UpdateHealthCheckerDetails{
				Protocol: sp("TCP"), Port: sp(8080), ReturnCode: sp(200), Retries: sp(2), IntervalInMillis: sp(5000),
				TimeoutInMillis: sp(3000), ResponseBodyRegex: sp(""),
			},
		})
		require.NoError(t, herr)
		step()

		got, herr := e.lb.GetHealthChecker(ctx, loadbalancer.GetHealthCheckerRequest{LoadBalancerId: &id, BackendSetName: sp("web")})
		require.NoError(t, herr)
		assert.Equal(t, "TCP", *got.Protocol)
		assert.Equal(t, 8080, *got.Port)
		assert.Equal(t, 3000, *got.TimeoutInMillis)

		_, herr = e.lb.UpdateHealthChecker(ctx, loadbalancer.UpdateHealthCheckerRequest{
			LoadBalancerId: &id, BackendSetName: sp("web"),
			UpdateHealthCheckerDetails: loadbalancer.UpdateHealthCheckerDetails{
				Protocol: sp("HTTP"), Port: sp(0), ReturnCode: sp(200), Retries: sp(1), IntervalInMillis: sp(1000),
				TimeoutInMillis: sp(1000), ResponseBodyRegex: sp("("),
			},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, herr).GetHTTPStatusCode())
	})

	t.Run("update and delete set", func(t *testing.T) {
		step()

		_, uerr := e.lb.UpdateBackendSet(ctx, loadbalancer.UpdateBackendSetRequest{
			LoadBalancerId: &id, BackendSetName: sp("web"),
			UpdateBackendSetDetails: loadbalancer.UpdateBackendSetDetails{
				Policy: sp("IP_HASH"), HealthChecker: hc(), Backends: []loadbalancer.BackendDetails{a.detail(), b.detail()},
			},
		})
		require.NoError(t, uerr)
		step()

		got, uerr := e.lb.GetBackendSet(ctx, loadbalancer.GetBackendSetRequest{LoadBalancerId: &id, BackendSetName: sp("web")})
		require.NoError(t, uerr)
		assert.Equal(t, "IP_HASH", *got.Policy)
		assert.Len(t, got.Backends, 2)
		assert.Equal(t, name, *got.Backends[0].Name)

		sets, uerr := e.lb.ListBackendSets(ctx, loadbalancer.ListBackendSetsRequest{LoadBalancerId: &id})
		require.NoError(t, uerr)
		assert.Len(t, sets.Items, 1)

		_, uerr = e.lb.DeleteBackendSet(ctx, loadbalancer.DeleteBackendSetRequest{LoadBalancerId: &id, BackendSetName: sp("web")})
		require.NoError(t, uerr)
		step()

		_, uerr = e.lb.GetBackendSet(ctx, loadbalancer.GetBackendSetRequest{LoadBalancerId: &id, BackendSetName: sp("web")})
		assert.Equal(t, http.StatusNotFound, svcErr(t, uerr).GetHTTPStatusCode())
	})
}

func itoa(n int) string { return strconv.Itoa(n) }

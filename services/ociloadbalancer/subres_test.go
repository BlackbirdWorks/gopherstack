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
		Path: new(path), BackendSetName: new(set),
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
		Name: new(
			"web",
		),
		Policy:        new("LEAST_CONNECTIONS"),
		HealthChecker: hc(),
		Backends:      []loadbalancer.BackendDetails{a.detail()},
	})
	require.NoError(t, err)

	step()

	set, err := e.lb.GetBackendSet(
		ctx,
		loadbalancer.GetBackendSetRequest{LoadBalancerId: &id, BackendSetName: new("web")},
	)
	require.NoError(t, err)
	assert.Equal(t, "LEAST_CONNECTIONS", *set.Policy)
	require.Len(t, set.Backends, 1)
	assert.Equal(t, 1, *set.Backends[0].Weight)
	assert.False(t, *set.Backends[0].Backup)
	assert.Equal(t, 0, *set.HealthChecker.Retries)

	{ // duplicate set
		_, derr := e.lb.CreateBackendSet(ctx, loadbalancer.CreateBackendSetRequest{
			LoadBalancerId: &id,
			Name:           new("web"), Policy: new("ROUND_ROBIN"), HealthChecker: hc(),
		})
		assert.Equal(t, http.StatusConflict, svcErr(t, derr).GetHTTPStatusCode())
	}

	name := a.ip() + ":" + itoa(a.port())

	{ // backend lifecycle
		step()

		_, cerr := e.lb.CreateBackend(ctx, loadbalancer.CreateBackendRequest{
			LoadBalancerId: &id, BackendSetName: new("web"),
			IpAddress: new(b.ip()), Port: new(b.port()), Weight: new(3),
		})
		require.NoError(t, cerr)
		step()

		_, cerr = e.lb.CreateBackend(ctx, loadbalancer.CreateBackendRequest{
			LoadBalancerId: &id, BackendSetName: new("web"),
			IpAddress: new(b.ip()), Port: new(b.port()),
		})
		assert.Equal(t, http.StatusConflict, svcErr(t, cerr).GetHTTPStatusCode())

		_, cerr = e.lb.CreateBackend(ctx, loadbalancer.CreateBackendRequest{
			LoadBalancerId: &id, BackendSetName: new("web"),
			IpAddress: new("not-an-ip"), Port: new(80),
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, cerr).GetHTTPStatusCode())

		list, cerr := e.lb.ListBackends(
			ctx,
			loadbalancer.ListBackendsRequest{LoadBalancerId: &id, BackendSetName: new("web")},
		)
		require.NoError(t, cerr)
		assert.Len(t, list.Items, 2)

		bname := b.ip() + ":" + itoa(b.port())
		_, cerr = e.lb.UpdateBackend(ctx, loadbalancer.UpdateBackendRequest{
			LoadBalancerId: &id, BackendSetName: new("web"), BackendName: &bname,
			Weight: new(5), Backup: new(true), Drain: new(false), Offline: new(false),
		})
		require.NoError(t, cerr)
		step()

		got, cerr := e.lb.GetBackend(
			ctx,
			loadbalancer.GetBackendRequest{LoadBalancerId: &id, BackendSetName: new("web"), BackendName: &bname},
		)
		require.NoError(t, cerr)
		assert.Equal(t, 5, *got.Weight)
		assert.True(t, *got.Backup)

		_, cerr = e.lb.DeleteBackend(
			ctx,
			loadbalancer.DeleteBackendRequest{LoadBalancerId: &id, BackendSetName: new("web"), BackendName: &bname},
		)
		require.NoError(t, cerr)
		step()

		_, cerr = e.lb.GetBackend(
			ctx,
			loadbalancer.GetBackendRequest{LoadBalancerId: &id, BackendSetName: new("web"), BackendName: &bname},
		)
		assert.Equal(t, http.StatusNotFound, svcErr(t, cerr).GetHTTPStatusCode())
	}

	{ // health checker
		step()

		_, herr := e.lb.UpdateHealthChecker(ctx, loadbalancer.UpdateHealthCheckerRequest{
			LoadBalancerId: &id, BackendSetName: new("web"),
			Protocol: new("TCP"), Port: new(8080), ReturnCode: new(200), Retries: new(2), IntervalInMillis: new(5000),
			TimeoutInMillis: new(3000), ResponseBodyRegex: new(""),
		})
		require.NoError(t, herr)
		step()

		got, herr := e.lb.GetHealthChecker(
			ctx,
			loadbalancer.GetHealthCheckerRequest{LoadBalancerId: &id, BackendSetName: new("web")},
		)
		require.NoError(t, herr)
		assert.Equal(t, "TCP", *got.Protocol)
		assert.Equal(t, 8080, *got.Port)
		assert.Equal(t, 3000, *got.TimeoutInMillis)

		_, herr = e.lb.UpdateHealthChecker(ctx, loadbalancer.UpdateHealthCheckerRequest{
			LoadBalancerId: &id, BackendSetName: new("web"),
			Protocol: new("HTTP"), Port: new(0), ReturnCode: new(200), Retries: new(1), IntervalInMillis: new(1000),
			TimeoutInMillis: new(1000), ResponseBodyRegex: new("("),
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, herr).GetHTTPStatusCode())
	}

	{ // update and delete set
		step()

		_, uerr := e.lb.UpdateBackendSet(ctx, loadbalancer.UpdateBackendSetRequest{
			LoadBalancerId: &id,
			BackendSetName: new("web"),
			Policy: new(
				"IP_HASH",
			),
			HealthChecker: hc(),
			Backends:      []loadbalancer.BackendDetails{a.detail(), b.detail()},
		})
		require.NoError(t, uerr)
		step()

		got, uerr := e.lb.GetBackendSet(
			ctx,
			loadbalancer.GetBackendSetRequest{LoadBalancerId: &id, BackendSetName: new("web")},
		)
		require.NoError(t, uerr)
		assert.Equal(t, "IP_HASH", *got.Policy)
		assert.Len(t, got.Backends, 2)
		assert.Equal(t, name, *got.Backends[0].Name)

		sets, uerr := e.lb.ListBackendSets(ctx, loadbalancer.ListBackendSetsRequest{LoadBalancerId: &id})
		require.NoError(t, uerr)
		assert.Len(t, sets.Items, 1)

		_, uerr = e.lb.DeleteBackendSet(
			ctx,
			loadbalancer.DeleteBackendSetRequest{LoadBalancerId: &id, BackendSetName: new("web")},
		)
		require.NoError(t, uerr)
		step()

		_, uerr = e.lb.GetBackendSet(
			ctx,
			loadbalancer.GetBackendSetRequest{LoadBalancerId: &id, BackendSetName: new("web")},
		)
		assert.Equal(t, http.StatusNotFound, svcErr(t, uerr).GetHTTPStatusCode())
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

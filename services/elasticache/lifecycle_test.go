package elasticache_test

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// fakeClock is a manually-advanced clock so lifecycle transitions can be
// observed deterministically without sleeping.
type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.t
}

// advance moves the clock forward by d. Every current call site happens to
// pass 2*delay (safely past a transition's deadline), but the parameter is
// kept general -- this is a shared test helper, not a single-use function --
// rather than hardcoding one test's margin into it.
//
//nolint:unparam // general-purpose test helper API; see comment above
func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}

// lifecycleOps abstracts a resource type's create/status/modify/delete so a
// single table can drive every resource through its intermediate states.
type lifecycleOps struct {
	create func(ctx context.Context, b *elasticache.InMemoryBackend) error
	status func(ctx context.Context, b *elasticache.InMemoryBackend) (status string, found bool, err error)
	modify func(ctx context.Context, b *elasticache.InMemoryBackend) error
	delete func(ctx context.Context, b *elasticache.InMemoryBackend) error
}

func clusterOps() lifecycleOps {
	return lifecycleOps{
		create: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.CreateClusterWithOptions(ctx, "lc-cluster", "redis", "", "", "", "", 1, 0)

			return err
		},
		status: func(ctx context.Context, b *elasticache.InMemoryBackend) (string, bool, error) {
			p, err := b.DescribeClusters(ctx, "lc-cluster", "", 0, false)

			return firstStatus(p.Data, err, elasticache.ErrClusterNotFound, func(c elasticache.Cluster) string {
				return c.Status
			})
		},
		modify: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.ModifyCluster(ctx, "lc-cluster", "cache.t3.small", "", "", "", "", 0)

			return err
		},
		delete: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			return b.DeleteCluster(ctx, "lc-cluster")
		},
	}
}

func replicationGroupOps() lifecycleOps {
	return lifecycleOps{
		create: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.CreateReplicationGroup(ctx, "lc-rg", "desc")

			return err
		},
		status: func(ctx context.Context, b *elasticache.InMemoryBackend) (string, bool, error) {
			p, err := b.DescribeReplicationGroups(ctx, "lc-rg", "", 0)

			return firstStatus(p.Data, err, elasticache.ErrReplicationGroupNotFound,
				func(r elasticache.ReplicationGroup) string { return r.Status })
		},
		modify: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.ModifyReplicationGroup(ctx, "lc-rg", "new-desc", "", "", "", "", "", nil, nil)

			return err
		},
		delete: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			return b.DeleteReplicationGroup(ctx, "lc-rg")
		},
	}
}

func serverlessOps() lifecycleOps {
	return lifecycleOps{
		create: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.CreateServerlessCache(ctx, "lc-sc", "desc", "redis")

			return err
		},
		status: func(ctx context.Context, b *elasticache.InMemoryBackend) (string, bool, error) {
			p, err := b.DescribeServerlessCaches(ctx, "lc-sc", "", 0)

			return firstStatus(p.Data, err, elasticache.ErrServerlessCacheNotFound,
				func(s elasticache.ServerlessCache) string { return s.Status })
		},
		modify: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.ModifyServerlessCache(ctx, "lc-sc", "new-desc")

			return err
		},
		delete: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.DeleteServerlessCache(ctx, "lc-sc")

			return err
		},
	}
}

func globalReplicationGroupOps() lifecycleOps {
	const id = "ldgnf-lc-grg"

	return lifecycleOps{
		create: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.CreateGlobalReplicationGroup(ctx, "lc-grg", "desc", "primary")

			return err
		},
		status: func(ctx context.Context, b *elasticache.InMemoryBackend) (string, bool, error) {
			p, err := b.DescribeGlobalReplicationGroups(ctx, id, "", 0)

			return firstStatus(p.Data, err, elasticache.ErrGlobalReplicationGroupNotFound,
				func(g elasticache.GlobalReplicationGroup) string { return g.Status })
		},
		modify: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.ModifyGlobalReplicationGroup(ctx, id, "new-desc", "", false, true)

			return err
		},
		delete: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.DeleteGlobalReplicationGroup(ctx, id, true)

			return err
		},
	}
}

func snapshotOps() lifecycleOps {
	return lifecycleOps{
		create: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			if _, err := b.CreateClusterWithOptions(ctx, "snap-src", "redis", "", "", "", "", 1, 0); err != nil {
				return err
			}
			_, err := b.CreateSnapshot(ctx, "lc-snap", "snap-src", "")

			return err
		},
		status: func(ctx context.Context, b *elasticache.InMemoryBackend) (string, bool, error) {
			p, err := b.DescribeSnapshots(ctx, "lc-snap", "", "", "", "", 0)

			return firstStatus(p.Data, err, elasticache.ErrSnapshotNotFound,
				func(s elasticache.CacheSnapshot) string { return s.Status })
		},
		modify: nil,
		delete: func(ctx context.Context, b *elasticache.InMemoryBackend) error {
			_, err := b.DeleteSnapshot(ctx, "lc-snap")

			return err
		},
	}
}

// firstStatus extracts the status of the first item, mapping the not-found
// sentinel to found=false.
func firstStatus[T any](data []T, err, notFound error, get func(T) string) (string, bool, error) {
	if err != nil {
		if errors.Is(err, notFound) {
			return "", false, nil
		}

		return "", false, err
	}
	if len(data) == 0 {
		return "", false, nil
	}

	return get(data[0]), true, nil
}

func TestLifecycleIntermediateStatesObservable(t *testing.T) {
	t.Parallel()

	const delay = time.Hour

	tests := []struct {
		ops        func() lifecycleOps
		name       string
		wantCreate string
		wantModify string
		wantDelete string
	}{
		{name: "cluster", ops: clusterOps, wantCreate: "creating", wantModify: "modifying", wantDelete: "deleting"},
		{
			name: "replication_group", ops: replicationGroupOps,
			wantCreate: "creating", wantModify: "modifying", wantDelete: "deleting",
		},
		{
			name: "serverless_cache", ops: serverlessOps,
			wantCreate: "creating", wantModify: "modifying", wantDelete: "deleting",
		},
		{
			name: "global_replication_group", ops: globalReplicationGroupOps,
			wantCreate: "creating", wantModify: "modifying", wantDelete: "deleting",
		},
		{name: "snapshot", ops: snapshotOps, wantCreate: "creating", wantDelete: "deleting"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			clock := newFakeClock()
			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			b.SetClock(clock.now)
			b.SetLifecycleDelay(delay)

			ops := tt.ops()

			// Create: the resource must dwell in its "creating" state.
			require.NoError(t, ops.create(ctx, b))
			status, found, err := ops.status(ctx, b)
			require.NoError(t, err)
			require.True(t, found, "resource should be visible while creating")
			assert.Equal(t, tt.wantCreate, status, "expected intermediate create state")

			// After the delay elapses it settles to available.
			clock.advance(2 * delay)
			status, found, err = ops.status(ctx, b)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, "available", status, "expected terminal state after delay")

			// Modify: observable "modifying" state (where the resource supports it).
			if ops.modify != nil {
				require.NoError(t, ops.modify(ctx, b))
				status, found, err = ops.status(ctx, b)
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, tt.wantModify, status, "expected intermediate modify state")

				clock.advance(2 * delay)
				status, _, err = ops.status(ctx, b)
				require.NoError(t, err)
				assert.Equal(t, "available", status)
			}

			// Delete: observable "deleting" state, then gone once the delay passes.
			require.NoError(t, ops.delete(ctx, b))
			status, found, err = ops.status(ctx, b)
			require.NoError(t, err)
			require.True(t, found, "resource should still be visible while deleting")
			assert.Equal(t, tt.wantDelete, status, "expected intermediate delete state")

			clock.advance(2 * delay)
			_, found, err = ops.status(ctx, b)
			require.NoError(t, err)
			assert.False(t, found, "resource should be gone after deletion completes")
		})
	}
}

func TestLifecycleDefaultFastIsInstant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	// No lifecycle delay configured (the default): create is immediately available.
	_, err := b.CreateClusterWithOptions(ctx, "fast", "redis", "", "", "", "", 1, 0)
	require.NoError(t, err)

	p, err := b.DescribeClusters(ctx, "fast", "", 0, false)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "available", p.Data[0].Status)

	// Delete is synchronous with no delay.
	require.NoError(t, b.DeleteCluster(ctx, "fast"))
	_, err = b.DescribeClusters(ctx, "fast", "", 0, false)
	require.ErrorIs(t, err, elasticache.ErrClusterNotFound)
}

func TestEmbeddedEndpointIsConnectable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1", nil)

	cluster, err := b.CreateCluster(ctx, "conn-cache", "redis", "", 0)
	require.NoError(t, err)
	require.NotEmpty(t, cluster.ConnectAddress, "embedded cluster must publish a connectable address")
	require.Positive(t, cluster.Port)

	host, _, err := net.SplitHostPort(cluster.ConnectAddress)
	require.NoError(t, err)
	assert.True(t, net.ParseIP(host).IsLoopback(), "connect host should be loopback, got %q", host)

	// The published address must reach the real embedded redis: a RESP PING
	// returns +PONG.
	conn, err := net.DialTimeout("tcp", cluster.ConnectAddress, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	require.NoError(t, err)

	line, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, "+PONG\r\n", line, "embedded redis should answer PING")
}

func TestConcurrentClusterCreationSucceeds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1", nil)

	const n = 12

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			id := "cc-" + string(rune('a'+i))
			_, errs[i] = b.CreateCluster(ctx, id, "redis", "", 0)
		})
	}
	wg.Wait()

	for i := range n {
		require.NoError(t, errs[i])
	}

	// Every cluster started its own engine on a distinct real port.
	ports := map[int]bool{}
	all := b.ListAll()
	require.Len(t, all, n)
	for _, c := range all {
		require.Positive(t, c.Port)
		assert.False(t, ports[c.Port], "duplicate port %d across concurrent clusters", c.Port)
		ports[c.Port] = true
	}
}

// recordingDNS implements both the base DNSRegistrar and the optional
// dnsRecordRegistrar capability so the A-record wiring can be asserted.
type recordingDNS struct {
	records    map[string][]string
	registered []string
	mu         sync.Mutex
}

func newRecordingDNS() *recordingDNS {
	return &recordingDNS{records: make(map[string][]string)}
}

func (r *recordingDNS) Register(hostname string) {
	r.mu.Lock()
	r.registered = append(r.registered, hostname)
	r.mu.Unlock()
}

func (r *recordingDNS) Deregister(string) {}

func (r *recordingDNS) RegisterRecord(hostname, recordType string, values []string) {
	r.mu.Lock()
	r.records[recordType+"|"+hostname] = values
	r.mu.Unlock()
}

func (r *recordingDNS) recordFor(hostname string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.records["A|"+hostname]
}

func TestEmbeddedEndpointRegistersConnectableARecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dns := newRecordingDNS()
	b := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1", nil)
	b.SetDNSRegistrar(dns)

	cluster, err := b.CreateCluster(ctx, "dns-cache", "redis", "", 0)
	require.NoError(t, err)

	// The synthetic endpoint hostname must resolve (via an A record) to the real
	// loopback address the embedded redis is bound to, so it is truly reachable.
	values := dns.recordFor(cluster.Endpoint)
	require.Len(t, values, 1, "an A record must be registered for the endpoint")

	host, _, err := net.SplitHostPort(cluster.ConnectAddress)
	require.NoError(t, err)
	assert.Equal(t, host, values[0], "A record must point at the bound host")
	assert.True(t, net.ParseIP(values[0]).IsLoopback())
}

// TestLifecycleFullVariantsAreObservable drives the "Full" create/modify
// variants (used by CreateReplicationGroup/CreateServerlessCache's real
// wire-routed paths) through creating -> available -> modifying. It also
// asserts the state-transition guard: AWS rejects a Modify while the
// resource is still "creating" with Invalid<Resource>State(Fault) (verified
// against aws-sdk-go-v2's per-operation error deserializers -- every mutating
// op on these resource types models that fault), so a Modify issued before
// the resource settles must fail, not silently queue.
func TestLifecycleFullVariantsAreObservable(t *testing.T) {
	t.Parallel()

	const delay = time.Hour

	tests := []struct {
		create       func(ctx context.Context, b *elasticache.InMemoryBackend) (string, error)
		modify       func(ctx context.Context, b *elasticache.InMemoryBackend) (string, error)
		wantGuardErr error
		name         string
	}{
		{
			name: "replication_group_full",
			create: func(ctx context.Context, b *elasticache.InMemoryBackend) (string, error) {
				rg, err := b.CreateReplicationGroupFull(ctx, elasticache.ReplicationGroupCreateOpts{
					ID:          "rg-full",
					Description: "d",
				})
				if err != nil {
					return "", err
				}

				return rg.Status, nil
			},
			modify: func(ctx context.Context, b *elasticache.InMemoryBackend) (string, error) {
				rg, err := b.ModifyReplicationGroupFull(ctx, "rg-full", elasticache.ReplicationGroupModifyOpts{
					Description: "d2",
				})
				if err != nil {
					return "", err
				}

				return rg.Status, nil
			},
			wantGuardErr: elasticache.ErrReplicationGroupNotAvailable,
		},
		{
			name: "serverless_full",
			create: func(ctx context.Context, b *elasticache.InMemoryBackend) (string, error) {
				sc, err := b.CreateServerlessCacheFull(ctx, elasticache.ServerlessCreateOpts{
					Name:   "sc-full",
					Engine: "redis",
				})
				if err != nil {
					return "", err
				}

				return sc.Status, nil
			},
			modify: func(ctx context.Context, b *elasticache.InMemoryBackend) (string, error) {
				sc, err := b.ModifyServerlessCacheFull(ctx, "sc-full", elasticache.ServerlessModifyOpts{
					Description: "d2",
				})
				if err != nil {
					return "", err
				}

				return sc.Status, nil
			},
			wantGuardErr: elasticache.ErrServerlessCacheNotAvailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			clock := newFakeClock()
			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			b.SetClock(clock.now)
			b.SetLifecycleDelay(delay)

			status, err := tt.create(ctx, b)
			require.NoError(t, err)
			assert.Equal(t, "creating", status, "full-variant create must report creating")

			// A Modify issued while still "creating" must be rejected with the
			// resource's Invalid<Resource>State(Fault) sentinel, not silently
			// succeed and queue -- AWS requires the resource to reach
			// "available" first.
			_, err = tt.modify(ctx, b)
			require.ErrorIs(t, err, tt.wantGuardErr,
				"modify while still creating must be rejected by the state guard")

			// Once the create delay elapses the resource is available and the
			// same Modify call now succeeds, observably reporting "modifying".
			clock.advance(2 * delay)
			status, err = tt.modify(ctx, b)
			require.NoError(t, err)
			assert.Equal(t, "modifying", status, "full-variant modify must report modifying")
		})
	}
}

// TestStateGuardRejectsMutationWhilePending drives every wire-routed
// state-guard case through the real aws-sdk-go-v2 client: a resource still
// "creating" (SetLifecycleDelay forces a non-instant transition) must reject
// a mutating call with the resource's modeled Invalid<Resource>State(Fault),
// HTTP 400 -- proving the guard is wired at the handler/wire layer, not just
// asserted against the backend's Go sentinel.
func TestStateGuardRejectsMutationWhilePending(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create     func(t *testing.T, client *elasticachesdk.Client)
		mutate     func(t *testing.T, client *elasticachesdk.Client) error
		checkFault func(t *testing.T, err error)
		name       string
	}{
		{
			name: "cache_cluster_modify",
			create: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("sg-cluster"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
			mutate: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.ModifyCacheCluster(t.Context(), &elasticachesdk.ModifyCacheClusterInput{
					CacheClusterId: aws.String("sg-cluster"),
					CacheNodeType:  aws.String("cache.t3.small"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.InvalidCacheClusterStateFault](t, err)
			},
		},
		{
			name: "cache_cluster_delete",
			create: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("sg-cluster-del"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
			},
			mutate: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DeleteCacheCluster(t.Context(), &elasticachesdk.DeleteCacheClusterInput{
					CacheClusterId: aws.String("sg-cluster-del"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.InvalidCacheClusterStateFault](t, err)
			},
		},
		{
			name: "replication_group_modify",
			create: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("sg-rg"),
					ReplicationGroupDescription: aws.String("d"),
				})
				require.NoError(t, err)
			},
			mutate: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
					ReplicationGroupId: aws.String("sg-rg"),
					ReplicationGroupDescription: aws.String(
						"d2",
					),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.InvalidReplicationGroupStateFault](t, err)
			},
		},
		{
			name: "replication_group_delete",
			create: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("sg-rg-del"),
					ReplicationGroupDescription: aws.String("d"),
				})
				require.NoError(t, err)
			},
			mutate: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DeleteReplicationGroup(t.Context(), &elasticachesdk.DeleteReplicationGroupInput{
					ReplicationGroupId: aws.String("sg-rg-del"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.InvalidReplicationGroupStateFault](t, err)
			},
		},
		{
			name: "serverless_cache_modify",
			create: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sg-sc"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
			mutate: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.ModifyServerlessCache(t.Context(), &elasticachesdk.ModifyServerlessCacheInput{
					ServerlessCacheName: aws.String("sg-sc"),
					Description:         aws.String("d2"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.InvalidServerlessCacheStateFault](t, err)
			},
		},
		{
			name: "serverless_cache_delete",
			create: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sg-sc-del"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
			mutate: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DeleteServerlessCache(t.Context(), &elasticachesdk.DeleteServerlessCacheInput{
					ServerlessCacheName: aws.String("sg-sc-del"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.InvalidServerlessCacheStateFault](t, err)
			},
		},
		{
			name: "global_replication_group_modify",
			create: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("sg-grg"),
						PrimaryReplicationGroupId:      aws.String("primary"),
					},
				)
				require.NoError(t, err)
			},
			mutate: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.ModifyGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.ModifyGlobalReplicationGroupInput{
						GlobalReplicationGroupId:          aws.String("ldgnf-sg-grg"),
						GlobalReplicationGroupDescription: aws.String("d2"),
						ApplyImmediately:                  aws.Bool(true),
					},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.InvalidGlobalReplicationGroupStateFault](t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newTestStackWithBackend(t)
			backend.SetLifecycleDelay(time.Hour)

			tt.create(t, client)

			err := tt.mutate(t, client)
			require.Error(t, err, "mutating a still-creating resource must fail")
			tt.checkFault(t, err)
			requireHTTPStatus(t, err, http.StatusBadRequest)
		})
	}
}

func TestConcurrentDuplicateCreateYieldsOneWinner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1", nil)

	const n = 8

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		okCount int
		dupErr  int
	)
	for range n {
		wg.Go(func() {
			_, err := b.CreateCluster(ctx, "same-id", "redis", "", 0)
			mu.Lock()
			switch {
			case err == nil:
				okCount++
			case errors.Is(err, elasticache.ErrClusterAlreadyExists):
				dupErr++
			}
			mu.Unlock()
		})
	}
	wg.Wait()

	assert.Equal(t, 1, okCount, "exactly one concurrent create should win")
	assert.Equal(t, n-1, dupErr, "the rest should get AlreadyExists")

	// The losing racers must not leak an engine: only one cluster exists.
	assert.Len(t, b.ListAll(), 1)
}

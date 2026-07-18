package elb_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/elb"
)

// rtLBInput returns a minimal valid CreateLoadBalancerInput for the given AZ.
func rtLBInput(name, az string) elb.CreateLoadBalancerInput {
	return elb.CreateLoadBalancerInput{
		LoadBalancerName:  name,
		AvailabilityZones: []string{az},
		Listeners: []elb.Listener{
			{Protocol: "HTTP", InstanceProtocol: "HTTP", LoadBalancerPort: 80, InstancePort: 8080},
		},
	}
}

// TestSnapshotRestoreRoundTrip exercises a full Snapshot -> Restore round trip
// across multiple regions, load balancers, tags, and policies, proving the
// Phase 3.3 pkgs/store conversion (composite "region|name" keyed tables --
// see store_setup.go) preserves state exactly through persistence, including
// region isolation and the policiesByLB cascade-delete index.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := elb.RegionContextForTest(context.Background(), "us-east-1")
	ctxWest := elb.RegionContextForTest(context.Background(), "us-west-2")

	// Same-named LB in two regions to exercise the composite region|name key.
	_, err := backend.CreateLoadBalancer(ctxEast, rtLBInput("rt-lb", "us-east-1a"))
	require.NoError(t, err)

	_, err = backend.CreateLoadBalancer(ctxWest, rtLBInput("rt-lb", "us-west-2a"))
	require.NoError(t, err)

	require.NoError(t, backend.AddTags(ctxEast, []string{"rt-lb"}, []tags.KV{{Key: "env", Value: "prod"}}))
	require.NoError(t, backend.CreateAppCookieStickinessPolicy(ctxEast, "rt-lb", "east-pol", "SESSION"))
	require.NoError(t, backend.CreateLBCookieStickinessPolicy(ctxWest, "rt-lb", "west-pol", 60))

	snap := backend.Snapshot(context.Background())
	require.NotEmpty(t, snap)

	restored := elb.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(context.Background(), snap))

	assert.Equal(t, 2, restored.LoadBalancerCount())
	assert.Equal(t, 2, restored.PolicyCount())

	eastList, err := restored.DescribeLoadBalancers(ctxEast, []string{"rt-lb"})
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, []string{"us-east-1a"}, eastList[0].AvailabilityZones)
	assert.Equal(t, "us-east-1", eastList[0].Region)

	westList, err := restored.DescribeLoadBalancers(ctxWest, []string{"rt-lb"})
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, []string{"us-west-2a"}, westList[0].AvailabilityZones)
	assert.Equal(t, "us-west-2", westList[0].Region)

	eastTags, err := restored.DescribeTags(ctxEast, []string{"rt-lb"})
	require.NoError(t, err)
	require.Len(t, eastTags["rt-lb"], 1)
	assert.Equal(t, "prod", eastTags["rt-lb"][0].Value)

	westTags, err := restored.DescribeTags(ctxWest, []string{"rt-lb"})
	require.NoError(t, err)
	assert.Empty(t, westTags["rt-lb"], "tags must not leak across regions after restore")

	eastPolicies, err := restored.DescribeLoadBalancerPolicies(ctxEast, "rt-lb", nil)
	require.NoError(t, err)
	require.Len(t, eastPolicies, 1)
	assert.Equal(t, "east-pol", eastPolicies[0].PolicyName)

	westPolicies, err := restored.DescribeLoadBalancerPolicies(ctxWest, "rt-lb", nil)
	require.NoError(t, err)
	require.Len(t, westPolicies, 1)
	assert.Equal(t, "west-pol", westPolicies[0].PolicyName)

	// Deleting the east LB after restore must cascade-delete only its own
	// policy, proving the policiesByLB secondary index was correctly rebuilt
	// by Table.Restore (not just the primary policies table).
	require.NoError(t, restored.DeleteLoadBalancer(ctxEast, "rt-lb"))
	assert.Equal(t, 1, restored.PolicyCount())

	westPoliciesAfter, err := restored.DescribeLoadBalancerPolicies(ctxWest, "rt-lb", nil)
	require.NoError(t, err)
	require.Len(t, westPoliciesAfter, 1)
}

// TestSnapshotRestoreVersionMismatch verifies that Restore discards a
// snapshot whose Version field does not match the current on-disk shape (see
// the Version guard in persistence.go) instead of attempting to partially
// decode an incompatible (pre-Phase-3.3, region-nested) payload as the
// current flat, composite-keyed shape, and that doing so resets the backend
// to empty rather than returning an error.
func TestSnapshotRestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", "us-east-1")

	ctx := elb.RegionContextForTest(context.Background(), "us-east-1")
	_, err := backend.CreateLoadBalancer(ctx, rtLBInput("stale-lb", "us-east-1a"))
	require.NoError(t, err)

	// A structurally-current-shaped payload tagged with a stale Version number
	// must still be discarded on the Version check alone (not decoded and
	// adopted), proving Restore never trusts payload shape over the Version
	// field. A genuinely pre-Phase-3.3 (region-nested-map) payload fails to
	// even unmarshal into the current flat-list backendSnapshot -- that is
	// the separate "malformed -> error" path, not this one.
	staleVersionedSnapshot := []byte(
		`{"loadBalancers":[],"policies":[],` +
			`"accountId":"000000000000","region":"us-east-1","version":3}`,
	)

	require.NoError(t, backend.Restore(context.Background(), staleVersionedSnapshot))
	assert.Equal(
		t, 0, backend.LoadBalancerCount(),
		"incompatible snapshot version must reset to empty, not be adopted",
	)

	// A genuinely malformed (pre-Phase-3.3, region-nested-map) payload must
	// surface as an unmarshal error rather than silently succeed.
	_, err = backend.CreateLoadBalancer(ctx, rtLBInput("stale-lb-2", "us-east-1a"))
	require.NoError(t, err)

	malformedSnapshot := []byte(
		`{"loadBalancers":{"us-east-1":{"stale-lb-2":{}}},"policies":{},` +
			`"accountId":"000000000000","region":"us-east-1","version":3}`,
	)

	require.Error(t, backend.Restore(context.Background(), malformedSnapshot))
	assert.Equal(
		t, 1, backend.LoadBalancerCount(),
		"a malformed snapshot must error out and leave existing state untouched",
	)
}

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore
// (persistence.go) delegate to the backend -- the shape persistence.Manager
// actually drives. cli.go's setupPersistence registers a service.Registerable
// (the *Handler returned by Provider.Init) in the persistence.Manager only if
// that Handler itself satisfies Snapshot(ctx)/Restore(ctx, []byte);
// InMemoryBackend implementing the same two methods is not enough on its own,
// since Handler.Backend is the StorageBackend interface and does not promote
// them. Mirrors services/securityhub's Test_Handler_SnapshotRestore.
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := elb.NewInMemoryBackend("000000000000", "us-east-1")
	h := elb.NewHandler(backend)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	ctxEast := elb.RegionContextForTest(ctx, "us-east-1")
	_, err := backend.CreateLoadBalancer(ctxEast, rtLBInput("handler-lb", "us-east-1a"))
	require.NoError(t, err)

	data := h.Snapshot(ctx)
	require.NotEmpty(t, data)

	restoredBackend := elb.NewInMemoryBackend("000000000000", "us-east-1")
	restoredHandler := elb.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	assert.Equal(t, 1, restoredBackend.LoadBalancerCount())

	lbs, err := restoredBackend.DescribeLoadBalancers(ctxEast, []string{"handler-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)
	assert.Equal(t, "handler-lb", lbs[0].LoadBalancerName)
}

func TestPersistenceFullState(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "full-snap-lb")

	// Register an instance.
	doELB(t, h, url.Values{
		"Action":                        {"RegisterInstancesWithLoadBalancer"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"full-snap-lb"},
		"Instances.member.1.InstanceId": {"i-aaaaaaaa01"},
	})

	// Configure health check.
	doELB(t, h, url.Values{
		"Action":                         {"ConfigureHealthCheck"},
		"Version":                        {"2012-06-01"},
		"LoadBalancerName":               {"full-snap-lb"},
		"HealthCheck.Target":             {"HTTP:80/health"},
		"HealthCheck.Interval":           {"30"},
		"HealthCheck.Timeout":            {"5"},
		"HealthCheck.UnhealthyThreshold": {"2"},
		"HealthCheck.HealthyThreshold":   {"3"},
	})

	// Add a policy.
	doELB(t, h, url.Values{
		"Action":           {"CreateLBCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"full-snap-lb"},
		"PolicyName":       {"snap-pol"},
	})

	// Add tags.
	doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"full-snap-lb"},
		"Tags.member.1.Key":          {"Env"},
		"Tags.member.1.Value":        {"test"},
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elb.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify LB exists.
	lbs, err := b2.DescribeLoadBalancers(context.Background(), []string{"full-snap-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)

	lb := lbs[0]
	assert.Len(t, lb.Instances, 1)
	assert.NotNil(t, lb.HealthCheck)
	assert.Equal(t, "HTTP:80/health", lb.HealthCheck.Target)

	// Verify policy.
	assert.Equal(t, 1, b2.PolicyCount())

	// Verify tags.
	tagMap, err := b2.DescribeTags(context.Background(), []string{"full-snap-lb"})
	require.NoError(t, err)
	require.Len(t, tagMap["full-snap-lb"], 1)
	assert.Equal(t, "Env", tagMap["full-snap-lb"][0].Key)
}

// TestSnapshotSchemaVersion verifies that a snapshot includes a
// non-zero schema version and round-trips correctly.
func TestSnapshotSchemaVersion(t *testing.T) {
	t.Parallel()

	b := elb.NewInMemoryBackend("123456789012", "us-east-1")
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "snap-lb")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)
	assert.Contains(t, string(snap), `"version"`)

	// Restore into a fresh backend.
	b2 := elb.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	lbs, err := b2.DescribeLoadBalancers(context.Background(), []string{"snap-lb"})
	require.NoError(t, err)
	assert.Len(t, lbs, 1)
}

// TestRestorePreservesRegion verifies that restoring a snapshot into a
// backend that already has a region set does not overwrite that region.
func TestRestorePreservesRegion(t *testing.T) {
	t.Parallel()

	// Create snapshot from us-east-1 backend.
	b1 := elb.NewInMemoryBackend("123456789012", "us-east-1")
	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore into us-west-2 backend — region must stay us-west-2.
	b2 := elb.NewInMemoryBackend("123456789012", "us-west-2")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// The backend's region is not directly exported, but we can verify via the
	// DNS name of a newly-created LB (which incorporates the region).
	h2 := elb.NewHandler(b2)
	rec := doELB(t, h2, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"region-drift-lb"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
		"AvailabilityZones.member.1":          {"us-west-2a"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
		Result  struct {
			DNSName string `xml:"DNSName"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp.Result.DNSName, "us-west-2", "DNS should contain us-west-2 region")
}

// TestPersistenceRoundTripTagsAndPolicy verifies Snapshot/Restore preserves LBs and policies.
func TestPersistenceRoundTripTagsAndPolicy(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "persist-lb")

	doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"persist-lb"},
		"Tags.member.1.Key":          {"Env"},
		"Tags.member.1.Value":        {"prod"},
	})

	doELB(t, h, url.Values{
		"Action":           {"CreateAppCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"persist-lb"},
		"PolicyName":       {"my-pol"},
		"CookieName":       {"SID"},
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := newBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.LoadBalancerCount())
	assert.Equal(t, 1, b2.PolicyCount())

	// Verify tags were persisted.
	lbs, err := b2.DescribeLoadBalancers(context.Background(), []string{"persist-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)

	tagMap, err := b2.DescribeTags(context.Background(), []string{"persist-lb"})
	require.NoError(t, err)
	require.Len(t, tagMap["persist-lb"], 1)
	assert.Equal(t, "Env", tagMap["persist-lb"][0].Key)
	assert.Equal(t, "prod", tagMap["persist-lb"][0].Value)
}

// TestPersistenceEmptySnapshot verifies Snapshot on empty backend is valid JSON.
func TestPersistenceEmptySnapshot(t *testing.T) {
	t.Parallel()

	b := newBackend()
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)
	require.True(t, json.Valid(snap))
}

// TestPersistenceRoundTripBackendServerDescriptions verifies that Snapshot/Restore preserves
// BackendServerDescriptions and all other load balancer fields.
func TestPersistenceRoundTripBackendServerDescriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, b *elb.InMemoryBackend)
		name          string
		lbName        string
		wantBSDPorts  []int32
		wantListeners int
		wantInstances int
	}{
		{
			name:   "bsd_preserved_across_snapshot_restore",
			lbName: "persist-test-lb",
			setup: func(t *testing.T, b *elb.InMemoryBackend) {
				t.Helper()
				h := elb.NewHandler(b)

				// Create LB with a listener
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"persist-test-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})

				// Create a policy
				doELB(t, h, url.Values{
					"Action":           {"CreateLBCookieStickinessPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"persist-test-lb"},
					"PolicyName":       {"my-lb-cookie"},
				})

				// Set backend server descriptions
				doELB(t, h, url.Values{
					"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
					"Version":              {"2012-06-01"},
					"LoadBalancerName":     {"persist-test-lb"},
					"InstancePort":         {"8080"},
					"PolicyNames.member.1": {"my-lb-cookie"},
				})
			},
			wantBSDPorts:  []int32{8080},
			wantListeners: 1,
		},
		{
			name:   "empty_lb_restored_with_non_nil_slices",
			lbName: "empty-lb",
			setup: func(t *testing.T, b *elb.InMemoryBackend) {
				t.Helper()
				h := elb.NewHandler(b)
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"empty-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"80"},
				})
			},
			wantBSDPorts:  []int32{},
			wantListeners: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elb.NewInMemoryBackend("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			// Snapshot and restore into a new backend.
			snap := backend.Snapshot(t.Context())
			require.NotNil(t, snap)

			restored := elb.NewInMemoryBackend("", "")
			require.NoError(t, restored.Restore(t.Context(), snap))

			// Verify the LB can be described on the restored backend.
			lbs, err := restored.DescribeLoadBalancers(context.Background(), []string{tt.lbName})
			require.NoError(t, err)
			require.Len(t, lbs, 1)

			lb := lbs[0]
			assert.Len(t, lb.Listeners, tt.wantListeners)
			assert.NotNil(t, lb.BackendServerDescriptions, "BackendServerDescriptions must not be nil after restore")

			bsdPorts := make([]int32, 0, len(lb.BackendServerDescriptions))
			for _, bsd := range lb.BackendServerDescriptions {
				bsdPorts = append(bsdPorts, bsd.InstancePort)
			}

			assert.Equal(t, tt.wantBSDPorts, bsdPorts)
		})
	}
}

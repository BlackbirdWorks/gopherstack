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

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elb"
)

// --- helpers ---

func newBackend() *elb.InMemoryBackend {
	return elb.NewInMemoryBackend("123456789012", "us-east-1")
}

// --- tests ---

// TestRefinement1_Reset verifies that Reset() clears all backend state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "lb-reset-1")
	mustCreateLB(t, h, "lb-reset-2")

	require.Equal(t, 2, b.LoadBalancerCount())

	b.Reset()

	assert.Equal(t, 0, b.LoadBalancerCount())
	assert.Equal(t, 0, b.PolicyCount())
}

// TestRefinement1_MultipleResetCycle verifies that repeated Reset calls are safe.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	for range 5 {
		mustCreateLB(t, h, "cycle-lb")
		b.Reset()
		assert.Equal(t, 0, b.LoadBalancerCount())
	}
}

// TestRefinement1_HandlerReset verifies that Handler.Reset() delegates to the backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "hr-lb")

	require.Equal(t, 1, b.LoadBalancerCount())
	h.Reset()
	assert.Equal(t, 0, b.LoadBalancerCount())
}

// TestRefinement1_ProviderInit_NilCtx verifies that Init rejects a nil AppContext.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &elb.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, elb.ErrNilAppContext)
}

// TestRefinement1_HandlerOpsPreBuilt verifies that the dispatch table is pre-built at
// construction time and has at least 29 entries.
func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	assert.GreaterOrEqual(t, h.HandlerOpsLen(), 29)
}

// TestRefinement1_GetSupportedOperations_AllOps verifies the supported ops list has all 29 entries.
func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := elb.NewHandler(newBackend())
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateLoadBalancer",
		"DeleteLoadBalancer",
		"DescribeLoadBalancers",
		"CreateLoadBalancerListeners",
		"DeleteLoadBalancerListeners",
		"RegisterInstancesWithLoadBalancer",
		"DeregisterInstancesFromLoadBalancer",
		"ConfigureHealthCheck",
		"ModifyLoadBalancerAttributes",
		"DescribeLoadBalancerAttributes",
		"AddTags",
		"DescribeTags",
		"RemoveTags",
		"ApplySecurityGroupsToLoadBalancer",
		"AttachLoadBalancerToSubnets",
		"DetachLoadBalancerFromSubnets",
		"EnableAvailabilityZonesForLoadBalancer",
		"DisableAvailabilityZonesForLoadBalancer",
		"SetLoadBalancerListenerSSLCertificate",
		"SetLoadBalancerPoliciesOfListener",
		"SetLoadBalancerPoliciesForBackendServer",
		"CreateAppCookieStickinessPolicy",
		"CreateLBCookieStickinessPolicy",
		"CreateLoadBalancerPolicy",
		"DeleteLoadBalancerPolicy",
		"DescribeAccountLimits",
		"DescribeInstanceHealth",
		"DescribeLoadBalancerPolicies",
		"DescribeLoadBalancerPolicyTypes",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "GetSupportedOperations missing %q", op)
	}
}

// TestRefinement1_SeedHelpers verifies AddLoadBalancerInternal inserts an LB correctly.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := newBackend()
	b.AddLoadBalancerInternal(elb.LoadBalancer{
		LoadBalancerName: "seeded-lb",
		Scheme:           "internal",
	})

	assert.Equal(t, 1, b.LoadBalancerCount())
}

// TestRefinement1_ExportCountHelpers verifies LoadBalancerCount and PolicyCount.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	assert.Equal(t, 0, b.LoadBalancerCount())
	assert.Equal(t, 0, b.PolicyCount())

	mustCreateLB(t, h, "count-lb")

	assert.Equal(t, 1, b.LoadBalancerCount())

	doELB(t, h, url.Values{
		"Action":           {"CreateAppCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"count-lb"},
		"PolicyName":       {"my-policy"},
		"CookieName":       {"SESS"},
	})

	assert.Equal(t, 1, b.PolicyCount())
}

// TestRefinement1_DeleteLoadBalancer_CascadesPolicies verifies that DeleteLoadBalancer
// removes all policies associated with the deleted LB.
func TestRefinement1_DeleteLoadBalancer_CascadesPolicies(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "cascade-lb")

	doELB(t, h, url.Values{
		"Action":           {"CreateAppCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"cascade-lb"},
		"PolicyName":       {"pol-1"},
		"CookieName":       {"SID"},
	})

	require.Equal(t, 1, b.PolicyCount())

	doELB(t, h, url.Values{
		"Action":           {"DeleteLoadBalancer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"cascade-lb"},
	})

	assert.Equal(t, 0, b.LoadBalancerCount())
	assert.Equal(t, 0, b.PolicyCount())
}

// TestRefinement1_PolicyNotFound_Returns404 verifies ErrPolicyNotFound maps to HTTP 404.
func TestRefinement1_PolicyNotFound_Returns404(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "err-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"DeleteLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"err-lb"},
		"PolicyName":       {"no-such-policy"},
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)

	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "PolicyNotFound", errResp.Error.Code)
}

// TestRefinement1_PolicyAlreadyExists_Returns409 verifies ErrPolicyAlreadyExists maps to HTTP 409.
func TestRefinement1_PolicyAlreadyExists_Returns409(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "dup-pol-lb")

	doELB(t, h, url.Values{
		"Action":           {"CreateAppCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"dup-pol-lb"},
		"PolicyName":       {"my-pol"},
		"CookieName":       {"SID"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"CreateAppCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"dup-pol-lb"},
		"PolicyName":       {"my-pol"},
		"CookieName":       {"SID"},
	})

	assert.Equal(t, http.StatusConflict, rec.Code)

	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "DuplicatePolicyName", errResp.Error.Code)
}

// TestRefinement1_ErrValidationMapping verifies ErrValidation maps to HTTP 400.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := elb.NewHandler(newBackend())

	rec := doELB(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2012-06-01"},
		// Missing LoadBalancerName → ErrInvalidParameter → 400
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_DeepCopy_Listeners verifies that the slice returned by DescribeLoadBalancers
// is a deep copy — mutations do not affect the stored state.
func TestRefinement1_DeepCopy_Listeners(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "dc-lb")

	// First describe: copy has 1 listener (from mustCreateLB).
	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"dc-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)

	originalCount := len(lbs[0].Listeners)

	// Append a listener to the returned copy — must not affect the stored LB.
	lbs[0].Listeners = append(lbs[0].Listeners, elb.Listener{Protocol: "TCP", LoadBalancerPort: 443, InstancePort: 443})
	require.Len(t, lbs[0].Listeners, originalCount+1)

	// Second describe: stored state must still have only originalCount listeners.
	lbs2, err := b.DescribeLoadBalancers(context.Background(), []string{"dc-lb"})
	require.NoError(t, err)
	assert.Len(t, lbs2[0].Listeners, originalCount, "mutation of returned copy must not modify stored state")
}

// TestRefinement1_NonNilSlices verifies that all slice fields on a newly created LB are non-nil.
func TestRefinement1_NonNilSlices(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "non-nil-lb")

	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"non-nil-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)

	lb := lbs[0]

	assert.NotNil(t, lb.Listeners)
	assert.NotNil(t, lb.Instances)
	assert.NotNil(t, lb.AvailabilityZones)
	assert.NotNil(t, lb.SecurityGroups)
	assert.NotNil(t, lb.Subnets)
}

// TestRefinement1_ARNSet verifies that ARN is populated on the returned LB.
func TestRefinement1_ARNSet(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "arn-lb")

	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"arn-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)

	assert.NotEmpty(t, lbs[0].ARN, "ARN must be set on created load balancer")
	assert.Contains(t, lbs[0].ARN, "loadbalancer/arn-lb")
}

// TestRefinement1_VPCId_SetFromSubnets verifies that VPCId is derived when subnets are provided.
func TestRefinement1_VPCId_SetFromSubnets(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"vpc-lb"},
		"Subnets.member.1":                    {"subnet-abc123"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"vpc-lb"})
	require.NoError(t, err)
	assert.NotEmpty(t, lbs[0].VPCId, "VPCId must be set when subnets are provided")
}

// TestRefinement1_VPCId_EmptyWithoutSubnets verifies VPCId is empty for classic (no subnets) LBs.
func TestRefinement1_VPCId_EmptyWithoutSubnets(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"classic-lb"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"classic-lb"})
	require.NoError(t, err)
	assert.Empty(t, lbs[0].VPCId, "VPCId must be empty for classic (non-VPC) load balancers")
}

// TestRefinement1_SortedDescribeLoadBalancers verifies alphabetical ordering of DescribeLoadBalancers.
func TestRefinement1_SortedDescribeLoadBalancers(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	for _, name := range []string{"z-lb", "a-lb", "m-lb"} {
		mustCreateLB(t, h, name)
	}

	lbs, err := b.DescribeLoadBalancers(context.Background(), nil)
	require.NoError(t, err)

	names := make([]string, len(lbs))
	for i, lb := range lbs {
		names[i] = lb.LoadBalancerName
	}

	assert.Equal(t, []string{"a-lb", "m-lb", "z-lb"}, names)
}

// TestRefinement1_DescribeAccountLimits_Locked verifies DescribeAccountLimits succeeds
// and returns exactly 3 limits.
func TestRefinement1_DescribeAccountLimits_Locked(t *testing.T) {
	t.Parallel()

	b := newBackend()
	limits, err := b.DescribeAccountLimits(context.Background())
	require.NoError(t, err)
	assert.Len(t, limits, 3)
}

// TestRefinement1_DescribeLoadBalancerPolicyTypes_UnknownReturnsError verifies that
// requesting an unknown policy type returns an error (not a silent empty list).
func TestRefinement1_DescribeLoadBalancerPolicyTypes_UnknownReturnsError(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.DescribeLoadBalancerPolicyTypes(context.Background(), []string{"NoSuchPolicyType"})
	require.Error(t, err)
	require.ErrorIs(t, err, elb.ErrPolicyNotFound)
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore preserves LBs and policies.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
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

// TestRefinement1_Persistence_EmptySnapshot verifies Snapshot on empty backend is valid JSON.
func TestRefinement1_Persistence_EmptySnapshot(t *testing.T) {
	t.Parallel()

	b := newBackend()
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)
	require.True(t, json.Valid(snap))
}

// TestRefinement1_SeedHelper_DeepCopy verifies AddLoadBalancerInternal performs a deep copy.
func TestRefinement1_SeedHelper_DeepCopy(t *testing.T) {
	t.Parallel()

	b := newBackend()

	original := elb.LoadBalancer{
		LoadBalancerName:  "seed-dc-lb",
		Listeners:         []elb.Listener{{Protocol: "HTTP", LoadBalancerPort: 80, InstancePort: 8080}},
		AvailabilityZones: []string{"us-east-1a"},
	}

	b.AddLoadBalancerInternal(original)

	// Mutate the original after insertion — must not affect stored state.
	original.Listeners = append(
		original.Listeners,
		elb.Listener{Protocol: "HTTPS", LoadBalancerPort: 443, InstancePort: 8443},
	)

	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"seed-dc-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)
	assert.Len(t, lbs[0].Listeners, 1, "stored LB must not reflect post-seed mutation")
}

// TestRefinement1_DescribeInstanceHealth_SortedByID verifies that DescribeInstanceHealth
// returns states sorted by instance ID when no specific instances are requested.
func TestRefinement1_DescribeInstanceHealth_SortedByID(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "health-sort-lb")

	for _, id := range []string{"i-cccccccc00", "i-aaaaaaaabb", "i-bbbbbbbbcc"} {
		doELB(t, h, url.Values{
			"Action":                        {"RegisterInstancesWithLoadBalancer"},
			"Version":                       {"2012-06-01"},
			"LoadBalancerName":              {"health-sort-lb"},
			"Instances.member.1.InstanceId": {id},
		})
	}

	rec := doELB(t, h, url.Values{
		"Action":           {"DescribeInstanceHealth"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"health-sort-lb"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeInstanceHealthResponse"`
		Result  struct {
			InstanceStates struct {
				Members []struct {
					InstanceID string `xml:"InstanceId"`
				} `xml:"member"`
			} `xml:"InstanceStates"`
		} `xml:"DescribeInstanceHealthResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	ids := make([]string, 0, 3)
	for _, s := range resp.Result.InstanceStates.Members {
		ids = append(ids, s.InstanceID)
	}

	assert.Equal(t, []string{"i-aaaaaaaabb", "i-bbbbbbbbcc", "i-cccccccc00"}, ids)
}

// TestRefinement1_ErrNilAppContext is a package-level sentinel check.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	require.Error(t, elb.ErrNilAppContext)
}

// TestRefinement1_ProviderInit_ValidCtx verifies Init succeeds with a valid context.
func TestRefinement1_ProviderInit_ValidCtx(t *testing.T) {
	t.Parallel()

	p := &elb.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

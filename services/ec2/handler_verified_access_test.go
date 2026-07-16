package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- VerifiedAccess ----.
func TestVerifiedAccess(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var instanceID, groupID, endpointID, trustProviderID string

	t.Run("create instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		inst, err := b.CreateVerifiedAccessInstance("test instance")
		require.NoError(t, err)
		assert.NotEmpty(t, inst.VerifiedAccessInstanceID)
		assert.Equal(t, "active", inst.Status)
		instanceID = inst.VerifiedAccessInstanceID
	})

	t.Run("describe instances", func(t *testing.T) { //nolint:paralleltest // existing issue.
		instances := b.DescribeVerifiedAccessInstances([]string{instanceID})
		require.Len(t, instances, 1)
		assert.Equal(t, "test instance", instances[0].Description)
	})

	t.Run("create trust provider", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tp, err := b.CreateVerifiedAccessTrustProvider("user", "test provider")
		require.NoError(t, err)
		assert.NotEmpty(t, tp.VerifiedAccessTrustProviderID)
		trustProviderID = tp.VerifiedAccessTrustProviderID
	})

	t.Run("attach trust provider to instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.AttachVerifiedAccessTrustProvider(instanceID, trustProviderID))
		// double-attach is idempotent
		require.NoError(t, b.AttachVerifiedAccessTrustProvider(instanceID, trustProviderID))
	})

	t.Run("detach trust provider from instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DetachVerifiedAccessTrustProvider(instanceID, trustProviderID))
	})

	t.Run("create group", func(t *testing.T) { //nolint:paralleltest // existing issue.
		grp, err := b.CreateVerifiedAccessGroup(instanceID, "test group")
		require.NoError(t, err)
		assert.NotEmpty(t, grp.VerifiedAccessGroupID)
		groupID = grp.VerifiedAccessGroupID
	})

	t.Run("describe groups", func(t *testing.T) { //nolint:paralleltest // existing issue.
		groups := b.DescribeVerifiedAccessGroups([]string{groupID})
		require.Len(t, groups, 1)
		assert.Equal(t, instanceID, groups[0].VerifiedAccessInstanceID)
	})

	t.Run("create endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ep, err := b.CreateVerifiedAccessEndpoint(groupID, "load-balancer", "test endpoint")
		require.NoError(t, err)
		assert.NotEmpty(t, ep.VerifiedAccessEndpointID)
		endpointID = ep.VerifiedAccessEndpointID
	})

	t.Run("describe endpoints", func(t *testing.T) { //nolint:paralleltest // existing issue.
		eps := b.DescribeVerifiedAccessEndpoints([]string{endpointID})
		require.Len(t, eps, 1)
		assert.Equal(t, groupID, eps[0].VerifiedAccessGroupID)
	})

	t.Run("modify endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyVerifiedAccessEndpoint(endpointID, "updated endpoint"))
		eps := b.DescribeVerifiedAccessEndpoints([]string{endpointID})
		require.Len(t, eps, 1)
		assert.Equal(t, "updated endpoint", eps[0].Description)
	})

	t.Run("describe trust providers", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tps := b.DescribeVerifiedAccessTrustProviders([]string{trustProviderID})
		require.Len(t, tps, 1)
		assert.Equal(t, "user", tps[0].TrustProviderType)
	})

	t.Run("delete endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVerifiedAccessEndpoint(endpointID))
		eps := b.DescribeVerifiedAccessEndpoints(nil)
		assert.Empty(t, eps)
	})

	t.Run("delete group", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVerifiedAccessGroup(groupID))
		groups := b.DescribeVerifiedAccessGroups(nil)
		assert.Empty(t, groups)
	})

	t.Run("delete trust provider", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVerifiedAccessTrustProvider(trustProviderID))
		tps := b.DescribeVerifiedAccessTrustProviders(nil)
		assert.Empty(t, tps)
	})

	t.Run("delete instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVerifiedAccessInstance(instanceID))
		instances := b.DescribeVerifiedAccessInstances(nil)
		assert.Empty(t, instances)
	})

	t.Run( //nolint:paralleltest // existing issue.
		"create group for non-existent instance still works",
		func(t *testing.T) {
			// Group creation doesn't validate instance existence in mock
			_, err := b.CreateVerifiedAccessGroup("vai-nonexistent", "group")
			require.NoError(t, err)
		},
	)

	t.Run("attach to non-existent instance returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tp2, err := b.CreateVerifiedAccessTrustProvider("device", "desc")
		require.NoError(t, err)
		require.Error(t, b.AttachVerifiedAccessTrustProvider("vai-nonexistent", tp2.VerifiedAccessTrustProviderID))
	})
}

// TestVerifiedAccess_InstanceCRUD verifies full instance lifecycle.
func TestVerifiedAccess_InstanceCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	inst, err := b.CreateVerifiedAccessInstance("test instance")
	require.NoError(t, err)
	assert.Contains(t, inst.VerifiedAccessInstanceID, "vai-")

	insts := b.DescribeVerifiedAccessInstances([]string{inst.VerifiedAccessInstanceID})
	require.Len(t, insts, 1)
	assert.Equal(t, inst.VerifiedAccessInstanceID, insts[0].VerifiedAccessInstanceID)

	require.NoError(t, b.DeleteVerifiedAccessInstance(inst.VerifiedAccessInstanceID))
	insts2 := b.DescribeVerifiedAccessInstances(nil)
	assert.Empty(t, insts2)
}

// TestVerifiedAccess_TrustProviderCRUD verifies trust provider lifecycle.

// TestVerifiedAccess_TrustProviderCRUD verifies trust provider lifecycle.
func TestVerifiedAccess_TrustProviderCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	tp, err := b.CreateVerifiedAccessTrustProvider("user", "test provider")
	require.NoError(t, err)
	assert.Contains(t, tp.VerifiedAccessTrustProviderID, "vatp-")

	tps := b.DescribeVerifiedAccessTrustProviders([]string{tp.VerifiedAccessTrustProviderID})
	require.Len(t, tps, 1)

	require.NoError(t, b.DeleteVerifiedAccessTrustProvider(tp.VerifiedAccessTrustProviderID))
}

// TestVerifiedAccess_GroupCRUD verifies group lifecycle.

// TestVerifiedAccess_GroupCRUD verifies group lifecycle.
func TestVerifiedAccess_GroupCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	inst, err := b.CreateVerifiedAccessInstance("group test")
	require.NoError(t, err)

	grp, err := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "test group")
	require.NoError(t, err)
	assert.Contains(t, grp.VerifiedAccessGroupID, "vagr-")

	grps := b.DescribeVerifiedAccessGroups([]string{grp.VerifiedAccessGroupID})
	require.Len(t, grps, 1)

	require.NoError(t, b.DeleteVerifiedAccessGroup(grp.VerifiedAccessGroupID))
}

// TestVerifiedAccess_EndpointCRUD verifies endpoint lifecycle.

// TestVerifiedAccess_EndpointCRUD verifies endpoint lifecycle.
func TestVerifiedAccess_EndpointCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	inst, err := b.CreateVerifiedAccessInstance("ep test")
	require.NoError(t, err)

	grp, err := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "ep group")
	require.NoError(t, err)

	ep, err := b.CreateVerifiedAccessEndpoint(grp.VerifiedAccessGroupID, "subnet-default", "load-balancer")
	require.NoError(t, err)
	assert.Contains(t, ep.VerifiedAccessEndpointID, "vae-")

	eps := b.DescribeVerifiedAccessEndpoints([]string{ep.VerifiedAccessEndpointID})
	require.Len(t, eps, 1)

	require.NoError(t, b.DeleteVerifiedAccessEndpoint(ep.VerifiedAccessEndpointID))
}

// ============================================================================
// Helper
// ============================================================================

// extractBatch4XMLValue extracts the first text content of an XML element by tag name.

func TestParityFinalHTTP_ModifyVerifiedAccessGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	inst, err := h.Backend.CreateVerifiedAccessInstance("inst")
	require.NoError(t, err)
	grp, err := h.Backend.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "orig")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                {"ModifyVerifiedAccessGroup"},
		"VerifiedAccessGroupId": {grp.VerifiedAccessGroupID},
		"Description":           {"updated"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<ModifyVerifiedAccessGroupResponse>")
	assert.Contains(t, resp, "<description>updated</description>")
	assert.Contains(t, resp, "<verifiedAccessGroupId>"+grp.VerifiedAccessGroupID+"</verifiedAccessGroupId>")
}

func TestParityFinalHTTP_ModifyVerifiedAccessInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	inst, err := h.Backend.CreateVerifiedAccessInstance("orig")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                   {"ModifyVerifiedAccessInstance"},
		"VerifiedAccessInstanceId": {inst.VerifiedAccessInstanceID},
		"Description":              {"updated"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<ModifyVerifiedAccessInstanceResponse>")
	assert.Contains(t, resp, "<description>updated</description>")
}

func TestParityFinalHTTP_ModifyVerifiedAccessTrustProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tp, err := h.Backend.CreateVerifiedAccessTrustProvider("user", "orig")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                        {"ModifyVerifiedAccessTrustProvider"},
		"VerifiedAccessTrustProviderId": {tp.VerifiedAccessTrustProviderID},
		"Description":                   {"updated"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<ModifyVerifiedAccessTrustProviderResponse>")
	assert.Contains(t, resp, "<description>updated</description>")

	// Not-found surfaces as a 400, not a 500 InternalFailure.
	rec := postForm(t, h, "Action=ModifyVerifiedAccessTrustProvider&VerifiedAccessTrustProviderId=vatp-missing")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidVerifiedAccessTrustProviderId.NotFound")
}

package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func (e *env) regionArmor(t *testing.T) *compute.RegionSecurityPoliciesClient {
	t.Helper()

	return client(t, e, compute.NewRegionSecurityPoliciesRESTClient)
}

func (e *env) regionPolicy(t *testing.T, p *computepb.SecurityPolicy) error {
	t.Helper()

	op, err := e.regionArmor(t).Insert(t.Context(), &computepb.InsertRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicyResource: p,
	})

	return e.finish(t, op, err)
}

func (e *env) regionBS(t *testing.T, name, scheme string) error {
	t.Helper()

	op, err := client(t, e, compute.NewRegionBackendServicesRESTClient).Insert(t.Context(),
		&computepb.InsertRegionBackendServiceRequest{
			Project: project, Region: region,
			BackendServiceResource: &computepb.BackendService{Name: s(name), LoadBalancingScheme: s(scheme)},
		})

	return e.finish(t, op, err)
}

func TestRegionSecurityPolicyLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	rc := e.regionArmor(t)
	reqRule := func(prio int32, ranges ...string) *computepb.AddRuleRegionSecurityPolicyRequest {
		return &computepb.AddRuleRegionSecurityPolicyRequest{
			Project: project, Region: region, SecurityPolicy: "rp",
			SecurityPolicyRuleResource: srcRule(prio, "deny(403)", ranges...),
		}
	}

	require.NoError(t, e.regionPolicy(t, &computepb.SecurityPolicy{Name: s("rp"), Description: s("d")}))
	assertAPIError(t, e.regionPolicy(t, &computepb.SecurityPolicy{Name: s("rp")}), http.StatusConflict, "alreadyExists")
	assertAPIError(t, e.regionPolicy(t, &computepb.SecurityPolicy{Name: s("edge"), Type: s("CLOUD_ARMOR_EDGE")}),
		http.StatusBadRequest, "invalid")

	got, err := rc.Get(t.Context(), &computepb.GetRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp",
	})
	require.NoError(t, err)
	assert.Contains(t, got.GetRegion(), "/regions/"+region)
	assert.Contains(t, got.GetSelfLink(), "/regions/"+region+"/securityPolicies/rp")
	require.Len(t, got.GetRules(), 1)
	assert.Equal(t, defaultPrio, got.GetRules()[0].GetPriority())

	_, err = e.armor(t).Get(t.Context(), &computepb.GetSecurityPolicyRequest{Project: project, SecurityPolicy: "rp"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	op, err := rc.AddRule(t.Context(), reqRule(10, "198.51.100.0/24"))
	require.NoError(t, e.finish(t, op, err))
	op, err = rc.AddRule(t.Context(), reqRule(10, "198.51.100.0/24"))
	assertAPIError(t, e.finish(t, op, err), http.StatusBadRequest, "invalid")

	rule, err := rc.GetRule(t.Context(), &computepb.GetRuleRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp", Priority: new(int32(10)),
	})
	require.NoError(t, err)
	assert.Equal(t, "deny(403)", rule.GetAction())

	op, err = rc.PatchRule(t.Context(), &computepb.PatchRuleRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp", Priority: new(int32(10)),
		SecurityPolicyRuleResource: srcRule(10, "deny(404)", "203.0.113.0/24"),
	})
	require.NoError(t, e.finish(t, op, err))

	rule, err = rc.GetRule(t.Context(), &computepb.GetRuleRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp", Priority: new(int32(10)),
	})
	require.NoError(t, err)
	assert.Equal(t, "deny(404)", rule.GetAction())

	op, err = rc.RemoveRule(t.Context(), &computepb.RemoveRuleRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp", Priority: new(int32(10)),
	})
	require.NoError(t, e.finish(t, op, err))

	op, err = rc.SetLabels(t.Context(), &computepb.SetLabelsRegionSecurityPolicyRequest{
		Project: project, Region: region, Resource: "rp",
		RegionSetLabelsRequestResource: &computepb.RegionSetLabelsRequest{
			Labels: map[string]string{"a": "b"}, LabelFingerprint: s(got.GetLabelFingerprint()),
		},
	})
	require.NoError(t, e.finish(t, op, err))

	op, err = rc.Patch(t.Context(), &computepb.PatchRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp",
		SecurityPolicyResource: &computepb.SecurityPolicy{Description: s("new")},
	})
	require.NoError(t, e.finish(t, op, err))

	got, err = rc.Get(t.Context(), &computepb.GetRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp",
	})
	require.NoError(t, err)
	assert.Equal(t, "new", got.GetDescription())
	assert.Equal(t, map[string]string{"a": "b"}, got.GetLabels())

	it := rc.List(t.Context(), &computepb.ListRegionSecurityPoliciesRequest{Project: project, Region: region})
	p, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, "rp", p.GetName())

	_, err = it.Next()
	require.ErrorIs(t, err, iterator.Done)

	op, err = rc.Delete(t.Context(), &computepb.DeleteRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp",
	})
	require.NoError(t, e.finish(t, op, err))

	_, err = rc.Get(t.Context(), &computepb.GetRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp",
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestSecurityPolicyAggregatedList(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("g1")}))
	require.NoError(t, e.regionPolicy(t, &computepb.SecurityPolicy{Name: s("r1")}))

	got := map[string]string{}
	it := e.armor(t).AggregatedList(t.Context(), &computepb.AggregatedListSecurityPoliciesRequest{Project: project})

	for {
		kv, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			break
		}

		for _, p := range kv.Value.GetSecurityPolicies() {
			got[p.GetName()] = kv.Key
		}
	}

	assert.Equal(t, map[string]string{"g1": "global", "r1": "regions/" + region}, got)
}

func TestListPreconfiguredExpressionSetsNotRouted(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, path string }{
		{
			"project scope",
			"/compute/v1/projects/" + project + "/global/securityPolicies/listPreconfiguredExpressionSets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, _ := rawDo(t, newEnv(t), http.MethodGet, tt.path, "")
			assert.Equal(t, http.StatusNotFound, status)
		})
	}
}

func TestRegionSecurityPolicyAttachment(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.regionPolicy(t, &computepb.SecurityPolicy{Name: s("rp")}))
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("gp")}))
	require.NoError(t, e.regionBS(t, "rbs", "INTERNAL_MANAGED"))

	rbs := client(t, e, compute.NewRegionBackendServicesRESTClient)
	set := func(ref string) error {
		op, err := rbs.SetSecurityPolicy(t.Context(), &computepb.SetSecurityPolicyRegionBackendServiceRequest{
			Project: project, Region: region, BackendService: "rbs",
			SecurityPolicyReferenceResource: &computepb.SecurityPolicyReference{SecurityPolicy: s(ref)},
		})

		return e.finish(t, op, err)
	}

	assertAPIError(t, set("regions/"+region+"/securityPolicies/ghost"), http.StatusNotFound, "notFound")
	assertAPIError(t, set("global/securityPolicies/gp"), http.StatusBadRequest, "invalid")
	require.NoError(t, set("regions/"+region+"/securityPolicies/rp"))

	_, err := e.regionArmor(t).Delete(t.Context(), &computepb.DeleteRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp",
	})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	require.NoError(t, set(""))

	op, err := e.regionArmor(t).Delete(t.Context(), &computepb.DeleteRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp",
	})
	require.NoError(t, e.finish(t, op, err))
}

func TestEdgeSecurityPolicyAttachment(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.lbStack(t)
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("edge"), Type: s("CLOUD_ARMOR_EDGE")}))
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("pol")}))

	bbc := client(t, e, compute.NewBackendBucketsRESTClient)
	op, err := bbc.Insert(t.Context(), &computepb.InsertBackendBucketRequest{
		Project: project, BackendBucketResource: &computepb.BackendBucket{Name: s("bb"), BucketName: s("my-bucket")},
	})
	e.done(t, op, err)

	bsc := e.backendServices(t)
	setBS := func(ref string) error {
		sop, serr := bsc.SetEdgeSecurityPolicy(t.Context(), &computepb.SetEdgeSecurityPolicyBackendServiceRequest{
			Project: project, BackendService: "bs",
			SecurityPolicyReferenceResource: &computepb.SecurityPolicyReference{SecurityPolicy: s(ref)},
		})

		return e.finish(t, sop, serr)
	}
	setBB := func(ref string) error {
		sop, serr := bbc.SetEdgeSecurityPolicy(t.Context(), &computepb.SetEdgeSecurityPolicyBackendBucketRequest{
			Project: project, BackendBucket: "bb",
			SecurityPolicyReferenceResource: &computepb.SecurityPolicyReference{SecurityPolicy: s(ref)},
		})

		return e.finish(t, sop, serr)
	}

	assertAPIError(t, setBS("global/securityPolicies/ghost"), http.StatusNotFound, "notFound")
	assertAPIError(t, setBS("global/securityPolicies/pol"), http.StatusBadRequest, "invalid")
	assertAPIError(t, setBB("global/securityPolicies/pol"), http.StatusBadRequest, "invalid")
	require.NoError(t, setBS("global/securityPolicies/edge"))
	require.NoError(t, setBB("global/securityPolicies/edge"))

	bs, err := bsc.Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
	require.NoError(t, err)
	assert.Contains(t, bs.GetEdgeSecurityPolicy(), "/global/securityPolicies/edge")

	bb, err := bbc.Get(t.Context(), &computepb.GetBackendBucketRequest{Project: project, BackendBucket: "bb"})
	require.NoError(t, err)
	assert.Contains(t, bb.GetEdgeSecurityPolicy(), "/global/securityPolicies/edge")

	op, err = bbc.Patch(t.Context(), &computepb.PatchBackendBucketRequest{
		Project: project, BackendBucket: "bb",
		BackendBucketResource: &computepb.BackendBucket{Description: s("x")},
	})
	e.done(t, op, err)

	bb, err = bbc.Get(t.Context(), &computepb.GetBackendBucketRequest{Project: project, BackendBucket: "bb"})
	require.NoError(t, err)
	assert.Contains(t, bb.GetEdgeSecurityPolicy(), "/securityPolicies/edge")

	_, err = e.armor(t).
		Delete(t.Context(), &computepb.DeleteSecurityPolicyRequest{Project: project, SecurityPolicy: "edge"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	require.NoError(t, setBS(""))
	_, err = e.armor(t).
		Delete(t.Context(), &computepb.DeleteSecurityPolicyRequest{Project: project, SecurityPolicy: "edge"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
	require.NoError(t, setBB(""))

	op, err = e.armor(t).
		Delete(t.Context(), &computepb.DeleteSecurityPolicyRequest{Project: project, SecurityPolicy: "edge"})
	require.NoError(t, e.finish(t, op, err))
}

func TestAdaptiveProtectionPlacement(t *testing.T) {
	t.Parallel()

	adaptive := func() *computepb.SecurityPolicyAdaptiveProtectionConfig {
		return &computepb.SecurityPolicyAdaptiveProtectionConfig{
			Layer7DdosDefenseConfig: &computepb.SecurityPolicyAdaptiveProtectionConfigLayer7DdosDefenseConfig{
				Enable: new(true), RuleVisibility: s("PREMIUM"),
			},
		}
	}

	e := newEnv(t)
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("pol"), AdaptiveProtectionConfig: adaptive()}))
	assert.Equal(
		t,
		"PREMIUM",
		e.getPolicy(t).GetAdaptiveProtectionConfig().GetLayer7DdosDefenseConfig().GetRuleVisibility(),
	)

	assertAPIError(t, e.regionPolicy(t, &computepb.SecurityPolicy{Name: s("rp"), AdaptiveProtectionConfig: adaptive()}),
		http.StatusBadRequest, "invalid")
	assertAPIError(t, e.policy(t, &computepb.SecurityPolicy{
		Name: s("net"), Type: s("CLOUD_ARMOR_NETWORK"), AdaptiveProtectionConfig: adaptive(),
	}), http.StatusBadRequest, "invalid")
}

func TestScopedSecurityPolicyRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.lbStack(t)
	require.NoError(t, src.regionPolicy(t, &computepb.SecurityPolicy{Name: s("rp")}))
	require.NoError(t, src.policy(t, &computepb.SecurityPolicy{Name: s("edge"), Type: s("CLOUD_ARMOR_EDGE")}))

	op, err := src.backendServices(t).SetEdgeSecurityPolicy(t.Context(),
		&computepb.SetEdgeSecurityPolicyBackendServiceRequest{
			Project: project, BackendService: "bs",
			SecurityPolicyReferenceResource: &computepb.SecurityPolicyReference{
				SecurityPolicy: s("global/securityPolicies/edge"),
			},
		})
	src.done(t, op, err)

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), src.backend.Snapshot(t.Context())))

	got, err := dst.regionArmor(t).Get(t.Context(), &computepb.GetRegionSecurityPolicyRequest{
		Project: project, Region: region, SecurityPolicy: "rp",
	})
	require.NoError(t, err)
	assert.Equal(t, "rp", got.GetName())

	bs, err := dst.backendServices(t).
		Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
	require.NoError(t, err)
	assert.Contains(t, bs.GetEdgeSecurityPolicy(), "/securityPolicies/edge")
}

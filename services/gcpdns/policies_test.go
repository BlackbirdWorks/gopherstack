package gcpdns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
)

const netURL = "https://www.googleapis.com/compute/v1/projects/p/global/networks/"

func TestPolicyLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	p, err := e.svc.Policies.Create(project, &dnsapi.Policy{
		Name: "pol", Description: "d", EnableInboundForwarding: true, EnableLogging: true,
		Networks: []*dnsapi.PolicyNetwork{{NetworkUrl: netURL + "n1"}},
		AlternativeNameServerConfig: &dnsapi.PolicyAlternativeNameServerConfig{
			TargetNameServers: []*dnsapi.PolicyAlternativeNameServerConfigTargetNameServer{{Ipv4Address: "10.0.0.2"}},
		},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "dns#policy", p.Kind)
	assert.NotZero(t, p.Id)
	assert.Equal(t, "dns#policyNetwork", p.Networks[0].Kind)
	assert.True(t, p.EnableInboundForwarding)

	_, err = e.svc.Policies.Create(project, &dnsapi.Policy{Name: "pol"}).Context(t.Context()).Do()
	assertAPIError(t, err, 409, "alreadyExists")

	_, err = e.svc.Policies.Create(project, &dnsapi.Policy{
		Name: "other", Networks: []*dnsapi.PolicyNetwork{{NetworkUrl: netURL + "n1"}},
	}).Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")

	patched, err := e.svc.Policies.Patch(project, "pol", &dnsapi.Policy{Description: "new"}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "new", patched.Policy.Description)
	assert.True(t, patched.Policy.EnableLogging, "patch keeps untouched fields")
	assert.Len(t, patched.Policy.Networks, 1)

	updated, err := e.svc.Policies.Update(project, "pol", &dnsapi.Policy{Description: "replaced"}).
		Context(t.Context()).
		Do()
	require.NoError(t, err)
	assert.False(t, updated.Policy.EnableLogging, "update replaces the policy")
	assert.Empty(t, updated.Policy.Networks)
	assert.Equal(t, p.Id, updated.Policy.Id)

	_, err = e.svc.Policies.Patch(project, "pol", &dnsapi.Policy{Name: "renamed"}).Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")

	_, err = e.svc.Policies.Patch(project, "missing", &dnsapi.Policy{Description: "x"}).Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")

	list, err := e.svc.Policies.List(project).Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, list.Policies, 1)
	assert.Equal(t, "dns#policiesListResponse", list.Kind)

	require.NoError(t, e.svc.Policies.Delete(project, "pol").Context(t.Context()).Do())
	_, err = e.svc.Policies.Get(project, "pol").Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")
}

func TestPolicyValidationAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy *dnsapi.Policy
		name   string
	}{
		{&dnsapi.Policy{}, "no name"},
		{&dnsapi.Policy{Name: "Bad Name"}, "bad name"},
		{&dnsapi.Policy{Name: "p", Networks: []*dnsapi.PolicyNetwork{{}}}, "network without url"},
		{&dnsapi.Policy{Name: "p", AlternativeNameServerConfig: &dnsapi.PolicyAlternativeNameServerConfig{
			TargetNameServers: []*dnsapi.PolicyAlternativeNameServerConfigTargetNameServer{{}},
		}}, "target without address"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newEnv(t).svc.Policies.Create(project, tt.policy).Context(t.Context()).Do()
			require.Error(t, err)
			assert.Equal(t, 400, apiErr(t, err).Code)
		})
	}

	e := newEnv(t)
	_, err := e.svc.Policies.Create(project, &dnsapi.Policy{
		Name: "attached", Networks: []*dnsapi.PolicyNetwork{{NetworkUrl: netURL + "n"}},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assertAPIError(t, e.svc.Policies.Delete(project, "attached").Context(t.Context()).Do(), 400, "containerNotEmpty")
	assertAPIError(t, e.svc.Policies.Delete(project, "missing").Context(t.Context()).Do(), 404, "notFound")
}

func TestResponsePolicyAndRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	rp, err := e.svc.ResponsePolicies.Create(project, &dnsapi.ResponsePolicy{
		ResponsePolicyName: "rp", Description: "d", Labels: map[string]string{"a": "b"},
		Networks: []*dnsapi.ResponsePolicyNetwork{{NetworkUrl: netURL + "n"}},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "dns#responsePolicy", rp.Kind)
	assert.NotZero(t, rp.Id)

	_, err = e.svc.ResponsePolicies.Create(project, &dnsapi.ResponsePolicy{ResponsePolicyName: "rp"}).
		Context(t.Context()).
		Do()
	assertAPIError(t, err, 409, "alreadyExists")

	patched, err := e.svc.ResponsePolicies.Patch(project, "rp", &dnsapi.ResponsePolicy{Description: "new"}).
		Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "new", patched.ResponsePolicy.Description)
	assert.Equal(t, map[string]string{"a": "b"}, patched.ResponsePolicy.Labels)

	updated, err := e.svc.ResponsePolicies.Update(project, "rp", &dnsapi.ResponsePolicy{Description: "u"}).
		Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Empty(t, updated.ResponsePolicy.Networks)

	local := &dnsapi.ResponsePolicyRule{
		RuleName: "r1", DnsName: "blocked.example.com.",
		LocalData: &dnsapi.ResponsePolicyRuleLocalData{LocalDatas: []*dnsapi.ResourceRecordSet{
			{Name: "blocked.example.com.", Type: "A", Ttl: 60, Rrdatas: []string{"0.0.0.0"}},
		}},
	}
	r, err := e.svc.ResponsePolicyRules.Create(project, "rp", local).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "dns#responsePolicyRule", r.Kind)
	assert.Equal(t, "0.0.0.0", r.LocalData.LocalDatas[0].Rrdatas[0])

	bypass, err := e.svc.ResponsePolicyRules.Create(project, "rp", &dnsapi.ResponsePolicyRule{
		RuleName: "r2", DnsName: "ok.example.com.", Behavior: "bypassResponsePolicy",
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "bypassResponsePolicy", bypass.Behavior)

	for name, rule := range map[string]*dnsapi.ResponsePolicyRule{
		"neither": {RuleName: "r3", DnsName: "x.com."},
		"both":    {RuleName: "r3", DnsName: "x.com.", Behavior: "bypassResponsePolicy", LocalData: local.LocalData},
		"no dot":  {RuleName: "r3", DnsName: "x.com", Behavior: "bypassResponsePolicy"},
		"bad data": {RuleName: "r3", DnsName: "x.com.", LocalData: &dnsapi.ResponsePolicyRuleLocalData{
			LocalDatas: []*dnsapi.ResourceRecordSet{{Name: "x.com.", Type: "A", Rrdatas: []string{"bad"}}},
		}},
	} {
		_, err = e.svc.ResponsePolicyRules.Create(project, "rp", rule).Context(t.Context()).Do()
		assert.Equal(t, 400, apiErr(t, err).Code, name)
	}

	_, err = e.svc.ResponsePolicyRules.Create(project, "rp", local).Context(t.Context()).Do()
	assertAPIError(t, err, 409, "alreadyExists")

	_, err = e.svc.ResponsePolicyRules.Create(project, "missing", local).Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")

	pr, err := e.svc.ResponsePolicyRules.Patch(project, "rp", "r2", &dnsapi.ResponsePolicyRule{
		DnsName: "other.example.com.",
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "other.example.com.", pr.ResponsePolicyRule.DnsName)
	assert.Equal(t, "bypassResponsePolicy", pr.ResponsePolicyRule.Behavior)

	ur, err := e.svc.ResponsePolicyRules.Update(project, "rp", "r2", &dnsapi.ResponsePolicyRule{
		DnsName: "u.example.com.", Behavior: "bypassResponsePolicy",
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "u.example.com.", ur.ResponsePolicyRule.DnsName)

	rules, err := e.svc.ResponsePolicyRules.List(project, "rp").Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, rules.ResponsePolicyRules, 2)
	assert.Equal(t, "r1", rules.ResponsePolicyRules[0].RuleName)

	assertAPIError(t, e.svc.ResponsePolicies.Delete(project, "rp").Context(t.Context()).Do(), 400, "containerNotEmpty")

	require.NoError(t, e.svc.ResponsePolicyRules.Delete(project, "rp", "r1").Context(t.Context()).Do())
	require.NoError(t, e.svc.ResponsePolicyRules.Delete(project, "rp", "r2").Context(t.Context()).Do())
	assertAPIError(t, e.svc.ResponsePolicyRules.Delete(project, "rp", "r2").Context(t.Context()).Do(), 404, "notFound")

	_, err = e.svc.ResponsePolicies.Patch(project, "rp", &dnsapi.ResponsePolicy{Networks: nil}).
		Context(t.Context()).
		Do()
	require.NoError(t, err)

	require.NoError(t, e.svc.ResponsePolicies.Delete(project, "rp").Context(t.Context()).Do())

	list, err := e.svc.ResponsePolicies.List(project).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Empty(t, list.ResponsePolicies)
}

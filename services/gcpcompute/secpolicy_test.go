package gcpcompute_test

import (
	"net/http"
	"strings"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const defaultPrio = int32(2147483647)

func (e *env) armor(t *testing.T) *compute.SecurityPoliciesClient {
	t.Helper()

	return client(t, e, compute.NewSecurityPoliciesRESTClient)
}

func (e *env) policy(t *testing.T, p *computepb.SecurityPolicy) error {
	t.Helper()

	op, err := e.armor(t).
		Insert(t.Context(), &computepb.InsertSecurityPolicyRequest{Project: project, SecurityPolicyResource: p})

	return e.finish(t, op, err)
}

func (e *env) getPolicy(t *testing.T) *computepb.SecurityPolicy {
	t.Helper()

	p, err := e.armor(t).Get(t.Context(), &computepb.GetSecurityPolicyRequest{Project: project, SecurityPolicy: "pol"})
	require.NoError(t, err)

	return p
}

func srcRule(prio int32, action string, ranges ...string) *computepb.SecurityPolicyRule {
	return &computepb.SecurityPolicyRule{
		Priority: new(prio), Action: s(action), Match: &computepb.SecurityPolicyRuleMatcher{
			VersionedExpr: s("SRC_IPS_V1"), Config: &computepb.SecurityPolicyRuleMatcherConfig{SrcIpRanges: ranges},
		},
	}
}

func (e *env) ruleOp(t *testing.T, prio int32, verb string, rule *computepb.SecurityPolicyRule) error {
	t.Helper()

	c := e.armor(t)

	var (
		op  *compute.Operation
		err error
	)

	switch verb {
	case "add":
		op, err = c.AddRule(t.Context(), &computepb.AddRuleSecurityPolicyRequest{
			Project: project, SecurityPolicy: "pol", SecurityPolicyRuleResource: rule,
		})
	case "patch":
		op, err = c.PatchRule(t.Context(), &computepb.PatchRuleSecurityPolicyRequest{
			Project: project, SecurityPolicy: "pol", Priority: new(prio), SecurityPolicyRuleResource: rule,
		})
	default:
		op, err = c.RemoveRule(t.Context(), &computepb.RemoveRuleSecurityPolicyRequest{
			Project: project, SecurityPolicy: "pol", Priority: new(prio),
		})
	}

	return e.finish(t, op, err)
}

func TestSecurityPolicyDefaults(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	op, err := e.armor(t).Insert(t.Context(), &computepb.InsertSecurityPolicyRequest{
		Project: project, SecurityPolicyResource: &computepb.SecurityPolicy{Name: s("pol"), Description: s("d")},
	})
	require.NoError(t, err)
	assert.Equal(t, "compute.securityPolicies.insert", op.Proto().GetOperationType())
	e.done(t, op, nil)

	p := e.getPolicy(t)
	assert.Equal(t, "CLOUD_ARMOR", p.GetType())
	assert.Equal(t, "compute#securityPolicy", p.GetKind())
	assert.Contains(t, p.GetSelfLink(), "/global/securityPolicies/pol")
	assert.NotEmpty(t, p.GetFingerprint())
	require.Len(t, p.GetRules(), 1)

	r := p.GetRules()[0]
	assert.Equal(t, defaultPrio, r.GetPriority())
	assert.Equal(t, "allow", r.GetAction())
	assert.Equal(t, "SRC_IPS_V1", r.GetMatch().GetVersionedExpr())
	assert.Equal(t, []string{"*"}, r.GetMatch().GetConfig().GetSrcIpRanges())
	assert.Equal(t, "Default rule, higher priority overrides it", r.GetDescription())
	assert.Equal(t, "compute#securityPolicyRule", r.GetKind())
	assert.False(t, r.GetPreview())

	assertAPIError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("pol")}), http.StatusConflict, "alreadyExists")

	op, err = e.armor(t).
		Delete(t.Context(), &computepb.DeleteSecurityPolicyRequest{Project: project, SecurityPolicy: "pol"})
	e.done(t, op, err)

	_, err = e.armor(t).Get(t.Context(), &computepb.GetSecurityPolicyRequest{Project: project, SecurityPolicy: "pol"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestSecurityPolicyInsertValidation(t *testing.T) {
	t.Parallel()

	deflt := srcRule(defaultPrio, "allow", "*")
	tooMany := make([]string, 11)

	for i := range tooMany {
		tooMany[i] = "10.0.0.1"
	}

	tests := []struct {
		pol   *computepb.SecurityPolicy
		name  string
		valid bool
	}{
		{name: "valid rules", valid: true, pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			srcRule(100, "deny(403)", "10.0.0.0/8", "192.0.2.1", "2001:db8::/32"), deflt,
		}}},
		{name: "cel expr", valid: true, pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			{Priority: new(int32(5)), Action: s("deny(404)"), Match: &computepb.SecurityPolicyRuleMatcher{
				Expr: &computepb.Expr{Expression: s("origin.region_code == 'US' && !(request.path.matches('/x'))")},
			}}, deflt,
		}}},
		{name: "rate limit", valid: true, pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			{
				Priority: new(int32(7)), Action: s("throttle"),
				Match: srcRule(7, "x", "*").GetMatch(),
				RateLimitOptions: &computepb.SecurityPolicyRuleRateLimitOptions{
					RateLimitThreshold: &computepb.SecurityPolicyRuleRateLimitOptionsThreshold{
						Count: proto.Int32(10), IntervalSec: proto.Int32(60),
					},
					ExceedAction: s("deny(429)"), EnforceOnKey: s("IP"),
				},
			}, deflt,
		}}},
		{
			name: "missing default",
			pol:  &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{srcRule(1, "allow", "*")}},
		},
		{name: "duplicate priority", pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			srcRule(1, "allow", "*"), srcRule(1, "allow", "*"), deflt,
		}}},
		{
			name: "bad action",
			pol: &computepb.SecurityPolicy{
				Rules: []*computepb.SecurityPolicyRule{srcRule(1, "deny(500)", "*"), deflt},
			},
		},
		{
			name: "bad range",
			pol:  &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{srcRule(1, "allow", "nope"), deflt}},
		},
		{
			name: "too many ranges",
			pol: &computepb.SecurityPolicy{
				Rules: []*computepb.SecurityPolicyRule{srcRule(1, "allow", tooMany...), deflt},
			},
		},
		{name: "unknown versioned expr", pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			{Priority: new(int32(1)), Action: s("allow"), Match: &computepb.SecurityPolicyRuleMatcher{
				VersionedExpr: s(
					"SRC_IPS_V2",
				), Config: &computepb.SecurityPolicyRuleMatcherConfig{SrcIpRanges: []string{"*"}},
			}}, deflt,
		}}},
		{name: "expr and versioned", pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			{Priority: new(int32(1)), Action: s("allow"), Match: &computepb.SecurityPolicyRuleMatcher{
				VersionedExpr: s("SRC_IPS_V1"), Expr: &computepb.Expr{Expression: s("true")},
				Config: &computepb.SecurityPolicyRuleMatcherConfig{SrcIpRanges: []string{"*"}},
			}}, deflt,
		}}},
		{name: "unbalanced expr", pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			{Priority: new(int32(1)), Action: s("allow"), Match: &computepb.SecurityPolicyRuleMatcher{
				Expr: &computepb.Expr{Expression: s("origin.ip == '1.2.3.4' && (a")},
			}}, deflt,
		}}},
		{name: "missing match", pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			{Priority: new(int32(1)), Action: s("allow")}, deflt,
		}}},
		{name: "throttle without options", pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			srcRule(1, "throttle", "*"), deflt,
		}}},
		{name: "options without throttle", pol: &computepb.SecurityPolicy{Rules: []*computepb.SecurityPolicyRule{
			{
				Priority: new(int32(1)), Action: s("allow"), Match: srcRule(1, "x", "*").GetMatch(),
				RateLimitOptions: &computepb.SecurityPolicyRuleRateLimitOptions{},
			}, deflt,
		}}},
		{
			name: "default not match all",
			pol: &computepb.SecurityPolicy{
				Rules: []*computepb.SecurityPolicyRule{srcRule(defaultPrio, "allow", "10.0.0.0/8")},
			},
		},
		{name: "bad type", pol: &computepb.SecurityPolicy{Type: s("CLOUD_ARMOR_INTERNAL_SERVICE")}},
		{
			name: "bad json parsing",
			pol: &computepb.SecurityPolicy{
				AdvancedOptionsConfig: &computepb.SecurityPolicyAdvancedOptionsConfig{JsonParsing: s("BOGUS")},
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tt.pol.Name = s("p" + strings.Repeat("x", i))
			err := e.policy(t, tt.pol)

			if tt.valid {
				require.NoError(t, err)

				return
			}

			assertAPIError(t, err, http.StatusBadRequest, "")
		})
	}
}

func TestSecurityPolicyRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("pol")}))

	fp := e.getPolicy(t).GetFingerprint()

	require.NoError(t, e.ruleOp(t, 0, "add", srcRule(1000, "deny(403)", "198.51.100.0/24")))
	require.NoError(t, e.ruleOp(t, 0, "add", srcRule(0, "allow", "203.0.113.7")))

	assertAPIError(t, e.ruleOp(t, 0, "add", srcRule(1000, "allow", "*")), http.StatusBadRequest, "invalid")
	assertAPIError(t, e.ruleOp(t, 0, "add", srcRule(defaultPrio, "deny(403)", "*")), http.StatusBadRequest, "invalid")
	assertAPIError(t, e.ruleOp(t, 0, "add", srcRule(-5, "allow", "*")), http.StatusBadRequest, "invalid")

	p := e.getPolicy(t)
	assert.NotEqual(t, fp, p.GetFingerprint())
	require.Len(t, p.GetRules(), 3)
	assert.Equal(
		t,
		[]int32{0, 1000, defaultPrio},
		[]int32{p.GetRules()[0].GetPriority(), p.GetRules()[1].GetPriority(), p.GetRules()[2].GetPriority()},
	)

	got, err := e.armor(t).GetRule(t.Context(), &computepb.GetRuleSecurityPolicyRequest{
		Project: project, SecurityPolicy: "pol", Priority: new(int32(1000)),
	})
	require.NoError(t, err)
	assert.Equal(t, "deny(403)", got.GetAction())
	assert.Equal(t, []string{"198.51.100.0/24"}, got.GetMatch().GetConfig().GetSrcIpRanges())

	_, err = e.armor(t).GetRule(t.Context(), &computepb.GetRuleSecurityPolicyRequest{
		Project: project, SecurityPolicy: "pol", Priority: new(int32(77)),
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	patch := &computepb.SecurityPolicyRule{Action: s("deny(404)"), Description: s("patched"), Preview: new(true)}
	require.NoError(t, e.ruleOp(t, 1000, "patch", patch))

	got, err = e.armor(t).GetRule(t.Context(), &computepb.GetRuleSecurityPolicyRequest{
		Project: project, SecurityPolicy: "pol", Priority: new(int32(1000)),
	})
	require.NoError(t, err)
	assert.Equal(t, "deny(404)", got.GetAction())
	assert.Equal(t, "patched", got.GetDescription())
	assert.True(t, got.GetPreview())
	assert.Equal(
		t,
		[]string{"198.51.100.0/24"},
		got.GetMatch().GetConfig().GetSrcIpRanges(),
		"unspecified fields are kept",
	)

	assertAPIError(
		t,
		e.ruleOp(t, 1000, "patch", &computepb.SecurityPolicyRule{Action: s("bogus")}),
		http.StatusBadRequest,
		"invalid",
	)
	assertAPIError(t, e.ruleOp(t, 55, "patch", patch), http.StatusNotFound, "notFound")
	assertAPIError(t, e.ruleOp(t, defaultPrio, "remove", nil), http.StatusBadRequest, "invalid")
	assertAPIError(t, e.ruleOp(t, 55, "remove", nil), http.StatusNotFound, "notFound")
	require.NoError(t, e.ruleOp(t, 1000, "remove", nil))
	assert.Len(t, e.getPolicy(t).GetRules(), 2)
}

func TestSecurityPolicyPatchAndLabels(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("pol"), Type: s("CLOUD_ARMOR_EDGE")}))

	p := e.getPolicy(t)
	assert.Equal(t, "CLOUD_ARMOR_EDGE", p.GetType())

	patch := func(body *computepb.SecurityPolicy) error {
		op, err := e.armor(t).Patch(t.Context(), &computepb.PatchSecurityPolicyRequest{
			Project: project, SecurityPolicy: "pol", SecurityPolicyResource: body,
		})

		return e.finish(t, op, err)
	}

	require.NoError(t, patch(&computepb.SecurityPolicy{
		Description: s("new"),
		AdvancedOptionsConfig: &computepb.SecurityPolicyAdvancedOptionsConfig{
			JsonParsing: s("STANDARD"),
			LogLevel:    s("VERBOSE"),
		},
		Type:  s("CLOUD_ARMOR"),
		Rules: []*computepb.SecurityPolicyRule{srcRule(1, "deny(403)", "*")},
	}))

	after := e.getPolicy(t)
	assert.Equal(t, "new", after.GetDescription())
	assert.Equal(t, "VERBOSE", after.GetAdvancedOptionsConfig().GetLogLevel())
	assert.Equal(t, "CLOUD_ARMOR_EDGE", after.GetType(), "type is fixed at creation")
	assert.Len(t, after.GetRules(), 1, "patch does not touch rules")
	assert.NotEqual(t, p.GetFingerprint(), after.GetFingerprint())

	assertAPIError(
		t,
		patch(&computepb.SecurityPolicy{Fingerprint: s("stale")}),
		http.StatusPreconditionFailed,
		"conditionNotMet",
	)
	assertAPIError(
		t,
		patch(
			&computepb.SecurityPolicy{
				AdvancedOptionsConfig: &computepb.SecurityPolicyAdvancedOptionsConfig{LogLevel: s("LOUD")},
			},
		),
		http.StatusBadRequest,
		"invalid",
	)

	setLabels := func(fp string, labels map[string]string) error {
		op, err := e.armor(t).SetLabels(t.Context(), &computepb.SetLabelsSecurityPolicyRequest{
			Project: project, Resource: "pol",
			GlobalSetLabelsRequestResource: &computepb.GlobalSetLabelsRequest{Labels: labels, LabelFingerprint: s(fp)},
		})

		return e.finish(t, op, err)
	}

	require.NoError(t, setLabels(after.GetLabelFingerprint(), map[string]string{"team": "net"}))
	assert.Equal(t, map[string]string{"team": "net"}, e.getPolicy(t).GetLabels())
	assertAPIError(
		t,
		setLabels(after.GetLabelFingerprint(), map[string]string{"x": "y"}),
		http.StatusPreconditionFailed,
		"conditionNotMet",
	)
}

func TestSecurityPolicyBackendServiceAttachment(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.lbStack(t)
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("pol")}))
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("edge"), Type: s("CLOUD_ARMOR_EDGE")}))

	bs := e.backendServices(t)
	setPolicy := func(ref string) error {
		op, err := bs.SetSecurityPolicy(t.Context(), &computepb.SetSecurityPolicyBackendServiceRequest{
			Project: project, BackendService: "bs",
			SecurityPolicyReferenceResource: &computepb.SecurityPolicyReference{SecurityPolicy: s(ref)},
		})

		return e.finish(t, op, err)
	}

	assertAPIError(t, setPolicy("global/securityPolicies/ghost"), http.StatusNotFound, "notFound")
	assertAPIError(t, setPolicy("global/securityPolicies/edge"), http.StatusBadRequest, "invalid")
	require.NoError(t, setPolicy("global/securityPolicies/pol"))

	got, err := bs.Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
	require.NoError(t, err)
	assert.Contains(t, got.GetSecurityPolicy(), "/global/securityPolicies/pol")

	_, err = e.armor(t).
		Delete(t.Context(), &computepb.DeleteSecurityPolicyRequest{Project: project, SecurityPolicy: "pol"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	require.NoError(t, setPolicy(""))

	got, err = bs.Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
	require.NoError(t, err)
	assert.Empty(t, got.GetSecurityPolicy())

	op, err := e.armor(t).
		Delete(t.Context(), &computepb.DeleteSecurityPolicyRequest{Project: project, SecurityPolicy: "pol"})
	e.done(t, op, err)

	t.Run("insert time attachment", func(t *testing.T) {
		t.Parallel()

		ee := newEnv(t)
		ee.lbHC(t)
		require.NoError(t, ee.policy(t, &computepb.SecurityPolicy{Name: s("pol")}))
		require.NoError(
			t,
			ee.lbBS(t, &computepb.BackendService{Name: s("with"), SecurityPolicy: s("global/securityPolicies/pol")}),
		)
		assertAPIError(
			t,
			ee.lbBS(t, &computepb.BackendService{Name: s("bad"), SecurityPolicy: s("global/securityPolicies/ghost")}),
			http.StatusNotFound,
			"notFound",
		)
	})
}

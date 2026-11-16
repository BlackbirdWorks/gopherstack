package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDrgNatPolicyRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	pol, err := e.client.CreateDrgNatPolicy(ctx, core.CreateDrgNatPolicyRequest{
		CompartmentId: new(testComp),
		DisplayName:   new("nat"),
	})
	require.NoError(t, err)
	assert.Equal(t, core.DrgNatPolicyLifecycleStateCreating, pol.LifecycleState)
	e.settle()

	rule := func(prio int64, os, ts, od, td string) core.AddDrgNatRuleDetails {
		d := core.AddDrgNatRuleDetails{DrgNatRulePriority: new(prio)}
		if os != "" {
			d.OriginalSource, d.TranslatedSource = new(os), new(ts)
		}

		if od != "" {
			d.OriginalDestination, d.TranslatedDestination = new(od), new(td)
		}

		return d
	}

	tests := []struct {
		name     string
		rules    []core.AddDrgNatRuleDetails
		wantCode int
	}{
		{"snat and dnat", []core.AddDrgNatRuleDetails{
			rule(10, "10.0.1.0/24", "192.168.20.0/24", "192.168.10.0/24", "10.0.1.0/24"),
		}, 0},
		{
			"half pair",
			[]core.AddDrgNatRuleDetails{{DrgNatRulePriority: new(int64(1)), OriginalSource: new("10.0.0.0/24")}},
			http.StatusBadRequest,
		},
		{"no translation", []core.AddDrgNatRuleDetails{{DrgNatRulePriority: new(int64(1))}}, http.StatusBadRequest},
		{
			"ipv6",
			[]core.AddDrgNatRuleDetails{rule(1, "2001:db8::/64", "2001:db8:1::/64", "", "")},
			http.StatusBadRequest,
		},
		{"dup priority", []core.AddDrgNatRuleDetails{
			rule(5, "10.1.0.0/24", "10.2.0.0/24", "", ""), rule(5, "10.3.0.0/24", "10.4.0.0/24", "", ""),
		}, http.StatusConflict},
	}

	for _, tc := range tests {
		// sequential: every case works on the same policy and a rejected request must leave it unchanged
		_, aerr := e.client.AddDrgNatRules(ctx, core.AddDrgNatRulesRequest{DrgNatPolicyId: pol.Id, Rules: tc.rules})
		if tc.wantCode != 0 {
			assert.Equal(t, tc.wantCode, svcErr(t, aerr).GetHTTPStatusCode(), tc.name)

			continue
		}

		require.NoError(t, aerr, tc.name)
		e.settle()
	}

	l, err := e.client.ListDrgNatRules(ctx, core.ListDrgNatRulesRequest{DrgNatPolicyId: pol.Id})
	require.NoError(t, err)
	require.Len(t, l.Items, 1, "rejected requests change nothing")
	assert.EqualValues(t, 10, *l.Items[0].DrgNatRulePriority)
	assert.Equal(t, *pol.Id, *l.Items[0].DrgNatPolicyId)

	up, err := e.client.UpdateDrgNatRules(ctx, core.UpdateDrgNatRulesRequest{
		DrgNatPolicyId: pol.Id,
		Rules: []core.UpdateDrgNatRuleDetails{{
			Id: l.Items[0].Id, DrgNatRulePriority: new(int64(3)),
		}},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *up.OpcWorkRequestId)
	e.settle()

	l, err = e.client.ListDrgNatRules(ctx, core.ListDrgNatRulesRequest{DrgNatPolicyId: pol.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 3, *l.Items[0].DrgNatRulePriority)

	_, err = e.client.RemoveDrgNatRules(ctx, core.RemoveDrgNatRulesRequest{
		DrgNatPolicyId: pol.Id,
		RuleIds:        []string{"ocid1.drgnatrule.oc1.iad.nope"},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.RemoveDrgNatRules(ctx, core.RemoveDrgNatRulesRequest{
		DrgNatPolicyId: pol.Id,
		RuleIds:        []string{*l.Items[0].Id},
	})
	require.NoError(t, err)
}

func TestDrgNatPolicyAttachment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	d := e.mkDrg(t, "natdrg")
	v := e.mkVcn(t, "10.0.0.0/16", "natvcn")

	pol, err := e.client.CreateDrgNatPolicy(ctx, core.CreateDrgNatPolicyRequest{
		CompartmentId: new(testComp),
	})
	require.NoError(t, err)

	_, err = e.client.CreateDrgAttachment(
		ctx,
		core.CreateDrgAttachmentRequest{DrgId: d.Id, VcnId: v.Id, DrgNatPolicyId: pol.Id},
	)
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "the policy is still being created")

	e.settle()

	att, err := e.client.CreateDrgAttachment(
		ctx,
		core.CreateDrgAttachmentRequest{
			DrgId:                             d.Id,
			VcnId:                             v.Id,
			DrgNatPolicyId:                    pol.Id,
			DoesPreserveOriginalRoutesWithNat: new(true),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, *pol.Id, *att.DrgNatPolicyId)
	assert.True(t, *att.DoesPreserveOriginalRoutesWithNat)

	_, err = e.client.DeleteDrgNatPolicy(ctx, core.DeleteDrgNatPolicyRequest{DrgNatPolicyId: pol.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "an attachment uses the policy")

	rm, err := e.client.RemoveDrgNatPolicy(ctx, core.RemoveDrgNatPolicyRequest{DrgAttachmentId: att.Id})
	require.NoError(t, err)
	assert.Nil(t, rm.DrgNatPolicyId)

	_, err = e.client.UpdateDrgAttachment(ctx, core.UpdateDrgAttachmentRequest{
		DrgAttachmentId:                   att.Id,
		DoesPreserveOriginalRoutesWithNat: new(true),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "the flag needs a policy")

	dl, err := e.client.DeleteDrgNatPolicy(ctx, core.DeleteDrgNatPolicyRequest{DrgNatPolicyId: pol.Id})
	require.NoError(t, err)
	assert.NotEmpty(t, *dl.OpcWorkRequestId)
}

func TestDrgNatPolicyLimits(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	for range 8 {
		_, err := e.client.CreateDrgNatPolicy(ctx, core.CreateDrgNatPolicyRequest{
			CompartmentId: new(testComp),
		})
		require.NoError(t, err)
	}

	_, err := e.client.CreateDrgNatPolicy(ctx, core.CreateDrgNatPolicyRequest{
		CompartmentId: new(testComp),
	})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())
}

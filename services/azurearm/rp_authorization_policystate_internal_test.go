package azurearm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicyComplianceStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		effect        string
		location      string
		wantCompliant bool
		wantDeny      bool
		wantState     bool
	}{
		{name: "audit noncompliant", effect: "audit", location: "eastus", wantState: true},
		{name: "audit compliant", effect: "audit", location: "westus", wantCompliant: true, wantState: true},
		{name: "deny", effect: "deny", location: "eastus", wantDeny: true, wantState: true},
		{name: "disabled leaves no state", effect: "disabled", location: "eastus"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := NewAuthorizationProvider("", nil)
			p.policyAssigns["a"] = &storedPolicyAssign{Name: "a1", Scope: limitsSub, Props: map[string]any{
				authFieldPolicyDefinitionID: limitsSub + "/providers/Microsoft.Authorization/policyDefinitions/d1",
			}}
			p.policyDefs[authKey(limitsSub, "d1")] = &storedPolicy{Name: "d1", Scope: limitsSub, Props: map[string]any{
				"policyRule": map[string]any{
					"if":   map[string]any{"field": "location", "equals": "eastus"},
					"then": map[string]any{"effect": tc.effect},
				},
			}}

			out := p.EvaluatePolicies(policyResource{
				id: limitsSub + "/resourceGroups/r", typ: policyResourceGroupType, name: "r", location: tc.location,
				body: map[string]any{"location": tc.location},
			})

			assert.Equal(t, tc.wantDeny, out.Deny != nil)

			if !tc.wantState {
				assert.Empty(t, p.policyStates)

				return
			}

			require.Len(t, p.policyStates, 1)

			for _, st := range p.policyStates {
				assert.Equal(t, tc.wantCompliant, st.Compliant)
				assert.Equal(t, tc.effect, st.Effect)
			}
		})
	}
}

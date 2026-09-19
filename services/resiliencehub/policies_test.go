package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/stretchr/testify/require"
)

// TestCreateResiliencyPolicy_RequiresThreeDisruptionTypes verifies
// validatePolicyMap's rule: a policy must carry a FailurePolicy entry for
// every required DisruptionType (Software/Hardware/AZ). Region is optional --
// the hashicorp/aws provider's aws_resiliencehub_resiliency_policy resource
// marks policy.region as Optional, so a real client's request can omit it.
func TestCreateResiliencyPolicy_RequiresThreeDisruptionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy map[string]types.FailurePolicy
		name   string
	}{
		{name: "empty policy", policy: map[string]types.FailurePolicy{}},
		{name: "missing AZ", policy: map[string]types.FailurePolicy{
			"Software": {RtoInSecs: 60, RpoInSecs: 60},
			"Hardware": {RtoInSecs: 60, RpoInSecs: 60},
			"Region":   {RtoInSecs: 60, RpoInSecs: 60},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			_, err := client.CreateResiliencyPolicy(t.Context(), &resiliencehubsdk.CreateResiliencyPolicyInput{
				PolicyName: aws.String("p"), Tier: types.ResiliencyPolicyTierCritical, Policy: tt.policy,
			})
			require.Error(t, err)

			var validationErr *types.ValidationException
			require.ErrorAs(t, err, &validationErr)
		})
	}
}

// TestCreateResiliencyPolicy_DisruptionTypesSucceeds verifies both a policy
// omitting the optional Region entry and one carrying all four disruption
// types are accepted.
func TestCreateResiliencyPolicy_DisruptionTypesSucceeds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy    map[string]types.FailurePolicy
		name      string
		wantCount int
	}{
		{
			name: "without Region",
			policy: map[string]types.FailurePolicy{
				"Software": {RtoInSecs: 60, RpoInSecs: 60},
				"Hardware": {RtoInSecs: 60, RpoInSecs: 60},
				"AZ":       {RtoInSecs: 60, RpoInSecs: 60},
			},
			wantCount: 3,
		},
		{
			name: "with Region",
			policy: map[string]types.FailurePolicy{
				"Software": {RtoInSecs: 60, RpoInSecs: 60},
				"Hardware": {RtoInSecs: 60, RpoInSecs: 60},
				"AZ":       {RtoInSecs: 60, RpoInSecs: 60},
				"Region":   {RtoInSecs: 60, RpoInSecs: 60},
			},
			wantCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			out, err := client.CreateResiliencyPolicy(t.Context(), &resiliencehubsdk.CreateResiliencyPolicyInput{
				PolicyName: aws.String("p"), Tier: types.ResiliencyPolicyTierCritical, Policy: tt.policy,
			})
			require.NoError(t, err)
			require.Len(t, out.Policy.Policy, tt.wantCount)
		})
	}
}

// TestListSuggestedResiliencyPolicies_IsAStaticStandIn verifies the
// documented stand-in table returns one entry per tier, distinct from
// (never mutated by) the real ListResiliencyPolicies table.
func TestListSuggestedResiliencyPolicies_IsAStaticStandIn(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	suggested, err := client.ListSuggestedResiliencyPolicies(
		ctx,
		&resiliencehubsdk.ListSuggestedResiliencyPoliciesInput{},
	)
	require.NoError(t, err)
	require.Len(t, suggested.ResiliencyPolicies, 5)

	genuine, err := client.ListResiliencyPolicies(ctx, &resiliencehubsdk.ListResiliencyPoliciesInput{})
	require.NoError(t, err)
	require.Empty(t, genuine.ResiliencyPolicies)
}

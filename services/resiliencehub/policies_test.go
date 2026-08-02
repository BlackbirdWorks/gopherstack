package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/stretchr/testify/require"
)

// TestCreateResiliencyPolicy_RequiresAllFourDisruptionTypes verifies
// validatePolicyMap's documented rule: a policy must carry a FailurePolicy
// entry for every DisruptionType (Software/Hardware/AZ/Region).
func TestCreateResiliencyPolicy_RequiresAllFourDisruptionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy map[string]types.FailurePolicy
		name   string
	}{
		{name: "empty policy", policy: map[string]types.FailurePolicy{}},
		{name: "missing Region", policy: map[string]types.FailurePolicy{
			"Software": {RtoInSecs: 60, RpoInSecs: 60},
			"Hardware": {RtoInSecs: 60, RpoInSecs: 60},
			"AZ":       {RtoInSecs: 60, RpoInSecs: 60},
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

// TestCreateResiliencyPolicy_AllFourDisruptionTypesSucceeds is the positive
// counterpart: a policy carrying all four disruption types is accepted.
func TestCreateResiliencyPolicy_AllFourDisruptionTypesSucceeds(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	out, err := client.CreateResiliencyPolicy(t.Context(), &resiliencehubsdk.CreateResiliencyPolicyInput{
		PolicyName: aws.String("p"), Tier: types.ResiliencyPolicyTierCritical,
		Policy: map[string]types.FailurePolicy{
			"Software": {RtoInSecs: 60, RpoInSecs: 60},
			"Hardware": {RtoInSecs: 60, RpoInSecs: 60},
			"AZ":       {RtoInSecs: 60, RpoInSecs: 60},
			"Region":   {RtoInSecs: 60, RpoInSecs: 60},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Policy.Policy, 4)
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

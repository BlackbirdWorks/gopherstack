package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrivacyBudgetTemplate_PartialUpdateKeepsUnsetParameters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update    crtypes.DifferentialPrivacyTemplateUpdateParameters
		name      string
		wantEps   int32
		wantNoise int32
	}{
		{
			name:      "epsilon only keeps noise",
			update:    crtypes.DifferentialPrivacyTemplateUpdateParameters{Epsilon: aws.Int32(12)},
			wantEps:   12,
			wantNoise: 100,
		},
		{
			name:      "noise only keeps epsilon",
			update:    crtypes.DifferentialPrivacyTemplateUpdateParameters{UsersNoisePerQuery: aws.Int32(7)},
			wantEps:   10,
			wantNoise: 7,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			_, memID := createCollaborationAndMembership(t, client)
			ctx := t.Context()

			created, err := client.CreatePrivacyBudgetTemplate(ctx, &cleanroomssdk.CreatePrivacyBudgetTemplateInput{
				MembershipIdentifier: aws.String(memID),
				PrivacyBudgetType:    crtypes.PrivacyBudgetTypeDifferentialPrivacy,
				AutoRefresh:          crtypes.PrivacyBudgetTemplateAutoRefreshCalendarMonth,
				Parameters: &crtypes.PrivacyBudgetTemplateParametersInputMemberDifferentialPrivacy{
					Value: crtypes.DifferentialPrivacyTemplateParametersInput{
						Epsilon: aws.Int32(10), UsersNoisePerQuery: aws.Int32(100),
					},
				},
			})
			require.NoError(t, err)

			tmplID := created.PrivacyBudgetTemplate.Id
			upd := &crtypes.PrivacyBudgetTemplateUpdateParametersMemberDifferentialPrivacy{Value: tc.update}

			_, err = client.UpdatePrivacyBudgetTemplate(ctx, &cleanroomssdk.UpdatePrivacyBudgetTemplateInput{
				MembershipIdentifier:            aws.String(memID),
				PrivacyBudgetTemplateIdentifier: tmplID,
				PrivacyBudgetType:               crtypes.PrivacyBudgetTypeDifferentialPrivacy,
				Parameters:                      upd,
			})
			require.NoError(t, err)

			got, err := client.GetPrivacyBudgetTemplate(ctx, &cleanroomssdk.GetPrivacyBudgetTemplateInput{
				MembershipIdentifier:            aws.String(memID),
				PrivacyBudgetTemplateIdentifier: tmplID,
			})
			require.NoError(t, err)

			tmpl := got.PrivacyBudgetTemplate
			dp, ok := tmpl.Parameters.(*crtypes.PrivacyBudgetTemplateParametersOutputMemberDifferentialPrivacy)
			require.True(t, ok)
			assert.Equal(t, tc.wantEps, aws.ToInt32(dp.Value.Epsilon))
			assert.Equal(t, tc.wantNoise, aws.ToInt32(dp.Value.UsersNoisePerQuery))
			assert.Equal(t, crtypes.PrivacyBudgetTemplateAutoRefreshCalendarMonth, tmpl.AutoRefresh)
		})
	}
}

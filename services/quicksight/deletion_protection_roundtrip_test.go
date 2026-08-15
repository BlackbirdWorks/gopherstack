package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestDeleteAccountSubscription_TerminationProtectionRoundTrip proves
// UpdateAccountSettings' TerminationProtectionEnabled has an effect on
// DeleteAccountSubscription, not just on what DescribeAccountSettings echoes back.
// DeleteAccountSubscription's own client doc says "This operation will result in an
// error message if you have configured your account termination protection settings
// to True", and its deserializer models PreconditionNotMetException as a typed error
// for this op -- before the fix, gopherstack stored the setting and never read it back
// anywhere, so DeleteAccountSubscription always succeeded regardless.
func TestDeleteAccountSubscription_TerminationProtectionRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		protected bool
		wantErr   bool
	}{
		{"protected blocks delete", true, true},
		{"unprotected allows delete", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := quicksight.NewInMemoryBackend("000000000000", rtQSTestRegion)
			h := quicksight.NewHandler(backend)
			client := newTestQuickSightClient(t, h)
			ctx := t.Context()

			_, err := client.CreateAccountSubscription(ctx, &quicksightsdk.CreateAccountSubscriptionInput{
				AwsAccountId:         aws.String("000000000000"),
				AccountName:          aws.String("dp-rt-" + tt.name),
				Edition:              types.EditionEnterprise,
				AuthenticationMethod: types.AuthenticationMethodOptionIamAndQuicksight,
				NotificationEmail:    aws.String("dp-rt@example.com"),
			})
			require.NoError(t, err)

			_, err = client.UpdateAccountSettings(ctx, &quicksightsdk.UpdateAccountSettingsInput{
				AwsAccountId:                 aws.String("000000000000"),
				DefaultNamespace:             aws.String("default"),
				TerminationProtectionEnabled: tt.protected,
			})
			require.NoError(t, err)

			_, err = client.DeleteAccountSubscription(ctx, &quicksightsdk.DeleteAccountSubscriptionInput{
				AwsAccountId: aws.String("000000000000"),
			})

			if tt.wantErr {
				require.Error(t, err)

				var preconditionNotMet *types.PreconditionNotMetException
				require.ErrorAs(t, err, &preconditionNotMet,
					"expected a typed PreconditionNotMetException, got %v", err)

				return
			}

			require.NoError(t, err)
		})
	}
}

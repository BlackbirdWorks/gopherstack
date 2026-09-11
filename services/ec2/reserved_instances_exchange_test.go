package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestGetReservedInstancesExchangeQuote covers gopherstack-1qth: the quote
// must reflect real backend state (only Convertible RIs are exchangeable,
// unknown IDs error) instead of always returning IsValidExchange=true with
// no computed values.
func TestGetReservedInstancesExchangeQuote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *ec2.InMemoryBackend) ([]string, []ec2.TargetConfigurationRequest)
		check func(t *testing.T, quote *ec2.ReservedInstancesExchangeQuote, err error)
		name  string
	}{
		{
			name: "valid_convertible_exchange",
			setup: func(t *testing.T, b *ec2.InMemoryBackend) ([]string, []ec2.TargetConfigurationRequest) {
				t.Helper()

				b.SeedReservedInstancesOffering(
					"rio-valid-src", "t3.micro", "us-east-1a", "Linux/UNIX", "All Upfront", "convertible",
					31536000, 300.0, 0.02,
				)
				ri, err := b.PurchaseReservedInstancesOffering("rio-valid-src", 1)
				require.NoError(t, err)

				b.SeedReservedInstancesOffering(
					"rio-valid-tgt", "t3.small", "us-east-1a", "Linux/UNIX", "All Upfront", "convertible",
					31536000, 400.0, 0.03,
				)

				return []string{ri.ReservedInstancesID},
					[]ec2.TargetConfigurationRequest{{OfferingID: "rio-valid-tgt", InstanceCount: 1}}
			},
			check: func(t *testing.T, quote *ec2.ReservedInstancesExchangeQuote, err error) {
				t.Helper()
				require.NoError(t, err)
				require.NotNil(t, quote)
				assert.True(t, quote.IsValidExchange)
				assert.Empty(t, quote.ValidationFailureReason)
				assert.Equal(t, "USD", quote.CurrencyCode)

				require.Len(t, quote.ReservedInstanceValueSet, 1)
				// FixedPrice's remaining fraction is computed against elapsed
				// wall-clock time since purchase, which is sub-millisecond here
				// against a 1-year term -- InDelta absorbs that noise.
				assert.InDelta(t, 300.0, quote.ReservedInstanceValueSet[0].Value.RemainingUpfrontValue, 0.5)
				assert.InEpsilon(t, 0.02, quote.ReservedInstanceValueRollup.HourlyPrice, 1e-9)

				require.Len(t, quote.TargetConfigurationValueSet, 1)
				assert.Equal(t, "rio-valid-tgt", quote.TargetConfigurationValueSet[0].OfferingID)
				assert.InEpsilon(t, 400.0, quote.TargetConfigurationValueSet[0].Value.RemainingUpfrontValue, 1e-9)
				assert.InEpsilon(t, 0.03, quote.TargetConfigurationValueRollup.HourlyPrice, 1e-9)

				assert.False(t, quote.OutputReservedInstancesWillExpireAt.IsZero())
				assert.GreaterOrEqual(t, quote.PaymentDue, 0.0)
			},
		},
		{
			name: "standard_ri_rejected",
			setup: func(t *testing.T, b *ec2.InMemoryBackend) ([]string, []ec2.TargetConfigurationRequest) {
				t.Helper()

				b.SeedReservedInstancesOffering(
					"rio-standard-src", "t3.micro", "us-east-1a", "Linux/UNIX", "All Upfront", "standard",
					31536000, 300.0, 0.02,
				)
				ri, err := b.PurchaseReservedInstancesOffering("rio-standard-src", 1)
				require.NoError(t, err)

				return []string{ri.ReservedInstancesID}, nil
			},
			check: func(t *testing.T, quote *ec2.ReservedInstancesExchangeQuote, err error) {
				t.Helper()
				require.NoError(t, err)
				require.NotNil(t, quote)
				assert.False(t, quote.IsValidExchange)
				assert.NotEmpty(t, quote.ValidationFailureReason)
				assert.Empty(t, quote.ReservedInstanceValueSet)
				assert.Empty(t, quote.CurrencyCode)
			},
		},
		{
			name: "unknown_reserved_instance_id",
			setup: func(t *testing.T, _ *ec2.InMemoryBackend) ([]string, []ec2.TargetConfigurationRequest) {
				t.Helper()

				return []string{"r-doesnotexist"}, nil
			},
			check: func(t *testing.T, quote *ec2.ReservedInstancesExchangeQuote, err error) {
				t.Helper()
				require.Error(t, err)
				require.ErrorIs(t, err, ec2.ErrReservedInstancesNotFound)
				assert.Nil(t, quote)
			},
		},
		{
			name: "unknown_target_offering_id",
			setup: func(t *testing.T, b *ec2.InMemoryBackend) ([]string, []ec2.TargetConfigurationRequest) {
				t.Helper()

				b.SeedReservedInstancesOffering(
					"rio-unk-tgt-src", "t3.micro", "us-east-1a", "Linux/UNIX", "All Upfront", "convertible",
					31536000, 300.0, 0.02,
				)
				ri, err := b.PurchaseReservedInstancesOffering("rio-unk-tgt-src", 1)
				require.NoError(t, err)

				return []string{ri.ReservedInstancesID},
					[]ec2.TargetConfigurationRequest{{OfferingID: "rio-doesnotexist", InstanceCount: 1}}
			},
			check: func(t *testing.T, quote *ec2.ReservedInstancesExchangeQuote, err error) {
				t.Helper()
				require.Error(t, err)
				require.ErrorIs(t, err, ec2.ErrReservedInstancesNotFound)
				assert.Nil(t, quote)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			ids, targets := tt.setup(t, b)

			quote, err := b.GetReservedInstancesExchangeQuote(ids, targets)
			tt.check(t, quote, err)
		})
	}
}

// TestGetReservedInstancesExchangeQuote_RealClient drives
// GetReservedInstancesExchangeQuote through the real aws-sdk-go-v2 EC2
// client to prove the wire keys against ec2@v1.329.0's deserializer
// (deserializers.go:221192 awsEc2query_deserializeOpDocumentGetReservedInstancesExchangeQuoteOutput):
// currencyCode, isValidExchange, reservedInstanceValueSet>item>reservedInstanceId/
// reservationValue>hourlyPrice, targetConfigurationValueSet>item>targetConfiguration>offeringId.
func TestGetReservedInstancesExchangeQuote_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	b.SeedReservedInstancesOffering(
		"rio-quote-source", "t3.micro", "us-east-1a", "Linux/UNIX", "All Upfront", "convertible",
		31536000, 300.0, 0.02,
	)
	srcRI, err := b.PurchaseReservedInstancesOffering("rio-quote-source", 1)
	require.NoError(t, err)

	b.SeedReservedInstancesOffering(
		"rio-quote-target", "t3.small", "us-east-1a", "Linux/UNIX", "All Upfront", "convertible",
		31536000, 400.0, 0.03,
	)

	client := newTestEC2Client(t, h)

	out, err := client.GetReservedInstancesExchangeQuote(t.Context(), &ec2sdk.GetReservedInstancesExchangeQuoteInput{
		ReservedInstanceIds: []string{srcRI.ReservedInstancesID},
		TargetConfigurations: []types.TargetConfigurationRequest{
			{OfferingId: aws.String("rio-quote-target"), InstanceCount: aws.Int32(1)},
		},
	})
	require.NoError(t, err)

	require.NotNil(t, out.IsValidExchange)
	assert.True(t, *out.IsValidExchange)
	require.NotNil(t, out.CurrencyCode)
	assert.Equal(t, "USD", *out.CurrencyCode)

	require.Len(t, out.ReservedInstanceValueSet, 1)
	assert.Equal(t, srcRI.ReservedInstancesID, aws.ToString(out.ReservedInstanceValueSet[0].ReservedInstanceId))
	require.NotNil(t, out.ReservedInstanceValueSet[0].ReservationValue)
	assert.Equal(t, "0.02", aws.ToString(out.ReservedInstanceValueSet[0].ReservationValue.HourlyPrice))

	require.Len(t, out.TargetConfigurationValueSet, 1)
	require.NotNil(t, out.TargetConfigurationValueSet[0].TargetConfiguration)
	assert.Equal(t, "rio-quote-target", aws.ToString(out.TargetConfigurationValueSet[0].TargetConfiguration.OfferingId))
	require.NotNil(t, out.TargetConfigurationValueSet[0].ReservationValue)
	assert.Equal(t, "0.03", aws.ToString(out.TargetConfigurationValueSet[0].ReservationValue.HourlyPrice))

	require.NotNil(t, out.ReservedInstanceValueRollup)
	require.NotNil(t, out.TargetConfigurationValueRollup)
	require.NotNil(t, out.PaymentDue)
	require.NotNil(t, out.OutputReservedInstancesWillExpireAt)
	assert.False(t, out.OutputReservedInstancesWillExpireAt.IsZero())
}

// TestGetReservedInstancesExchangeQuote_StandardRi_RealClient proves a
// standard (non-convertible) RI comes back as IsValidExchange=false with a
// ValidationFailureReason on the wire, not an API error.
func TestGetReservedInstancesExchangeQuote_StandardRi_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	b.SeedReservedInstancesOffering(
		"rio-quote-standard", "t3.micro", "us-east-1a", "Linux/UNIX", "All Upfront", "standard",
		31536000, 300.0, 0.02,
	)
	ri, err := b.PurchaseReservedInstancesOffering("rio-quote-standard", 1)
	require.NoError(t, err)

	client := newTestEC2Client(t, h)

	out, err := client.GetReservedInstancesExchangeQuote(t.Context(), &ec2sdk.GetReservedInstancesExchangeQuoteInput{
		ReservedInstanceIds: []string{ri.ReservedInstancesID},
	})
	require.NoError(t, err)

	require.NotNil(t, out.IsValidExchange)
	assert.False(t, *out.IsValidExchange)
	assert.NotEmpty(t, aws.ToString(out.ValidationFailureReason))
	assert.Empty(t, out.ReservedInstanceValueSet)
}

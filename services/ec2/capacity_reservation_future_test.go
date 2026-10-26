package ec2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	futureDay      = 24 * time.Hour
	futureCommit   = int64(30 * 24 * 3600)
	futureInstance = "m5.xlarge"
)

func newFutureCRClient(t *testing.T) (*ec2sdk.Client, *fakeClock) {
	t.Helper()

	clk := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	backend.SetClock(clk.Now)

	return newTestEC2Client(t, ec2.NewHandler(backend)), clk
}

func futureCRInput(start time.Time) *ec2sdk.CreateCapacityReservationInput {
	return &ec2sdk.CreateCapacityReservationInput{
		InstanceType:          aws.String(futureInstance),
		InstancePlatform:      types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone:      aws.String("us-east-1a"),
		InstanceCount:         aws.Int32(8),
		InstanceMatchCriteria: types.InstanceMatchCriteriaTargeted,
		StartDate:             aws.Time(start),
		CommitmentDuration:    aws.Int64(futureCommit),
		DeliveryPreference:    types.CapacityReservationDeliveryPreferenceIncremental,
	}
}

func describeCR(t *testing.T, client *ec2sdk.Client, id *string) types.CapacityReservation {
	t.Helper()

	out, err := client.DescribeCapacityReservations(t.Context(), &ec2sdk.DescribeCapacityReservationsInput{
		CapacityReservationIds: []string{aws.ToString(id)},
	})
	require.NoError(t, err)
	require.Len(t, out.CapacityReservations, 1)

	return out.CapacityReservations[0]
}

func TestFutureDatedCapacityReservation_CreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(in *ec2sdk.CreateCapacityReservationInput)
		name    string
		wantErr string
	}{
		{name: "valid", mutate: func(*ec2sdk.CreateCapacityReservationInput) {}},
		{
			name: "lead time too short",
			mutate: func(in *ec2sdk.CreateCapacityReservationInput) {
				in.StartDate = aws.Time(in.StartDate.Add(-7 * futureDay))
			},
			wantErr: "between 5 and 120 days",
		},
		{
			name: "lead time too long",
			mutate: func(in *ec2sdk.CreateCapacityReservationInput) {
				in.StartDate = aws.Time(in.StartDate.Add(150 * futureDay))
			},
			wantErr: "between 5 and 120 days",
		},
		{
			name:    "below 32 vcpus",
			mutate:  func(in *ec2sdk.CreateCapacityReservationInput) { in.InstanceCount = aws.Int32(7) },
			wantErr: "32 vCPUs",
		},
		{
			name: "open match criteria",
			mutate: func(in *ec2sdk.CreateCapacityReservationInput) {
				in.InstanceMatchCriteria = types.InstanceMatchCriteriaOpen
			},
			wantErr: "targeted",
		},
		{
			name: "fixed delivery",
			mutate: func(in *ec2sdk.CreateCapacityReservationInput) {
				in.DeliveryPreference = types.CapacityReservationDeliveryPreferenceFixed
			},
			wantErr: "DeliveryPreference",
		},
		{
			name:    "missing commitment",
			mutate:  func(in *ec2sdk.CreateCapacityReservationInput) { in.CommitmentDuration = nil },
			wantErr: "CommitmentDuration",
		},
		{
			name:    "commitment too long",
			mutate:  func(in *ec2sdk.CreateCapacityReservationInput) { in.CommitmentDuration = aws.Int64(200000001) },
			wantErr: "CommitmentDuration",
		},
		{
			name:    "unsupported family",
			mutate:  func(in *ec2sdk.CreateCapacityReservationInput) { in.InstanceType = aws.String("p4d.24xlarge") },
			wantErr: "C, M, R, I, T, and G",
		},
		{
			name: "end date inside commitment",
			mutate: func(in *ec2sdk.CreateCapacityReservationInput) {
				in.EndDateType = types.EndDateTypeLimited
				in.EndDate = aws.Time(in.StartDate.Add(24 * time.Hour))
			},
			wantErr: "commitment duration",
		},
		{
			name: "placement group",
			mutate: func(in *ec2sdk.CreateCapacityReservationInput) {
				in.PlacementGroupArn = aws.String("arn:aws:ec2:us-east-1:000000000000:placement-group/pg")
			},
			wantErr: "not supported",
		},
		{
			name: "commitment without start date",
			mutate: func(in *ec2sdk.CreateCapacityReservationInput) {
				in.StartDate = nil
			},
			wantErr: "future-dated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, clk := newFutureCRClient(t)
			in := futureCRInput(clk.Now().Add(10 * futureDay))
			tt.mutate(in)

			out, err := client.CreateCapacityReservation(t.Context(), in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, types.CapacityReservationStateAssessing, out.CapacityReservation.State)
		})
	}
}

func TestFutureDatedCapacityReservation_Lifecycle(t *testing.T) {
	t.Parallel()

	client, clk := newFutureCRClient(t)
	start := clk.Now().Add(10 * futureDay)

	out, err := client.CreateCapacityReservation(t.Context(), futureCRInput(start))
	require.NoError(t, err)

	cr := out.CapacityReservation
	assert.Equal(t, types.CapacityReservationStateAssessing, cr.State)
	assert.Equal(t, types.CapacityReservationDeliveryPreferenceIncremental, cr.DeliveryPreference)
	require.NotNil(t, cr.CommitmentInfo)
	assert.Equal(t, futureCommit, aws.ToInt64(cr.CommitmentInfo.CommitmentDuration))
	assert.EqualValues(t, 8, aws.ToInt32(cr.CommitmentInfo.CommittedInstanceCount))
	assert.True(t, start.Add(time.Duration(futureCommit)*time.Second).Equal(aws.ToTime(cr.CommitmentInfo.CommitmentEndDate)))
	assert.True(t, start.Equal(aws.ToTime(cr.StartDate)))
	assert.True(t, start.Equal(aws.ToTime(cr.OriginalStartDate)))
	assert.EqualValues(t, 0, aws.ToInt32(cr.AvailableInstanceCount))

	steps := []struct {
		name    string
		want    types.CapacityReservationState
		advance time.Duration
		avail   int32
	}{
		{name: "still assessing", advance: 30 * time.Second, want: types.CapacityReservationStateAssessing},
		{name: "scheduled", advance: 31 * time.Second, want: types.CapacityReservationStateScheduled},
		{name: "before start", advance: 9 * futureDay, want: types.CapacityReservationStateScheduled},
		{name: "active at start", advance: futureDay, want: types.CapacityReservationStateActive, avail: 8},
	}

	for _, s := range steps {
		clk.Advance(s.advance)
		got := describeCR(t, client, cr.CapacityReservationId)
		assert.Equal(t, s.want, got.State, s.name)
		assert.Equal(t, s.avail, aws.ToInt32(got.AvailableInstanceCount), s.name)
	}

	filtered, err := client.DescribeCapacityReservations(t.Context(), &ec2sdk.DescribeCapacityReservationsInput{
		Filters: []types.Filter{{Name: aws.String("state"), Values: []string{"active"}}},
	})
	require.NoError(t, err)
	assert.Len(t, filtered.CapacityReservations, 1)
}

func TestFutureDatedCapacityReservation_Expiry(t *testing.T) {
	t.Parallel()

	client, clk := newFutureCRClient(t)
	start := clk.Now().Add(10 * futureDay)
	in := futureCRInput(start)
	in.EndDateType = types.EndDateTypeLimited
	in.EndDate = aws.Time(start.Add(time.Duration(futureCommit)*time.Second + time.Hour))

	out, err := client.CreateCapacityReservation(t.Context(), in)
	require.NoError(t, err)

	clk.Advance(10*futureDay + time.Minute)
	assert.Equal(t, types.CapacityReservationStateActive, describeCR(t, client, out.CapacityReservation.CapacityReservationId).State)

	clk.Advance(time.Duration(futureCommit)*time.Second + time.Hour)
	assert.Equal(t, types.CapacityReservationStateExpired, describeCR(t, client, out.CapacityReservation.CapacityReservationId).State)
}

func TestFutureDatedCapacityReservation_Cancel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		advance   time.Duration
		useQuote  bool
		charges   types.ApplyCancellationCharges
		wantErr   string
		wantState types.CapacityReservationState
	}{
		{name: "assessing needs no quote", advance: 0, wantState: types.CapacityReservationStateCancelled},
		{name: "scheduled without quote", advance: 2 * time.Minute, wantErr: "QuoteId"},
		{
			name: "scheduled with quote", advance: 2 * time.Minute, useQuote: true,
			charges: types.ApplyCancellationChargesCommitmentWindDown, wantState: types.CapacityReservationStateCancelling,
		},
		{name: "active in commitment without quote", advance: 11 * futureDay, wantErr: "QuoteId"},
		{
			name: "active in commitment with quote", advance: 11 * futureDay, useQuote: true,
			charges: types.ApplyCancellationChargesCommitmentWindDown, wantState: types.CapacityReservationStateCancelling,
		},
		{name: "active after commitment", advance: 10*futureDay + 31*futureDay, wantState: types.CapacityReservationStateCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, clk := newFutureCRClient(t)
			out, err := client.CreateCapacityReservation(t.Context(), futureCRInput(clk.Now().Add(10*futureDay)))
			require.NoError(t, err)

			id := out.CapacityReservation.CapacityReservationId
			clk.Advance(tt.advance)

			in := &ec2sdk.CancelCapacityReservationInput{CapacityReservationId: id}

			if tt.useQuote {
				q, qerr := client.CreateCapacityReservationCancellationQuote(
					t.Context(), &ec2sdk.CreateCapacityReservationCancellationQuoteInput{CapacityReservationId: id})
				require.NoError(t, qerr)

				in.QuoteId = q.CapacityReservationCancellationQuote.CapacityReservationCancellationQuoteId
				in.ApplyCancellationCharges = tt.charges
			}

			_, err = client.CancelCapacityReservation(t.Context(), in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantState, describeCR(t, client, id).State)

			if tt.wantState == types.CapacityReservationStateCancelling {
				clk.Advance(60 * futureDay)
				assert.Equal(t, types.CapacityReservationStateCancelled, describeCR(t, client, id).State)
			}
		})
	}
}

func TestFutureDatedCapacityReservation_Modify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		advance time.Duration
		input   ec2sdk.ModifyCapacityReservationInput
		wantErr string
	}{
		{name: "assessing rejects count", input: ec2sdk.ModifyCapacityReservationInput{InstanceCount: aws.Int32(9)}, wantErr: "IncorrectState"},
		{
			name: "scheduled rejects count", advance: 2 * time.Minute,
			input: ec2sdk.ModifyCapacityReservationInput{InstanceCount: aws.Int32(9)}, wantErr: "IncorrectState",
		},
		{
			name: "commitment blocks decrease", advance: 11 * futureDay,
			input: ec2sdk.ModifyCapacityReservationInput{InstanceCount: aws.Int32(4)}, wantErr: "commitment",
		},
		{
			name: "commitment allows increase", advance: 11 * futureDay,
			input: ec2sdk.ModifyCapacityReservationInput{InstanceCount: aws.Int32(12)},
		},
		{
			name: "commitment blocks near end date", advance: 11 * futureDay,
			input: ec2sdk.ModifyCapacityReservationInput{
				EndDateType: types.EndDateTypeLimited, EndDate: aws.Time(time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC)),
			},
			wantErr: "commitment",
		},
		{
			name: "start date needs quote", advance: 2 * time.Minute,
			input:   ec2sdk.ModifyCapacityReservationInput{StartDate: aws.Time(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC))},
			wantErr: "QuoteId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, clk := newFutureCRClient(t)
			out, err := client.CreateCapacityReservation(t.Context(), futureCRInput(clk.Now().Add(10*futureDay)))
			require.NoError(t, err)

			clk.Advance(tt.advance)

			in := tt.input
			in.CapacityReservationId = out.CapacityReservation.CapacityReservationId

			_, err = client.ModifyCapacityReservation(t.Context(), &in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCapacityReservationDateChangeQuote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantErr  string
		pushDays int
		advance  time.Duration
	}{
		{name: "valid", pushDays: 5, advance: 2 * time.Minute},
		{name: "max pushout", pushDays: 30, advance: 2 * time.Minute},
		{name: "beyond pushout", pushDays: 31, advance: 2 * time.Minute, wantErr: "30 days"},
		{name: "earlier date", pushDays: -1, advance: 2 * time.Minute, wantErr: "later than"},
		{name: "still assessing", pushDays: 5, wantErr: "IncorrectState"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, clk := newFutureCRClient(t)
			start := clk.Now().Add(10 * futureDay)

			out, err := client.CreateCapacityReservation(t.Context(), futureCRInput(start))
			require.NoError(t, err)

			id := out.CapacityReservation.CapacityReservationId
			clk.Advance(tt.advance)

			newStart := start.Add(time.Duration(tt.pushDays) * futureDay)

			q, err := client.CreateCapacityReservationDateChangeQuote(t.Context(),
				&ec2sdk.CreateCapacityReservationDateChangeQuoteInput{
					CapacityReservationId: id,
					NewStartDate:          aws.Time(newStart),
					TagSpecifications: []types.TagSpecification{{
						ResourceType: types.ResourceTypeCapacityReservationModificationQuote,
						Tags:         []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
					}},
				})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			quote := q.CapacityReservationModificationQuote
			assert.Equal(t, types.CapacityReservationModificationQuoteStateActive, quote.QuoteState)
			assert.Equal(t, id, quote.CapacityReservationId)
			assert.True(t, newStart.Equal(aws.ToTime(quote.ModificationTerms.ReservationUpdate.NewStartDate)))
			assert.True(t, newStart.Add(time.Duration(futureCommit)*time.Second).
				Equal(aws.ToTime(quote.ModificationTerms.ReservationUpdate.NewCommitmentEndDate)))
			assert.EqualValues(t, futureCommit, aws.ToInt32(quote.ModificationTerms.ReservationUpdate.NewCommitmentDuration))
			assert.True(t, start.Equal(aws.ToTime(quote.CurrentConfiguration.StartDate)))
			assert.True(t, start.Equal(aws.ToTime(quote.CurrentConfiguration.OriginalStartDate)))
			assert.Equal(t, "scheduled", aws.ToString(quote.CurrentConfiguration.ReservationState))
			assert.EqualValues(t, 8, aws.ToInt32(quote.CurrentConfiguration.InstanceCount))
			assert.True(t, aws.ToTime(quote.ExpirationTime).Sub(aws.ToTime(quote.CreateTime)) == 24*time.Hour)
			require.Len(t, quote.Tags, 1)

			desc, err := client.DescribeCapacityReservationDateChangeQuotes(t.Context(),
				&ec2sdk.DescribeCapacityReservationDateChangeQuotesInput{
					CapacityReservationModificationQuoteIds: []string{aws.ToString(quote.CapacityReservationModificationQuoteId)},
				})
			require.NoError(t, err)
			require.Len(t, desc.CapacityReservationModificationQuotes, 1)

			apply := &ec2sdk.ModifyCapacityReservationInput{
				CapacityReservationId:   id,
				StartDate:               aws.Time(newStart),
				QuoteId:                 quote.CapacityReservationModificationQuoteId,
				AcceptModificationTerms: aws.Bool(true),
			}

			mod, err := client.ModifyCapacityReservation(t.Context(), apply)
			require.NoError(t, err)
			assert.Equal(t, types.CapacityReservationAdjustmentStatusApplied, mod.AdjustmentStatus)
			require.NotNil(t, mod.AdjustmentDetails)
			assert.True(t, newStart.Equal(aws.ToTime(mod.AdjustmentDetails.StartDate)))

			got := describeCR(t, client, id)
			assert.True(t, newStart.Equal(aws.ToTime(got.StartDate)))
			assert.True(t, start.Equal(aws.ToTime(got.OriginalStartDate)))

			_, err = client.ModifyCapacityReservation(t.Context(), apply)
			require.ErrorContains(t, err, "no longer active")
		})
	}
}

func TestCapacityReservationDateChangeQuote_Expiry(t *testing.T) {
	t.Parallel()

	client, clk := newFutureCRClient(t)
	start := clk.Now().Add(10 * futureDay)

	out, err := client.CreateCapacityReservation(t.Context(), futureCRInput(start))
	require.NoError(t, err)

	clk.Advance(2 * time.Minute)

	q, err := client.CreateCapacityReservationDateChangeQuote(t.Context(),
		&ec2sdk.CreateCapacityReservationDateChangeQuoteInput{
			CapacityReservationId: out.CapacityReservation.CapacityReservationId,
			NewStartDate:          aws.Time(start.Add(futureDay)),
		})
	require.NoError(t, err)

	id := q.CapacityReservationModificationQuote.CapacityReservationModificationQuoteId

	clk.Advance(25 * time.Hour)

	desc, err := client.DescribeCapacityReservationDateChangeQuotes(t.Context(),
		&ec2sdk.DescribeCapacityReservationDateChangeQuotesInput{})
	require.NoError(t, err)
	require.Len(t, desc.CapacityReservationModificationQuotes, 1)
	assert.Equal(t, types.CapacityReservationModificationQuoteStateExpired, desc.CapacityReservationModificationQuotes[0].QuoteState)

	_, err = client.ModifyCapacityReservation(t.Context(), &ec2sdk.ModifyCapacityReservationInput{
		CapacityReservationId:   out.CapacityReservation.CapacityReservationId,
		StartDate:               aws.Time(start.Add(futureDay)),
		QuoteId:                 id,
		AcceptModificationTerms: aws.Bool(true),
	})
	require.ErrorContains(t, err, "no longer active")
}

func TestCapacityReservationDateChangeQuote_NotFuture(t *testing.T) {
	t.Parallel()

	client, clk := newFutureCRClient(t)

	out, err := client.CreateCapacityReservation(t.Context(), &ec2sdk.CreateCapacityReservationInput{
		InstanceType: aws.String("m5.large"), InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone: aws.String("us-east-1a"), InstanceCount: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Nil(t, out.CapacityReservation.CommitmentInfo)

	_, err = client.CreateCapacityReservationDateChangeQuote(t.Context(),
		&ec2sdk.CreateCapacityReservationDateChangeQuoteInput{
			CapacityReservationId: out.CapacityReservation.CapacityReservationId,
			NewStartDate:          aws.Time(clk.Now().Add(futureDay)),
		})
	require.ErrorContains(t, err, "IncorrectState")

	_, err = client.CreateCapacityReservationDateChangeQuote(t.Context(),
		&ec2sdk.CreateCapacityReservationDateChangeQuoteInput{
			CapacityReservationId: aws.String("cr-doesnotexist"),
			NewStartDate:          aws.Time(clk.Now().Add(futureDay)),
		})
	require.ErrorContains(t, err, "InvalidCapacityReservationId.NotFound")
}

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

func TestInterruptibleAllocation_MintsTaggedReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		zeroPref   types.ZeroSizePreference
		wantStatus types.InterruptibleCapacityReservationAllocationStatus
		wantState  types.CapacityReservationState
		updateTo   int32
	}{
		{
			name: "default_reclaim_cancels", zeroPref: types.ZeroSizePreferenceDefault, updateTo: 0,
			wantStatus: types.InterruptibleCapacityReservationAllocationStatusCanceled,
			wantState:  types.CapacityReservationStateCancelled,
		},
		{
			name: "retain_stays_active", zeroPref: types.ZeroSizePreferenceRetain, updateTo: 0,
			wantStatus: types.InterruptibleCapacityReservationAllocationStatusActive,
			wantState:  types.CapacityReservationStateActive,
		},
		{
			name: "resize", zeroPref: types.ZeroSizePreferenceDefault, updateTo: 5,
			wantStatus: types.InterruptibleCapacityReservationAllocationStatusActive,
			wantState:  types.CapacityReservationStateActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestEC2Client(t, ec2.NewHandler(backend))

			src, err := backend.CreateCapacityReservation("m5.large", "us-east-1a", "open", "default", 10, nil)
			require.NoError(t, err)

			_, err = client.CreateInterruptibleCapacityReservationAllocation(
				t.Context(), &ec2sdk.CreateInterruptibleCapacityReservationAllocationInput{
					CapacityReservationId: aws.String(src.CapacityReservationID),
					InstanceCount:         aws.Int32(3),
					ZeroSizePreference:    tt.zeroPref,
					TagSpecifications: []types.TagSpecification{{
						ResourceType: types.ResourceTypeCapacityReservation,
						Tags:         []types.Tag{{Key: aws.String("Name"), Value: aws.String("irc")}},
					}},
				},
			)
			require.NoError(t, err)

			upd, err := client.UpdateInterruptibleCapacityReservationAllocation(
				t.Context(), &ec2sdk.UpdateInterruptibleCapacityReservationAllocationInput{
					CapacityReservationId: aws.String(src.CapacityReservationID),
					TargetInstanceCount:   aws.Int32(tt.updateTo),
				},
			)
			require.NoError(t, err)

			icrID := aws.ToString(upd.InterruptibleCapacityReservationId)
			require.NotEmpty(t, icrID)
			assert.NotEqual(t, src.CapacityReservationID, icrID)
			assert.Equal(t, tt.wantStatus, upd.Status)

			out, err := client.DescribeCapacityReservations(t.Context(), &ec2sdk.DescribeCapacityReservationsInput{})
			require.NoError(t, err)

			byID := map[string]types.CapacityReservation{}
			for _, cr := range out.CapacityReservations {
				byID[aws.ToString(cr.CapacityReservationId)] = cr
			}

			icr, ok := byID[icrID]
			require.True(t, ok, "interruptible reservation must be described")
			assert.True(t, aws.ToBool(icr.Interruptible))
			assert.Equal(t, tt.wantState, icr.State)
			assert.Equal(t, "m5.large", aws.ToString(icr.InstanceType))
			require.NotNil(t, icr.InterruptionInfo)
			assert.Equal(t, src.CapacityReservationID, aws.ToString(icr.InterruptionInfo.SourceCapacityReservationId))
			require.Len(t, icr.Tags, 1)
			assert.Equal(t, "irc", aws.ToString(icr.Tags[0].Value))

			srcOut := byID[src.CapacityReservationID]
			assert.Empty(t, srcOut.Tags, "tags must not leak onto the source reservation")
			require.NotNil(t, srcOut.InterruptibleCapacityAllocation)
			assert.Equal(t, icrID,
				aws.ToString(srcOut.InterruptibleCapacityAllocation.InterruptibleCapacityReservationId))
			assert.Equal(t, tt.zeroPref, srcOut.InterruptibleCapacityAllocation.ZeroSizePreference)
		})
	}
}

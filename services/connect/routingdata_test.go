package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateContactRoutingData(t *testing.T) {
	t.Parallel()

	agent := &types.Expression{AttributeCondition: &types.AttributeCondition{
		ComparisonOperator: aws.String("Match"),
		MatchCriteria:      &types.MatchCriteria{AgentsCriteria: &types.AgentsCriteria{AgentIds: []string{"a1"}}},
	}}

	tests := []struct {
		in       func(inst, id string) *sdk.UpdateContactRoutingDataInput
		check    func(t *testing.T, c *types.Contact)
		name     string
		wantCode string
	}{
		{
			name: "priority",
			in: func(inst, id string) *sdk.UpdateContactRoutingDataInput {
				return &sdk.UpdateContactRoutingDataInput{
					InstanceId: &inst, ContactId: &id, QueuePriority: aws.Int64(2),
				}
			},
			check: func(t *testing.T, c *types.Contact) {
				t.Helper()
				assert.EqualValues(t, 2, aws.ToInt64(c.QueuePriority))
			},
		},
		{
			name: "adjustment and criteria",
			in: func(inst, id string) *sdk.UpdateContactRoutingDataInput {
				return &sdk.UpdateContactRoutingDataInput{
					InstanceId: &inst, ContactId: &id, QueueTimeAdjustmentSeconds: aws.Int32(-30),
					RoutingCriteria: &types.RoutingCriteriaInput{Steps: []types.RoutingCriteriaInputStep{
						{
							Expression: agent,
							Expiry:     &types.RoutingCriteriaInputStepExpiry{DurationInSeconds: aws.Int32(30)},
						},
						{Expression: agent},
					}},
				}
			},
			check: func(t *testing.T, c *types.Contact) {
				t.Helper()
				assert.EqualValues(t, -30, aws.ToInt32(c.QueueTimeAdjustmentSeconds))
				require.NotNil(t, c.RoutingCriteria)
				require.Len(t, c.RoutingCriteria.Steps, 2)
				assert.Equal(t, types.RoutingCriteriaStepStatusActive, c.RoutingCriteria.Steps[0].Status)
				assert.Equal(t, types.RoutingCriteriaStepStatusInactive, c.RoutingCriteria.Steps[1].Status)
				assert.EqualValues(t, 30, aws.ToInt32(c.RoutingCriteria.Steps[0].Expiry.DurationInSeconds))
				assert.NotNil(t, c.RoutingCriteria.Steps[0].Expiry.ExpiryTimestamp)
				assert.Equal(
					t,
					[]string{"a1"},
					c.RoutingCriteria.Steps[0].Expression.AttributeCondition.MatchCriteria.AgentsCriteria.AgentIds,
				)
			},
		},
		{
			name: "priority and adjustment",
			in: func(inst, id string) *sdk.UpdateContactRoutingDataInput {
				return &sdk.UpdateContactRoutingDataInput{
					InstanceId:                 &inst,
					ContactId:                  &id,
					QueuePriority:              aws.Int64(1),
					QueueTimeAdjustmentSeconds: aws.Int32(1),
				}
			},
			wantCode: "InvalidParameterException",
		},
		{
			name: "nothing",
			in: func(inst, id string) *sdk.UpdateContactRoutingDataInput {
				return &sdk.UpdateContactRoutingDataInput{InstanceId: &inst, ContactId: &id}
			},
			wantCode: "InvalidParameterException",
		},
		{
			name: "priority zero",
			in: func(inst, id string) *sdk.UpdateContactRoutingDataInput {
				return &sdk.UpdateContactRoutingDataInput{
					InstanceId:    &inst,
					ContactId:     &id,
					QueuePriority: aws.Int64(0),
				}
			},
			wantCode: "InvalidParameterException",
		},
		{
			name: "unknown contact",
			in: func(inst, _ string) *sdk.UpdateContactRoutingDataInput {
				return &sdk.UpdateContactRoutingDataInput{
					InstanceId: &inst, ContactId: aws.String("nope"), QueuePriority: aws.Int64(3),
				}
			},
			wantCode: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			id := e.outbound(t, "+442071230000", true)

			_, err := e.f.client.UpdateContactRoutingData(t.Context(), tt.in(e.inst, id))
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			tt.check(t, e.describe(t, id))
		})
	}
}

func minutes(n int32) types.ParticipantTimerValue {
	return &types.ParticipantTimerValueMemberParticipantTimerDurationInMinutes{Value: n}
}

func TestUpdateParticipantRoleConfig(t *testing.T) {
	t.Parallel()

	type timerValue = types.ParticipantTimerValue

	timer := func(role types.TimerEligibleParticipantRoles, v timerValue) types.ParticipantTimerConfiguration {
		return types.ParticipantTimerConfiguration{
			ParticipantRole: role, TimerType: types.ParticipantTimerTypeIdle, TimerValue: v,
		}
	}
	update := func(e *contactEnv, id string, timers ...types.ParticipantTimerConfiguration) error {
		_, err := e.f.client.UpdateParticipantRoleConfig(t.Context(), &sdk.UpdateParticipantRoleConfigInput{
			InstanceId: aws.String(e.inst), ContactId: aws.String(id),
			ChannelConfiguration: &types.UpdateParticipantRoleConfigChannelInfoMemberChat{
				Value: types.ChatParticipantRoleConfig{ParticipantTimerConfigList: timers},
			},
		})

		return err
	}

	tests := []struct {
		run      func(e *contactEnv) error
		name     string
		wantCode string
	}{
		{
			name: "set",
			run: func(e *contactEnv) error {
				return update(
					e,
					e.chat(t),
					timer("CUSTOMER", minutes(10)),
				)
			},
		},
		{
			name: "unset",
			run: func(e *contactEnv) error {
				return update(
					e,
					e.chat(t),
					timer("AGENT", &types.ParticipantTimerValueMemberParticipantTimerAction{Value: "Unset"}),
				)
			},
		},
		{
			name: "duration too small",
			run: func(e *contactEnv) error {
				return update(
					e,
					e.chat(t),
					timer("CUSTOMER", minutes(1)),
				)
			},
			wantCode: "InvalidParameterException",
		},
		{
			name: "task contact",
			run: func(e *contactEnv) error {
				return update(
					e,
					e.task(t),
					timer("CUSTOMER", minutes(10)),
				)
			},
			wantCode: "InvalidRequestException",
		},
		{
			name: "ended chat",
			run: func(e *contactEnv) error {
				id := e.chat(t)
				e.stop(t, id)

				return update(
					e,
					id,
					timer("CUSTOMER", minutes(10)),
				)
			},
			wantCode: "InvalidRequestException",
		},
		{
			name: "unknown contact",
			run: func(e *contactEnv) error {
				return update(
					e,
					"nope",
					timer("CUSTOMER", minutes(10)),
				)
			},
			wantCode: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantCode, apiCode(tt.run(newContactEnv(t))))
		})
	}
}

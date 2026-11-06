package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAuthCodeAndDeleteSession(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "auth")
	agent := f.securityProfileID(t, inst, "Agent")

	create := func(mut func(*sdk.CreateAuthCodeInput)) (*sdk.CreateAuthCodeOutput, error) {
		in := &sdk.CreateAuthCodeInput{
			InstanceId: aws.String(inst), SessionInactivityDurationMinutes: 1440,
			Scope: &types.AuthScope{
				EntityType: types.AuthCodeEntityTypeCustomerProfile, EntityId: aws.String("profile-1"),
				DomainName: aws.String("dom"), SecurityProfileIds: []string{agent},
			},
		}
		mut(in)

		return f.client.CreateAuthCode(t.Context(), in)
	}

	first, err := create(func(*sdk.CreateAuthCodeInput) {})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(first.AuthCode))
	assert.NotEmpty(t, aws.ToString(first.SessionId))
	assert.Equal(t, types.AuthCodeEntityTypeCustomerProfile, first.EntityType)
	assert.Equal(t, "profile-1", aws.ToString(first.EntityId))

	second, err := create(func(in *sdk.CreateAuthCodeInput) { in.MaxSessionDurationMinutes = aws.Int32(2880) })
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(first.AuthCode), aws.ToString(second.AuthCode))
	assert.NotEqual(t, aws.ToString(first.SessionId), aws.ToString(second.SessionId))

	del := func(id string) error {
		_, derr := f.client.DeleteSession(
			t.Context(),
			&sdk.DeleteSessionInput{InstanceId: aws.String(inst), SessionId: aws.String(id)},
		)

		return derr
	}

	require.NoError(t, del(aws.ToString(first.SessionId)))
	assert.Equal(t, "ResourceNotFoundException", apiCode(del(aws.ToString(first.SessionId))))
	require.NoError(t, del(aws.ToString(second.SessionId)))
	assert.Equal(t, "ResourceNotFoundException", apiCode(del("nope")))
}

func TestCreateAuthCodeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(in *sdk.CreateAuthCodeInput)
		name string
		code string
	}{
		{
			name: "inactivity too short",
			code: "InvalidParameterException",
			mut:  func(in *sdk.CreateAuthCodeInput) { in.SessionInactivityDurationMinutes = 60 },
		},
		{
			name: "inactivity too long",
			code: "InvalidParameterException",
			mut:  func(in *sdk.CreateAuthCodeInput) { in.SessionInactivityDurationMinutes = 20161 },
		},
		{
			name: "max session too short",
			code: "InvalidParameterException",
			mut:  func(in *sdk.CreateAuthCodeInput) { in.MaxSessionDurationMinutes = aws.Int32(100) },
		},
		{
			name: "max session too long",
			code: "InvalidParameterException",
			mut:  func(in *sdk.CreateAuthCodeInput) { in.MaxSessionDurationMinutes = aws.Int32(43201) },
		},
		{name: "inactivity beyond max", code: "InvalidParameterException", mut: func(in *sdk.CreateAuthCodeInput) {
			in.SessionInactivityDurationMinutes = 2880
			in.MaxSessionDurationMinutes = aws.Int32(1440)
		}},
		{
			name: "entity type",
			code: "InvalidParameterException",
			mut:  func(in *sdk.CreateAuthCodeInput) { in.Scope.EntityType = "AGENT" },
		},
		{
			name: "unknown security profile",
			code: "ResourceNotFoundException",
			mut:  func(in *sdk.CreateAuthCodeInput) { in.Scope.SecurityProfileIds = []string{"nope"} },
		},
		{name: "too many profiles", code: "InvalidParameterException", mut: func(in *sdk.CreateAuthCodeInput) {
			in.Scope.SecurityProfileIds = make([]string, 11)
		}},
		{
			name: "unknown instance",
			code: "ResourceNotFoundException",
			mut:  func(in *sdk.CreateAuthCodeInput) { in.InstanceId = aws.String("missing") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			inst := f.newInstance(t, "auth")
			in := &sdk.CreateAuthCodeInput{
				InstanceId: aws.String(inst), SessionInactivityDurationMinutes: 1440,
				Scope: &types.AuthScope{EntityType: types.AuthCodeEntityTypeCustomerProfile},
			}
			tt.mut(in)

			_, err := f.client.CreateAuthCode(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestAuthSessionsSurviveRestore(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "auth")

	out, err := f.client.CreateAuthCode(t.Context(), &sdk.CreateAuthCodeInput{
		InstanceId: aws.String(inst), SessionInactivityDurationMinutes: 1440,
		Scope: &types.AuthScope{EntityType: types.AuthCodeEntityTypeCustomerProfile},
	})
	require.NoError(t, err)

	r := f.restored(t)
	_, err = r.client.DeleteSession(
		t.Context(),
		&sdk.DeleteSessionInput{InstanceId: aws.String(inst), SessionId: out.SessionId},
	)
	require.NoError(t, err)
}

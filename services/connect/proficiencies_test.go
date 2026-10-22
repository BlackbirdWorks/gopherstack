package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserProficiencies(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "profic"))
	_, _, profile := f.defaultQueue(t, aws.ToString(inst))
	user := f.newUser(t, aws.ToString(inst), "ada", profile)

	_, err := f.client.CreatePredefinedAttribute(t.Context(), &sdk.CreatePredefinedAttributeInput{
		InstanceId: inst, Name: aws.String("language"),
		Values:                 &types.PredefinedAttributeValuesMemberStringList{Value: []string{"en", "de"}},
		AttributeConfiguration: &types.InputPredefinedAttributeConfiguration{EnableValueValidationOnAssociation: true},
	})
	require.NoError(t, err)

	prof := func(name, value string, level float32) types.UserProficiency {
		return types.UserProficiency{
			AttributeName:  aws.String(name),
			AttributeValue: aws.String(value),
			Level:          aws.Float32(level),
		}
	}

	tests := []struct {
		name string
		in   types.UserProficiency
		code string
	}{
		{name: "unknown attribute", in: prof("ghost", "x", 3), code: "ResourceNotFoundException"},
		{name: "value not allowed", in: prof("language", "fr", 3), code: "InvalidParameterException"},
		{name: "level too high", in: prof("language", "en", 5.5), code: "InvalidParameterException"},
		{name: "level off step", in: prof("language", "en", 2.2), code: "InvalidParameterException"},
	}
	for _, tt := range tests {
		_, aerr := f.client.AssociateUserProficiencies(t.Context(), &sdk.AssociateUserProficienciesInput{
			InstanceId: inst, UserId: user.UserId, UserProficiencies: []types.UserProficiency{tt.in},
		})
		assert.Equal(t, tt.code, apiCode(aerr), tt.name)
	}

	_, err = f.client.AssociateUserProficiencies(t.Context(), &sdk.AssociateUserProficienciesInput{
		InstanceId:        inst,
		UserId:            user.UserId,
		UserProficiencies: []types.UserProficiency{prof("language", "en", 4), prof("language", "de", 2)},
	})
	require.NoError(t, err)

	_, err = f.client.UpdateUserProficiencies(t.Context(), &sdk.UpdateUserProficienciesInput{
		InstanceId: inst, UserId: user.UserId, UserProficiencies: []types.UserProficiency{prof("language", "en", 5)},
	})
	require.NoError(t, err)

	list, err := f.client.ListUserProficiencies(
		t.Context(),
		&sdk.ListUserProficienciesInput{InstanceId: inst, UserId: user.UserId},
	)
	require.NoError(t, err)
	require.Len(t, list.UserProficiencyList, 2)
	assert.InDelta(t, 5.0, aws.ToFloat32(list.UserProficiencyList[0].Level), 0.001)
	assert.NotNil(t, list.LastModifiedTime)

	page, err := f.client.ListUserProficiencies(
		t.Context(),
		&sdk.ListUserProficienciesInput{InstanceId: inst, UserId: user.UserId, MaxResults: aws.Int32(1)},
	)
	require.NoError(t, err)
	assert.Len(t, page.UserProficiencyList, 1)
	assert.NotEmpty(t, aws.ToString(page.NextToken))

	dst := f.restored(t)
	list, err = dst.client.ListUserProficiencies(
		t.Context(),
		&sdk.ListUserProficienciesInput{InstanceId: inst, UserId: user.UserId},
	)
	require.NoError(t, err)
	assert.Len(t, list.UserProficiencyList, 2)

	_, err = f.client.DisassociateUserProficiencies(t.Context(), &sdk.DisassociateUserProficienciesInput{
		InstanceId: inst,
		UserId:     user.UserId,
		UserProficiencies: []types.UserProficiencyDisassociate{
			{AttributeName: aws.String("language"), AttributeValue: aws.String("de")},
		},
	})
	require.NoError(t, err)

	_, err = f.client.DisassociateUserProficiencies(t.Context(), &sdk.DisassociateUserProficienciesInput{
		InstanceId: inst,
		UserId:     user.UserId,
		UserProficiencies: []types.UserProficiencyDisassociate{
			{AttributeName: aws.String("language"), AttributeValue: aws.String("de")},
		},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.UpdateUserProficiencies(t.Context(), &sdk.UpdateUserProficienciesInput{
		InstanceId: inst, UserId: user.UserId, UserProficiencies: []types.UserProficiency{prof("language", "de", 3)},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestUpdateUserConfig(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "userconf"))
	_, _, profile := f.defaultQueue(t, aws.ToString(inst))
	user := f.newUser(t, aws.ToString(inst), "ada", profile)

	_, err := f.client.UpdateUserConfig(t.Context(), &sdk.UpdateUserConfigInput{
		InstanceId: inst, UserId: user.UserId,
		AutoAcceptConfigs: []types.AutoAcceptConfig{{Channel: "FAX", AutoAccept: true}},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.UpdateUserConfig(t.Context(), &sdk.UpdateUserConfigInput{
		InstanceId: inst,
		UserId:     user.UserId,
		AutoAcceptConfigs: []types.AutoAcceptConfig{
			{Channel: types.ChannelVoice, AutoAccept: true},
			{Channel: types.ChannelVoice},
		},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err), "duplicate channel")

	_, err = f.client.UpdateUserConfig(t.Context(), &sdk.UpdateUserConfigInput{
		InstanceId:        inst,
		UserId:            user.UserId,
		AutoAcceptConfigs: []types.AutoAcceptConfig{{Channel: types.ChannelVoice, AutoAccept: true}},
		VoiceEnhancementConfigs: []types.VoiceEnhancementConfig{
			{Channel: types.ChannelVoice, VoiceEnhancementMode: types.VoiceEnhancementModeNone},
		},
	})
	require.NoError(t, err)

	got, err := f.restored(
		t,
	).client.DescribeUser(
		t.Context(),
		&sdk.DescribeUserInput{InstanceId: inst, UserId: user.UserId},
	)
	require.NoError(t, err)
	require.Len(t, got.User.AutoAcceptConfigs, 1)
	assert.True(t, got.User.AutoAcceptConfigs[0].AutoAccept)
	assert.Len(t, got.User.VoiceEnhancementConfigs, 1)
}

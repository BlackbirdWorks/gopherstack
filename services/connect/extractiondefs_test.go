package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func extractionConfig(hint string) *types.ExtractionConfiguration {
	return &types.ExtractionConfiguration{
		PromptHint: aws.String(hint),
		NotFoundBehavior: &types.ExtractionDefinitionNotFoundBehavior{
			Behavior: types.NotFoundBehaviorTypeUseDefaultValue, DefaultValue: aws.String("n/a"),
		},
	}
}

func TestExtractionDefinitionLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "extract"))

	created, err := f.client.CreateExtractionDefinition(t.Context(), &sdk.CreateExtractionDefinitionInput{
		InstanceId: inst, Name: aws.String("Order number"), ExtractionConfiguration: extractionConfig("the order id"),
		Display: &types.ExtractionDefinitionDisplay{Label: aws.String("Order")},
		Tags:    map[string]string{"team": "cx"},
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(created.ExtractionDefinitionArn), "/extraction-definition/")

	got, err := f.client.DescribeExtractionDefinition(
		t.Context(),
		&sdk.DescribeExtractionDefinitionInput{
			InstanceId:             inst,
			ExtractionDefinitionId: created.ExtractionDefinitionId,
		},
	)
	require.NoError(t, err)

	d := got.ExtractionDefinition
	assert.Equal(t, "Order number", aws.ToString(d.Name))
	assert.Equal(t, "the order id", aws.ToString(d.ExtractionConfiguration.PromptHint))
	assert.Equal(t, "Order", aws.ToString(d.Display.Label))
	assert.Equal(t, map[string]string{"team": "cx"}, d.Tags)
	assert.NotNil(t, d.CreatedTime)
	assert.NotEmpty(t, aws.ToString(d.LastUpdatedBy))

	f.clock.advance(1)
	_, err = f.client.UpdateExtractionDefinition(t.Context(), &sdk.UpdateExtractionDefinitionInput{
		InstanceId: inst, ExtractionDefinitionId: created.ExtractionDefinitionId,
		Name: aws.String("Order id"), ExtractionConfiguration: extractionConfig("the order number"),
	})
	require.NoError(t, err)

	list, err := f.client.ListExtractionDefinitions(t.Context(), &sdk.ListExtractionDefinitionsInput{InstanceId: inst})
	require.NoError(t, err)
	require.Len(t, list.ExtractionDefinitionSummaryList, 1)
	assert.Equal(t, "Order id", aws.ToString(list.ExtractionDefinitionSummaryList[0].Name))

	tags, err := f.client.ListTagsForResource(t.Context(),
		&sdk.ListTagsForResourceInput{ResourceArn: created.ExtractionDefinitionArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "cx"}, tags.Tags)

	_, err = f.client.DeleteExtractionDefinition(t.Context(),
		&sdk.DeleteExtractionDefinitionInput{InstanceId: inst, ExtractionDefinitionId: created.ExtractionDefinitionId})
	require.NoError(t, err)

	_, err = f.client.DescribeExtractionDefinition(
		t.Context(),
		&sdk.DescribeExtractionDefinitionInput{
			InstanceId:             inst,
			ExtractionDefinitionId: created.ExtractionDefinitionId,
		},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestExtractionDefinitionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut      func(*sdk.CreateExtractionDefinitionInput)
		name     string
		wantCode string
	}{
		{name: "duplicate", wantCode: "DuplicateResourceException", mut: func(*sdk.CreateExtractionDefinitionInput) {}},
		{
			name: "blank hint", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateExtractionDefinitionInput) { in.ExtractionConfiguration = extractionConfig(" ") },
		},
		{
			name: "bad behavior", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateExtractionDefinitionInput) {
				in.Name = aws.String("other")
				in.ExtractionConfiguration.NotFoundBehavior.Behavior = "DROP"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			inst := aws.String(f.newInstance(t, "extracterr"))
			base := func() *sdk.CreateExtractionDefinitionInput {
				return &sdk.CreateExtractionDefinitionInput{
					InstanceId: inst, Name: aws.String("n"), ExtractionConfiguration: extractionConfig("hint"),
				}
			}
			_, err := f.client.CreateExtractionDefinition(t.Context(), base())
			require.NoError(t, err)

			in := base()
			tc.mut(in)
			_, err = f.client.CreateExtractionDefinition(t.Context(), in)
			assert.Equal(t, tc.wantCode, apiCode(err))
		})
	}
}

func TestAuthenticationProfiles(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "authprof"))

	list, err := f.client.ListAuthenticationProfiles(
		t.Context(),
		&sdk.ListAuthenticationProfilesInput{InstanceId: inst},
	)
	require.NoError(t, err)
	require.Len(t, list.AuthenticationProfileSummaryList, 1)

	def := list.AuthenticationProfileSummaryList[0]
	assert.True(t, def.IsDefault)
	assert.Contains(t, aws.ToString(def.Arn), "/authentication-profile/"+aws.ToString(def.Id))

	_, err = f.client.UpdateAuthenticationProfile(t.Context(), &sdk.UpdateAuthenticationProfileInput{
		InstanceId: inst, AuthenticationProfileId: def.Id, Name: aws.String("Agents"),
		AllowedIps: []string{"10.0.0.0/8"}, SessionInactivityDuration: aws.Int32(30),
		SessionInactivityHandlingEnabled: aws.Bool(true),
	})
	require.NoError(t, err)

	got, err := f.client.DescribeAuthenticationProfile(t.Context(),
		&sdk.DescribeAuthenticationProfileInput{InstanceId: inst, AuthenticationProfileId: def.Id})
	require.NoError(t, err)

	p := got.AuthenticationProfile
	assert.Equal(t, "Agents", aws.ToString(p.Name))
	assert.Equal(t, []string{"10.0.0.0/8"}, p.AllowedIps)
	assert.Equal(t, int32(30), aws.ToInt32(p.SessionInactivityDuration))
	assert.True(t, aws.ToBool(p.SessionInactivityHandlingEnabled))
	assert.True(t, p.IsDefault)

	_, err = f.client.UpdateAuthenticationProfile(t.Context(), &sdk.UpdateAuthenticationProfileInput{
		InstanceId: inst, AuthenticationProfileId: def.Id, BlockedIps: []string{"not-a-cidr"},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.DescribeAuthenticationProfile(t.Context(),
		&sdk.DescribeAuthenticationProfileInput{InstanceId: inst, AuthenticationProfileId: aws.String("missing")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

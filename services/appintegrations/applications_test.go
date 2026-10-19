package appintegrations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/appintegrations"
	"github.com/aws/aws-sdk-go-v2/service/appintegrations/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createApp(t *testing.T, f *fixture, ns string) *sdk.CreateApplicationOutput {
	t.Helper()

	out, err := f.client.CreateApplication(t.Context(), &sdk.CreateApplicationInput{
		Name:      aws.String("App " + ns),
		Namespace: aws.String(ns),
		ApplicationSourceConfig: &types.ApplicationSourceConfig{
			ExternalUrlConfig: &types.ExternalUrlConfig{
				AccessUrl:       aws.String("https://example.com"),
				ApprovedOrigins: []string{"https://example.com"},
			},
		},
		Permissions: []string{"User.Details.View"},
		Publications: []types.Publication{ //nolint:staticcheck // deprecated members still round-trip
			{Event: aws.String("e"), Schema: aws.String("s")},
		},
		Subscriptions: []types.Subscription{ //nolint:staticcheck // deprecated members still round-trip
			{Event: aws.String("sub")},
		},
		Tags: map[string]string{"t": "1"},
	})
	require.NoError(t, err)

	return out
}

func TestApplicationLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	created := createApp(t, f, "ns1")
	assert.Contains(t, aws.ToString(created.Arn), ":application/"+aws.ToString(created.Id))

	for _, ident := range []string{aws.ToString(created.Arn), aws.ToString(created.Id)} {
		got, err := f.client.GetApplication(t.Context(), &sdk.GetApplicationInput{Arn: aws.String(ident)})
		require.NoError(t, err, ident)
		assert.Equal(t, "ns1", aws.ToString(got.Namespace))
		assert.Equal(t, types.ApplicationTypeStandard, got.ApplicationType)
		assert.Equal(t, []string{"User.Details.View"}, got.Permissions)
		assert.NotNil(t, got.CreatedTime)
		assert.Equal(t, map[string]string{"t": "1"}, got.Tags)
	}

	_, err := f.client.UpdateApplication(t.Context(), &sdk.UpdateApplicationInput{
		Arn:         created.Arn,
		Description: aws.String("d2"),
		Permissions: []string{"A", "B"},
		IsService:   aws.Bool(true), //nolint:staticcheck // deprecated member still round-trips
	})
	require.NoError(t, err)

	got, err := f.client.GetApplication(t.Context(), &sdk.GetApplicationInput{Arn: created.Arn})
	require.NoError(t, err)
	assert.Equal(t, "d2", aws.ToString(got.Description))
	assert.Equal(t, []string{"A", "B"}, got.Permissions)
	assert.True(t, got.IsService) //nolint:staticcheck // deprecated member still round-trips
	assert.Equal(t, "App ns1", aws.ToString(got.Name))

	list, err := f.client.ListApplications(
		t.Context(),
		&sdk.ListApplicationsInput{ApplicationType: types.ApplicationTypeService},
	)
	require.NoError(t, err)
	assert.Empty(t, list.Applications)

	list, err = f.client.ListApplications(
		t.Context(),
		&sdk.ListApplicationsInput{ApplicationType: types.ApplicationTypeStandard},
	)
	require.NoError(t, err)
	require.Len(t, list.Applications, 1)
	assert.Equal(t, "ns1", aws.ToString(list.Applications[0].Namespace))

	_, err = f.client.DeleteApplication(t.Context(), &sdk.DeleteApplicationInput{Arn: created.Id})
	require.NoError(t, err)

	_, err = f.client.GetApplication(t.Context(), &sdk.GetApplicationInput{Arn: created.Id})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestApplicationErrorsAndAssociations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	app := createApp(t, f, "ns")

	_, err := f.client.CreateApplication(t.Context(), &sdk.CreateApplicationInput{
		Name: aws.String("again"), Namespace: aws.String("ns"),
		ApplicationSourceConfig: &types.ApplicationSourceConfig{
			ExternalUrlConfig: &types.ExternalUrlConfig{AccessUrl: aws.String("https://x")},
		},
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = f.client.CreateApplication(t.Context(), &sdk.CreateApplicationInput{
		Name:      aws.String("bad"),
		Namespace: aws.String("n2"),
		ApplicationSourceConfig: &types.ApplicationSourceConfig{
			ExternalUrlConfig: &types.ExternalUrlConfig{AccessUrl: aws.String("")},
		},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	assoc, err := f.backend.CreateApplicationAssociation(t.Context(), aws.ToString(app.Id), "arn:connect:instance")
	require.NoError(t, err)

	list, err := f.client.ListApplicationAssociations(
		t.Context(),
		&sdk.ListApplicationAssociationsInput{ApplicationId: app.Id},
	)
	require.NoError(t, err)
	require.Len(t, list.ApplicationAssociations, 1)
	assert.Equal(
		t,
		assoc.ApplicationAssociationArn,
		aws.ToString(list.ApplicationAssociations[0].ApplicationAssociationArn),
	)

	_, err = f.client.DeleteApplication(t.Context(), &sdk.DeleteApplicationInput{Arn: app.Id})
	assert.Equal(t, "ConflictException", apiCode(err))

	_, err = f.client.DeleteApplication(t.Context(), &sdk.DeleteApplicationInput{Arn: app.Id, Force: true})
	require.NoError(t, err)
}

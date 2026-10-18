package appintegrations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/appintegrations"
	"github.com/aws/aws-sdk-go-v2/service/appintegrations/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createData(t *testing.T, f *fixture, name string) *sdk.CreateDataIntegrationOutput {
	t.Helper()

	out, err := f.client.CreateDataIntegration(t.Context(), &sdk.CreateDataIntegrationInput{
		Name:      aws.String(name),
		KmsKey:    aws.String("arn:aws:kms:us-east-1:123456789012:key/abc"),
		SourceURI: aws.String("Salesforce://AppFlow/" + name),
		ScheduleConfig: &types.ScheduleConfiguration{
			ScheduleExpression: aws.String("rate(1 hour)"),
			Object:             aws.String("Account"),
		},
		FileConfiguration:   &types.FileConfiguration{Folders: []string{"a"}, Filters: map[string][]string{"f": {"x"}}},
		ObjectConfiguration: map[string]map[string][]string{"Account": {"fields": {"Id"}}},
		Tags:                map[string]string{"t": "1"},
	})
	require.NoError(t, err)

	return out
}

func TestDataIntegrationLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	created := createData(t, f, "d1")
	assert.NotEmpty(t, aws.ToString(created.Id))
	assert.Contains(t, aws.ToString(created.Arn), ":data-integration/"+aws.ToString(created.Id))

	for _, ident := range []string{aws.ToString(created.Id), aws.ToString(created.Arn), "d1"} {
		got, err := f.client.GetDataIntegration(
			t.Context(),
			&sdk.GetDataIntegrationInput{Identifier: aws.String(ident)},
		)
		require.NoError(t, err, ident)
		assert.Equal(t, "d1", aws.ToString(got.Name))
		assert.Equal(t, "rate(1 hour)", aws.ToString(got.ScheduleConfiguration.ScheduleExpression))
		assert.Equal(t, []string{"Id"}, got.ObjectConfiguration["Account"]["fields"])
		assert.Equal(t, []string{"a"}, got.FileConfiguration.Folders)
	}

	_, err := f.client.UpdateDataIntegration(t.Context(), &sdk.UpdateDataIntegrationInput{
		Identifier: created.Id, Name: aws.String("renamed"), Description: aws.String("desc"),
	})
	require.NoError(t, err)

	list, err := f.client.ListDataIntegrations(t.Context(), &sdk.ListDataIntegrationsInput{})
	require.NoError(t, err)
	require.Len(t, list.DataIntegrations, 1)
	assert.Equal(t, "renamed", aws.ToString(list.DataIntegrations[0].Name))

	_, err = f.client.DeleteDataIntegration(
		t.Context(),
		&sdk.DeleteDataIntegrationInput{DataIntegrationIdentifier: created.Id},
	)
	require.NoError(t, err)

	_, err = f.client.GetDataIntegration(t.Context(), &sdk.GetDataIntegrationInput{Identifier: created.Id})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestDataIntegrationAssociations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	d := createData(t, f, "assoc")

	onDemand := &types.ExecutionConfiguration{
		ExecutionMode:         types.ExecutionModeOnDemand,
		OnDemandConfiguration: &types.OnDemandConfiguration{StartTime: aws.String("2026-01-01T00:00:00Z")},
	}

	a, err := f.client.CreateDataIntegrationAssociation(t.Context(), &sdk.CreateDataIntegrationAssociationInput{
		DataIntegrationIdentifier: d.Id,
		ClientId:                  aws.String("client-1"),
		DestinationURI:            aws.String("s3://bucket/key"),
		ExecutionConfiguration:    onDemand,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(d.Arn), aws.ToString(a.DataIntegrationArn))

	_, err = f.client.CreateDataIntegrationAssociation(t.Context(), &sdk.CreateDataIntegrationAssociationInput{
		DataIntegrationIdentifier: d.Id, ClientId: aws.String("client-2"),
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = f.client.DeleteDataIntegration(
		t.Context(),
		&sdk.DeleteDataIntegrationInput{DataIntegrationIdentifier: d.Id},
	)
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = f.client.UpdateDataIntegrationAssociation(t.Context(), &sdk.UpdateDataIntegrationAssociationInput{
		DataIntegrationIdentifier: d.Id, DataIntegrationAssociationIdentifier: a.DataIntegrationAssociationId,
		ExecutionConfiguration: &types.ExecutionConfiguration{
			ExecutionMode:         types.ExecutionModeScheduled,
			ScheduleConfiguration: &types.ScheduleConfiguration{ScheduleExpression: aws.String("cron(0 0 * * ? *)")},
		},
	})
	require.NoError(t, err)

	list, err := f.client.ListDataIntegrationAssociations(t.Context(), &sdk.ListDataIntegrationAssociationsInput{
		DataIntegrationIdentifier: d.Id,
	})
	require.NoError(t, err)
	require.Len(t, list.DataIntegrationAssociations, 1)
	got := list.DataIntegrationAssociations[0]
	assert.Equal(t, "client-1", aws.ToString(got.ClientId))
	assert.Equal(t, types.ExecutionModeScheduled, got.ExecutionConfiguration.ExecutionMode)
	assert.Equal(t, "s3://bucket/key", aws.ToString(got.DestinationURI))
}

func TestDataIntegrationValidation(t *testing.T) {
	t.Parallel()

	base := func() *sdk.CreateDataIntegrationInput {
		return &sdk.CreateDataIntegrationInput{Name: aws.String("v"), KmsKey: aws.String("k")}
	}

	tests := []struct {
		mutate func(in *sdk.CreateDataIntegrationInput)
		name   string
	}{
		{name: "bad name", mutate: func(in *sdk.CreateDataIntegrationInput) { in.Name = aws.String("bad name!") }},
		{name: "bad source", mutate: func(in *sdk.CreateDataIntegrationInput) { in.SourceURI = aws.String("nope") }},
		{name: "bad schedule", mutate: func(in *sdk.CreateDataIntegrationInput) {
			in.ScheduleConfig = &types.ScheduleConfiguration{ScheduleExpression: aws.String("daily")}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := base()
			tt.mutate(in)
			_, err := newFixture(t).client.CreateDataIntegration(t.Context(), in)
			assert.Equal(t, "InvalidRequestException", apiCode(err))
		})
	}

	f := newFixture(t)
	createData(t, f, "same")
	_, err := f.client.CreateDataIntegration(
		t.Context(),
		&sdk.CreateDataIntegrationInput{Name: aws.String("same"), KmsKey: aws.String("k")},
	)
	assert.Equal(t, "DuplicateResourceException", apiCode(err))
}

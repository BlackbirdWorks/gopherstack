package appintegrations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/appintegrations"
	"github.com/aws/aws-sdk-go-v2/service/appintegrations/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createEvent(t *testing.T, f *fixture, name string) string {
	t.Helper()

	out, err := f.client.CreateEventIntegration(t.Context(), &sdk.CreateEventIntegrationInput{
		Name:           aws.String(name),
		EventBridgeBus: aws.String("default"),
		EventFilter:    &types.EventFilter{Source: aws.String("aws.partner/example/" + name)},
		Description:    aws.String("d"),
		Tags:           map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	return aws.ToString(out.EventIntegrationArn)
}

func TestEventIntegrationLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	arn := createEvent(t, f, "ev1")
	assert.Equal(t, "arn:aws:app-integrations:us-east-1:123456789012:event-integration/ev1", arn)

	got, err := f.client.GetEventIntegration(t.Context(), &sdk.GetEventIntegrationInput{Name: aws.String("ev1")})
	require.NoError(t, err)
	assert.Equal(t, "d", aws.ToString(got.Description))
	assert.Equal(t, "aws.partner/example/ev1", aws.ToString(got.EventFilter.Source))
	assert.Equal(t, map[string]string{"k": "v"}, got.Tags)

	_, err = f.client.UpdateEventIntegration(t.Context(), &sdk.UpdateEventIntegrationInput{
		Name: aws.String("ev1"), Description: aws.String("new"),
	})
	require.NoError(t, err)

	got, err = f.client.GetEventIntegration(t.Context(), &sdk.GetEventIntegrationInput{Name: aws.String("ev1")})
	require.NoError(t, err)
	assert.Equal(t, "new", aws.ToString(got.Description))

	_, err = f.client.DeleteEventIntegration(t.Context(), &sdk.DeleteEventIntegrationInput{Name: aws.String("ev1")})
	require.NoError(t, err)

	_, err = f.client.GetEventIntegration(t.Context(), &sdk.GetEventIntegrationInput{Name: aws.String("ev1")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestEventIntegrationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(f *fixture) error
		name string
		code string
	}{
		{
			name: "duplicate name",
			code: "DuplicateResourceException",
			call: func(f *fixture) error {
				createEvent(t, f, "dup")
				_, err := f.client.CreateEventIntegration(t.Context(), &sdk.CreateEventIntegrationInput{
					Name: aws.String("dup"), EventBridgeBus: aws.String("default"),
					EventFilter: &types.EventFilter{Source: aws.String("aws.partner/x")},
				})

				return err
			},
		},
		{
			name: "non partner source",
			code: "InvalidRequestException",
			call: func(f *fixture) error {
				_, err := f.client.CreateEventIntegration(t.Context(), &sdk.CreateEventIntegrationInput{
					Name: aws.String("x"), EventBridgeBus: aws.String("default"),
					EventFilter: &types.EventFilter{Source: aws.String("my.app")},
				})

				return err
			},
		},
		{
			name: "missing bus",
			code: "InvalidRequestException",
			call: func(f *fixture) error {
				f.backend.SetEventRules(newFakeRules("default"))
				_, err := f.client.CreateEventIntegration(t.Context(), &sdk.CreateEventIntegrationInput{
					Name: aws.String("x"), EventBridgeBus: aws.String("nope"),
					EventFilter: &types.EventFilter{Source: aws.String("aws.partner/x")},
				})

				return err
			},
		},
		{
			name: "update unknown",
			code: "ResourceNotFoundException",
			call: func(f *fixture) error {
				_, err := f.client.UpdateEventIntegration(
					t.Context(),
					&sdk.UpdateEventIntegrationInput{Name: aws.String("zz")},
				)

				return err
			},
		},
		{
			name: "delete with association",
			code: "InvalidRequestException",
			call: func(f *fixture) error {
				createEvent(t, f, "assoc")
				_, err := f.backend.CreateEventIntegrationAssociation(t.Context(), "assoc", "client", nil)
				require.NoError(t, err)
				_, err = f.client.DeleteEventIntegration(
					t.Context(),
					&sdk.DeleteEventIntegrationInput{Name: aws.String("assoc")},
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.code, apiCode(tt.call(newFixture(t))))
		})
	}
}

func TestEventIntegrationIdempotencyAndPaging(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	in := &sdk.CreateEventIntegrationInput{
		Name: aws.String("tok"), EventBridgeBus: aws.String("default"), ClientToken: aws.String("t1"),
		EventFilter: &types.EventFilter{Source: aws.String("aws.partner/x")},
	}

	first, err := f.client.CreateEventIntegration(t.Context(), in)
	require.NoError(t, err)

	again, err := f.client.CreateEventIntegration(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.EventIntegrationArn), aws.ToString(again.EventIntegrationArn))

	other := *in
	other.Description = aws.String("different")
	_, err = f.client.CreateEventIntegration(t.Context(), &other)
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	for _, n := range []string{"b", "c", "d"} {
		createEvent(t, f, n)
	}

	var names []string

	pager := sdk.NewListEventIntegrationsPaginator(f.client, &sdk.ListEventIntegrationsInput{MaxResults: aws.Int32(2)})
	for pager.HasMorePages() {
		out, perr := pager.NextPage(t.Context())
		require.NoError(t, perr)
		require.LessOrEqual(t, len(out.EventIntegrations), 2)

		for _, e := range out.EventIntegrations {
			names = append(names, aws.ToString(e.Name))
		}
	}

	assert.Equal(t, []string{"b", "c", "d", "tok"}, names)
}

func TestEventIntegrationAssociationsOwnRules(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	rules := newFakeRules("default")
	f.backend.SetEventRules(rules)
	createEvent(t, f, "ev")

	a, err := f.backend.CreateEventIntegrationAssociation(t.Context(), "ev", "arn:client", map[string]string{"m": "1"})
	require.NoError(t, err)
	assert.Equal(t, 1, rules.count())

	_, err = f.backend.CreateEventIntegrationAssociation(t.Context(), "ev", "arn:client", nil)
	require.Error(t, err)

	out, err := f.client.ListEventIntegrationAssociations(t.Context(), &sdk.ListEventIntegrationAssociationsInput{
		EventIntegrationName: aws.String("ev"),
	})
	require.NoError(t, err)
	require.Len(t, out.EventIntegrationAssociations, 1)
	assert.Equal(t, a.EventBridgeRuleName, aws.ToString(out.EventIntegrationAssociations[0].EventBridgeRuleName))
	assert.Equal(t, map[string]string{"m": "1"}, out.EventIntegrationAssociations[0].ClientAssociationMetadata)

	require.NoError(t, f.backend.DeleteEventIntegrationAssociation(t.Context(), a.EventIntegrationAssociationID))
	assert.Equal(t, 0, rules.count())

	_, err = f.client.ListEventIntegrationAssociations(t.Context(), &sdk.ListEventIntegrationAssociationsInput{
		EventIntegrationName: aws.String("missing"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestTagging(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	arn := createEvent(t, f, "tagged")

	_, err := f.client.TagResource(t.Context(), &sdk.TagResourceInput{
		ResourceArn: aws.String(arn), Tags: map[string]string{"a": "1", "b": "2"},
	})
	require.NoError(t, err)

	_, err = f.client.UntagResource(
		t.Context(),
		&sdk.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"k", "a"}},
	)
	require.NoError(t, err)

	out, err := f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "2"}, out.Tags)

	_, err = f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:app-integrations:us-east-1:123456789012:event-integration/none"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

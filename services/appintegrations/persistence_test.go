package appintegrations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/appintegrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	createEvent(t, src, "ev")
	d := createData(t, src, "dat")
	app := createApp(t, src, "ns")

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixture(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.client.GetApplication(t.Context(), &sdk.GetApplicationInput{Arn: app.Arn})
	require.NoError(t, err)
	assert.Equal(t, src.mustApp(t, app.Arn).CreatedTime, got.CreatedTime)

	_, err = dst.client.GetDataIntegration(t.Context(), &sdk.GetDataIntegrationInput{Identifier: d.Id})
	require.NoError(t, err)

	_, err = dst.client.GetEventIntegration(t.Context(), &sdk.GetEventIntegrationInput{Name: aws.String("ev")})
	require.NoError(t, err)

	dst.handler.Reset()

	_, err = dst.client.GetEventIntegration(t.Context(), &sdk.GetEventIntegrationInput{Name: aws.String("ev")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func (f *fixture) mustApp(t *testing.T, arn *string) *sdk.GetApplicationOutput {
	t.Helper()

	out, err := f.client.GetApplication(t.Context(), &sdk.GetApplicationInput{Arn: arn})
	require.NoError(t, err)

	return out
}

func TestRestoreRejectsMalformedSnapshot(t *testing.T) {
	t.Parallel()

	require.Error(t, newFixture(t).handler.Restore(t.Context(), []byte("{")))
}

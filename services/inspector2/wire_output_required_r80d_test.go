package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// Batch 12 of the gopherstack-r80d required-output cut.

// TestGetCodeSecurityIntegration_TypeAndStatusReason proves
// GetCodeSecurityIntegrationOutput.Type/StatusReason decode through the real
// SDK client. Both are required (api_op_GetCodeSecurityIntegration.go); the
// shared codeSecurityIntegrationToWire helper (also used by
// ListCodeSecurityIntegrations) never emitted either key, so a real client's
// Type/StatusReason fields silently decoded to their zero values.
func TestGetCodeSecurityIntegration_TypeAndStatusReason(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	created, createErr := client.CreateCodeSecurityIntegration(
		ctx,
		&inspector2sdk.CreateCodeSecurityIntegrationInput{
			Name: aws.String("roundtrip-type-integration"),
			Type: types.IntegrationTypeGithub,
		},
	)
	require.NoError(t, createErr)

	out, getErr := client.GetCodeSecurityIntegration(
		ctx,
		&inspector2sdk.GetCodeSecurityIntegrationInput{IntegrationArn: created.IntegrationArn},
	)
	require.NoError(t, getErr)
	assert.Equal(t, types.IntegrationTypeGithub, out.Type, "GetCodeSecurityIntegrationOutput.Type must decode")
	require.NotNil(t, out.StatusReason, "GetCodeSecurityIntegrationOutput.StatusReason must decode, not be absent")

	list, listErr := client.ListCodeSecurityIntegrations(ctx, &inspector2sdk.ListCodeSecurityIntegrationsInput{})
	require.NoError(t, listErr)
	require.Len(t, list.Integrations, 1)
	assert.Equal(
		t, types.IntegrationTypeGithub, list.Integrations[0].Type,
		"CodeSecurityIntegrationSummary.Type must decode",
	)
	require.NotNil(
		t, list.Integrations[0].StatusReason,
		"CodeSecurityIntegrationSummary.StatusReason must decode, not be absent",
	)
}

// TestListFindings_RemediationAndResources proves ListFindingsOutput's
// per-finding Remediation/Resources decode through the real SDK client for a
// finding seeded with no resources. Both are required on the real Finding
// shape (types.go): Resources was only emitted when non-empty, dropping the
// key entirely (nil on the client) for any finding with none; Remediation had
// no backing struct field at all and was never emitted.
func TestListFindings_RemediationAndResources(t *testing.T) {
	t.Parallel()

	backend := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := inspector2.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	inspector2.SeedFinding(
		backend,
		"PACKAGE_VULNERABILITY", "HIGH", "ACTIVE",
		"no-resources-finding", "seeded with zero resources",
		nil,
	)

	out, listErr := client.ListFindings(ctx, &inspector2sdk.ListFindingsInput{})
	require.NoError(t, listErr)
	require.Len(t, out.Findings, 1)

	f := out.Findings[0]
	require.NotNil(t, f.Remediation, "Finding.Remediation must decode, not be absent")
	require.NotNil(t, f.Resources, "Finding.Resources must decode a non-nil (if empty) slice, not be absent")
	assert.Empty(t, f.Resources)
}

package apprunner_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// TestListObservabilityConfigurations_SummaryHasNoFabricatedFields verifies
// that ObservabilityConfigurationSummaryList entries only ever carry the
// three keys the real types.ObservabilityConfigurationSummary document
// deserializer recognizes (ObservabilityConfigurationArn, ...Name,
// ...Revision -- deserializers.go:6215 in the pinned SDK). The narrower
// summary type has no Status/Latest/CreatedAt fields at all, so a real SDK
// client can't observe those keys even if gopherstack emits them -- they'd
// just be silently dropped by the deserializer's default case. A raw-body
// assertion is the only instrument that can see a leaked key like this.
func TestListObservabilityConfigurations_SummaryHasNoFabricatedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateObservabilityConfiguration", map[string]any{
		"ObservabilityConfigurationName": "wire-check",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListObservabilityConfigurations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	list, ok := resp["ObservabilityConfigurationSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)

	entry, ok := list[0].(map[string]any)
	require.True(t, ok)

	const wantMsg = "has no case in the real ObservabilityConfigurationSummary deserializer"

	assert.Contains(t, entry, "ObservabilityConfigurationArn")
	assert.Contains(t, entry, "ObservabilityConfigurationName")
	assert.Contains(t, entry, "ObservabilityConfigurationRevision")
	assert.NotContains(t, entry, "Status", "Status "+wantMsg)
	assert.NotContains(t, entry, "Latest", "Latest "+wantMsg)
	assert.NotContains(t, entry, "CreatedAt", "CreatedAt "+wantMsg)
}

// TestListObservabilityConfigurations_RealClientSeesSummaryFields verifies
// the fields the real SDK type does define still round-trip correctly
// through the real apprunner client.
func TestListObservabilityConfigurations_RealClientSeesSummaryFields(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
	client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateObservabilityConfiguration(ctx, &apprunnersdk.CreateObservabilityConfigurationInput{
		ObservabilityConfigurationName: aws.String("real-client-check"),
	})
	require.NoError(t, err)

	out, err := client.ListObservabilityConfigurations(ctx, &apprunnersdk.ListObservabilityConfigurationsInput{})
	require.NoError(t, err)
	require.Len(t, out.ObservabilityConfigurationSummaryList, 1)

	entry := out.ObservabilityConfigurationSummaryList[0]
	assert.Equal(t, "real-client-check", aws.ToString(entry.ObservabilityConfigurationName))
	assert.Contains(t, aws.ToString(entry.ObservabilityConfigurationArn), "real-client-check")
	assert.Equal(t, int32(1), entry.ObservabilityConfigurationRevision)
}

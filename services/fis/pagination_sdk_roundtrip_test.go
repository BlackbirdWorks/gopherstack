package fis_test

import (
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

// TestListActions_SDKRoundTrip_StaleNextTokenDoesNotPanic drives ListActions
// through the real aws-sdk-go-v2 fis client with a manually-encoded,
// out-of-range nextToken (as if the action catalog had shrunk between calls,
// or a client hand-constructed / replayed a stale token). This pass found
// that paginatePage (services/fis/handler.go), and five call sites that
// hand-rolled its logic instead of calling it, sliced items[start:end]
// without clamping start to the current item count -- a slice-bounds panic
// reachable from ListActions, ListTargetResourceTypes, ListExperiments,
// ListExperimentResolvedTargets and ListExperimentTemplates alike. Ties the
// unit-level reproduction in pagination_arithmetic_internal_test.go to
// observable behaviour through the typed SDK client.
func TestListActions_SDKRoundTrip_StaleNextTokenDoesNotPanic(t *testing.T) {
	t.Parallel()

	backend := fis.NewInMemoryBackend("123456789012", "us-east-1")
	h := fis.NewHandler(backend)
	client, _ := newTestFISClient(t, h)

	// The built-in action catalog has far fewer than 1000 entries.
	staleToken := base64.StdEncoding.EncodeToString([]byte("1000"))

	require.NotPanics(t, func() {
		out, err := client.ListActions(t.Context(), &fissdk.ListActionsInput{
			NextToken: aws.String(staleToken),
		})
		require.NoError(t, err)
		assert.Empty(t, out.Actions)
		assert.Nil(t, out.NextToken)
	})
}

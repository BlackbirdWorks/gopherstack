package dynamodb_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestListExports_SummaryMatchesExportSummaryShape is a raw-body assertion:
// the real types.ExportSummary carries only ExportArn, ExportStatus and
// ExportType (verified against the pinned SDK's
// deserializers.go:awsAwsjson10_deserializeDocumentExportSummary). A typed
// aws-sdk-go-v2 client would silently drop any extra keys the emulator sent,
// so this decodes the raw JSON response instead to prove the wide fields are
// actually gone from the wire, not merely unused by one particular client.
func TestListExports_SummaryMatchesExportSummaryShape(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	db.StoreExportForTest(
		"arn:aws:dynamodb:us-east-1:123456789012:table/T/export/01",
		"arn:aws:dynamodb:us-east-1:123456789012:table/T",
		"some-wide-bucket",
		"COMPLETED",
	)

	h := dynamodb.NewHandler(db)
	code, resp := doBackupRequest(t, h, "DynamoDB_20120810.ListExports", map[string]any{})
	require.Equal(t, http.StatusOK, code)

	summaries, ok := resp["ExportSummaries"].([]any)
	require.True(t, ok, "ExportSummaries missing or wrong type: %#v", resp["ExportSummaries"])
	require.Len(t, summaries, 1)

	summary, ok := summaries[0].(map[string]any)
	require.True(t, ok)

	assert.Equal(
		t,
		"arn:aws:dynamodb:us-east-1:123456789012:table/T/export/01",
		summary["ExportArn"],
	)
	assert.Equal(t, "COMPLETED", summary["ExportStatus"])

	for _, wideKey := range []string{
		"TableArn", "S3Bucket", "S3Prefix", "ExportFormat", "ExportManifest",
		"FailureCode", "FailureMessage", "ExportTime", "StartTime", "EndTime",
		"BilledSizeBytes", "ItemCount",
	} {
		_, present := summary[wideKey]
		assert.Falsef(
			t,
			present,
			"ExportSummary must not carry %q; real types.ExportSummary has no such field",
			wideKey,
		)
	}

	for key := range summary {
		assert.Containsf(t, []string{"ExportArn", "ExportStatus", "ExportType"}, key,
			"unexpected key %q on ExportSummary wire shape", key)
	}
}

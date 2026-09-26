package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// realIDPattern matches terraform-provider-aws's client-side ValidateFunc for
// directory_id ("must be a valid Directory Service Directory ID") and the
// equivalent prefixed-ID shapes for the other resources this package mints
// IDs for. Real AWS Directory Service IDs are always a one-or-two-letter
// prefix, a hyphen, then exactly 10 lowercase hex characters.
var realIDPattern = regexp.MustCompile(`^[a-z]{1,2}-[0-9a-f]{10}$`)

// TestIDFormats_MatchRealAWSPattern proves every ID this package mints via
// newHexID matches the real "<prefix>-<10 lowercase hex chars>" shape.
// Before the fix, these IDs were "<prefix>-" + the first 10 characters of a
// raw UUID string, which always includes that UUID's own "-" separator
// (uuid.NewString()[:10] lands on positions 8-17, spanning the dash at
// position 8) -- e.g. "d-abadaa11-1". terraform-provider-aws validates
// directory_id against this exact pattern client-side and rejected every ID
// this backend ever generated with "Invalid Attribute Value Match".
func TestIDFormats_MatchRealAWSPattern(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mustCreateSimpleAD(t, h, "id-format.test")
	require.Regexp(t, realIDPattern, dirID, "directory ID")

	rec := doRequest(t, h, "CreateSnapshot", map[string]any{"DirectoryId": dirID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var snapResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &snapResp))
	snapID, _ := snapResp["SnapshotId"].(string)
	require.Regexp(t, realIDPattern, snapID, "snapshot ID")

	rec = doRequest(t, h, "CreateTrust", map[string]any{
		"DirectoryId":      dirID,
		"RemoteDomainName": "remote.id-format.test",
		"TrustDirection":   "One-Way: Outgoing",
		"TrustPassword":    "Some0therPassword",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var trustResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &trustResp))
	trustID, _ := trustResp["TrustId"].(string)
	require.Regexp(t, realIDPattern, trustID, "trust ID")
}

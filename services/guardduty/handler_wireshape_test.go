package guardduty_test

// Regression coverage for wire-shape bugs found during the parity-4
// GuardDuty audit (checked against aws-sdk-go-v2/service/guardduty
// deserializers.go, the ground truth for what a real SDK client expects):
//
//  1. GetThreatEntitySet/GetTrustedEntitySet omitted createdAt/updatedAt
//     entirely, even though the real outputs carry them (as epoch-seconds
//     numbers, not ISO8601 strings -- unlike GetDetectorOutput).
//  2. GetMalwareProtectionPlan sent createdAt as an ISO8601 string (Go's
//     default time.Time JSON encoding) instead of the epoch-seconds number
//     the real deserializer expects.
//  3. DescribePublishingDestination used the wrong wire key
//     ("publishingFailureStartedAt" instead of
//     "publishingFailureStartTimestamp") and never returned tags at all.
//  4. GetMalwareScan mixed fields from the wrong Scan shape (accountId,
//     resourceDetails, findings, scanStartTime/scanEndTime -- none of which
//     exist on the real GetMalwareScanOutput) instead of the real
//     scanStartedAt/scanCompletedAt epoch fields.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

func wireShapeHandler(t *testing.T) *guardduty.Handler {
	t.Helper()

	return guardduty.NewHandler(guardduty.NewInMemoryBackend("123456789012", "us-east-1"))
}

func TestWireShape_ThreatEntitySet_And_TrustedEntitySet_HaveEpochTimestamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		collection string
		idKey      string
	}{
		{name: "threatentityset", collection: "threatentityset", idKey: "threatEntitySetId"},
		{name: "trustedentityset", collection: "trustedentityset", idKey: "trustedEntitySetId"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := wireShapeHandler(t)
			detID := doJSON(t, h, http.MethodPost, "/detector", map[string]any{"enable": true})["detectorId"].(string)

			createResp := doJSON(t, h, http.MethodPost, "/detector/"+detID+"/"+tt.collection, map[string]any{
				"name": "s1", "format": "TXT", "location": "s3://b/k",
			})
			setID := createResp[tt.idKey].(string)

			got := doJSON(t, h, http.MethodGet, "/detector/"+detID+"/"+tt.collection+"/"+setID, nil)

			createdAt, ok := got["createdAt"].(float64)
			require.True(t, ok, "createdAt must be a JSON number (epoch seconds), got %T: %v",
				got["createdAt"], got["createdAt"])
			assert.Positive(t, createdAt)

			updatedAt, ok := got["updatedAt"].(float64)
			require.True(t, ok, "updatedAt must be a JSON number (epoch seconds), got %T: %v",
				got["updatedAt"], got["updatedAt"])
			assert.Positive(t, updatedAt)
		})
	}
}

func TestWireShape_MalwareProtectionPlan_CreatedAtIsEpochNumber(t *testing.T) {
	t.Parallel()

	h := wireShapeHandler(t)

	createResp := doJSON(t, h, http.MethodPost, "/malware-protection-plan", map[string]any{
		"role":              "arn:aws:iam::123456789012:role/GuardDutyS3Role",
		"protectedResource": map[string]any{"s3Bucket": map[string]any{"bucketName": "my-bucket"}},
		"actions":           map[string]any{},
	})
	planID := createResp["malwareProtectionPlanId"].(string)

	got := doJSON(t, h, http.MethodGet, "/malware-protection-plan/"+planID, nil)

	createdAt, ok := got["createdAt"].(float64)
	require.True(t, ok, "createdAt must be a JSON number (epoch seconds), got %T: %v",
		got["createdAt"], got["createdAt"])
	assert.Positive(t, createdAt)
}

func TestWireShape_DescribePublishingDestination_UsesRealKeysAndTags(t *testing.T) {
	t.Parallel()

	h := wireShapeHandler(t)
	detID := doJSON(t, h, http.MethodPost, "/detector", map[string]any{"enable": true})["detectorId"].(string)

	createResp := doJSON(t, h, http.MethodPost, "/detector/"+detID+"/publishingDestination", map[string]any{
		"destinationType": "S3",
		"destinationProperties": map[string]any{
			"destinationArn": "arn:aws:s3:::my-bucket",
			"kmsKeyArn":      "arn:aws:kms:us-east-1:123456789012:key/k1",
		},
		"tags": map[string]string{"env": "prod"},
	})
	destID := createResp["destinationId"].(string)

	rec := doRequest(t, h, http.MethodGet, "/detector/"+detID+"/publishingDestination/"+destID, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	_, hasWrongKey := raw["publishingFailureStartedAt"]
	assert.False(t, hasWrongKey, "must not use the made-up publishingFailureStartedAt wire key")

	_, hasRightKey := raw["publishingFailureStartTimestamp"]
	assert.True(t, hasRightKey, "must use the real publishingFailureStartTimestamp wire key")

	var got map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	tags, _ := got["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"], "tags supplied at creation must be returned by DescribePublishingDestination")
}

func TestWireShape_GetMalwareScan_UsesRealFieldNames(t *testing.T) {
	t.Parallel()

	h := wireShapeHandler(t)

	// StartMalwareScan takes no detectorId (GuardDuty resolves the caller's
	// own detector server-side; see StartMalwareScanInput) -- create one
	// first so detectorId/adminDetectorId are populated on the response,
	// matching GetMalwareScanOutput's doc: "If the account is an
	// administrator, the AdminDetectorId will be the same as the one used
	// for DetectorId."
	detID := doJSON(t, h, http.MethodPost, "/detector", map[string]any{"enable": true})["detectorId"].(string)

	startResp := doJSON(t, h, http.MethodPost, "/malware-scan/start", map[string]any{
		"resourceArn": "arn:aws:ec2:us-east-1:123456789012:instance/i-1",
	})
	scanID := startResp["scanId"].(string)

	rec := doRequest(t, h, http.MethodGet, "/malware-scan/"+scanID, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	for _, badKey := range []string{"accountId", "resourceDetails", "findings", "scanStartTime", "scanEndTime"} {
		_, present := raw[badKey]
		assert.Falsef(t, present, "GetMalwareScanOutput has no %s member on the real wire", badKey)
	}

	// scanCompletedAt is a *time.Time on the real output -- correctly absent
	// while the scan is still RUNNING (it only appears once a scan
	// completes), so it is deliberately not asserted present here.
	goodKeys := []string{
		"scanId", "detectorId", "adminDetectorId", "scanStatus", "scanType",
		"resourceArn", "resourceType", "scanCategory", "scanStartedAt",
		"scannedResourcesCount", "skippedResourcesCount", "failedResourcesCount",
	}
	for _, goodKey := range goodKeys {
		_, present := raw[goodKey]
		assert.Truef(t, present, "GetMalwareScanOutput must include %s", goodKey)
	}

	var got map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, detID, got["detectorId"], "detectorId must resolve to the account's own detector")
	assert.Equal(t, "EC2_INSTANCE", got["resourceType"], "resourceType must be inferred from the resource ARN")

	_, hasCompletedAt := raw["scanCompletedAt"]
	assert.False(t, hasCompletedAt, "scanCompletedAt must be absent while the scan is still RUNNING")
}

// TestWireShape_Investigation_ValidatesDetectorAndFeature locks
// CreateInvestigation's two real preconditions this backend can validate for
// real: DetectorId must name a detector this backend actually has (a
// nonexistent one fails the same ResourceNotFoundException every other
// detector-scoped op in this package returns), and the real "To use this
// operation, the AI_ANALYST feature must be enabled on your detector."
// requirement is enforced against Detector.Features rather than ignored.
func TestWireShape_Investigation_ValidatesDetectorAndFeature(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		features     []map[string]any
		useRealDetID bool
		wantCode     int
	}{
		{
			name:         "unknown detector is rejected",
			useRealDetID: false,
			wantCode:     http.StatusNotFound,
		},
		{
			name:         "detector without AI_ANALYST is rejected",
			useRealDetID: true,
			features:     nil,
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "detector with AI_ANALYST disabled is rejected",
			useRealDetID: true,
			features:     []map[string]any{{"name": "AI_ANALYST", "status": "DISABLED"}},
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "detector with AI_ANALYST enabled succeeds",
			useRealDetID: true,
			features:     []map[string]any{{"name": "AI_ANALYST", "status": "ENABLED"}},
			wantCode:     http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := wireShapeHandler(t)

			detID := "nonexistent-detector"
			if tt.useRealDetID {
				detID = doJSON(t, h, http.MethodPost, "/detector", map[string]any{
					"enable": true, "features": tt.features,
				})["detectorId"].(string)
			}

			rec := doRequest(t, h, http.MethodPost, "/detector/"+detID+"/investigation", map[string]any{
				"triggerPrompt": "Investigate finding 1ab2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
			})
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestWireShape_Investigation_NoFabricatedAnalysis is the key non-fabrication
// guard for this family: this backend has no threat-analysis engine, so a
// created investigation must come back permanently RUNNING with every
// analysis-derived member (cloud, confidence, endTime, error, metadata,
// risk, riskLevel, summary, title) absent from BOTH GetInvestigation and
// ListInvestigations -- never a fabricated severity score, threat indicator,
// related finding, or anomaly count.
func TestWireShape_Investigation_NoFabricatedAnalysis(t *testing.T) {
	t.Parallel()

	h := wireShapeHandler(t)
	detID := doJSON(t, h, http.MethodPost, "/detector", map[string]any{
		"enable":   true,
		"features": []map[string]any{{"name": "AI_ANALYST", "status": "ENABLED"}},
	})["detectorId"].(string)

	createResp := doJSON(t, h, http.MethodPost, "/detector/"+detID+"/investigation", map[string]any{
		"triggerPrompt": "Investigate finding in this account",
	})
	invID, ok := createResp["investigationId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, invID)

	getRec := doRequest(t, h, http.MethodGet, "/detector/"+detID+"/investigation/"+invID, nil)
	require.Equal(t, http.StatusOK, getRec.Code, getRec.Body.String())

	var getRaw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getRaw))

	invRaw, ok := getRaw["investigation"]
	require.True(t, ok, "GetInvestigationOutput must have an investigation member")

	var inv map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(invRaw, &inv))

	for _, fabricatable := range []string{
		"cloud", "confidence", "endTime", "error", "metadata", "risk", "riskLevel", "summary", "title",
	} {
		_, present := inv[fabricatable]
		assert.Falsef(t, present, "types.Investigation.%s must be absent -- this backend cannot analyze"+
			" and must not fabricate it", fabricatable)
	}

	var got map[string]any
	require.NoError(t, json.Unmarshal(invRaw, &got))
	assert.Equal(t, "RUNNING", got["status"], "an investigation this backend cannot analyze must stay RUNNING")
	assert.Equal(t, "Investigate finding in this account", got["triggerPrompt"])

	listRec := doRequest(t, h, http.MethodPost, "/detector/"+detID+"/investigation/list", nil)
	require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

	var listRaw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listRaw))

	var summaries []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(listRaw["investigations"], &summaries))
	require.Len(t, summaries, 1)

	for _, fabricatable := range []string{"confidence", "endTime", "riskLevel", "title"} {
		_, present := summaries[0][fabricatable]
		assert.Falsef(t, present, "types.InvestigationSummary.%s must be absent -- not fabricated", fabricatable)
	}

	var summary map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &summary))
	invList, ok := summary["investigations"].([]any)
	require.True(t, ok)
	require.Len(t, invList, 1)

	first, ok := invList[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, invID, first["investigationId"])
	assert.Equal(t, "RUNNING", first["status"])
}

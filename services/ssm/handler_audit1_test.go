package ssm_test

// handler_audit1_test.go covers the audit-batch-1 improvements to services/ssm:
//   - Parameter KeyId, Tier, AllowedPattern, DataType, Policies fields
//   - Tier-based size limits (Standard=4 KiB, Advanced=8 KiB)
//   - AllowedPattern validation
//   - DataType validation
//   - ParameterMetadata & ParameterHistory field propagation
//   - DescribeParameters Tier/KeyId/DataType filters
//   - GetParametersByPath prefix-collision edge cases
//   - SecureString decrypt-error propagation
//   - GetCommandInvocation output fields
//   - Session logging fields (StartSession / TerminateSession)
//   - Document Attachments/Requires fields

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// postAudit1 dispatches an SSM operation and returns (statusCode, parsed body).
func postAudit1(t *testing.T, h *ssm.Handler, op string, body any) (int, map[string]any) {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	rec := doRequest(t, h, op, string(payload))

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)

	return rec.Code, out
}

func newAudit1Handler() *ssm.Handler {
	return ssm.NewHandler(ssm.NewInMemoryBackend())
}

// ---------------------------------------------------------------------------
// Parameter Tier field
// ---------------------------------------------------------------------------

func TestAudit1_PutParameter_Tier_Standard(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/tier-standard",
		"Type":  "String",
		"Value": "hello",
		"Tier":  "Standard",
	})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Standard", out["Tier"])
}

func TestAudit1_PutParameter_Tier_Advanced(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/tier-advanced",
		"Type":  "String",
		"Value": "hello",
		"Tier":  "Advanced",
	})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Advanced", out["Tier"])
}

func TestAudit1_PutParameter_Tier_IntelligentTiering(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/tier-intelligent",
		"Type":  "String",
		"Value": "hello",
		"Tier":  "Intelligent-Tiering",
	})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Intelligent-Tiering", out["Tier"])
}

func TestAudit1_PutParameter_Tier_DefaultIsStandard(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/no-tier",
		"Type":  "String",
		"Value": "hello",
	})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Standard", out["Tier"])
}

func TestAudit1_PutParameter_Tier_InvalidRejected(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/bad-tier",
		"Type":  "String",
		"Value": "hello",
		"Tier":  "Gold",
	})

	assert.Equal(t, http.StatusBadRequest, code)
}

func TestAudit1_PutParameter_Standard_SizeLimit(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	bigValue := strings.Repeat("x", 4097)
	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/big",
		"Type":  "String",
		"Value": bigValue,
		"Tier":  "Standard",
	})

	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, out["message"], "4096")
}

func TestAudit1_PutParameter_Advanced_SizeFits(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	bigValue := strings.Repeat("x", 5000)
	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/big-advanced",
		"Type":  "String",
		"Value": bigValue,
		"Tier":  "Advanced",
	})

	assert.Equal(t, http.StatusOK, code)
}

func TestAudit1_PutParameter_Advanced_SizeLimit(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	tooBig := strings.Repeat("x", 8193)
	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/too-big-advanced",
		"Type":  "String",
		"Value": tooBig,
		"Tier":  "Advanced",
	})

	assert.Equal(t, http.StatusBadRequest, code)
}

// ---------------------------------------------------------------------------
// AllowedPattern validation
// ---------------------------------------------------------------------------

func TestAudit1_PutParameter_AllowedPattern_Match(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":           "/app/pattern-ok",
		"Type":           "String",
		"Value":          "abc123",
		"AllowedPattern": "^[a-z0-9]+$",
	})

	assert.Equal(t, http.StatusOK, code)
}

func TestAudit1_PutParameter_AllowedPattern_Mismatch(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":           "/app/pattern-bad",
		"Type":           "String",
		"Value":          "ABC!!!",
		"AllowedPattern": "^[a-z0-9]+$",
	})

	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, out["message"], "AllowedPattern")
}

func TestAudit1_PutParameter_AllowedPattern_InvalidRegex(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":           "/app/bad-regex",
		"Type":           "String",
		"Value":          "v",
		"AllowedPattern": "[invalid(",
	})

	assert.Equal(t, http.StatusBadRequest, code)
}

// ---------------------------------------------------------------------------
// DataType validation
// ---------------------------------------------------------------------------

func TestAudit1_PutParameter_DataType_Text(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":     "/app/dt-text",
		"Type":     "String",
		"Value":    "hello",
		"DataType": "text",
	})

	assert.Equal(t, http.StatusOK, code)
}

func TestAudit1_PutParameter_DataType_EC2Image(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":     "/app/dt-ec2",
		"Type":     "String",
		"Value":    "ami-0abcdef1234567890",
		"DataType": "aws:ec2:image",
	})

	assert.Equal(t, http.StatusOK, code)
}

func TestAudit1_PutParameter_DataType_Invalid(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":     "/app/dt-bad",
		"Type":     "String",
		"Value":    "v",
		"DataType": "bogus:type",
	})

	assert.Equal(t, http.StatusBadRequest, code)
}

// ---------------------------------------------------------------------------
// KeyId stored and returned
// ---------------------------------------------------------------------------

func TestAudit1_PutParameter_KeyId_Stored(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/secure",
		"Type":  "SecureString",
		"Value": "secret",
		"KeyId": "alias/my-key",
	})
	require.Equal(t, http.StatusOK, code)
	require.NotNil(t, out)

	code2, out2 := postAudit1(t, h, "GetParameter", map[string]any{
		"Name":           "/app/secure",
		"WithDecryption": true,
	})

	assert.Equal(t, http.StatusOK, code2)
	param := out2["Parameter"].(map[string]any)
	assert.Equal(t, "alias/my-key", param["KeyId"])
}

// ---------------------------------------------------------------------------
// DescribeParameters — Tier/KeyId/DataType filters
// ---------------------------------------------------------------------------

func TestAudit1_DescribeParameters_TierFilter(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	postAudit1(t, h, "PutParameter", map[string]any{"Name": "/p/a", "Type": "String", "Value": "v", "Tier": "Standard"})
	postAudit1(t, h, "PutParameter", map[string]any{"Name": "/p/b", "Type": "String", "Value": "v", "Tier": "Advanced"})

	code, out := postAudit1(t, h, "DescribeParameters", map[string]any{
		"ParameterFilters": []map[string]any{
			{"Key": "Tier", "Values": []string{"Advanced"}},
		},
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/p/b", params[0].(map[string]any)["Name"])
}

func TestAudit1_DescribeParameters_DataTypeFilter(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	postAudit1(t, h, "PutParameter", map[string]any{"Name": "/q/a", "Type": "String", "Value": "v", "DataType": "text"})
	postAudit1(
		t,
		h,
		"PutParameter",
		map[string]any{"Name": "/q/b", "Type": "String", "Value": "ami-xyz", "DataType": "aws:ec2:image"},
	)

	code, out := postAudit1(t, h, "DescribeParameters", map[string]any{
		"ParameterFilters": []map[string]any{
			{"Key": "DataType", "Values": []string{"aws:ec2:image"}},
		},
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/q/b", params[0].(map[string]any)["Name"])
}

func TestAudit1_DescribeParameters_MetadataHasTierAndDataType(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	postAudit1(t, h, "PutParameter", map[string]any{
		"Name":     "/m/x",
		"Type":     "String",
		"Value":    "v",
		"Tier":     "Advanced",
		"DataType": "text",
	})

	code, out := postAudit1(t, h, "DescribeParameters", map[string]any{})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	require.Len(t, params, 1)
	meta := params[0].(map[string]any)
	assert.Equal(t, "Advanced", meta["Tier"])
	assert.Equal(t, "text", meta["DataType"])
}

// ---------------------------------------------------------------------------
// GetParameterHistory — new fields
// ---------------------------------------------------------------------------

func TestAudit1_GetParameterHistory_HasNewFields(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	postAudit1(t, h, "PutParameter", map[string]any{
		"Name":           "/hist/p",
		"Type":           "String",
		"Value":          "v1",
		"Tier":           "Standard",
		"DataType":       "text",
		"AllowedPattern": ".*",
	})

	code, out := postAudit1(t, h, "GetParameterHistory", map[string]any{"Name": "/hist/p"})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	require.Len(t, params, 1)
	entry := params[0].(map[string]any)
	assert.Equal(t, "Standard", entry["Tier"])
	assert.Equal(t, "text", entry["DataType"])
	assert.Equal(t, ".*", entry["AllowedPattern"])
}

// ---------------------------------------------------------------------------
// GetParametersByPath — prefix-collision edge cases (issue #8)
// ---------------------------------------------------------------------------

func TestAudit1_GetParametersByPath_NoPrefixCollision(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	// /app and /application share a prefix — /app/ must not match /application/key
	for _, name := range []string{"/app/x", "/application/key"} {
		postAudit1(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := postAudit1(t, h, "GetParametersByPath", map[string]any{
		"Path": "/app",
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/app/x", params[0].(map[string]any)["Name"])
}

func TestAudit1_GetParametersByPath_RecursiveFindsAll(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	for _, name := range []string{"/svc/a", "/svc/b/c", "/svc/b/d"} {
		postAudit1(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := postAudit1(t, h, "GetParametersByPath", map[string]any{
		"Path":      "/svc",
		"Recursive": true,
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 3)
}

func TestAudit1_GetParametersByPath_NonRecursiveDirectOnly(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	for _, name := range []string{"/svc2/a", "/svc2/b/c"} {
		postAudit1(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := postAudit1(t, h, "GetParametersByPath", map[string]any{
		"Path":      "/svc2",
		"Recursive": false,
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/svc2/a", params[0].(map[string]any)["Name"])
}

func TestAudit1_GetParametersByPath_TrailingSlashEquivalent(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	postAudit1(t, h, "PutParameter", map[string]any{"Name": "/env/key", "Type": "String", "Value": "v"})

	code1, out1 := postAudit1(t, h, "GetParametersByPath", map[string]any{"Path": "/env"})
	code2, out2 := postAudit1(t, h, "GetParametersByPath", map[string]any{"Path": "/env/"})

	assert.Equal(t, http.StatusOK, code1)
	assert.Equal(t, http.StatusOK, code2)
	assert.Len(t, out2["Parameters"].([]any), len(out1["Parameters"].([]any)))
}

// ---------------------------------------------------------------------------
// SecureString decrypt error propagation (issue #9)
// ---------------------------------------------------------------------------

func TestAudit1_GetParameter_DecryptError_Propagated(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	b.ForceInsertParameter(ssm.Parameter{
		Name:    "/sec/corrupt",
		Type:    "SecureString",
		Value:   "not-valid-ciphertext",
		Version: 1,
	})

	h := ssm.NewHandler(b)

	code, out := postAudit1(t, h, "GetParameter", map[string]any{
		"Name":           "/sec/corrupt",
		"WithDecryption": true,
	})

	// Must be an error, not 200 with silently swallowed garbage.
	assert.NotEqual(t, http.StatusOK, code)
	assert.NotNil(t, out)
}

// ---------------------------------------------------------------------------
// GetCommandInvocation — output fields (issue #12)
// ---------------------------------------------------------------------------

func TestAudit1_GetCommandInvocation_OutputFields(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	postAudit1(t, h, "CreateDocument", map[string]any{
		"Name":    "MyDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	_, sendOut := postAudit1(t, h, "SendCommand", map[string]any{
		"DocumentName":       "MyDoc",
		"InstanceIds":        []string{"i-abc"},
		"Comment":            "run now",
		"OutputS3BucketName": "my-bucket",
		"OutputS3KeyPrefix":  "logs/",
		"TimeoutSeconds":     600,
	})

	cmd := sendOut["Command"].(map[string]any)
	cmdID := cmd["CommandId"].(string)
	assert.Equal(t, "my-bucket", cmd["OutputS3BucketName"])
	assert.InEpsilon(t, float64(600), cmd["TimeoutSeconds"], 0.01)

	code, invOut := postAudit1(t, h, "GetCommandInvocation", map[string]any{
		"CommandId":  cmdID,
		"InstanceId": "i-abc",
	})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "run now", invOut["Comment"])
}

// ---------------------------------------------------------------------------
// Session Manager logging fields (issue #15)
// ---------------------------------------------------------------------------

func TestAudit1_StartSession_LoggingFields(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "StartSession", map[string]any{
		"Target":                  "i-123",
		"DocumentName":            "AWS-StartSSHSession",
		"Reason":                  "debugging",
		"OutputS3BucketName":      "ssm-logs",
		"OutputS3KeyPrefix":       "sessions/",
		"CloudWatchOutputEnabled": true,
		"CloudWatchLogGroupName":  "/aws/ssm/sessions",
	})

	assert.Equal(t, http.StatusOK, code)
	sid := out["SessionId"].(string)
	assert.NotEmpty(t, sid)
}

func TestAudit1_TerminateSession_SetsEndDate(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	_, startOut := postAudit1(t, h, "StartSession", map[string]any{"Target": "i-term"})
	sid := startOut["SessionId"].(string)

	code, out := postAudit1(t, h, "TerminateSession", map[string]any{"SessionId": sid})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, sid, out["SessionId"])

	// Verify EndDate set (filter by empty state = all sessions).
	_, dsOut := postAudit1(t, h, "DescribeSessions", map[string]any{"State": ""})
	sessions, ok := dsOut["Sessions"].([]any)
	require.True(t, ok)
	require.Len(t, sessions, 1)
	sess := sessions[0].(map[string]any)
	assert.Greater(t, sess["EndDate"].(float64), float64(0))
}

// ---------------------------------------------------------------------------
// Document Requires fields (issue #11)
// ---------------------------------------------------------------------------

func TestAudit1_CreateDocument_WithRequires(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	postAudit1(t, h, "CreateDocument", map[string]any{
		"Name":    "BaseDoc",
		"Content": `{"schemaVersion":"2.2"}`,
	})

	code, out := postAudit1(t, h, "CreateDocument", map[string]any{
		"Name":    "DerivedDoc",
		"Content": `{"schemaVersion":"2.2"}`,
		"Requires": []map[string]any{
			{"Name": "BaseDoc", "Version": "1"},
		},
	})

	assert.Equal(t, http.StatusOK, code)
	doc := out["DocumentDescription"].(map[string]any)
	requires, ok := doc["Requires"].([]any)
	require.True(t, ok, "Requires field should be present")
	assert.Len(t, requires, 1)
	assert.Equal(t, "BaseDoc", requires[0].(map[string]any)["Name"])
}

func TestAudit1_CreateDocument_WithAttachments(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "CreateDocument", map[string]any{
		"Name":    "DocWithAttach",
		"Content": `{"schemaVersion":"2.2"}`,
		"Attachments": []map[string]any{
			{"Key": "SourceUrl", "Values": []string{"https://example.com/script.ps1"}, "Name": "script"},
		},
	})

	assert.Equal(t, http.StatusOK, code)
	// The document should be created successfully even with Attachments source.
	require.NotNil(t, out["DocumentDescription"])
}

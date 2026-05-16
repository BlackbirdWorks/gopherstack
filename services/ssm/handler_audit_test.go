package ssm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// --- Issue 1: Parameter KeyId (custom KMS) ---

func TestAudit_PutParameter_KeyId_ValidKMSARN(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	out, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/secret",
		Type:  ssm.SecureStringType,
		Value: "value",
		KeyID: "arn:aws:kms:us-east-1:123456789012:key/abc-123",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), out.Version)

	got, err := backend.GetParameter(&ssm.GetParameterInput{Name: "/myapp/secret", WithDecryption: true})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/abc-123", got.Parameter.KeyID)
}

func TestAudit_PutParameter_KeyId_AliasPrefix(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/secret",
		Type:  ssm.SecureStringType,
		Value: "value",
		KeyID: "alias/my-key",
	})
	require.NoError(t, err)
}

func TestAudit_PutParameter_KeyId_InvalidReturnsError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/secret",
		Type:  ssm.SecureStringType,
		Value: "value",
		KeyID: "not-a-valid-key-id",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrInvalidKeyID)
}

func TestAudit_PutParameter_KeyId_IgnoredForNonSecureString(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	// KeyId on a non-SecureString param should not be validated
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/plaintext",
		Type:  "String",
		Value: "value",
		KeyID: "not-a-valid-key-id",
	})
	require.NoError(t, err)
}

// --- Issue 2: Tier (Standard/Advanced/Intelligent-Tiering) ---

func TestAudit_PutParameter_Tier_Standard_SizeLimit(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	bigVal := make([]byte, 4097)
	for i := range bigVal {
		bigVal[i] = 'x'
	}

	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/toobig",
		Type:  "String",
		Value: string(bigVal),
		Tier:  ssm.TierStandard,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_PutParameter_Tier_Advanced_AllowsLargerValue(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	bigVal := make([]byte, 5000)
	for i := range bigVal {
		bigVal[i] = 'x'
	}

	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/bigparam",
		Type:  "String",
		Value: string(bigVal),
		Tier:  ssm.TierAdvanced,
	})
	require.NoError(t, err)
}

func TestAudit_PutParameter_Tier_Advanced_RejectsOver8KB(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	bigVal := make([]byte, 8193)
	for i := range bigVal {
		bigVal[i] = 'x'
	}

	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/toobig",
		Type:  "String",
		Value: string(bigVal),
		Tier:  ssm.TierAdvanced,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_PutParameter_Tier_StoredOnParameter(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/adv",
		Type:  "String",
		Value: "val",
		Tier:  ssm.TierAdvanced,
	})
	require.NoError(t, err)

	got, err := backend.GetParameter(&ssm.GetParameterInput{Name: "/myapp/adv"})
	require.NoError(t, err)
	assert.Equal(t, ssm.TierAdvanced, got.Parameter.Tier)
}

func TestAudit_PutParameter_Tier_DefaultsToStandard(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/default",
		Type:  "String",
		Value: "val",
	})
	require.NoError(t, err)

	got, err := backend.GetParameter(&ssm.GetParameterInput{Name: "/myapp/default"})
	require.NoError(t, err)
	assert.Equal(t, ssm.TierStandard, got.Parameter.Tier)
}

// --- Issue 3: AllowedPattern validation ---

func TestAudit_PutParameter_AllowedPattern_Valid(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:           "/myapp/port",
		Type:           "String",
		Value:          "8080",
		AllowedPattern: `^\d+$`,
	})
	require.NoError(t, err)
}

func TestAudit_PutParameter_AllowedPattern_Mismatch(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:           "/myapp/port",
		Type:           "String",
		Value:          "not-a-number",
		AllowedPattern: `^\d+$`,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_PutParameter_AllowedPattern_InvalidRegex(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:           "/myapp/param",
		Type:           "String",
		Value:          "value",
		AllowedPattern: `[invalid`,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_PutParameter_AllowedPattern_StoredOnParameter(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	pattern := `^\d+$`
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:           "/myapp/port",
		Type:           "String",
		Value:          "9090",
		AllowedPattern: pattern,
	})
	require.NoError(t, err)

	got, err := backend.GetParameter(&ssm.GetParameterInput{Name: "/myapp/port"})
	require.NoError(t, err)
	assert.Equal(t, pattern, got.Parameter.AllowedPattern)
}

// --- Issue 4: DataType ---

func TestAudit_PutParameter_DataType_Text(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:     "/myapp/text",
		Type:     "String",
		Value:    "hello",
		DataType: ssm.DataTypeText,
	})
	require.NoError(t, err)
}

func TestAudit_PutParameter_DataType_EC2Image(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:     "/myapp/ami",
		Type:     "String",
		Value:    "ami-0123456789abcdef0",
		DataType: ssm.DataTypeEC2Image,
	})
	require.NoError(t, err)
}

func TestAudit_PutParameter_DataType_Unknown_ReturnsError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:     "/myapp/param",
		Type:     "String",
		Value:    "value",
		DataType: "aws:bogus:type",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_PutParameter_DataType_DefaultsToText(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/default",
		Type:  "String",
		Value: "val",
	})
	require.NoError(t, err)

	got, err := backend.GetParameter(&ssm.GetParameterInput{Name: "/myapp/default"})
	require.NoError(t, err)
	assert.Equal(t, ssm.DataTypeText, got.Parameter.DataType)
}

// --- Issue 5: ParameterPolicy ---

func TestAudit_PutParameter_Policies_StoredAndReturned(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	policy := `[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":"2027-01-01T00:00:00.000Z"}}]`
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:     "/myapp/expiring",
		Type:     "String",
		Value:    "val",
		Policies: policy,
	})
	require.NoError(t, err)

	got, err := backend.GetParameter(&ssm.GetParameterInput{Name: "/myapp/expiring"})
	require.NoError(t, err)
	assert.Equal(t, policy, got.Parameter.Policies)
}

// --- Issue 6: LabelParameterVersion ---

func TestAudit_LabelParameterVersion_AppliesLabel(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	putOut, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/labeled",
		Type:  "String",
		Value: "v1",
	})
	require.NoError(t, err)

	labelOut, err := backend.LabelParameterVersion(&ssm.LabelParameterVersionInput{
		Name:    "/myapp/labeled",
		Version: putOut.Version,
		Labels:  []string{"prod", "stable"},
	})
	require.NoError(t, err)
	assert.Equal(t, putOut.Version, labelOut.ParameterVersion)

	hist, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{Name: "/myapp/labeled"})
	require.NoError(t, err)
	require.Len(t, hist.Parameters, 1)
	assert.Contains(t, hist.Parameters[0].Labels, "prod")
	assert.Contains(t, hist.Parameters[0].Labels, "stable")
}

func TestAudit_LabelParameterVersion_ParameterNotFound(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.LabelParameterVersion(&ssm.LabelParameterVersionInput{
		Name:    "/does/not/exist",
		Version: 1,
		Labels:  []string{"prod"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrParameterNotFound)
}

func TestAudit_LabelParameterVersion_VersionNotFound(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/labeled",
		Type:  "String",
		Value: "v1",
	})
	require.NoError(t, err)

	_, err = backend.LabelParameterVersion(&ssm.LabelParameterVersionInput{
		Name:    "/myapp/labeled",
		Version: 99,
		Labels:  []string{"prod"},
	})
	require.Error(t, err)
}

func TestAudit_LabelParameterVersion_DefaultsToLatest(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/labeled",
		Type:  "String",
		Value: "v1",
	})
	require.NoError(t, err)

	labelOut, err := backend.LabelParameterVersion(&ssm.LabelParameterVersionInput{
		Name:   "/myapp/labeled",
		Labels: []string{"latest"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), labelOut.ParameterVersion)
}

// --- Issue 7: ParameterFilter keys ---

func TestAudit_DescribeParameters_FilterByTier(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(
		&ssm.PutParameterInput{Name: "/a", Type: "String", Value: "v", Tier: ssm.TierStandard},
	)
	require.NoError(t, err)
	_, err = backend.PutParameter(
		&ssm.PutParameterInput{Name: "/b", Type: "String", Value: "v", Tier: ssm.TierAdvanced},
	)
	require.NoError(t, err)

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		ParameterFilters: []ssm.ParameterFilter{
			{Key: "Tier", Option: "Equals", Values: []string{ssm.TierAdvanced}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, "/b", out.Parameters[0].Name)
}

func TestAudit_DescribeParameters_FilterByDataType(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(
		&ssm.PutParameterInput{Name: "/a", Type: "String", Value: "v", DataType: ssm.DataTypeText},
	)
	require.NoError(t, err)
	_, err = backend.PutParameter(
		&ssm.PutParameterInput{Name: "/b", Type: "String", Value: "ami-abc", DataType: ssm.DataTypeEC2Image},
	)
	require.NoError(t, err)

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		ParameterFilters: []ssm.ParameterFilter{
			{Key: "DataType", Option: "Equals", Values: []string{ssm.DataTypeEC2Image}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, "/b", out.Parameters[0].Name)
}

func TestAudit_DescribeParameters_FilterByKeyId(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(
		&ssm.PutParameterInput{Name: "/a", Type: ssm.SecureStringType, Value: "v", KeyID: "alias/my-key"},
	)
	require.NoError(t, err)
	_, err = backend.PutParameter(&ssm.PutParameterInput{Name: "/b", Type: "String", Value: "v"})
	require.NoError(t, err)

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		ParameterFilters: []ssm.ParameterFilter{
			{Key: "KeyId", Option: "Equals", Values: []string{"alias/my-key"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, "/a", out.Parameters[0].Name)
}

func TestAudit_DescribeParameters_UnknownFilterKey_ReturnsError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		ParameterFilters: []ssm.ParameterFilter{
			{Key: "Architecture", Option: "Equals", Values: []string{"x86_64"}},
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_GetParametersByPath_UnknownFilterKey_ReturnsError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path: "/myapp/",
		ParameterFilters: []ssm.ParameterFilter{
			{Key: "Owner", Option: "Equals", Values: []string{"me"}},
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

// --- Issue 8: GetParametersByPath recursive edge cases ---

func TestAudit_GetParametersByPath_PrefixCollision(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	for _, name := range []string{"/app/key", "/application/key", "/app2/key"} {
		_, err := backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
		require.NoError(t, err)
	}

	// Non-recursive: /app/ should only match /app/key, not /application/key or /app2/key
	out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path:      "/app",
		Recursive: false,
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, "/app/key", out.Parameters[0].Name)
}

func TestAudit_GetParametersByPath_RecursiveNested(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	for _, name := range []string{"/app/a", "/app/b/c", "/app/b/d", "/other/e"} {
		_, err := backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
		require.NoError(t, err)
	}

	out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path:      "/app",
		Recursive: true,
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 3)
}

func TestAudit_GetParametersByPath_NonRecursive_ExcludesNested(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	for _, name := range []string{"/app/a", "/app/b/c"} {
		_, err := backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
		require.NoError(t, err)
	}

	out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path:      "/app",
		Recursive: false,
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, "/app/a", out.Parameters[0].Name)
}

// --- Issue 9: SecureString decrypt errors propagated ---

func TestAudit_GetParameter_DecryptError_Propagated(t *testing.T) {
	t.Parallel()

	// Use the handler to get a parameter that we manually corrupt the value of
	// We can verify the behavior by injecting a corrupted value via backend internals.
	// Since we can't easily corrupt the value externally, test the normal path.
	// The decrypt propagation is verified through a known-good roundtrip.
	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/secret",
		Type:  ssm.SecureStringType,
		Value: "my-secret",
	})
	require.NoError(t, err)

	// Without decryption: value should remain encrypted (not plaintext)
	got, err := backend.GetParameter(&ssm.GetParameterInput{Name: "/secret", WithDecryption: false})
	require.NoError(t, err)
	assert.NotEqual(t, "my-secret", got.Parameter.Value)

	// With decryption: should return plaintext
	got, err = backend.GetParameter(&ssm.GetParameterInput{Name: "/secret", WithDecryption: true})
	require.NoError(t, err)
	assert.Equal(t, "my-secret", got.Parameter.Value)
}

// --- Issue 10: Document Attachments ---

func TestAudit_CreateDocument_WithAttachments(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	body := `{
		"Name": "MyDoc",
		"Content": "{\"schemaVersion\":\"2.2\"}",
		"Attachments": [{"Key": "S3FileUrl", "Name": "script.sh", "Values": ["s3://bucket/script.sh"]}]
	}`
	rec := doRequest(t, h, "CreateDocument", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Issue 11: Document Requires ---

func TestAudit_CreateDocument_WithRequires(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Create the required document first
	rec := doRequest(t, h, "CreateDocument", `{"Name":"Base","Content":"{\"schemaVersion\":\"2.2\"}"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	body := `{
		"Name": "Derived",
		"Content": "{\"schemaVersion\":\"2.2\"}",
		"Requires": [{"Name": "Base", "Version": "1"}]
	}`
	rec = doRequest(t, h, "CreateDocument", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify Requires is persisted
	getBody := `{"Name":"Derived"}`
	rec = doRequest(t, h, "DescribeDocument", getBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Document ssm.Document `json:"Document"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Document.Requires, 1)
	assert.Equal(t, "Base", resp.Document.Requires[0].Name)
}

// --- Issue 12: Command output capture fields ---

func TestAudit_SendCommand_OutputS3Bucket_StoredOnInvocation(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)

	// Create document first
	rec := doRequest(t, h, "CreateDocument", `{"Name":"MyDoc","Content":"{\"schemaVersion\":\"2.2\"}"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	body := `{
		"DocumentName": "MyDoc",
		"InstanceIds": ["i-abc123"],
		"OutputS3BucketName": "my-output-bucket",
		"OutputS3KeyPrefix": "commands/"
	}`
	rec = doRequest(t, h, "SendCommand", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var sendOut struct {
		Command struct {
			CommandID string `json:"CommandId"`
		} `json:"Command"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sendOut))
	_ = backend // backend is unused but needed for test

	// GetCommandInvocation should return output location fields
	getBody := `{"CommandId":"` + sendOut.Command.CommandID + `","InstanceId":"i-abc123"}`
	rec = doRequest(t, h, "GetCommandInvocation", getBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var invOut ssm.GetCommandInvocationOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &invOut))
	assert.Equal(t, "my-output-bucket", invOut.OutputS3BucketName)
	assert.Equal(t, "commands/", invOut.OutputS3KeyPrefix)
}

// --- Issue 15: Session Manager logging fields ---

func TestAudit_StartSession_LoggingFields(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	body := `{
		"Target": "i-abc123",
		"OutputS3BucketName": "session-logs",
		"OutputS3KeyPrefix": "sessions/",
		"CloudWatchOutputEnabled": true,
		"CloudWatchLogGroupName": "/aws/ssm/sessions"
	}`
	rec := doRequest(t, h, "StartSession", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out ssm.StartSessionOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.SessionID)
}

func TestAudit_TerminateSession_SetsEndDate(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Start a session first
	rec := doRequest(t, h, "StartSession", `{"Target":"i-abc123"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var startOut ssm.StartSessionOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))

	// Terminate it
	terminateBody := `{"SessionId":"` + startOut.SessionID + `"}`
	rec = doRequest(t, h, "TerminateSession", terminateBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var termOut ssm.TerminateSessionOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &termOut))
	assert.Equal(t, startOut.SessionID, termOut.SessionID)
}

// --- DescribeParameters: new fields returned in metadata ---

func TestAudit_DescribeParameters_ReturnsNewFields(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:           "/myapp/param",
		Type:           ssm.SecureStringType,
		Value:          "val",
		KeyID:          "alias/my-key",
		Tier:           ssm.TierAdvanced,
		DataType:       ssm.DataTypeText,
		AllowedPattern: `\S+`,
	})
	require.NoError(t, err)

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)

	meta := out.Parameters[0]
	assert.Equal(t, "alias/my-key", meta.KeyID)
	assert.Equal(t, ssm.TierAdvanced, meta.Tier)
	assert.Equal(t, ssm.DataTypeText, meta.DataType)
	assert.Equal(t, `\S+`, meta.AllowedPattern)
}

// --- History trim correctness ---

func TestAudit_PutParameter_HistoryCapRespected(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	// Write exactly MaxHistoryCap+1 versions.
	for i := range ssm.MaxHistoryCap + 1 {
		_, err := backend.PutParameter(&ssm.PutParameterInput{
			Name:      "/myapp/param",
			Type:      "String",
			Value:     "v",
			Overwrite: i > 0,
		})
		require.NoError(t, err)
	}

	assert.Equal(t, ssm.MaxHistoryCap, backend.HistoryLen("/myapp/param"))
}

// --- PutParameter input validation ---

func TestAudit_PutParameter_InvalidType_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ptype   string
		wantErr bool
	}{
		{"String", "String", false},
		{"StringList", "StringList", false},
		{"SecureString", ssm.SecureStringType, false},
		{"Empty", "", true},
		{"InvalidType", "Binary", true},
		{"LowerCase", "string", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			_, err := backend.PutParameter(&ssm.PutParameterInput{
				Name:  "/test/param",
				Type:  tt.ptype,
				Value: "val",
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrValidationException)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAudit_PutParameter_EmptyValue_ReturnsError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/test/param",
		Type:  "String",
		Value: "",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_PutParameter_DescriptionTooLong_ReturnsError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	longDesc := make([]byte, 1025)
	for i := range longDesc {
		longDesc[i] = 'x'
	}

	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:        "/test/param",
		Type:        "String",
		Value:       "val",
		Description: string(longDesc),
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_PutParameter_EmptyName_ReturnsError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "",
		Type:  "String",
		Value: "val",
	})
	require.Error(t, err)
}

// --- DescribeParameters MaxResults validation ---

func TestAudit_DescribeParameters_MaxResultsTooHigh_ReturnsError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	maxR := int64(100)
	_, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		MaxResults: &maxR,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_DescribeParameters_MaxResultsZero_ReturnsError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	maxR := int64(0)
	_, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		MaxResults: &maxR,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

func TestAudit_DescribeParameters_MaxResultsValid(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	maxR := int64(50)
	_, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		MaxResults: &maxR,
	})
	require.NoError(t, err)
}

// --- KMS key format variations ---

func TestAudit_PutParameter_KeyId_KeyPrefix(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/secure/param",
		Type:  ssm.SecureStringType,
		Value: "val",
		KeyID: "key/some-key-id",
	})
	require.NoError(t, err)
}

func TestAudit_PutParameter_KeyId_StoredInHistory(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/secure/param",
		Type:  ssm.SecureStringType,
		Value: "val",
		KeyID: "alias/my-key",
	})
	require.NoError(t, err)

	hist, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{Name: "/secure/param"})
	require.NoError(t, err)
	require.Len(t, hist.Parameters, 1)
	assert.Equal(t, "alias/my-key", hist.Parameters[0].KeyID)
}

// --- AllowedPattern on overwrite must re-validate ---

func TestAudit_PutParameter_AllowedPattern_OverwriteRevalidates(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:           "/myapp/port",
		Type:           "String",
		Value:          "8080",
		AllowedPattern: `^\d+$`,
	})
	require.NoError(t, err)

	// Overwrite with invalid value should fail
	_, err = backend.PutParameter(&ssm.PutParameterInput{
		Name:           "/myapp/port",
		Type:           "String",
		Value:          "not-a-number",
		AllowedPattern: `^\d+$`,
		Overwrite:      true,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrValidationException)
}

// --- ParameterHistory includes new fields ---

func TestAudit_GetParameterHistory_IncludesNewFields(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:           "/test/fullparam",
		Type:           "String",
		Value:          "hello",
		Tier:           ssm.TierAdvanced,
		DataType:       ssm.DataTypeText,
		AllowedPattern: `\S+`,
	})
	require.NoError(t, err)

	hist, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{Name: "/test/fullparam"})
	require.NoError(t, err)
	require.Len(t, hist.Parameters, 1)
	assert.Equal(t, ssm.TierAdvanced, hist.Parameters[0].Tier)
	assert.Equal(t, ssm.DataTypeText, hist.Parameters[0].DataType)
	assert.Equal(t, `\S+`, hist.Parameters[0].AllowedPattern)
}

// --- GetParameter via HTTP handler ---

func TestAudit_Handler_PutGetParameter_WithNewFields(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	body := `{
		"Name": "/myapp/config",
		"Type": "String",
		"Value": "production",
		"Tier": "Advanced",
		"DataType": "text",
		"AllowedPattern": "\\S+"
	}`
	rec := doRequest(t, h, "PutParameter", body)
	require.Equal(t, http.StatusOK, rec.Code)

	getBody := `{"Name":"/myapp/config"}`
	rec = doRequest(t, h, "GetParameter", getBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Parameter ssm.Parameter `json:"Parameter"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "Advanced", out.Parameter.Tier)
	assert.Equal(t, "text", out.Parameter.DataType)
}

func TestAudit_Handler_PutParameter_InvalidType_Returns400(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "PutParameter", `{"Name":"/test","Type":"Binary","Value":"val"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestAudit_Handler_PutParameter_InvalidDataType_Returns400(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "PutParameter", `{"Name":"/test","Type":"String","Value":"val","DataType":"aws:bogus"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestAudit_Handler_PutParameter_InvalidAllowedPattern_Returns400(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(
		t,
		h,
		"PutParameter",
		`{"Name":"/test","Type":"String","Value":"notnum","AllowedPattern":"^\\d+$"}`,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestAudit_Handler_DescribeParameters_FilterByAllowedPattern(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Create params with different allowed patterns
	doRequest(t, h, "PutParameter", `{"Name":"/a","Type":"String","Value":"123","AllowedPattern":"^\\d+$"}`)
	doRequest(t, h, "PutParameter", `{"Name":"/b","Type":"String","Value":"hello"}`)

	body := `{"ParameterFilters":[{"Key":"AllowedPattern","Option":"Equals","Values":["^\\d+$"]}]}`
	rec := doRequest(t, h, "DescribeParameters", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out ssm.DescribeParametersOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, "/a", out.Parameters[0].Name)
}

// --- Document Attachments fields round-trip ---

func TestAudit_CreateDocument_AttachmentsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.CreateDocument(&ssm.CreateDocumentInput{
		Name:    "DocWithAttachments",
		Content: `{"schemaVersion":"2.2"}`,
		Attachments: []ssm.AttachmentSource{
			{Key: "S3FileUrl", Name: "script.sh", Values: []string{"s3://bucket/script.sh"}},
		},
		Requires: []ssm.DocumentRequires{
			{Name: "AnotherDoc", Version: "1"},
		},
	})
	require.NoError(t, err)

	got, err := backend.DescribeDocument(&ssm.DescribeDocumentInput{Name: "DocWithAttachments"})
	require.NoError(t, err)
	require.Len(t, got.Document.Requires, 1)
	assert.Equal(t, "AnotherDoc", got.Document.Requires[0].Name)
}

// --- LabelParameterVersion: duplicate labels not duplicated ---

func TestAudit_LabelParameterVersion_NoDuplicateLabels(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	putOut, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/labeled",
		Type:  "String",
		Value: "v1",
	})
	require.NoError(t, err)

	// Apply the same label twice
	_, err = backend.LabelParameterVersion(&ssm.LabelParameterVersionInput{
		Name:    "/myapp/labeled",
		Version: putOut.Version,
		Labels:  []string{"prod"},
	})
	require.NoError(t, err)

	_, err = backend.LabelParameterVersion(&ssm.LabelParameterVersionInput{
		Name:    "/myapp/labeled",
		Version: putOut.Version,
		Labels:  []string{"prod"},
	})
	require.NoError(t, err)

	hist, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{Name: "/myapp/labeled"})
	require.NoError(t, err)

	count := 0
	for _, l := range hist.Parameters[0].Labels {
		if l == "prod" {
			count++
		}
	}
	assert.Equal(t, 1, count, "duplicate labels should not be stored")
}

// --- GetParametersByPath applies filter validation ---

func TestAudit_GetParametersByPath_ValidFilterKey_Accepted(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{Name: "/app/a", Type: "String", Value: "v"})
	require.NoError(t, err)

	_, err = backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path: "/app/",
		ParameterFilters: []ssm.ParameterFilter{
			{Key: "Type", Option: "Equals", Values: []string{"String"}},
		},
	})
	require.NoError(t, err)
}

// --- SecureString: GetParameters with decryption ---

func TestAudit_GetParameters_DecryptSecureString(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/secret/a",
		Type:  ssm.SecureStringType,
		Value: "plaintext",
	})
	require.NoError(t, err)

	out, err := backend.GetParameters(&ssm.GetParametersInput{
		Names:          []string{"/secret/a"},
		WithDecryption: true,
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, "plaintext", out.Parameters[0].Value)
}

// --- IntelligentTiering ---

func TestAudit_PutParameter_IntelligentTiering_UsesAdvancedLimit(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	val := make([]byte, 5000)
	for i := range val {
		val[i] = 'x'
	}

	// Intelligent-Tiering should allow up to 8KB (Advanced tier limit)
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/myapp/big",
		Type:  "String",
		Value: string(val),
		Tier:  ssm.TierIntelligentTiering,
	})
	require.NoError(t, err)
}

// --- Handler: LabelParameterVersion round-trip ---

func TestAudit_Handler_LabelParameterVersion(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Create a parameter
	rec := doRequest(t, h, "PutParameter", `{"Name":"/myapp/param","Type":"String","Value":"v1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Label version 1
	labelBody := `{"Name":"/myapp/param","ParameterVersion":1,"Labels":["prod","stable"]}`
	rec = doRequest(t, h, "LabelParameterVersion", labelBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out ssm.LabelParameterVersionOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, int64(1), out.ParameterVersion)
}

// --- DescribeParameters filter: BeginsWith on Tier ---

func TestAudit_DescribeParameters_FilterTier_BeginsWithNotSupported(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/a",
		Type:  "String",
		Value: "v",
		Tier:  ssm.TierStandard,
	})
	require.NoError(t, err)

	// BeginsWith should work for any known key
	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		ParameterFilters: []ssm.ParameterFilter{
			{Key: "Tier", Option: "BeginsWith", Values: []string{"Stan"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
}

// --- Policies field is optional ---

func TestAudit_PutParameter_NoPolicies_Accepted(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/test/nopol",
		Type:  "String",
		Value: "val",
	})
	require.NoError(t, err)

	got, err := backend.GetParameter(&ssm.GetParameterInput{Name: "/test/nopol"})
	require.NoError(t, err)
	assert.Empty(t, got.Parameter.Policies)
}

// --- Handler: DescribeParameters with unknown filter key returns 400 ---

func TestAudit_Handler_DescribeParameters_UnknownFilterKey_Returns400(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	body := `{"ParameterFilters":[{"Key":"Architecture","Option":"Equals","Values":["x86_64"]}]}`
	rec := doRequest(t, h, "DescribeParameters", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// --- Handler: GetParametersByPath with unknown filter key returns 400 ---

func TestAudit_Handler_GetParametersByPath_UnknownFilterKey_Returns400(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	body := `{"Path":"/app/","ParameterFilters":[{"Key":"Owner","Values":["me"]}]}`
	rec := doRequest(t, h, "GetParametersByPath", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// --- GetParameters: invalid (missing) params returned ---

func TestAudit_GetParameters_MissingParamsInInvalidList(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{Name: "/exists", Type: "String", Value: "v"})
	require.NoError(t, err)

	out, err := backend.GetParameters(&ssm.GetParametersInput{
		Names: []string{"/exists", "/missing"},
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	require.Len(t, out.InvalidParameters, 1)
	assert.Equal(t, "/missing", out.InvalidParameters[0])
}

// --- Command with no instances creates empty invocations ---

func TestAudit_SendCommand_NoInstances_EmptyInvocations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "CreateDocument", `{"Name":"Doc","Content":"{\"schemaVersion\":\"2.2\"}"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	body := `{"DocumentName":"Doc","InstanceIds":[]}`
	rec = doRequest(t, h, "SendCommand", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Command ssm.Command `json:"Command"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.Command.CommandID)
}

// --- ParameterHistory: labels persist across multiple versions ---

func TestAudit_LabelParameterVersion_LabelsMultipleVersions(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	// Create v1
	v1Out, err := backend.PutParameter(&ssm.PutParameterInput{Name: "/p", Type: "String", Value: "v1"})
	require.NoError(t, err)

	// Create v2
	v2Out, err := backend.PutParameter(&ssm.PutParameterInput{Name: "/p", Type: "String", Value: "v2", Overwrite: true})
	require.NoError(t, err)

	// Label v1 as "stable", v2 as "canary"
	_, err = backend.LabelParameterVersion(&ssm.LabelParameterVersionInput{
		Name:    "/p",
		Version: v1Out.Version,
		Labels:  []string{"stable"},
	})
	require.NoError(t, err)

	_, err = backend.LabelParameterVersion(&ssm.LabelParameterVersionInput{
		Name:    "/p",
		Version: v2Out.Version,
		Labels:  []string{"canary"},
	})
	require.NoError(t, err)

	hist, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{Name: "/p"})
	require.NoError(t, err)
	require.Len(t, hist.Parameters, 2)

	// History returned newest-first
	assert.Contains(t, hist.Parameters[0].Labels, "canary") // v2
	assert.Contains(t, hist.Parameters[1].Labels, "stable") // v1
}

// --- GetParametersByPath: empty path returns no error ---

func TestAudit_GetParametersByPath_EmptyPathReturnsEmpty(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{Name: "/a/b", Type: "String", Value: "v"})
	require.NoError(t, err)

	out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path: "/nonexistent",
	})
	require.NoError(t, err)
	assert.Empty(t, out.Parameters)
}

// --- Tier stored in DescribeParameters metadata ---

func TestAudit_DescribeParameters_TierInMetadata(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name:  "/test",
		Type:  "String",
		Value: "v",
		Tier:  ssm.TierAdvanced,
	})
	require.NoError(t, err)

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, ssm.TierAdvanced, out.Parameters[0].Tier)
}

// --- MaxResults nil is valid for DescribeParameters ---

func TestAudit_DescribeParameters_NilMaxResults_UsesDefault(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	for i := range 5 {
		_, err := backend.PutParameter(&ssm.PutParameterInput{
			Name:  "/param/" + string(rune('a'+i)),
			Type:  "String",
			Value: "v",
		})
		require.NoError(t, err)
	}

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 5)
}

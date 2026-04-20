package secretsmanager_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// TestRefinement2_ValidateResourcePolicy tests the ValidateResourcePolicy operation.
func TestRefinement2_ValidateResourcePolicy(t *testing.T) {
	t.Parallel()

	// validFullPolicy is a minimal well-formed IAM policy document.
	const validFullPolicy = `{"ResourcePolicy":` +
		`"{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",` +
		`\"Principal\":{\"AWS\":\"arn:aws:iam::123456789012:root\"},` +
		`\"Action\":\"secretsmanager:GetSecretValue\",\"Resource\":\"*\"}]}"}`

	tests := []struct {
		name         string
		body         string
		wantStatus   int
		wantPassed   bool
		wantErrCount int
	}{
		{
			name:         "valid_policy",
			body:         validFullPolicy,
			wantStatus:   http.StatusOK,
			wantPassed:   true,
			wantErrCount: 0,
		},
		{
			name:         "missing_version",
			body:         `{"ResourcePolicy":"{\"Statement\":[]}"}`,
			wantStatus:   http.StatusOK,
			wantPassed:   false,
			wantErrCount: 1,
		},
		{
			name:         "missing_statement",
			body:         `{"ResourcePolicy":"{\"Version\":\"2012-10-17\"}"}`,
			wantStatus:   http.StatusOK,
			wantPassed:   false,
			wantErrCount: 1,
		},
		{
			name:       "invalid_json_policy",
			body:       `{"ResourcePolicy":"not-json"}`,
			wantStatus: http.StatusOK,
			wantPassed: false,
		},
		{
			name:       "empty_policy",
			body:       `{"ResourcePolicy":""}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(b)
			rec := doR1Request(t, h, "secretsmanager.ValidateResourcePolicy", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out secretsmanager.ValidateResourcePolicyOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantPassed, out.PolicyValidationPassed)

				if tt.wantErrCount > 0 {
					assert.Len(t, out.ValidationErrors, tt.wantErrCount)
				}
			}
		})
	}
}

// TestRefinement2_ValidateResourcePolicySecretExists verifies secret-scoped validation.
func TestRefinement2_ValidateResourcePolicySecretExists(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create a secret first.
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"validate-test","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// validPolicy is a minimal well-formed IAM policy JSON string.
	const validPolicy = `{"Version":"2012-10-17","Statement":[{` +
		`"Effect":"Allow","Principal":{"AWS":"*"},` +
		`"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`
	body, _ := json.Marshal(map[string]string{
		"SecretId":       "validate-test",
		"ResourcePolicy": validPolicy,
	})

	rec = doR1Request(t, h, "secretsmanager.ValidateResourcePolicy", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ValidateResourcePolicyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.True(t, out.PolicyValidationPassed)
}

// TestRefinement2_LastAccessedDateTracking verifies GetSecretValue updates LastAccessedDate.
func TestRefinement2_LastAccessedDateTracking(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create secret.
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"access-tracked","SecretString":"data"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeSecret before access should have no LastAccessedDate.
	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"access-tracked"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc1 secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc1))
	assert.Nil(t, desc1.LastAccessedDate)

	// GetSecretValue should update LastAccessedDate.
	rec = doR1Request(t, h, "secretsmanager.GetSecretValue", `{"SecretId":"access-tracked"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeSecret after access should have LastAccessedDate set to today.
	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"access-tracked"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc2 secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc2))
	require.NotNil(t, desc2.LastAccessedDate)
	// The LastAccessedDate should be today (day-granularity as AWS does).
	accessTime := time.Unix(int64(*desc2.LastAccessedDate), 0).UTC()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	assert.Equal(t, today.Day(), accessTime.Day())
	assert.Equal(t, today.Month(), accessTime.Month())
}

// TestRefinement2_RecoveryWindowInDays verifies DeleteSecret honors RecoveryWindowInDays.
func TestRefinement2_RecoveryWindowInDays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkForward func(t *testing.T, deletionDate float64)
		name         string
		body         string
		wantStatus   int
		wantDeleted  bool
	}{
		{
			name:        "default_30_day_window",
			body:        `{"SecretId":"recover-test"}`,
			wantStatus:  http.StatusOK,
			wantDeleted: false,
			checkForward: func(t *testing.T, deletionDate float64) {
				t.Helper()
				// DeletionDate should be ~30 days from now.
				td := time.Unix(int64(deletionDate), 0).UTC()
				expected := time.Now().UTC().Add(30 * 24 * time.Hour)
				diff := td.Sub(expected).Abs()
				assert.Less(t, diff, 5*time.Second)
			},
		},
		{
			name:        "custom_7_day_window",
			body:        `{"SecretId":"recover-test2","RecoveryWindowInDays":7}`,
			wantStatus:  http.StatusOK,
			wantDeleted: false,
			checkForward: func(t *testing.T, deletionDate float64) {
				t.Helper()
				td := time.Unix(int64(deletionDate), 0).UTC()
				expected := time.Now().UTC().Add(7 * 24 * time.Hour)
				diff := td.Sub(expected).Abs()
				assert.Less(t, diff, 5*time.Second)
			},
		},
		{
			name:       "invalid_window_too_small",
			body:       `{"SecretId":"recover-test","RecoveryWindowInDays":5}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_window_too_large",
			body:       `{"SecretId":"recover-test","RecoveryWindowInDays":31}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(b)

			// Create the secret(s) needed for the test.
			for _, name := range []string{"recover-test", "recover-test2"} {
				body, _ := json.Marshal(map[string]string{"Name": name, "SecretString": "v"})
				rec := doR1Request(t, h, "secretsmanager.CreateSecret", string(body))
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doR1Request(t, h, "secretsmanager.DeleteSecret", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.checkForward != nil {
				var out secretsmanager.DeleteSecretOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.checkForward(t, out.DeletionDate)
			}
		})
	}
}

// TestRefinement2_SecretNameValidation verifies CreateSecret rejects invalid names.
func TestRefinement2_SecretNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		secretName string
		wantStatus int
	}{
		{
			name:       "valid_path_name",
			secretName: "prod/db/password",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_alphanumeric",
			secretName: "mySecret123",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_with_special_chars",
			secretName: "my-secret_name.v1@env=prod+extra",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_name",
			secretName: "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_space",
			secretName: "my secret",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_dollar",
			secretName: "my$secret",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_exclamation",
			secretName: "my!secret",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(b)

			body, _ := json.Marshal(map[string]string{"Name": tt.secretName, "SecretString": "v"})
			rec := doR1Request(t, h, "secretsmanager.CreateSecret", string(body))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_SecretListEntryHasRotationFields verifies ListSecrets returns rotation metadata.
func TestRefinement2_SecretListEntryHasRotationFields(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create a secret.
	const createBody = `{"Name":"list-fields-test","SecretString":"v","Description":"test desc"}`
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", createBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// List secrets.
	rec = doR1Request(t, h, "secretsmanager.ListSecrets", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.SecretList, 1)

	entry := out.SecretList[0]
	assert.Equal(t, "list-fields-test", entry.Name)
	assert.Equal(t, "test desc", entry.Description)
	// LastChangedDate should be set (created with value).
	assert.NotNil(t, entry.LastChangedDate)
}

// TestRefinement2_UpdateSecretKmsKeyID verifies UpdateSecret can change the KMS key.
func TestRefinement2_UpdateSecretKmsKeyID(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create a secret.
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"kms-update-test","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Update KMS key.
	rec = doR1Request(
		t,
		h,
		"secretsmanager.UpdateSecret",
		`{"SecretId":"kms-update-test","KmsKeyId":"arn:aws:kms:us-east-1:123456789012:key/test-key-id"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via DescribeSecret.
	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"kms-update-test"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/test-key-id", desc.KmsKeyID)
}

// TestRefinement2_DescribeSecretHasAllFields verifies DescribeSecret includes all new metadata.
func TestRefinement2_DescribeSecretHasAllFields(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create secret with KMS key.
	rec := doR1Request(
		t,
		h,
		"secretsmanager.CreateSecret",
		`{"Name":"full-meta","SecretString":"v","Description":"full metadata","KmsKeyId":"alias/mykey"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe it.
	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"full-meta"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.Equal(t, "full-meta", desc.Name)
	assert.Equal(t, "full metadata", desc.Description)
	assert.Equal(t, "alias/mykey", desc.KmsKeyID)
	assert.NotNil(t, desc.LastChangedDate)
}

// TestRefinement2_RandomRunesOptimized verifies password generation still works with the optimized randomRunes.
func TestRefinement2_RandomRunesOptimized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		minLen int
	}{
		{
			name:   "default_length",
			body:   `{}`,
			minLen: 32,
		},
		{
			name:   "custom_length_64",
			body:   `{"PasswordLength":64}`,
			minLen: 64,
		},
		{
			name:   "custom_length_128",
			body:   `{"PasswordLength":128}`,
			minLen: 128,
		},
		{
			name:   "no_special_chars",
			body:   `{"ExcludePunctuation":true,"PasswordLength":20}`,
			minLen: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(b)
			rec := doR1Request(t, h, "secretsmanager.GetRandomPassword", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				RandomPassword string `json:"RandomPassword"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.GreaterOrEqual(t, len(out.RandomPassword), tt.minLen)
		})
	}
}

// TestRefinement2_SecretInCreateDescribeRoundtrip verifies a full create → describe cycle
// with all metadata fields populated correctly.
func TestRefinement2_SecretInCreateDescribeRoundtrip(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	createBody := map[string]any{
		"Name":         "roundtrip/secret",
		"Description":  "roundtrip test",
		"SecretString": "roundtrip-value",
		"KmsKeyId":     "alias/roundtrip",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "test"},
		},
	}

	body, _ := json.Marshal(createBody)
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"roundtrip/secret"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.Equal(t, "roundtrip/secret", desc.Name)
	assert.Equal(t, "roundtrip test", desc.Description)
	assert.Equal(t, "alias/roundtrip", desc.KmsKeyID)
	assert.NotNil(t, desc.LastChangedDate)
	assert.False(t, desc.RotationEnabled)
	assert.Contains(t, desc.ARN, "secret:roundtrip/secret-")
}

// TestRefinement2_PutSecretValueIdempotency verifies that re-sending the same
// ClientRequestToken and identical content returns the existing version.
func TestRefinement2_PutSecretValueIdempotency(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"idempotent","SecretString":"initial"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	const token = "550e8400-e29b-41d4-a716-446655440000"
	const putBody = `{"SecretId":"idempotent","SecretString":"new-value","ClientRequestToken":"` + token + `"}`

	// First put: creates a new version.
	rec = doR1Request(t, h, "secretsmanager.PutSecretValue", putBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out1 secretsmanager.PutSecretValueOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out1))
	assert.Equal(t, token, out1.VersionID)

	// Second put with same token + content: should return the existing version.
	rec = doR1Request(t, h, "secretsmanager.PutSecretValue", putBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out2 secretsmanager.PutSecretValueOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out2))
	assert.Equal(t, token, out2.VersionID)
	assert.Equal(t, out1.VersionID, out2.VersionID)
}

// TestRefinement2_DeleteSecretAlreadyDeleted verifies that deleting a secret that is
// already pending deletion returns an error.
func TestRefinement2_DeleteSecretAlreadyDeleted(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"double-delete","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// First delete: should succeed.
	rec = doR1Request(t, h, "secretsmanager.DeleteSecret", `{"SecretId":"double-delete"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Second delete: should fail.
	rec = doR1Request(t, h, "secretsmanager.DeleteSecret", `{"SecretId":"double-delete"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement2_ListSecretsSortOrder verifies SortOrder desc reverses the list.
func TestRefinement2_ListSecretsSortOrder(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		rec := doR1Request(t, h, "secretsmanager.CreateSecret",
			`{"Name":"`+name+`","SecretString":"v"}`)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doR1Request(t, h, "secretsmanager.ListSecrets", `{"SortOrder":"desc"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.SecretList, 3)
	assert.Equal(t, "gamma", out.SecretList[0].Name)
	assert.Equal(t, "alpha", out.SecretList[2].Name)
}

// TestRefinement2_ListSecretsFilterAll verifies the "all" filter key matches any field.
func TestRefinement2_ListSecretsFilterAll(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	body, _ := json.Marshal(map[string]any{
		"Name":         "findme/secret",
		"Description":  "unique-token",
		"SecretString": "v",
	})
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"other","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Filter by description prefix via the "all" key.
	filterBody := `{"Filters":[{"Key":"all","Values":["unique-token"]}]}`
	rec = doR1Request(t, h, "secretsmanager.ListSecrets", filterBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.SecretList, 1)
	assert.Equal(t, "findme/secret", out.SecretList[0].Name)
}

// TestRefinement2_SecretListEntryHasCreatedAndRotatedDate verifies that CreatedDate and
// LastRotatedDate are returned in ListSecrets entries.
func TestRefinement2_SecretListEntryHasCreatedAndRotatedDate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"datechecks","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.RotateSecret", `{"SecretId":"datechecks"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.ListSecrets", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.SecretList, 1)
	assert.NotNil(t, out.SecretList[0].CreatedDate)
	assert.NotNil(t, out.SecretList[0].LastRotatedDate)
}

// TestRefinement2_DescribeSecretHasCreatedDate verifies DescribeSecret returns CreatedDate.
func TestRefinement2_DescribeSecretHasCreatedDate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"createdate-test","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.DescribeSecret",
		`{"SecretId":"createdate-test"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc secretsmanager.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.NotNil(t, desc.CreatedDate)
	assert.Greater(t, *desc.CreatedDate, float64(0))
}

// TestRefinement2_RotateSecretInvalidDays verifies that AutomaticallyAfterDays outside
// the 1-365 range is rejected.
func TestRefinement2_RotateSecretInvalidDays(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"days-validation","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		name string
		days int
	}{
		{name: "zero_days", days: 0},
		{name: "too_many_days", days: 366},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rotBody, _ := json.Marshal(map[string]any{
				"SecretId":      "days-validation",
				"RotationRules": map[string]any{"AutomaticallyAfterDays": tt.days},
			})
			rotRec := doR1Request(t, h, "secretsmanager.RotateSecret", string(rotBody))
			assert.Equal(t, http.StatusBadRequest, rotRec.Code)
		})
	}
}

// TestRefinement2_GetSecretValueVersionStageMismatch verifies that providing both
// VersionId and a mismatched VersionStage returns an error.
func TestRefinement2_GetSecretValueVersionStageMismatch(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"stage-mismatch","SecretString":"v","ClientRequestToken":"abc-ver-1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	body, _ := json.Marshal(map[string]any{
		"SecretId":     "stage-mismatch",
		"VersionId":    "abc-ver-1",
		"VersionStage": "AWSPREVIOUS", // abc-ver-1 is AWSCURRENT, not AWSPREVIOUS
	})
	rec = doR1Request(t, h, "secretsmanager.GetSecretValue", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement2_RestoreActiveSecretFails verifies that RestoreSecret on a non-deleted
// (active) secret returns an error.
func TestRefinement2_RestoreActiveSecretFails(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"active-restore","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Attempt to restore an active (non-deleted) secret – should fail.
	rec = doR1Request(t, h, "secretsmanager.RestoreSecret",
		`{"SecretId":"active-restore"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement2_UpdateSecretClientRequestTokenIdempotency verifies that UpdateSecret
// with the same ClientRequestToken and content is idempotent.
func TestRefinement2_UpdateSecretClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"update-idempotent","SecretString":"initial"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	const token = "550e8400-e29b-41d4-a716-446655440001"
	body, _ := json.Marshal(map[string]any{
		"SecretId":           "update-idempotent",
		"SecretString":       "updated-value",
		"ClientRequestToken": token,
	})

	rec = doR1Request(t, h, "secretsmanager.UpdateSecret", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	var out1 secretsmanager.UpdateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out1))

	rec = doR1Request(t, h, "secretsmanager.UpdateSecret", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	var out2 secretsmanager.UpdateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out2))

	assert.Equal(t, out1.VersionID, out2.VersionID)
}

package stepfunctions_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// ─── Error Classification ────────────────────────────────────────────────────

func TestClassifyError_InvalidExecutionInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "input_within_limit_succeeds",
			input:    `{}`,
			wantCode: http.StatusOK,
			wantErr:  false,
		},
		{
			name:     "input_over_256kib_returns_400",
			input:    `{"data":"` + strings.Repeat("x", 256*1024+1) + `"}`,
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "input-limit-sm-"+tt.name)

			body, err := json.Marshal(map[string]string{
				"stateMachineArn": smARN,
				"input":           tt.input,
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "StartExecution", string(body))

			if tt.wantErr {
				assert.Equal(t, tt.wantCode, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "InvalidExecutionInput", resp["__type"])
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestClassifyError_InvalidArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		roleArn  string
		wantCode int
	}{
		{
			name:     "valid_role_arn",
			roleArn:  "arn:aws:iam::123456789012:role/sfn-role",
			wantCode: http.StatusOK,
		},
		{
			name:     "non_arn_prefix_returns_400",
			roleArn:  "not-an-arn",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "arn_with_whitespace_returns_400",
			roleArn:  "arn:aws:iam::123456 789012:role/sfn-role",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			body, err := json.Marshal(map[string]string{
				"name":       "arn-check-sm-" + tt.name,
				"definition": sfnPassDefinition,
				"roleArn":    tt.roleArn,
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "InvalidArn", resp["__type"])
			}
		})
	}
}

// ─── Name Validation ─────────────────────────────────────────────────────────

func TestStateMachineName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		smName   string
		wantCode int
	}{
		{
			name:     "valid_alphanumeric",
			smName:   "my-state-machine",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid_with_allowed_specials",
			smName:   "sfn_test.flow@prod",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid_max_length",
			smName:   strings.Repeat("a", 80),
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_name_rejected",
			smName:   "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "too_long_name_rejected",
			smName:   strings.Repeat("a", 81),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_chars_rejected",
			smName:   "bad<name>here",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			body, err := json.Marshal(map[string]string{
				"name":       tt.smName,
				"definition": sfnPassDefinition,
				"roleArn":    "arn:aws:iam::123456789012:role/role",
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "InvalidName", resp["__type"])
			}
		})
	}
}

func TestExecutionName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		execName string
		wantCode int
	}{
		{
			name:     "valid_name",
			execName: "my-execution-1",
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_name_allowed",
			execName: "",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid_max_length",
			execName: strings.Repeat("b", 80),
			wantCode: http.StatusOK,
		},
		{
			name:     "too_long_name_rejected",
			execName: strings.Repeat("b", 81),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_chars_rejected",
			execName: "bad<exec>",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "exec-name-sm-"+tt.name)

			body, err := json.Marshal(map[string]string{
				"stateMachineArn": smARN,
				"name":            tt.execName,
				"input":           "{}",
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "StartExecution", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestActivityName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		actName  string
		wantCode int
	}{
		{
			name:     "valid_name",
			actName:  "my-activity",
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_name_rejected",
			actName:  "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "too_long_name_rejected",
			actName:  strings.Repeat("c", 81),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_chars_rejected",
			actName:  "bad|name",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			body, err := json.Marshal(map[string]string{"name": tt.actName})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateActivity", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ─── Tags in CreateStateMachine ──────────────────────────────────────────────

func TestCreateStateMachine_InlineTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags map[string]string
		name     string
		tags     []map[string]string
	}{
		{
			name:     "no_inline_tags",
			tags:     nil,
			wantTags: map[string]string{},
		},
		{
			name: "single_inline_tag",
			tags: []map[string]string{
				{"key": "env", "value": "test"},
			},
			wantTags: map[string]string{"env": "test"},
		},
		{
			name: "multiple_inline_tags",
			tags: []map[string]string{
				{"key": "env", "value": "prod"},
				{"key": "owner", "value": "team"},
			},
			wantTags: map[string]string{"env": "prod", "owner": "team"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			reqBody := map[string]any{
				"name":       "tagged-sm-" + tt.name,
				"definition": sfnPassDefinition,
				"roleArn":    "arn:aws:iam::123456789012:role/role",
			}
			if tt.tags != nil {
				reqBody["tags"] = tt.tags
			}

			body, err := json.Marshal(reqBody)
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			smARN, _ := createResp["stateMachineArn"].(string)
			require.NotEmpty(t, smARN)

			// Verify tags were applied.
			listBody, _ := json.Marshal(map[string]string{"resourceArn": smARN})
			listRec := sfnPost(ctx, t, h, e, "ListTagsForResource", string(listBody))
			require.Equal(t, http.StatusOK, listRec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsResp))

			rawTags, _ := tagsResp["tags"].([]any)
			got := make(map[string]string, len(rawTags))
			for _, rt := range rawTags {
				m, _ := rt.(map[string]any)
				k, _ := m["key"].(string)
				v, _ := m["value"].(string)
				got[k] = v
			}
			assert.Equal(t, tt.wantTags, got)
		})
	}
}

// ─── Encryption Configuration ────────────────────────────────────────────────

func TestCreateStateMachine_EncryptionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encCfg   map[string]any
		name     string
		wantType string
	}{
		{
			name:     "no_encryption_config",
			encCfg:   nil,
			wantType: "",
		},
		{
			name: "customer_managed_key",
			encCfg: map[string]any{
				"type":     "CUSTOMER_MANAGED_KMS_KEY",
				"kmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/test-key",
			},
			wantType: "CUSTOMER_MANAGED_KMS_KEY",
		},
		{
			name: "aws_owned_key",
			encCfg: map[string]any{
				"type": "AWS_OWNED_KEY",
			},
			wantType: "AWS_OWNED_KEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			reqBody := map[string]any{
				"name":       "enc-sm-" + tt.name,
				"definition": sfnPassDefinition,
				"roleArn":    "arn:aws:iam::123456789012:role/role",
			}
			if tt.encCfg != nil {
				reqBody["encryptionConfiguration"] = tt.encCfg
			}

			body, err := json.Marshal(reqBody)
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			smARN, _ := createResp["stateMachineArn"].(string)
			require.NotEmpty(t, smARN)

			// Describe to verify encryption config is stored.
			descBody, _ := json.Marshal(map[string]string{"stateMachineArn": smARN})
			descRec := sfnPost(ctx, t, h, e, "DescribeStateMachine", string(descBody))
			require.Equal(t, http.StatusOK, descRec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))

			if tt.wantType == "" {
				// No encryption config present in response or nil.
				enc, ok := descResp["encryptionConfiguration"]
				assert.True(t, !ok || enc == nil, "expected no encryptionConfiguration, got %v", enc)
			} else {
				enc, ok := descResp["encryptionConfiguration"].(map[string]any)
				require.True(t, ok, "expected encryptionConfiguration in response")
				assert.Equal(t, tt.wantType, enc["type"])
			}
		})
	}
}

func TestUpdateStateMachine_EncryptionConfiguration(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "enc-update-sm")

	// Update with encryption config.
	body, err := json.Marshal(map[string]any{
		"stateMachineArn": smARN,
		"encryptionConfiguration": map[string]string{
			"type":     "CUSTOMER_MANAGED_KMS_KEY",
			"kmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/my-key",
		},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "UpdateStateMachine", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify via describe.
	descBody, _ := json.Marshal(map[string]string{"stateMachineArn": smARN})
	descRec := sfnPost(ctx, t, h, e, "DescribeStateMachine", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	enc, ok := descResp["encryptionConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "CUSTOMER_MANAGED_KMS_KEY", enc["type"])
}

// ─── Publish on CreateStateMachine / UpdateStateMachine ─────────────────────

func TestCreateStateMachine_WithPublish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		publish     bool
		wantVersion bool
	}{
		{
			name:        "publish_false_no_version",
			publish:     false,
			wantVersion: false,
		},
		{
			name:        "publish_true_creates_version",
			publish:     true,
			wantVersion: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			body, err := json.Marshal(map[string]any{
				"name":       "pub-sm-" + tt.name,
				"definition": sfnPassDefinition,
				"roleArn":    "arn:aws:iam::123456789012:role/role",
				"publish":    tt.publish,
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			smARN, _ := createResp["stateMachineArn"].(string)
			require.NotEmpty(t, smARN)

			// Check versions.
			versBody, _ := json.Marshal(map[string]string{"stateMachineArn": smARN})
			versRec := sfnPost(ctx, t, h, e, "ListStateMachineVersions", string(versBody))
			require.Equal(t, http.StatusOK, versRec.Code)

			var versResp map[string]any
			require.NoError(t, json.Unmarshal(versRec.Body.Bytes(), &versResp))
			versions, _ := versResp["stateMachineVersions"].([]any)

			if tt.wantVersion {
				assert.Len(t, versions, 1, "expected one version after publish=true")
			} else {
				assert.Empty(t, versions, "expected no versions when publish=false")
			}
		})
	}
}

func TestUpdateStateMachine_WithPublish(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "pub-update-sm")

	// Update with publish=true.
	newDef := `{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`
	body, err := json.Marshal(map[string]any{
		"stateMachineArn": smARN,
		"definition":      newDef,
		"publish":         true,
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "UpdateStateMachine", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	versBody, _ := json.Marshal(map[string]string{"stateMachineArn": smARN})
	versRec := sfnPost(ctx, t, h, e, "ListStateMachineVersions", string(versBody))
	require.Equal(t, http.StatusOK, versRec.Code)

	var versResp map[string]any
	require.NoError(t, json.Unmarshal(versRec.Body.Bytes(), &versResp))
	versions, _ := versResp["stateMachineVersions"].([]any)
	assert.Len(t, versions, 1, "expected one version after update with publish=true")
}

// ─── RedriveExecution with Count ─────────────────────────────────────────────

func TestRedriveExecution_RedriveCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		redrives         int
		wantRedriveCount int
	}{
		{
			name:             "single_redrive",
			redrives:         1,
			wantRedriveCount: 1,
		},
		{
			name:             "double_redrive",
			redrives:         2,
			wantRedriveCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			failDef := `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Err","Cause":"test"}}}`
			sm, err := b.CreateStateMachine("redrive-sm-"+tt.name, failDef, "arn:role", "STANDARD")
			require.NoError(t, err)

			exec, err := b.StartExecution(sm.StateMachineArn, "redrive-exec-"+tt.name, `{}`)
			require.NoError(t, err)

			// Wait for failure.
			require.Eventually(t, func() bool {
				d, e := b.DescribeExecution(exec.ExecutionArn)

				return e == nil && d.Status == "FAILED"
			}, 5*time.Second, 50*time.Millisecond)

			// Perform redrives.
			for range tt.redrives {
				require.Eventually(t, func() bool {
					d, e := b.DescribeExecution(exec.ExecutionArn)

					return e == nil && d.Status == "FAILED"
				}, 5*time.Second, 50*time.Millisecond)

				_, redriveErr := b.RedriveExecution(exec.ExecutionArn)
				require.NoError(t, redriveErr)
			}

			// Wait for final completion.
			require.Eventually(t, func() bool {
				d, e := b.DescribeExecution(exec.ExecutionArn)

				return e == nil && d.Status != "RUNNING"
			}, 5*time.Second, 50*time.Millisecond)

			described, err := b.DescribeExecution(exec.ExecutionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRedriveCount, described.RedriveCount)
			assert.NotNil(t, described.RedriveDate)
		})
	}
}

// ─── Backend Name Validation ─────────────────────────────────────────────────

func TestBackend_ValidateName_StateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		smName  string
	}{
		{
			name:    "valid_name",
			smName:  "good-sm",
			wantErr: nil,
		},
		{
			name:    "name_with_space_allowed",
			smName:  "my sm",
			wantErr: nil,
		},
		{
			name:    "empty_name_invalid",
			smName:  "",
			wantErr: stepfunctions.ErrInvalidName,
		},
		{
			name:    "name_too_long_invalid",
			smName:  strings.Repeat("x", 81),
			wantErr: stepfunctions.ErrInvalidName,
		},
		{
			name:    "name_with_dollar_invalid",
			smName:  "my$sm",
			wantErr: stepfunctions.ErrInvalidName,
		},
		{
			name:    "name_with_bracket_invalid",
			smName:  "my[sm]",
			wantErr: stepfunctions.ErrInvalidName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			_, err := b.CreateStateMachine(
				tt.smName,
				sfnPassDefinition,
				"arn:aws:iam::123456789012:role/role",
				"STANDARD",
			)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_ValidateName_Execution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		execName string
	}{
		{
			name:     "valid_name",
			execName: "good-exec",
			wantErr:  nil,
		},
		{
			name:     "empty_name_allowed",
			execName: "",
			wantErr:  nil,
		},
		{
			name:     "name_too_long",
			execName: strings.Repeat("e", 81),
			wantErr:  stepfunctions.ErrInvalidName,
		},
		{
			name:     "name_with_invalid_chars",
			execName: "bad<exec>",
			wantErr:  stepfunctions.ErrInvalidName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			smName := "name-val-sm-" + tt.name[:min(len(tt.name), 20)]
			sm, err := b.CreateStateMachine(smName, sfnPassDefinition, "arn:role", "STANDARD")
			require.NoError(t, err)

			_, err = b.StartExecution(sm.StateMachineArn, tt.execName, `{}`)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_ValidateName_Activity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		actName string
	}{
		{
			name:    "valid_name",
			actName: "my-activity",
			wantErr: nil,
		},
		{
			name:    "empty_name_invalid",
			actName: "",
			wantErr: stepfunctions.ErrInvalidName,
		},
		{
			name:    "name_too_long_invalid",
			actName: strings.Repeat("a", 81),
			wantErr: stepfunctions.ErrInvalidName,
		},
		{
			name:    "name_with_invalid_chars",
			actName: "bad#activity",
			wantErr: stepfunctions.ErrInvalidName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			_, err := b.CreateActivity(tt.actName)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ─── SetStateMachineConfigurations with Encryption ───────────────────────────

func TestSetStateMachineConfigurations_Encryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encryption *stepfunctions.EncryptionConfiguration
		name       string
		wantType   string
	}{
		{
			name:       "nil_encryption_no_change",
			encryption: nil,
			wantType:   "",
		},
		{
			name: "set_customer_managed_key",
			encryption: &stepfunctions.EncryptionConfiguration{
				Type:     "CUSTOMER_MANAGED_KMS_KEY",
				KMSKeyID: "arn:aws:kms:us-east-1:123456789012:key/test",
			},
			wantType: "CUSTOMER_MANAGED_KMS_KEY",
		},
		{
			name: "set_aws_owned_key",
			encryption: &stepfunctions.EncryptionConfiguration{
				Type: "AWS_OWNED_KEY",
			},
			wantType: "AWS_OWNED_KEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			sm, err := b.CreateStateMachine("enc-cfg-sm-"+tt.name, sfnPassDefinition, "arn:role", "STANDARD")
			require.NoError(t, err)

			err = b.SetStateMachineConfigurations(sm.StateMachineArn, nil, nil, tt.encryption)
			require.NoError(t, err)

			described, err := b.DescribeStateMachine(sm.StateMachineArn)
			require.NoError(t, err)

			if tt.wantType == "" {
				assert.Nil(t, described.EncryptionConfiguration)
			} else {
				require.NotNil(t, described.EncryptionConfiguration)
				assert.Equal(t, tt.wantType, described.EncryptionConfiguration.Type)
			}
		})
	}
}

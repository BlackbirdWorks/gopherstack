package awsconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigHandler_DeleteConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{
						"name":    "default",
						"roleARN": "arn:aws:iam::000000000000:role/config",
					},
				})
			},
			body:     map[string]any{"ConfigurationRecorderName": "default"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigurationRecorderName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteConfigurationRecorder", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRecorderRecordingGroupRoundtrip verifies RecordingGroup is stored and returned.
func TestRecorderRecordingGroupRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{
			"name":    "default",
			"roleARN": "arn:aws:iam::123456789012:role/config",
			"recordingGroup": map[string]any{
				"allSupported":               true,
				"includeGlobalResourceTypes": true,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAWSConfigRequest(t, h, "DescribeConfigurationRecorders", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConfigurationRecorders []struct {
			RecordingGroup *struct {
				AllSupported               bool `json:"allSupported"`
				IncludeGlobalResourceTypes bool `json:"includeGlobalResourceTypes"`
			} `json:"recordingGroup"`
		} `json:"ConfigurationRecorders"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConfigurationRecorders, 1)
	require.NotNil(t, out.ConfigurationRecorders[0].RecordingGroup)
	assert.True(t, out.ConfigurationRecorders[0].RecordingGroup.AllSupported)
	assert.True(t, out.ConfigurationRecorders[0].RecordingGroup.IncludeGlobalResourceTypes)
}

// TestRecorderStatusLastStatus verifies DescribeConfigurationRecorderStatus returns lastStatus.
func TestRecorderStatusLastStatus(t *testing.T) {
	t.Parallel()

	b := newTestAWSConfigHandler(t).Backend
	require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::123:role/r", nil))
	require.NoError(t, b.PutDeliveryChannel("default", "my-bucket", "", "", nil))

	statusBefore := b.DescribeConfigurationRecorderStatus(nil)
	require.Len(t, statusBefore, 1)
	assert.Equal(t, "PENDING", statusBefore[0].LastStatus)
	assert.False(t, statusBefore[0].Recording)

	require.NoError(t, b.StartConfigurationRecorder("default"))

	statusAfter := b.DescribeConfigurationRecorderStatus(nil)
	require.Len(t, statusAfter, 1)
	assert.Equal(t, "SUCCESS", statusAfter[0].LastStatus)
	assert.True(t, statusAfter[0].Recording)
}

// TestListConfigurationRecordersSummaries verifies ListConfigurationRecorders returns summaries.
func TestListConfigurationRecordersSummaries(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	b := h.Backend
	require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::123:role/r", nil))

	rec := doAWSConfigRequest(t, h, "ListConfigurationRecorders", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConfigurationRecorderSummaries []struct {
			Arn            string `json:"arn"`
			Name           string `json:"name"`
			RecordingScope string `json:"recordingScope"`
		} `json:"ConfigurationRecorderSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConfigurationRecorderSummaries, 1)
	assert.Equal(t, "default", out.ConfigurationRecorderSummaries[0].Name)
	assert.Contains(t, out.ConfigurationRecorderSummaries[0].Arn, "arn:aws:config:")
	assert.Equal(t, "INTERNAL", out.ConfigurationRecorderSummaries[0].RecordingScope)
}

func TestAWSConfigHandler_PutConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"ConfigurationRecorder": map[string]any{
					"name":    "default",
					"roleARN": "arn:aws:iam::000000000000:role/config",
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "PutConfigurationRecorder", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DescribeConfigurationRecorders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *awsconfig.Handler)
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "with_recorder",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{
						"name":    "default",
						"roleARN": "arn:aws:iam::000000000000:role/config",
					},
				})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ConfigurationRecorders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DescribeConfigurationRecorders", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestAWSConfigHandler_StartConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(t *testing.T, h *awsconfig.Handler)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{
						"name":    "default",
						"roleARN": "arn:aws:iam::000000000000:role/config",
					},
				})
				doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
					"DeliveryChannel": map[string]any{
						"name":         "default",
						"s3BucketName": "my-bucket",
					},
				})
			},
			body:     map[string]any{"ConfigurationRecorderName": "default"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigurationRecorderName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "StartConfigurationRecorder", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DescribeConfigurationRecorderStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(t *testing.T, h *awsconfig.Handler)
		name             string
		wantCode         int
		wantRecordingLen int
		wantRecording    bool
	}{
		{
			name:             "empty_returns_empty_list",
			wantCode:         http.StatusOK,
			wantRecordingLen: 0,
		},
		{
			name: "pending_recorder_is_not_recording",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": "arn:aws:iam::123:role/r"},
				})
			},
			wantCode:         http.StatusOK,
			wantRecordingLen: 1,
			wantRecording:    false,
		},
		{
			name: "active_recorder_is_recording",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": "arn:aws:iam::123:role/r"},
				})
				doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
					"DeliveryChannel": map[string]any{"name": "default", "s3BucketName": "my-bucket"},
				})
				doAWSConfigRequest(t, h, "StartConfigurationRecorder", map[string]any{
					"ConfigurationRecorderName": "default",
				})
			},
			wantCode:         http.StatusOK,
			wantRecordingLen: 1,
			wantRecording:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DescribeConfigurationRecorderStatus", map[string]any{})

			assert.Equal(t, tt.wantCode, rec.Code)

			var out struct {
				ConfigurationRecordersStatus []struct {
					Name      string `json:"name"`
					Recording bool   `json:"recording"`
				} `json:"ConfigurationRecordersStatus"`
			}

			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.ConfigurationRecordersStatus, tt.wantRecordingLen)

			if tt.wantRecordingLen > 0 {
				assert.Equal(t, tt.wantRecording, out.ConfigurationRecordersStatus[0].Recording)
			}
		})
	}
}

func TestAWSConfigHandler_AssociateResourceTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_with_arn",
			body: map[string]any{
				"ConfigurationRecorderArn": "arn:aws:config:us-east-1:000000000000:config-recorder/default",
				"ResourceTypes":            []string{"AWS::EC2::Instance"},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ConfigurationRecorder", "AWS::EC2::Instance"},
		},
		{
			name: "empty_resource_types",
			body: map[string]any{
				"ConfigurationRecorderArn": "arn:aws:config:us-east-1:000000000000:config-recorder/default",
				"ResourceTypes":            []string{},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ConfigurationRecorder"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			require.NoError(t, h.Backend.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/r", nil))

			rec := doAWSConfigRequest(t, h, "AssociateResourceTypes", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestAWSConfigHandler_AssociateResourceTypes_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "AssociateResourceTypes", map[string]any{
		"ConfigurationRecorderArn": "arn:aws:config:us-east-1:000000000000:config-recorder/unknown",
		"ResourceTypes":            []string{"AWS::EC2::Instance"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchConfigurationRecorderException")
}

func TestAWSConfigHandler_StopConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigurationRecorder("default", "arn:aws:iam::000:role/r", nil))
				require.NoError(t, h.Backend.PutDeliveryChannel("default", "my-bucket", "", "", nil))
				require.NoError(t, h.Backend.StartConfigurationRecorder("default"))
			},
			body:     map[string]any{"ConfigurationRecorderName": "default"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigurationRecorderName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "empty_name_returns_400",
			body:     map[string]any{"ConfigurationRecorderName": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "StopConfigurationRecorder", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_PutConfigurationRecorder_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantWire string
		wantCode int
	}{
		{
			name: "empty_name_returns_400",
			body: map[string]any{
				"ConfigurationRecorder": map[string]any{"name": "", "roleARN": "arn:aws:iam::000:role/r"},
			},
			wantCode: http.StatusBadRequest,
			wantWire: "InvalidConfigurationRecorderNameException",
		},
		{
			name: "empty_role_arn_returns_400",
			body: map[string]any{
				"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": ""},
			},
			wantCode: http.StatusBadRequest,
			wantWire: "InvalidRoleException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "PutConfigurationRecorder", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantWire)
		})
	}
}

func TestAWSConfigHandler_DescribeConfigurationRecorders_NameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "no_filter_returns_all",
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:      "filter_one_recorder",
			body:      map[string]any{"ConfigurationRecorderNames": []string{"rec-a"}},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
		{
			name:      "filter_nonexistent",
			body:      map[string]any{"ConfigurationRecorderNames": []string{"no-such"}},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			require.NoError(t, h.Backend.PutConfigurationRecorder("rec-a", "arn:aws:iam::123:role/r", nil))
			require.NoError(t, h.Backend.PutConfigurationRecorder("rec-b", "arn:aws:iam::123:role/r", nil))

			rec := doAWSConfigRequest(t, h, "DescribeConfigurationRecorders", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var out map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			var recorders []any
			require.NoError(t, json.Unmarshal(out["ConfigurationRecorders"], &recorders))
			assert.Len(t, recorders, tt.wantCount)
		})
	}
}

func TestAWSConfigHandler_AssociateResourceTypes_EmptyARN(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "AssociateResourceTypes", map[string]any{
		"ConfigurationRecorderArn": "",
		"ResourceTypes":            []string{"AWS::EC2::Instance"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestAWSConfigHandler_ServiceLinkedConfigurationRecorder(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)

	putRec := doAWSConfigRequest(t, h, "PutServiceLinkedConfigurationRecorder", map[string]any{
		"ServicePrincipal": "guardduty.amazonaws.com",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putOut struct {
		Arn  string `json:"Arn"`
		Name string `json:"Name"`
	}
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
	assert.Equal(t, "AWSConfigurationRecorderForGuardduty", putOut.Name)
	assert.NotEmpty(t, putOut.Arn)

	delRec := doAWSConfigRequest(t, h, "DeleteServiceLinkedConfigurationRecorder", map[string]any{
		"ServicePrincipal": "guardduty.amazonaws.com",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	var delOut struct {
		Arn  string `json:"Arn"`
		Name string `json:"Name"`
	}
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delOut))
	assert.Equal(t, putOut.Name, delOut.Name)

	// A second delete now 404s (NoSuchConfigurationRecorderException).
	delAgain := doAWSConfigRequest(t, h, "DeleteServiceLinkedConfigurationRecorder", map[string]any{
		"ServicePrincipal": "guardduty.amazonaws.com",
	})
	assert.Equal(t, http.StatusNotFound, delAgain.Code)
}

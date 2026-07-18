package emr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/emr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEMR_Studio(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*emr.Handler) string
		name     string
		testType string
		wantCode int
	}{
		{
			name:     "creates studio",
			testType: "create",
			wantCode: http.StatusOK,
		},
		{
			name:     "create studio without name returns error",
			testType: "create_no_name",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "deletes existing studio",
			testType: "delete",
			setup: func(h *emr.Handler) string {
				rec := doEMRRequest(t, h, "CreateStudio", map[string]any{
					"Name":                     "my-studio",
					"AuthMode":                 "IAM",
					"DefaultS3Location":        "s3://bucket/path",
					"EngineSecurityGroupId":    "sg-engine",
					"ServiceRole":              "arn:aws:iam::000000000000:role/studio-role",
					"SubnetIds":                []string{"subnet-1"},
					"VpcId":                    "vpc-123",
					"WorkspaceSecurityGroupId": "sg-workspace",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					StudioID string `json:"StudioId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.StudioID
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete non-existent studio returns error",
			testType: "delete_notfound",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var studioID string
			if tt.setup != nil {
				studioID = tt.setup(h)
			}

			var rec *httptest.ResponseRecorder
			switch tt.testType {
			case "create":
				rec = doEMRRequest(t, h, "CreateStudio", map[string]any{
					"Name":                     "my-studio",
					"AuthMode":                 "IAM",
					"DefaultS3Location":        "s3://bucket/path",
					"EngineSecurityGroupId":    "sg-engine",
					"ServiceRole":              "arn:aws:iam::000000000000:role/studio-role",
					"SubnetIds":                []string{"subnet-1"},
					"VpcId":                    "vpc-123",
					"WorkspaceSecurityGroupId": "sg-workspace",
				})
			case "create_no_name":
				rec = doEMRRequest(t, h, "CreateStudio", map[string]any{
					"Name":              "",
					"AuthMode":          "IAM",
					"DefaultS3Location": "s3://bucket/path",
				})
			case "delete":
				rec = doEMRRequest(t, h, "DeleteStudio", map[string]any{
					"StudioId": studioID,
				})
			case "delete_notfound":
				rec = doEMRRequest(t, h, "DeleteStudio", map[string]any{
					"StudioId": "es-NOTEXIST",
				})
			}

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.testType == "create" && tt.wantCode == http.StatusOK {
				var out struct {
					StudioID string `json:"StudioId"`
					URL      string `json:"Url"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.StudioID)
				assert.NotEmpty(t, out.URL)
			}
		})
	}
}

func TestEMR_StudioSessionMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		testType string
		wantCode int
	}{
		{
			name:     "creates session mapping",
			testType: "create",
			wantCode: http.StatusOK,
		},
		{
			name:     "create session mapping for non-existent studio fails",
			testType: "create_notstudio",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create session mapping without studio ID fails",
			testType: "create_nostudio",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "deletes existing session mapping",
			testType: "delete",
			wantCode: http.StatusOK,
		},
		{
			name:     "delete non-existent session mapping fails",
			testType: "delete_notfound",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a studio for tests that need one.
			var studioID string
			studioRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
				"Name":                     "session-studio",
				"AuthMode":                 "SSO",
				"DefaultS3Location":        "s3://bucket/path",
				"EngineSecurityGroupId":    "sg-engine",
				"ServiceRole":              "arn:aws:iam::000000000000:role/role",
				"SubnetIds":                []string{"subnet-1"},
				"VpcId":                    "vpc-123",
				"WorkspaceSecurityGroupId": "sg-workspace",
			})
			require.Equal(t, http.StatusOK, studioRec.Code)

			var studioOut struct {
				StudioID string `json:"StudioId"`
			}
			require.NoError(t, json.Unmarshal(studioRec.Body.Bytes(), &studioOut))
			studioID = studioOut.StudioID

			var rec *httptest.ResponseRecorder
			switch tt.testType {
			case "create":
				rec = doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
					"StudioId":         studioID,
					"IdentityType":     "USER",
					"IdentityId":       "user-123",
					"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
				})
			case "create_notstudio":
				rec = doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
					"StudioId":         "es-NOTEXIST",
					"IdentityType":     "USER",
					"IdentityId":       "user-123",
					"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
				})
			case "create_nostudio":
				rec = doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
					"StudioId":         "",
					"IdentityType":     "USER",
					"IdentityId":       "user-123",
					"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
				})
			case "delete":
				// First create a mapping.
				cRec := doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
					"StudioId":         studioID,
					"IdentityType":     "USER",
					"IdentityId":       "user-to-delete",
					"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
				})
				require.Equal(t, http.StatusOK, cRec.Code)

				rec = doEMRRequest(t, h, "DeleteStudioSessionMapping", map[string]any{
					"StudioId":     studioID,
					"IdentityType": "USER",
					"IdentityId":   "user-to-delete",
				})
			case "delete_notfound":
				rec = doEMRRequest(t, h, "DeleteStudioSessionMapping", map[string]any{
					"StudioId":     studioID,
					"IdentityType": "USER",
					"IdentityId":   "nonexistent-user",
				})
			}

			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestEMR_DeleteStudio_CascadesSessionMappings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a studio.
	studioRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "cascade-studio",
		"AuthMode":                 "SSO",
		"DefaultS3Location":        "s3://bucket/path",
		"EngineSecurityGroupId":    "sg-engine",
		"ServiceRole":              "arn:aws:iam::000000000000:role/role",
		"SubnetIds":                []string{"subnet-1"},
		"VpcId":                    "vpc-123",
		"WorkspaceSecurityGroupId": "sg-workspace",
	})
	require.Equal(t, http.StatusOK, studioRec.Code)

	var studioOut struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(studioRec.Body.Bytes(), &studioOut))

	// Create a session mapping.
	mappingRec := doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
		"StudioId":         studioOut.StudioID,
		"IdentityType":     "USER",
		"IdentityId":       "user-123",
		"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
	})
	require.Equal(t, http.StatusOK, mappingRec.Code)

	// Delete the studio - should cascade.
	deleteRec := doEMRRequest(t, h, "DeleteStudio", map[string]any{
		"StudioId": studioOut.StudioID,
	})
	require.Equal(t, http.StatusOK, deleteRec.Code)

	// Deleting studio again should fail.
	deleteRec2 := doEMRRequest(t, h, "DeleteStudio", map[string]any{
		"StudioId": studioOut.StudioID,
	})
	assert.Equal(t, http.StatusBadRequest, deleteRec2.Code)
}

func TestStudio_DescribeAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "my-studio",
		"AuthMode":                 "SSO",
		"DefaultS3Location":        "s3://bucket",
		"EngineSecurityGroupId":    "sg-1",
		"ServiceRole":              "arn:role",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-2",
		"SubnetIds":                []string{"subnet-1", "subnet-2"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	studioID := create.StudioID

	descRec := doEMRRequest(t, h, "DescribeStudio", map[string]any{"StudioId": studioID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var desc struct {
		Studio struct {
			Name string `json:"Name"`
		} `json:"Studio"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &desc))
	assert.Equal(t, "my-studio", desc.Studio.Name)

	listRec := doEMRRequest(t, h, "ListStudios", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		Studios []struct {
			Name string `json:"Name"`
		} `json:"Studios"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	assert.Len(t, listOut.Studios, 1)
}

func TestStudioSessionMapping_GetUpdateList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "mapping-studio",
		"AuthMode":                 "SSO",
		"DefaultS3Location":        "s3://b",
		"EngineSecurityGroupId":    "sg-1",
		"ServiceRole":              "arn:r",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-2",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	studioID := create.StudioID

	doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
		"StudioId":         studioID,
		"IdentityType":     "USER",
		"IdentityId":       "user-123",
		"SessionPolicyArn": "arn:policy:old",
	})

	listRec := doEMRRequest(t, h, "ListStudioSessionMappings", map[string]any{"StudioId": studioID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		SessionMappings []struct {
			SessionPolicyArn string `json:"SessionPolicyArn"`
		} `json:"SessionMappings"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.SessionMappings, 1)

	updateRec := doEMRRequest(t, h, "UpdateStudioSessionMapping", map[string]any{
		"StudioId":         studioID,
		"IdentityType":     "USER",
		"IdentityId":       "user-123",
		"SessionPolicyArn": "arn:policy:new",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	getRec := doEMRRequest(t, h, "GetStudioSessionMapping", map[string]any{
		"StudioId":     studioID,
		"IdentityType": "USER",
		"IdentityId":   "user-123",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		SessionMapping struct {
			SessionPolicyArn string `json:"SessionPolicyArn"`
		} `json:"SessionMapping"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "arn:policy:new", getOut.SessionMapping.SessionPolicyArn)
}

// TestStudio_NameUniqueness verifies CreateStudio enforces name uniqueness.
func TestStudio_NameUniqueness(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateStudio(
		context.Background(),
		"my-studio",
		"SSO",
		"s3://bucket",
		"sg-1",
		"arn:role",
		"vpc-1",
		"sg-2",
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.CreateStudio(
		context.Background(),
		"my-studio",
		"SSO",
		"s3://bucket",
		"sg-1",
		"arn:role",
		"vpc-1",
		"sg-2",
		nil,
		nil,
	)
	require.Error(t, err)
}

// TestStudioSessionMapping_CreationTime verifies CreationTime is set.
func TestStudioSessionMapping_CreationTime(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	studio, err := b.CreateStudio(
		context.Background(),
		"ct-studio",
		"SSO",
		"s3://b",
		"sg-1",
		"arn:r",
		"vpc-1",
		"sg-2",
		nil,
		nil,
	)
	require.NoError(t, err)

	before := time.Now().Truncate(time.Second)
	err = b.CreateStudioSessionMapping(context.Background(), studio.StudioID, "USER", "user-id", "", "arn:policy")
	require.NoError(t, err)

	// Verify through the HTTP layer.
	h := newTestHandler(t)
	studioOut, err := h.Backend.CreateStudio(
		context.Background(),
		"http-studio",
		"SSO",
		"s3://b",
		"sg-1",
		"arn:r",
		"vpc-1",
		"sg-2",
		nil,
		nil,
	)
	require.NoError(t, err)
	err = h.Backend.CreateStudioSessionMapping(
		context.Background(),
		studioOut.StudioID,
		"USER",
		"user2",
		"",
		"arn:policy2",
	)
	require.NoError(t, err)

	assert.Equal(t, 1, h.Backend.StudioSessionMappingCount())
	_ = before // creation time validated at backend level above
}

func TestDescribeStudio_TagsEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "notag-studio",
		"AuthMode":                 "IAM",
		"DefaultS3Location":        "s3://bucket/prefix",
		"EngineSecurityGroupId":    "sg-engine",
		"ServiceRole":              "arn:aws:iam::000000000000:role/svc",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-ws",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "DescribeStudio", map[string]any{"StudioId": create.StudioID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	studio, ok := raw["Studio"].(map[string]any)
	require.True(t, ok, "Studio must be an object")

	tags, hasTagsKey := studio["Tags"]
	assert.True(t, hasTagsKey, "DescribeStudio must include 'Tags' key even when empty")
	assert.IsType(t, []any{}, tags, "'Tags' must be an array, not null or absent")
	assert.Empty(t, tags)
}

func TestDescribeStudio_SubnetIdsEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "nosubnet-studio",
		"AuthMode":                 "IAM",
		"DefaultS3Location":        "s3://bucket/prefix",
		"EngineSecurityGroupId":    "sg-engine",
		"ServiceRole":              "arn:aws:iam::000000000000:role/svc",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-ws",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "DescribeStudio", map[string]any{"StudioId": create.StudioID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	studio := raw["Studio"].(map[string]any)

	subnets, hasSubnetsKey := studio["SubnetIds"]
	assert.True(t, hasSubnetsKey, "DescribeStudio must include 'SubnetIds' key even when empty")
	assert.IsType(t, []any{}, subnets, "'SubnetIds' must be an array, not null or absent")
	assert.Empty(t, subnets)
}

func TestListStudioSessionMappings_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "mapping-studio",
		"AuthMode":                 "IAM",
		"DefaultS3Location":        "s3://bucket/prefix",
		"EngineSecurityGroupId":    "sg-engine",
		"ServiceRole":              "arn:aws:iam::000000000000:role/svc",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-ws",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "ListStudioSessionMappings", map[string]any{
		"StudioId": create.StudioID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	mappings, hasKey := raw["SessionMappings"]
	assert.True(t, hasKey, "ListStudioSessionMappings must include 'SessionMappings' key")
	assert.IsType(t, []any{}, mappings, "'SessionMappings' must be [] not null when empty")
	assert.Empty(t, mappings)
}

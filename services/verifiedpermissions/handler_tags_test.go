package verifiedpermissions_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

func TestVPHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		tags     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "tag existing resource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				rec := doVPRequest(
					t,
					h,
					"CreatePolicyStore",
					map[string]any{"description": "test", "validationSettings": map[string]any{"mode": "OFF"}},
				)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["arn"].(string)
			},
			tags:     map[string]any{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name: "tag non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/nonexistent"
			},
			tags:     map[string]any{"key": "value"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing resource arn",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return ""
			},
			tags:     map[string]any{"key": "value"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			arn := tt.setup(t, h)

			rec := doVPRequest(t, h, "TagResource", map[string]any{
				"resourceArn": arn,
				"tags":        tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		name     string
		tagKeys  []string
		wantCode int
	}{
		{
			name: "untag existing resource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
					"description":        "test",
					"tags":               map[string]any{"env": "prod"},
					"validationSettings": map[string]any{"mode": "OFF"},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["arn"].(string)
			},
			tagKeys:  []string{"env"},
			wantCode: http.StatusOK,
		},
		{
			name: "untag non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/nonexistent"
			},
			tagKeys:  []string{"key"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing resource arn",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return ""
			},
			tagKeys:  []string{"key"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			arn := tt.setup(t, h)

			rec := doVPRequest(t, h, "UntagResource", map[string]any{
				"resourceArn": arn,
				"tagKeys":     tt.tagKeys,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestVPHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *verifiedpermissions.Handler) string
		name     string
		wantCode int
		wantTags bool
	}{
		{
			name: "list tags for existing resource",
			setup: func(t *testing.T, h *verifiedpermissions.Handler) string {
				t.Helper()

				rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
					"description":        "test",
					"tags":               map[string]any{"env": "prod"},
					"validationSettings": map[string]any{"mode": "OFF"},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["arn"].(string)
			},
			wantCode: http.StatusOK,
			wantTags: true,
		},
		{
			name: "list tags for non-existent resource",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing resource arn",
			setup: func(_ *testing.T, _ *verifiedpermissions.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			arn := tt.setup(t, h)

			rec := doVPRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": arn,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantTags {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "tags")
			}
		})
	}
}

func TestVPHandler_TagResource_TooManyTags(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)
	storeID := createTestPolicyStore(t, h)

	getRec := doVPRequest(t, h, "GetPolicyStore", map[string]any{"policyStoreId": storeID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	resourceARN := getResp["arn"].(string)

	tags := make(map[string]any, 51)
	for i := range 51 {
		tags["key"+strconv.Itoa(i)] = "value"
	}

	rec := doVPRequest(t, h, "TagResource", map[string]any{
		"resourceArn": resourceARN,
		"tags":        tags,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TooManyTagsException", resp["__type"])
}

// TestVPHandler_IsAuthorized_DeterminingPolicies_ObjectShape verifies
// determiningPolicies/errors elements are wire-encoded as objects
// ({"policyId": "..."} / {"errorDescription": "..."}), matching the real
// SDK's DeterminingPolicyItem/EvaluationErrorItem -- not bare strings.

func TestVPHandler_TagResource_PolicyResource(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	// Create policy store.
	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	// Get policy store ARN.
	getStoreRec := doVPRequest(t, h, "GetPolicyStore", map[string]any{
		"policyStoreId": policyStoreID,
	})
	require.Equal(t, http.StatusOK, getStoreRec.Code)
	var getStoreResp map[string]any
	require.NoError(t, json.Unmarshal(getStoreRec.Body.Bytes(), &getStoreResp))
	storeARN := getStoreResp["arn"].(string)
	_ = storeARN

	// Create a policy.
	policyRec := doVPRequest(t, h, "CreatePolicy", map[string]any{
		"policyStoreId": policyStoreID,
		"definition": map[string]any{
			"static": map[string]any{
				"statement":   `permit(principal, action, resource);`,
				"description": "tag-test policy",
			},
		},
	})
	require.Equal(t, http.StatusOK, policyRec.Code)
	var policyResp map[string]any
	require.NoError(t, json.Unmarshal(policyRec.Body.Bytes(), &policyResp))
	policyID := policyResp["policyId"].(string)

	// Build the policy ARN (format: arn:aws:verifiedpermissions::<acct>:policy/<storeID>/<policyID>)
	policyARN := fmt.Sprintf(
		"arn:aws:verifiedpermissions::123456789012:policy/%s/%s",
		policyStoreID, policyID,
	)

	// Tag the policy.
	tagRec := doVPRequest(t, h, "TagResource", map[string]any{
		"resourceArn": policyARN,
		"tags":        map[string]string{"Env": "test"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code, "TagResource body: %s", tagRec.Body.String())

	listRec1 := doVPRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": policyARN})
	require.Equal(t, http.StatusOK, listRec1.Code)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(listRec1.Body.Bytes(), &resp1))
	tags1, _ := resp1["tags"].(map[string]any)
	assert.Equal(t, "test", tags1["Env"])

	// Overwrite the tag.
	overwriteRec := doVPRequest(t, h, "TagResource", map[string]any{
		"resourceArn": policyARN,
		"tags":        map[string]string{"Env": "prod"},
	})
	require.Equal(t, http.StatusOK, overwriteRec.Code, "overwrite TagResource body: %s", overwriteRec.Body.String())

	listRec2 := doVPRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": policyARN})
	require.Equal(t, http.StatusOK, listRec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &resp2))
	tags2, _ := resp2["tags"].(map[string]any)
	assert.Equal(t, "prod", tags2["Env"])

	// Untag it.
	untagRec := doVPRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": policyARN,
		"tagKeys":     []string{"Env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code, "UntagResource body: %s", untagRec.Body.String())

	listRec := doVPRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": policyARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	_, envPresent := tags["Env"]
	assert.False(t, envPresent, "Env tag should be removed after UntagResource")
}

func TestVPHandler_TagResource_PolicyTemplateResource(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	tmplRec := doVPRequest(t, h, "CreatePolicyTemplate", map[string]any{
		"policyStoreId": policyStoreID,
		"statement":     `permit(principal == ?principal, action, resource == ?resource);`,
		"description":   "tag-test template",
	})
	require.Equal(t, http.StatusOK, tmplRec.Code)
	var tmplResp map[string]any
	require.NoError(t, json.Unmarshal(tmplRec.Body.Bytes(), &tmplResp))
	templateID := tmplResp["policyTemplateId"].(string)

	templateARN := fmt.Sprintf(
		"arn:aws:verifiedpermissions::123456789012:policy-template/%s/%s",
		policyStoreID, templateID,
	)

	tagRec := doVPRequest(t, h, "TagResource", map[string]any{
		"resourceArn": templateARN,
		"tags":        map[string]string{"Purpose": "parity-test"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code, "body: %s", tagRec.Body.String())

	listRec := doVPRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": templateARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	assert.Equal(t, "parity-test", tags["Purpose"])
}

func TestVPHandler_TagResource_IdentitySourceResource(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	storeRec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
		"validationSettings": map[string]any{"mode": "OFF"},
	})
	require.Equal(t, http.StatusOK, storeRec.Code)
	var storeResp map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &storeResp))
	policyStoreID := storeResp["policyStoreId"].(string)

	isRec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
		"policyStoreId":       policyStoreID,
		"principalEntityType": "User",
		"configuration": map[string]any{
			"cognitoUserPoolConfiguration": map[string]any{
				"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abc",
				"clientIds":   []string{"client1"},
			},
		},
	})
	require.Equal(t, http.StatusOK, isRec.Code, "CreateIdentitySource body: %s", isRec.Body.String())
	var isResp map[string]any
	require.NoError(t, json.Unmarshal(isRec.Body.Bytes(), &isResp))
	identitySourceID := isResp["identitySourceId"].(string)

	identitySourceARN := fmt.Sprintf(
		"arn:aws:verifiedpermissions::123456789012:identity-source/%s/%s",
		policyStoreID, identitySourceID,
	)

	tagRec := doVPRequest(t, h, "TagResource", map[string]any{
		"resourceArn": identitySourceARN,
		"tags":        map[string]string{"Owner": "parity"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code, "TagResource body: %s", tagRec.Body.String())

	listRec := doVPRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": identitySourceARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	assert.Equal(t, "parity", tags["Owner"])
}

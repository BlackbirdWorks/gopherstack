package iot_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// newTestHandler is a helper that creates a fresh Handler backed by a new InMemoryBackend.
func newTestHandler() *iot.Handler {
	return iot.NewHandler(iot.NewInMemoryBackend(), nil)
}

// doRequest issues an HTTP request against h and returns the recorded response.
func doRequest(t *testing.T, h *iot.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody []byte

	if body != nil {
		var err error

		reqBody, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(reqBody))

	if body != nil {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func TestRefinement2_ThingTypeName_Stored(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.CreateThing(&iot.CreateThingInput{
		ThingName:     "typed-thing",
		ThingTypeName: "SensorType",
	})
	require.NoError(t, err)

	th, err := backend.DescribeThing("typed-thing")
	require.NoError(t, err)
	assert.Equal(t, "SensorType", th.ThingTypeName)
}

func TestRefinement2_Thing_VersionStartsAt1(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.CreateThing(&iot.CreateThingInput{ThingName: "v1-thing"})
	require.NoError(t, err)

	th, err := backend.DescribeThing("v1-thing")
	require.NoError(t, err)
	assert.Equal(t, int64(1), th.Version)
}

func TestRefinement2_UpdateThing_IncrementsVersion(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.CreateThing(&iot.CreateThingInput{ThingName: "update-thing"})
	require.NoError(t, err)

	err = backend.UpdateThing(&iot.UpdateThingInput{
		ThingName: "update-thing",
		AttributePayload: &iot.AttributePayload{
			Attributes: map[string]string{"env": "prod"},
		},
	})
	require.NoError(t, err)

	th, err := backend.DescribeThing("update-thing")
	require.NoError(t, err)
	assert.Equal(t, int64(2), th.Version)
	assert.Equal(t, "prod", th.Attributes["env"])
}

func TestRefinement2_UpdateThing_RemoveThingType(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.CreateThing(&iot.CreateThingInput{
		ThingName:     "typed-remove",
		ThingTypeName: "OldType",
	})
	require.NoError(t, err)

	err = backend.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "typed-remove",
		RemoveThingType: true,
	})
	require.NoError(t, err)

	th, err := backend.DescribeThing("typed-remove")
	require.NoError(t, err)
	assert.Empty(t, th.ThingTypeName)
}

func TestRefinement2_UpdateThing_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.UpdateThing(&iot.UpdateThingInput{ThingName: "nonexistent"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestRefinement2_TopicRule_AWSIoTSQLVersion_Default(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName:         "sql-version-rule",
		TopicRulePayload: &iot.TopicRulePayload{SQL: "SELECT * FROM 'topic'"},
	})
	require.NoError(t, err)

	r, err := backend.GetTopicRule("sql-version-rule")
	require.NoError(t, err)
	assert.Equal(t, "2015-10-08", r.AWSIoTSQLVersion)
}

func TestRefinement2_TopicRule_AWSIoTSQLVersion_Custom(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "custom-sql-version",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:              "SELECT * FROM 'topic'",
			AWSIoTSQLVersion: "2016-03-23",
		},
	})
	require.NoError(t, err)

	r, err := backend.GetTopicRule("custom-sql-version")
	require.NoError(t, err)
	assert.Equal(t, "2016-03-23", r.AWSIoTSQLVersion)
}

func TestRefinement2_DisableTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddRuleInternal(iot.TopicRule{RuleName: "disable-me", SQL: "SELECT *"})

	err := backend.DisableTopicRule("disable-me")
	require.NoError(t, err)

	r, err := backend.GetTopicRule("disable-me")
	require.NoError(t, err)
	assert.False(t, r.Enabled)
}

func TestRefinement2_EnableTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddRuleInternal(iot.TopicRule{RuleName: "enable-me", SQL: "SELECT *"})

	require.NoError(t, backend.DisableTopicRule("enable-me"))
	require.NoError(t, backend.EnableTopicRule("enable-me"))

	r, err := backend.GetTopicRule("enable-me")
	require.NoError(t, err)
	assert.True(t, r.Enabled)
}

func TestRefinement2_DisableTopicRule_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.DisableTopicRule("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrRuleNotFound)
}

func TestRefinement2_ReplaceTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddRuleInternal(iot.TopicRule{RuleName: "replace-me", SQL: "SELECT * FROM 'old'"})

	err := backend.ReplaceTopicRule(&iot.ReplaceTopicRuleInput{
		RuleName: "replace-me",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:         "SELECT * FROM 'new'",
			Description: "new description",
		},
	})
	require.NoError(t, err)

	r, err := backend.GetTopicRule("replace-me")
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM 'new'", r.SQL) //nolint:unqueryvet // IoT SQL, not a database query
	assert.Equal(t, "new description", r.Description)
	assert.Equal(t, "2015-10-08", r.AWSIoTSQLVersion)
}

func TestRefinement2_ReplaceTopicRule_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.ReplaceTopicRule(&iot.ReplaceTopicRuleInput{RuleName: "ghost"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrRuleNotFound)
}

func TestRefinement2_GetPolicy(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddPolicyInternal(iot.Policy{PolicyName: "my-policy", PolicyDocument: `{"Version":"2012-10-17"}`})

	out, err := backend.GetPolicy("my-policy")
	require.NoError(t, err)
	assert.Equal(t, "my-policy", out.PolicyName)
	assert.NotEmpty(t, out.PolicyARN)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, out.PolicyDocument)
}

func TestRefinement2_GetPolicy_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.GetPolicy("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrPolicyNotFound)
}

func TestRefinement2_DeletePolicy(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddPolicyInternal(iot.Policy{PolicyName: "del-policy"})
	require.Equal(t, 1, backend.PolicyCount())

	require.NoError(t, backend.DeletePolicy("del-policy"))
	assert.Equal(t, 0, backend.PolicyCount())
}

func TestRefinement2_DeletePolicy_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.DeletePolicy("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrPolicyNotFound)
}

func TestRefinement2_ListPolicies_Sorted(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddPolicyInternal(iot.Policy{PolicyName: "z-policy"})
	backend.AddPolicyInternal(iot.Policy{PolicyName: "a-policy"})
	backend.AddPolicyInternal(iot.Policy{PolicyName: "m-policy"})

	policies := backend.ListPolicies()
	require.Len(t, policies, 3)
	assert.Equal(t, "a-policy", policies[0].PolicyName)
	assert.Equal(t, "m-policy", policies[1].PolicyName)
	assert.Equal(t, "z-policy", policies[2].PolicyName)
}

func TestRefinement2_ListThingPrincipals(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddThingInternal(iot.Thing{ThingName: "principal-thing"})

	require.NoError(t, backend.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
		ThingName: "principal-thing",
		Principal: "arn:aws:iot:us-east-1:000000000000:cert/abc123",
	}))

	principals, err := backend.ListThingPrincipals("principal-thing")
	require.NoError(t, err)
	require.Len(t, principals, 1)
	assert.Equal(t, "arn:aws:iot:us-east-1:000000000000:cert/abc123", principals[0])
}

func TestRefinement2_ListThingPrincipals_EmptyOnNoAttachment(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddThingInternal(iot.Thing{ThingName: "empty-thing"})

	principals, err := backend.ListThingPrincipals("empty-thing")
	require.NoError(t, err)
	assert.NotNil(t, principals)
	assert.Empty(t, principals)
}

func TestRefinement2_ListThingPrincipals_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.ListThingPrincipals("ghost-thing")
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestRefinement2_ThingPrincipalCount_Helper(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddThingInternal(iot.Thing{ThingName: "count-thing"})
	assert.Equal(t, 0, backend.ThingPrincipalCount("count-thing"))

	require.NoError(t, backend.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
		ThingName: "count-thing",
		Principal: "cert-1",
	}))
	assert.Equal(t, 1, backend.ThingPrincipalCount("count-thing"))
}

func TestRefinement2_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	expected := []string{
		"GetPolicy", "DeletePolicy", "ListPolicies",
		"DisableTopicRule", "EnableTopicRule", "ReplaceTopicRule",
		"UpdateThing", "ListThings", "ListTopicRules", "ListThingPrincipals",
	}

	for _, exp := range expected {
		assert.Contains(t, ops, exp, "missing operation: %s", exp)
	}
}

func TestRefinement2_Handler_ListPolicies_Response(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupPolicies []string
		wantCount     int
	}{
		{
			name:          "empty_list",
			setupPolicies: nil,
			wantCount:     0,
		},
		{
			name:          "two_policies",
			setupPolicies: []string{"pol-a", "pol-b"},
			wantCount:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := iot.NewInMemoryBackend()
			h := iot.NewHandler(backend, nil)

			for _, name := range tt.setupPolicies {
				backend.AddPolicyInternal(iot.Policy{PolicyName: name})
			}

			resp := doRequest(t, h, http.MethodGet, "/policies", nil)
			require.Equal(t, http.StatusOK, resp.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			policies, ok := out["policies"]
			require.True(t, ok, "response must have 'policies' key")

			policiesSlice, ok := policies.([]any)
			require.True(t, ok)
			assert.Len(t, policiesSlice, tt.wantCount)
		})
	}
}

func TestRefinement2_Handler_ListThings_ResponseFormat(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddThingInternal(iot.Thing{ThingName: "thing-alpha"})
	backend.AddThingInternal(iot.Thing{ThingName: "thing-beta"})

	resp := doRequest(t, h, http.MethodGet, "/things", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	things, ok := out["things"]
	require.True(t, ok, "response must have 'things' key")

	thingsSlice, ok := things.([]any)
	require.True(t, ok)
	assert.Len(t, thingsSlice, 2)
}

func TestRefinement2_Handler_ListTopicRules_ResponseFormat(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddRuleInternal(iot.TopicRule{RuleName: "rule-1", SQL: "SELECT *"})
	backend.AddRuleInternal(iot.TopicRule{RuleName: "rule-2", SQL: "SELECT *"})

	resp := doRequest(t, h, http.MethodGet, "/rules", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	rules, ok := out["rules"]
	require.True(t, ok, "response must have 'rules' key")

	rulesSlice, ok := rules.([]any)
	require.True(t, ok)
	assert.Len(t, rulesSlice, 2)
}

func TestRefinement2_Handler_GetTopicRule_IncludesAWSSQLVersion(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddRuleInternal(iot.TopicRule{
		RuleName:         "version-rule",
		SQL:              "SELECT *",
		AWSIoTSQLVersion: "2015-10-08",
	})

	resp := doRequest(t, h, http.MethodGet, "/rules/version-rule", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	rule, ok := out["rule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "2015-10-08", rule["awsIotSqlVersion"])
}

func TestRefinement2_Handler_DescribeThing_IncludesThingTypeName(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddThingInternal(iot.Thing{ThingName: "sensor-1", ThingTypeName: "SensorType"})

	resp := doRequest(t, h, http.MethodGet, "/things/sensor-1", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "SensorType", out["thingTypeName"])
}

func TestRefinement2_Handler_UpdateThing(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddThingInternal(iot.Thing{ThingName: "update-sensor"})

	resp := doRequest(t, h, http.MethodPatch, "/things/update-sensor", map[string]any{
		"attributePayload": map[string]any{
			"attributes": map[string]string{"location": "room-1"},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	th, err := backend.DescribeThing("update-sensor")
	require.NoError(t, err)
	assert.Equal(t, "room-1", th.Attributes["location"])
	assert.Equal(t, int64(2), th.Version)
}

func TestRefinement2_Handler_DisableEnableTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddRuleInternal(iot.TopicRule{RuleName: "toggle-rule", SQL: "SELECT *"})

	// Disable
	resp := doRequest(t, h, http.MethodPatch, "/rules/toggle-rule/disable", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	r, err := backend.GetTopicRule("toggle-rule")
	require.NoError(t, err)
	assert.False(t, r.Enabled)

	// Enable
	resp2 := doRequest(t, h, http.MethodPatch, "/rules/toggle-rule/enable", nil)
	require.Equal(t, http.StatusOK, resp2.Code)

	r2, err := backend.GetTopicRule("toggle-rule")
	require.NoError(t, err)
	assert.True(t, r2.Enabled)
}

func TestRefinement2_Handler_ReplaceTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddRuleInternal(iot.TopicRule{RuleName: "replace-rule", SQL: "SELECT old"})

	resp := doRequest(t, h, http.MethodPatch, "/rules/replace-rule", map[string]any{
		"topicRulePayload": map[string]any{
			"sql":         "SELECT new",
			"description": "updated",
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	r, err := backend.GetTopicRule("replace-rule")
	require.NoError(t, err)
	assert.Equal(t, "SELECT new", r.SQL)
	assert.Equal(t, "updated", r.Description)
}

func TestRefinement2_Handler_GetPolicy_HTTP(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddPolicyInternal(iot.Policy{PolicyName: "http-policy", PolicyDocument: `{}`})

	resp := doRequest(t, h, http.MethodGet, "/policies/http-policy", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "http-policy", out["policyName"])
	assert.NotEmpty(t, out["policyArn"])
}

func TestRefinement2_Handler_DeletePolicy_HTTP(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddPolicyInternal(iot.Policy{PolicyName: "delete-http-policy"})

	resp := doRequest(t, h, http.MethodDelete, "/policies/delete-http-policy", nil)
	require.Equal(t, http.StatusNoContent, resp.Code)

	assert.Equal(t, 0, backend.PolicyCount())
}

func TestRefinement2_Handler_GetPolicy_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)

	resp := doRequest(t, h, http.MethodGet, "/policies/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, resp.Code)
}

func TestRefinement2_Handler_ListThingPrincipals_HTTP(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddThingInternal(iot.Thing{ThingName: "http-principal-thing"})

	require.NoError(t, backend.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
		ThingName: "http-principal-thing",
		Principal: "arn:aws:iot:us-east-1:000000000000:cert/xyz789",
	}))

	resp := doRequest(t, h, http.MethodGet, "/things/http-principal-thing/principals", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	principals, ok := out["principals"].([]any)
	require.True(t, ok)
	require.Len(t, principals, 1)
	assert.Equal(t, "arn:aws:iot:us-east-1:000000000000:cert/xyz789", principals[0])
}

func TestRefinement2_PolicyTargetCount_Helper(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddPolicyInternal(iot.Policy{PolicyName: "attach-test-policy"})
	assert.Equal(t, 0, backend.PolicyTargetCount("attach-test-policy"))

	require.NoError(t, backend.AttachPolicy(&iot.AttachPolicyInput{
		PolicyName: "attach-test-policy",
		Target:     "arn:aws:iot:us-east-1:000000000000:cert/abc",
	}))
	assert.Equal(t, 1, backend.PolicyTargetCount("attach-test-policy"))
}

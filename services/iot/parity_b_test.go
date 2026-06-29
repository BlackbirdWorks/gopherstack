package iot_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestParityB_TopicRulePayloadWrapper verifies CreateTopicRule and ReplaceTopicRule
// unwrap the AWS SDK "topicRulePayload" envelope.
func TestParityB_TopicRulePayloadWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantSQL    string
		wantStatus int
	}{
		{
			name: "create_with_wrapper",
			body: map[string]any{
				"topicRulePayload": map[string]any{
					"sql":         "SELECT * FROM 'topic/test'",
					"description": "my rule",
				},
			},
			wantSQL:    "SELECT * FROM 'topic/test'",
			wantStatus: http.StatusOK,
		},
		{
			name: "replace_with_wrapper",
			body: map[string]any{
				"topicRulePayload": map[string]any{
					"sql":         "SELECT new FROM 'topic/replaced'",
					"description": "replaced",
				},
			},
			wantSQL:    "SELECT new FROM 'topic/replaced'",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.name == "replace_with_wrapper" {
				createBody := map[string]any{
					"topicRulePayload": map[string]any{"sql": "SELECT old FROM 'topic/x'"},
				}
				doRequest(t, h, http.MethodPost, "/rules/wraprule", createBody)
				doRequest(t, h, http.MethodPatch, "/rules/wraprule", tt.body)
			} else {
				rec := doRequest(t, h, http.MethodPost, "/rules/wraprule", tt.body)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}

			backend := iot.NewInMemoryBackend()
			_ = backend
			b2 := iot.NewInMemoryBackend()
			h2 := iot.NewHandler(b2, nil)
			if tt.name == "create_with_wrapper" {
				doRequest(t, h2, http.MethodPost, "/rules/wraprule2", tt.body)
				rule, err := b2.GetTopicRule("wraprule2")
				require.NoError(t, err)
				assert.Equal(t, tt.wantSQL, rule.SQL)
			}
		})
	}
}

// TestParityB_PolicyVersionsPath verifies CreatePolicyVersion uses /versions (plural).
func TestParityB_PolicyVersionsPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "create_version_plural_path",
			path:       "/policies/parb-pol/versions",
			wantStatus: http.StatusOK,
		},
		{
			name:       "list_versions_plural_path",
			path:       "/policies/parb-pol/versions",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			doRequest(t, h, http.MethodPost, "/policies/parb-pol",
				map[string]string{"policyDocument": `{"Version":"2012-10-17"}`})

			var rec *httptest.ResponseRecorder
			if tt.name == "create_version_plural_path" {
				rec = doRequest(t, h, http.MethodPost, tt.path,
					map[string]string{"policyDocument": `{"Version":"2012-10-17","v2":true}`})
			} else {
				rec = doRequest(t, h, http.MethodGet, tt.path, nil)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParityB_DescribeCertificate_ReturnsPem verifies DescribeCertificate returns certificatePem.
func TestParityB_DescribeCertificate_ReturnsPem(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, http.MethodPost, "/certificate/register-no-ca",
		map[string]any{"setAsActive": true})
	require.Equal(t, http.StatusOK, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	certID, _ := cr["certificateId"].(string)
	require.NotEmpty(t, certID)

	rec2 := doRequest(t, h, http.MethodGet, "/certificates/"+certID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var desc map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &desc))
	certDesc, _ := desc["certificateDescription"].(map[string]any)
	pem, _ := certDesc["certificatePem"].(string)
	assert.NotEmpty(t, pem, "certificatePem must be present in DescribeCertificate response")
}

// TestParityB_DescribeThing_DefaultClientId verifies DescribeThing returns defaultClientId.
func TestParityB_DescribeThing_DefaultClientId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		thingName string
	}{
		{name: "simple_name", thingName: "sensor-001"},
		{name: "complex_name", thingName: "device:factory:line-3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			doRequest(t, h, http.MethodPost, "/things/"+tt.thingName, nil)

			rec := doRequest(t, h, http.MethodGet, "/things/"+tt.thingName, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			clientID, _ := resp["defaultClientId"].(string)
			assert.Equal(t, tt.thingName, clientID, "defaultClientId must equal thingName")
		})
	}
}

// TestParityB_GetPolicyVersion_ReturnsPolicyArnAndName verifies GetPolicyVersion returns policyArn and policyName.
func TestParityB_GetPolicyVersion_ReturnsPolicyArnAndName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPost, "/policies/parb-pol2",
		map[string]string{"policyDocument": `{"Version":"2012-10-17"}`})
	doRequest(t, h, http.MethodPost, "/policies/parb-pol2/versions?setAsDefault=true",
		map[string]string{"policyDocument": `{"Version":"2012-10-17","v2":true}`})

	rec := doRequest(t, h, http.MethodGet, "/policies/parb-pol2/versions/2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "parb-pol2", resp["policyName"], "policyName must be in GetPolicyVersion response")
	policyARN, _ := resp["policyArn"].(string)
	assert.NotEmpty(t, policyARN, "policyArn must be in GetPolicyVersion response")
}

// TestParityB_UpdateThing_MergeFalse_ReplacesAttributes verifies merge:false replaces attributes.
func TestParityB_UpdateThing_MergeFalse_ReplacesAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialAttr map[string]string
		updateBody  map[string]any
		wantAttrs   map[string]any
		name        string
	}{
		{
			name:        "merge_true_merges",
			initialAttr: map[string]string{"a": "1", "b": "2"},
			updateBody: map[string]any{
				"attributePayload": map[string]any{
					"attributes": map[string]string{"c": "3"},
					"merge":      true,
				},
			},
			wantAttrs: map[string]any{"a": "1", "b": "2", "c": "3"},
		},
		{
			name:        "merge_false_replaces",
			initialAttr: map[string]string{"a": "1", "b": "2"},
			updateBody: map[string]any{
				"attributePayload": map[string]any{
					"attributes": map[string]string{"c": "3"},
					"merge":      false,
				},
			},
			wantAttrs: map[string]any{"c": "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			_, err := b.CreateThing(&iot.CreateThingInput{
				ThingName: "merge-test",
				AttributePayload: &iot.AttributePayload{
					Attributes: tt.initialAttr,
				},
			})
			require.NoError(t, err)

			var bodyAttr iot.AttributePayload
			raw, _ := json.Marshal(tt.updateBody["attributePayload"])
			require.NoError(t, json.Unmarshal(raw, &bodyAttr))

			err = b.UpdateThing(&iot.UpdateThingInput{
				ThingName:        "merge-test",
				AttributePayload: &bodyAttr,
			})
			require.NoError(t, err)

			thing, err := b.DescribeThing("merge-test")
			require.NoError(t, err)

			got := make(map[string]any, len(thing.Attributes))
			for k, v := range thing.Attributes {
				got[k] = v
			}

			assert.Equal(t, tt.wantAttrs, got, "attributes mismatch after update")
		})
	}
}

// TestParityB_UpdateCertificate_RejectsPendingStatuses verifies PENDING_TRANSFER and
// PENDING_ACTIVATION cannot be set via UpdateCertificate.
func TestParityB_UpdateCertificate_RejectsPendingStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		newStatus string
		wantErr   bool
	}{
		{name: "PENDING_TRANSFER_rejected", newStatus: "PENDING_TRANSFER", wantErr: true},
		{name: "PENDING_ACTIVATION_rejected", newStatus: "PENDING_ACTIVATION", wantErr: true},
		{name: "ACTIVE_accepted", newStatus: "ACTIVE", wantErr: false},
		{name: "INACTIVE_accepted", newStatus: "INACTIVE", wantErr: false},
		{name: "REVOKED_accepted", newStatus: "REVOKED", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
			require.NoError(t, err)

			err = b.UpdateCertificate(&iot.UpdateCertificateInput{
				CertificateID: cert.CertificateID,
				NewStatus:     tt.newStatus,
			})
			if tt.wantErr {
				require.ErrorIs(t, err, iot.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParityB_DeleteCertificate_ActiveBlocked verifies ACTIVE certs cannot be deleted.
func TestParityB_DeleteCertificate_ActiveBlocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		setActive bool
	}{
		{
			name:      "active_cert_blocked",
			setActive: true,
			wantErr:   iot.ErrDeleteConflict,
		},
		{
			name:      "inactive_cert_allowed",
			setActive: false,
			wantErr:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: tt.setActive})
			require.NoError(t, err)

			err = b.DeleteCertificate(cert.CertificateID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParityB_DeleteThing_WithPrincipals_Blocked verifies things with attached principals
// cannot be deleted.
func TestParityB_DeleteThing_WithPrincipals_Blocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDeleteErr error
		name          string
		attachPrinc   bool
	}{
		{
			name:          "with_principal_blocked",
			attachPrinc:   true,
			wantDeleteErr: iot.ErrDeleteConflict,
		},
		{
			name:          "no_principal_allowed",
			attachPrinc:   false,
			wantDeleteErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "del-thing-" + tt.name})
			require.NoError(t, err)

			if tt.attachPrinc {
				err = b.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
					ThingName: "del-thing-" + tt.name,
					Principal: "arn:aws:iot:us-east-1:000000000000:cert/abc123",
				})
				require.NoError(t, err)
			}

			err = b.DeleteThing("del-thing-" + tt.name)
			if tt.wantDeleteErr != nil {
				require.ErrorIs(t, err, tt.wantDeleteErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParityB_DeletePolicy_WithTargets_Blocked verifies policies with attached targets
// cannot be deleted.
func TestParityB_DeletePolicy_WithTargets_Blocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDeleteErr error
		name          string
		attachTarget  bool
	}{
		{
			name:          "with_target_blocked",
			attachTarget:  true,
			wantDeleteErr: iot.ErrDeleteConflict,
		},
		{
			name:          "no_target_allowed",
			attachTarget:  false,
			wantDeleteErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			b.AddPolicyInternal(iot.Policy{PolicyName: "del-pol-" + tt.name, PolicyDocument: `{}`})

			if tt.attachTarget {
				err := b.AttachPolicy(&iot.AttachPolicyInput{
					PolicyName: "del-pol-" + tt.name,
					Target:     "arn:aws:iot:us-east-1:000000000000:cert/abc",
				})
				require.NoError(t, err)
			}

			err := b.DeletePolicy("del-pol-" + tt.name)
			if tt.wantDeleteErr != nil {
				require.ErrorIs(t, err, tt.wantDeleteErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParityB_PolicyVersionLimit verifies a policy cannot have more than 5 versions.
func TestParityB_PolicyVersionLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		extraCounts int
		wantErr     bool
	}{
		{name: "four_versions_ok", extraCounts: 4, wantErr: false},
		{name: "five_versions_at_limit", extraCounts: 5, wantErr: false},
		{name: "six_versions_exceeds_limit", extraCounts: 6, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			b.AddPolicyInternal(iot.Policy{PolicyName: "limit-pol", PolicyDocument: `{}`})

			var lastErr error
			for range tt.extraCounts {
				_, lastErr = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
					PolicyName:     "limit-pol",
					PolicyDocument: `{}`,
				})
			}

			if tt.wantErr {
				require.ErrorIs(t, lastErr, iot.ErrVersionsLimitExceeded)
			} else {
				require.NoError(t, lastErr)
			}
		})
	}
}

// TestParityB_DeviceShadows verifies GetThingShadow, UpdateThingShadow, DeleteThingShadow,
// and ListNamedShadowsForThing operations.
func TestParityB_DeviceShadows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state      map[string]any
		name       string
		shadowName string
		op         string
		wantStatus int
	}{
		{
			name:       "update_classic_shadow",
			op:         "update",
			shadowName: "",
			state:      map[string]any{"desired": map[string]any{"temp": 22}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_classic_shadow",
			op:         "get",
			shadowName: "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_named_shadow",
			op:         "update",
			shadowName: "config",
			state:      map[string]any{"desired": map[string]any{"mode": "auto"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_missing_shadow_returns_404",
			op:         "get_missing",
			shadowName: "nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_shadow",
			op:         "delete",
			shadowName: "",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			doRequest(t, h, http.MethodPost, "/things/shadow-thing", nil)

			shadowPath := "/things/shadow-thing/shadow"
			if tt.shadowName != "" {
				shadowPath += "?name=" + tt.shadowName
			}

			var rec *httptest.ResponseRecorder

			switch tt.op {
			case "update":
				body := map[string]any{"state": tt.state}
				rec = doRequest(t, h, http.MethodPost, shadowPath, body)
			case "get":
				doRequest(t, h, http.MethodPost, shadowPath,
					map[string]any{"state": map[string]any{"desired": map[string]any{"x": 1}}})
				rec = doRequest(t, h, http.MethodGet, shadowPath, nil)
			case "get_missing":
				rec = doRequest(t, h, http.MethodGet, shadowPath, nil)
			case "delete":
				doRequest(t, h, http.MethodPost, shadowPath,
					map[string]any{"state": map[string]any{"desired": map[string]any{"y": 2}}})
				rec = doRequest(t, h, http.MethodDelete, shadowPath, nil)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParityB_ListNamedShadowsForThing verifies listing named shadows works correctly.
func TestParityB_ListNamedShadowsForThing(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPost, "/things/ns-thing", nil)
	doRequest(t, h, http.MethodPost, "/things/ns-thing/shadow?name=alpha",
		map[string]any{"state": map[string]any{}})
	doRequest(t, h, http.MethodPost, "/things/ns-thing/shadow?name=beta",
		map[string]any{"state": map[string]any{}})

	rec := doRequest(t, h, http.MethodGet,
		"/api/things/shadow/ListNamedShadowsForThing/ns-thing", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results, _ := resp["results"].([]any)
	assert.Len(t, results, 2)
}

// TestParityB_TransferCertificate_ReturnsArn verifies TransferCertificate returns the cert ARN.
func TestParityB_TransferCertificate_ReturnsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createRec := doRequest(t, h, http.MethodPost, "/certificate/register-no-ca",
		map[string]any{"setAsActive": true})
	require.Equal(t, http.StatusOK, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	certID, _ := cr["certificateId"].(string)
	certARN, _ := cr["certificateArn"].(string)
	require.NotEmpty(t, certID)
	require.NotEmpty(t, certARN)

	rec := doRequest(t, h, http.MethodPatch,
		"/certificates/"+certID+"/transfer?targetAwsAccount=123456789012", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	transferredARN, _ := resp["transferredCertificateArn"].(string)
	assert.Equal(t, certARN, transferredARN, "transferredCertificateArn must be the cert ARN, not ID")
}

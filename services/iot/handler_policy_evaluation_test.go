package iot_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestAuthorization(t *testing.T) {
	t.Parallel()

	const principal = "arn:aws:iot:us-east-1:123456789012:cert/abc123"

	h, b := newRefHandler()

	b.AddPolicyInternal(iot.Policy{
		PolicyName: "AllowConnect",
		PolicyDocument: `{"Version":"2012-10-17","Statement":[
			{"Effect":"Allow","Action":"iot:Connect","Resource":"*"}
		]}`,
	})

	require.NoError(t, b.AttachPrincipalPolicy(&iot.AttachPrincipalPolicyInput{
		PolicyName: "AllowConnect",
		Principal:  principal,
	}))

	tests := []struct {
		name       string
		actionType string
		wantDecide string
	}{
		{name: "allowed_action", actionType: "CONNECT", wantDecide: "ALLOWED"},
		{name: "implicit_deny_action", actionType: "PUBLISH", wantDecide: "IMPLICIT_DENY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{
				"principal": principal,
				"authInfos": []map[string]any{
					{"actionType": tt.actionType, "resources": []string{"*"}},
				},
			}

			rec := doRefRequest(t, h, http.MethodPost, "/test-authorization", body, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantDecide)
		})
	}

	t.Run("unknown_principal_required", func(t *testing.T) {
		t.Parallel()

		rec := doRefRequest(t, h, http.MethodPost, "/test-authorization", map[string]any{
			"authInfos": []map[string]any{{"actionType": "CONNECT", "resources": []string{"*"}}},
		}, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unknown_policy_to_add", func(t *testing.T) {
		t.Parallel()

		rec := doRefRequest(t, h, http.MethodPost, "/test-authorization", map[string]any{
			"principal":        principal,
			"policyNamesToAdd": []string{"NoSuchPolicy"},
			"authInfos":        []map[string]any{{"actionType": "CONNECT", "resources": []string{"*"}}},
		}, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestDetachPrincipalPolicy(t *testing.T) {
	t.Parallel()

	const principal = "arn:aws:iot:us-east-1:123456789012:cert/xyz"

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		b.AddPolicyInternal(iot.Policy{PolicyName: "MyPolicy"})
		require.NoError(t, b.AttachPrincipalPolicy(&iot.AttachPrincipalPolicyInput{
			PolicyName: "MyPolicy",
			Principal:  principal,
		}))

		require.Len(t, b.ListPrincipalPolicies(principal), 1)

		rec := doRefRequest(t, h, http.MethodDelete, "/principal-policies/MyPolicy", nil,
			map[string]string{"X-Amzn-Iot-Principal": principal})
		require.Equal(t, http.StatusOK, rec.Code)

		assert.Empty(t, b.ListPrincipalPolicies(principal))
	})

	t.Run("unknown_policy_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodDelete, "/principal-policies/NoSuchPolicy", nil,
			map[string]string{"X-Amzn-Iot-Principal": principal})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

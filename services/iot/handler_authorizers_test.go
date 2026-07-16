package iot_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch2_DefaultAuthorizer tests default authorizer operations.
func TestDefaultAuthorizer(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Describe (none set)
	iotExpectError(t, h, "/default-authorizer")

	// Set
	iotOK(t, h, http.MethodPost, "/default-authorizer", map[string]any{
		"authorizerName": "my-auth",
	})

	// Describe
	out := iotOK(t, h, http.MethodGet, "/default-authorizer", nil)
	if out["authorizerDescription"] == nil {
		t.Error("expected authorizerDescription")
	}

	// Clear
	iotOK(t, h, http.MethodDelete, "/default-authorizer", nil)

	// Describe again (none set)
	iotExpectError(t, h, "/default-authorizer")
}

// TestNewOps_Authorizer tests Authorizer CRUD.
func TestAuthorizer(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateAuthorizer
	out := iotOK(t, h, http.MethodPost, "/authorizer/my-auth", map[string]any{
		"authorizerFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:MyAuthFn",
		"status":                "ACTIVE",
	})
	if out["authorizerName"] != "my-auth" {
		t.Errorf("authorizerName mismatch: %v", out)
	}

	// DescribeAuthorizer
	out2 := iotOK(t, h, http.MethodGet, "/authorizer/my-auth", nil)
	desc, _ := out2["authorizerDescription"].(map[string]any)
	if desc["authorizerName"] != "my-auth" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListAuthorizers
	out3 := iotOK(t, h, http.MethodGet, "/authorizers", nil)
	authorizers, _ := out3["authorizers"].([]any)
	if len(authorizers) != 1 {
		t.Errorf("expected 1 authorizer, got %d", len(authorizers))
	}

	// UpdateAuthorizer
	iotOK(t, h, http.MethodPut, "/authorizer/my-auth", map[string]any{
		"status": "INACTIVE",
	})

	// DeleteAuthorizer
	iotOK(t, h, http.MethodDelete, "/authorizer/my-auth", nil)

	iotExpectError(t, h, "/authorizer/my-auth")
}

func TestTestInvokeAuthorizer(t *testing.T) {
	t.Parallel()

	h, b := newRefHandler()

	_, err := b.CreateAuthorizer(&iot.CreateAuthorizerInput{
		AuthorizerName:        "my-authorizer",
		AuthorizerFunctionARN: "arn:aws:lambda:us-east-1:123456789012:function:auth",
		SigningDisabled:       true,
		Status:                "ACTIVE",
	})
	require.NoError(t, err)

	rec := doRefRequest(t, h, http.MethodPost, "/authorizer/my-authorizer/test", map[string]any{
		"token": "sometoken",
	}, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"isAuthenticated":true`)
	assert.Contains(t, rec.Body.String(), "authz-")

	t.Run("unknown_authorizer", func(t *testing.T) {
		t.Parallel()

		unknownRec := doRefRequest(t, h, http.MethodPost, "/authorizer/no-such/test", map[string]any{
			"token": "x",
		}, nil)
		assert.Equal(t, http.StatusNotFound, unknownRec.Code)
	})

	t.Run("signing_required_missing_signature", func(t *testing.T) {
		t.Parallel()

		_, createErr := b.CreateAuthorizer(&iot.CreateAuthorizerInput{
			AuthorizerName: "signed-authorizer",
			Status:         "ACTIVE",
		})
		require.NoError(t, createErr)

		signedRec := doRefRequest(t, h, http.MethodPost, "/authorizer/signed-authorizer/test", map[string]any{
			"token": "sometoken",
		}, nil)
		require.Equal(t, http.StatusOK, signedRec.Code)
		assert.Contains(t, signedRec.Body.String(), `"isAuthenticated":false`)
	})
}

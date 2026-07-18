package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantID   bool
	}{
		{
			name:     "success",
			body:     map[string]any{"DomainName": "my-domain", "AuthMode": "IAM"},
			wantCode: http.StatusOK,
			wantID:   true,
		},
		{
			name:     "missing domain name",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateDomain", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["DomainId"])
				assert.NotEmpty(t, resp["Url"])
			}
		})
	}
}

func TestHandler_DomainLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create domain.
	recCreate := doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName": "test-domain",
		"AuthMode":   "IAM",
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	domainID := createOut["DomainId"].(string)

	// Describe domain by ID.
	recDesc := doSageMakerRequest(t, h, "DescribeDomain", map[string]any{"DomainId": domainID})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &descOut))
	assert.Equal(t, "test-domain", descOut["DomainName"])

	// List domains.
	recList := doSageMakerRequest(t, h, "ListDomains", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["Domains"].([]any), 1)

	// Update domain.
	recUpdate := doSageMakerRequest(t, h, "UpdateDomain", map[string]any{"DomainId": domainID})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	// Delete domain.
	recDelete := doSageMakerRequest(t, h, "DeleteDomain", map[string]any{"DomainId": domainID})
	assert.Equal(t, http.StatusOK, recDelete.Code)

	// Domain should be gone.
	recDesc2 := doSageMakerRequest(t, h, "DescribeDomain", map[string]any{"DomainId": domainID})
	assert.Equal(t, http.StatusBadRequest, recDesc2.Code)
}

func TestHandler_Domain_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeDomain", "UpdateDomain", "DeleteDomain"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"DomainId": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_CreateDomain_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doSageMakerRequest(t, h, "CreateDomain", map[string]any{"DomainName": "dup-domain"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateDomain", map[string]any{"DomainName": "dup-domain"})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

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
			name: "success",
			body: map[string]any{
				"DomainName":          "my-domain",
				"AuthMode":            "IAM",
				"DefaultUserSettings": map[string]any{"ExecutionRole": "arn:aws:iam::000000000000:role/test"},
			},
			wantCode: http.StatusOK,
			wantID:   true,
		},
		{
			name:     "missing domain name",
			body:     map[string]any{"DefaultUserSettings": map[string]any{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing default user settings",
			body:     map[string]any{"DomainName": "no-settings-domain"},
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
		"DomainName":           "test-domain",
		"AuthMode":             "IAM",
		"AppNetworkAccessType": "VpcOnly",
		"KmsKeyId":             "arn:aws:kms:us-east-1:000000000000:key/test",
		"SubnetIds":            []string{"subnet-1", "subnet-2"},
		"VpcId":                "vpc-1",
		"DefaultUserSettings":  map[string]any{"ExecutionRole": "arn:aws:iam::000000000000:role/test"},
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	domainID := createOut["DomainId"].(string)

	// Describe domain by ID — the fields accepted on Create must round-trip,
	// not be silently dropped (gopherstack-oc9v: this whole family was an
	// unaudited blind spot before this pass).
	recDesc := doSageMakerRequest(t, h, "DescribeDomain", map[string]any{"DomainId": domainID})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &descOut))
	assert.Equal(t, "test-domain", descOut["DomainName"])
	assert.Equal(t, "VpcOnly", descOut["AppNetworkAccessType"])
	assert.Equal(t, "vpc-1", descOut["VpcId"])
	assert.ElementsMatch(t, []any{"subnet-1", "subnet-2"}, descOut["SubnetIds"])
	assert.NotEmpty(t, descOut["DefaultUserSettings"])

	// List domains.
	recList := doSageMakerRequest(t, h, "ListDomains", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["Domains"].([]any), 1)

	// Update domain — a partial update must apply the overridable field
	// (AppNetworkAccessType) and leave the rest (VpcId) unchanged.
	recUpdate := doSageMakerRequest(t, h, "UpdateDomain", map[string]any{
		"DomainId":             domainID,
		"AppNetworkAccessType": "PublicInternetOnly",
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	recDescAfterUpdate := doSageMakerRequest(t, h, "DescribeDomain", map[string]any{"DomainId": domainID})
	require.Equal(t, http.StatusOK, recDescAfterUpdate.Code)

	var descAfterUpdate map[string]any
	require.NoError(t, json.Unmarshal(recDescAfterUpdate.Body.Bytes(), &descAfterUpdate))
	assert.Equal(t, "PublicInternetOnly", descAfterUpdate["AppNetworkAccessType"])
	assert.Equal(t, "vpc-1", descAfterUpdate["VpcId"])

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
	body := map[string]any{"DomainName": "dup-domain", "DefaultUserSettings": map[string]any{}}
	rec := doSageMakerRequest(t, h, "CreateDomain", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateDomain", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_ListDomains_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"mr-domain-a", "mr-domain-b", "mr-domain-c"} {
		rec := doSageMakerRequest(t, h, "CreateDomain", map[string]any{
			"DomainName":          name,
			"DefaultUserSettings": map[string]any{},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doSageMakerRequest(t, h, "ListDomains", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out["Domains"].([]any), 2, "MaxResults must cap the page, not just be parsed and ignored")
	assert.NotEmpty(t, out["NextToken"])
}

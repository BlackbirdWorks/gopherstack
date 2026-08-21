package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateWorkforce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{
		"WorkforceName": "my-workforce",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["WorkforceArn"], "my-workforce")
}

func TestHandler_DescribeWorkforce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{"WorkforceName": "wf-1"})
	rec := doSageMakerRequest(t, h, "DescribeWorkforce", map[string]any{"WorkforceName": "wf-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Workforce"])
}

func TestHandler_UpdateWorkforce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{"WorkforceName": "wf-upd"})
	rec := doSageMakerRequest(t, h, "UpdateWorkforce", map[string]any{"WorkforceName": "wf-upd"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Workforce"])
}

func TestHandler_CreateWorkforce_IPAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{
		"WorkforceName": "wf-dualstack",
		"IpAddressType": "dualstack",
	})

	rec := doSageMakerRequest(t, h, "DescribeWorkforce", map[string]any{"WorkforceName": "wf-dualstack"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wf := resp["Workforce"].(map[string]any)
	assert.Equal(t, "dualstack", wf["IpAddressType"])
}

func TestHandler_UpdateWorkforce_IPAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{
		"WorkforceName": "wf-upd-ip",
		"IpAddressType": "ipv4",
	})

	rec := doSageMakerRequest(t, h, "UpdateWorkforce", map[string]any{
		"WorkforceName": "wf-upd-ip",
		"IpAddressType": "dualstack",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wf := resp["Workforce"].(map[string]any)
	assert.Equal(t, "dualstack", wf["IpAddressType"])
}

func TestHandler_ListWorkforces_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{"WorkforceName": "wf-list-filter"})

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{name: "name contains match", body: map[string]any{"NameContains": "list-filter"}, wantCount: 1},
		{name: "name contains no match", body: map[string]any{"NameContains": "nope"}, wantCount: 0},
		{name: "max results zero page", body: map[string]any{"MaxResults": 0}, wantCount: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, "ListWorkforces", tc.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items, _ := resp["Workforces"].([]any)
			assert.Len(t, items, tc.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// FlowDefinition
// ---------------------------------------------------------------------------

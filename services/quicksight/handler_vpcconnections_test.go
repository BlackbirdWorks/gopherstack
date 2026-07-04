package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- VPC Connection CRUD round-trip, not-found, and duplicate errors ----

func TestQuickSight_VPCConnectionCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/vpc-connections"), map[string]any{
		"VPCConnectionId":  "vc1",
		"Name":             "VPC1",
		"VPCId":            "vpc-abc123",
		"SubnetIds":        []any{"subnet-1", "subnet-2"},
		"SecurityGroupIds": []any{"sg-1"},
		"RoleArn":          "arn:aws:iam::000000000000:role/vpc-role",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "vc1", createBody["VPCConnectionId"])
	assert.Contains(
		t,
		createBody["Arn"],
		"arn:aws:quicksight:us-east-1:000000000000:vpc-connection/vc1",
	)
	assert.Equal(t, "AVAILABLE", createBody["AvailabilityStatus"])

	// Duplicate create -> ResourceExistsException.
	dupRec := doRequest(t, h, http.MethodPost, accountPath("/vpc-connections"), map[string]any{
		"VPCConnectionId": "vc1",
		"Name":            "x",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	// Missing VPCConnectionId/Name -> validation error.
	invalidRec := doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/vpc-connections"),
		map[string]any{},
	)
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)
	assert.Equal(t, "InvalidParameterValueException", parseBody(t, invalidRec)["Code"])

	// Describe.
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/vpc-connections/vc1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	vc, ok := parseBody(t, describeRec)["VPCConnection"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "VPC1", vc["Name"])
	assert.Equal(t, "vpc-abc123", vc["VPCId"])
	subnetIDs, ok := vc["SubnetIds"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"subnet-1", "subnet-2"}, subnetIDs)

	// Describe missing -> 404.
	missingRec := doRequest(t, h, http.MethodGet, accountPath("/vpc-connections/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])

	// Update.
	updateRec := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/vpc-connections/vc1"),
		map[string]any{
			"Name":      "VPC1Renamed",
			"SubnetIds": []any{"subnet-3"},
		},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)
	updateBody := parseBody(t, updateRec)
	assert.Equal(t, "vc1", updateBody["VPCConnectionId"])
	assert.Equal(t, "UPDATE_SUCCESSFUL", updateBody["UpdateStatus"])

	describeAfterUpdate := doRequest(t, h, http.MethodGet, accountPath("/vpc-connections/vc1"), nil)
	afterVC := parseBody(t, describeAfterUpdate)["VPCConnection"].(map[string]any)
	assert.Equal(t, "VPC1Renamed", afterVC["Name"])
	assert.Equal(t, []any{"subnet-3"}, afterVC["SubnetIds"])

	// Update missing -> 404.
	updateMissingRec := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/vpc-connections/notexist"),
		map[string]any{"Name": "x"},
	)
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// Delete.
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/vpc-connections/vc1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Equal(t, "vc1", parseBody(t, deleteRec)["VPCConnectionId"])

	// Delete missing -> 404.
	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/vpc-connections/vc1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- ListVPCConnections pagination ----

func TestQuickSight_ListVPCConnections_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(
			t,
			h,
			http.MethodPost,
			accountPath("/vpc-connections"),
			map[string]any{"VPCConnectionId": id, "Name": id},
		)
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/vpc-connections?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["VPCConnectionSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["VPCConnectionId"].(string)] = true
	}

	page2Path := accountPath(fmt.Sprintf("/vpc-connections?max-results=2&next-token=%s", next))
	page2 := doRequest(t, h, http.MethodGet, page2Path, nil)
	require.Equal(t, http.StatusOK, page2.Code)
	items2 := parseBody(t, page2)["VPCConnectionSummaries"].([]any)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m := it.(map[string]any)
		assert.False(t, seen[m["VPCConnectionId"].(string)], "page 2 must not repeat page 1 items")
	}
}

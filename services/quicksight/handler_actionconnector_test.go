package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Action Connector CRUD round-trip, validation, not-found, and duplicate errors ----

func TestQuickSight_ActionConnectorCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	authConfig := map[string]any{
		"AuthenticationType": "API_KEY",
		"AuthenticationMetadata": map[string]any{
			"ApiKeyAuthMetadata": map[string]any{"ApiKeyValue": "secret"},
		},
	}

	createRec := doRequest(t, h, http.MethodPost, accountPath("/action-connectors"), map[string]any{
		"ActionConnectorId":    "ac1",
		"Name":                 "Salesforce Connector",
		"Type":                 "SALESFORCE_CRM",
		"Description":          "prod connector",
		"AuthenticationConfig": authConfig,
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "ac1", createBody["ActionConnectorId"])
	assert.Contains(t, createBody["Arn"], "arn:aws:quicksight:us-east-1:000000000000:action-connector/ac1")
	assert.Equal(t, "CREATION_SUCCESSFUL", createBody["CreationStatus"])

	// Duplicate create -> ResourceExistsException.
	dupRec := doRequest(t, h, http.MethodPost, accountPath("/action-connectors"), map[string]any{
		"ActionConnectorId":    "ac1",
		"Name":                 "x",
		"Type":                 "GENERIC_HTTP",
		"AuthenticationConfig": authConfig,
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	// Missing required fields -> validation error.
	invalidRec := doRequest(t, h, http.MethodPost, accountPath("/action-connectors"), map[string]any{
		"ActionConnectorId": "ac2",
	})
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)
	assert.Equal(t, "InvalidParameterValueException", parseBody(t, invalidRec)["Code"])

	// Describe.
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/action-connectors/ac1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	ac, ok := parseBody(t, describeRec)["ActionConnector"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Salesforce Connector", ac["Name"])
	assert.Equal(t, "SALESFORCE_CRM", ac["Type"])
	assert.Equal(t, "prod connector", ac["Description"])
	authOut, ok := ac["AuthenticationConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "API_KEY", authOut["AuthenticationType"])

	// Describe missing -> 404.
	missingRec := doRequest(t, h, http.MethodGet, accountPath("/action-connectors/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])

	// Update.
	updateRec := doRequest(t, h, http.MethodPut, accountPath("/action-connectors/ac1"), map[string]any{
		"Name":                 "Renamed Connector",
		"AuthenticationConfig": authConfig,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	updateBody := parseBody(t, updateRec)
	assert.Equal(t, "ac1", updateBody["ActionConnectorId"])
	assert.Equal(t, "UPDATE_SUCCESSFUL", updateBody["UpdateStatus"])

	describeAfterUpdate := doRequest(t, h, http.MethodGet, accountPath("/action-connectors/ac1"), nil)
	afterAC := parseBody(t, describeAfterUpdate)["ActionConnector"].(map[string]any)
	assert.Equal(t, "Renamed Connector", afterAC["Name"])

	// Update missing -> 404.
	updateMissingRec := doRequest(
		t, h, http.MethodPut, accountPath("/action-connectors/notexist"),
		map[string]any{"Name": "x"},
	)
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// Delete.
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/action-connectors/ac1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Equal(t, "ac1", parseBody(t, deleteRec)["ActionConnectorId"])

	// Delete missing -> 404.
	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/action-connectors/ac1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- ListActionConnectors pagination ----

func TestQuickSight_ListActionConnectors_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(t, h, http.MethodPost, accountPath("/action-connectors"), map[string]any{
			"ActionConnectorId": id,
			"Name":              id,
			"Type":              "GENERIC_HTTP",
			"AuthenticationConfig": map[string]any{
				"AuthenticationType":     "NO_AUTH",
				"AuthenticationMetadata": map[string]any{},
			},
		})
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/action-connectors?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["ActionConnectorSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["ActionConnectorId"].(string)] = true
		// List summaries must not include AuthenticationConfig (matches the
		// real ActionConnectorSummary shape).
		assert.NotContains(t, m, "AuthenticationConfig")
	}

	page2Path := accountPath(fmt.Sprintf("/action-connectors?max-results=2&next-token=%s", next))
	page2 := doRequest(t, h, http.MethodGet, page2Path, nil)
	require.Equal(t, http.StatusOK, page2.Code)
	items2 := parseBody(t, page2)["ActionConnectorSummaries"].([]any)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m := it.(map[string]any)
		assert.False(t, seen[m["ActionConnectorId"].(string)], "page 2 must not repeat page 1 items")
	}
}

// ---- SearchActionConnectors filters by name ----

func TestQuickSight_SearchActionConnectors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"Salesforce Prod", "Salesforce Dev", "Jira Prod"} {
		doRequest(t, h, http.MethodPost, accountPath("/action-connectors"), map[string]any{
			"ActionConnectorId": name,
			"Name":              name,
			"Type":              "GENERIC_HTTP",
			"AuthenticationConfig": map[string]any{
				"AuthenticationType":     "NO_AUTH",
				"AuthenticationMetadata": map[string]any{},
			},
		})
	}

	rec := doRequest(t, h, http.MethodPost, accountPath("/search/action-connectors"), map[string]any{
		"Filters": []any{
			map[string]any{"Name": "ACTION_CONNECTOR_NAME", "Operator": "StringLike", "Value": "Salesforce"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	items := parseBody(t, rec)["ActionConnectorSummaries"].([]any)
	assert.Len(t, items, 2)
}

// ---- Action Connector permissions grant/revoke ----

func TestQuickSight_ActionConnectorPermissions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/action-connectors"), map[string]any{
		"ActionConnectorId": "ac1",
		"Name":              "ac1",
		"Type":              "GENERIC_HTTP",
		"AuthenticationConfig": map[string]any{
			"AuthenticationType":     "NO_AUTH",
			"AuthenticationMetadata": map[string]any{},
		},
	})

	updateRec := doRequest(t, h, http.MethodPut, accountPath("/action-connectors/ac1/permissions"), map[string]any{
		"GrantPermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:DescribeActionConnector"},
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	perms := parseBody(t, updateRec)["Permissions"].([]any)
	require.Len(t, perms, 1)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/action-connectors/ac1/permissions"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describePerms := parseBody(t, describeRec)["Permissions"].([]any)
	require.Len(t, describePerms, 1)

	// Revoke the grant, leaving no permissions.
	revokeRec := doRequest(t, h, http.MethodPut, accountPath("/action-connectors/ac1/permissions"), map[string]any{
		"RevokePermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:DescribeActionConnector"},
			},
		},
	})
	require.Equal(t, http.StatusOK, revokeRec.Code)
	assert.Empty(t, parseBody(t, revokeRec)["Permissions"])

	// Permissions ops on a missing connector -> 404.
	missingRec := doRequest(t, h, http.MethodGet, accountPath("/action-connectors/notexist/permissions"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
}

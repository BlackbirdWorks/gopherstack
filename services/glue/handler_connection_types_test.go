package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteConnectionType verifies real state mutation: built-in types are
// undeletable (distinct AccessDeniedException), unknown types are
// EntityNotFoundException, and a registered custom type can be deleted once.
func TestDeleteConnectionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "empty body missing ConnectionType returns 400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidInputException",
		},
		{
			name:     "built-in Salesforce is undeletable",
			input:    map[string]any{"ConnectionType": "Salesforce"},
			wantCode: http.StatusBadRequest,
			wantType: "AccessDeniedException",
		},
		{
			name:     "unknown type returns EntityNotFound",
			input:    map[string]any{"ConnectionType": "NoSuchType"},
			wantCode: http.StatusBadRequest,
			wantType: "EntityNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "DeleteConnectionType", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.wantType)
		})
	}
}

// TestDeleteConnectionType_CustomRoundTrip verifies a custom type registered via
// RegisterConnectionType can be deleted, and a second delete is EntityNotFound.
func TestDeleteConnectionType_CustomRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	regRec := doGlueRequest(t, h, "RegisterConnectionType", map[string]any{
		"ConnectionType":  "MyCustomConn",
		"Description":     "custom connector",
		"IntegrationType": "REST",
		"ConnectionProperties": map[string]any{
			"Url": map[string]any{"Name": "endpoint"},
		},
		"ConnectorAuthenticationConfiguration": map[string]any{
			"AuthenticationTypes": []any{"BASIC"},
		},
		"RestConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	var regOut map[string]any
	require.NoError(t, json.Unmarshal(regRec.Body.Bytes(), &regOut))
	assert.NotEmpty(t, regOut["ConnectionTypeArn"], "RegisterConnectionType must return ConnectionTypeArn")

	delRec := doGlueRequest(t, h, "DeleteConnectionType", map[string]any{"ConnectionType": "MyCustomConn"})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Second delete: already gone → EntityNotFoundException.
	del2 := doGlueRequest(t, h, "DeleteConnectionType", map[string]any{"ConnectionType": "MyCustomConn"})
	assert.Equal(t, http.StatusBadRequest, del2.Code)
	assert.Contains(t, del2.Body.String(), "EntityNotFoundException")
}

// TestDescribeConnectionType verifies required-field validation and that unknown
// types return EntityNotFoundException while built-in types resolve.
func TestDescribeConnectionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{name: "missing_type_returns_400", input: map[string]any{}, wantCode: http.StatusBadRequest},
		{
			name:     "known_type_returns_200",
			input:    map[string]any{"ConnectionType": "JDBC"},
			wantCode: http.StatusOK,
		},
		{
			name:     "case_insensitive_returns_200",
			input:    map[string]any{"ConnectionType": "salesforce"},
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown_type_returns_400",
			input:    map[string]any{"ConnectionType": "CUSTOM_CONN"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "DescribeConnectionType", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

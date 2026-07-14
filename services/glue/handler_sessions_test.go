package glue_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewOps_Session tests Session + Statement CRUD.
func TestNewOps_Session(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// CreateSession
	out := dispatchNewOp(t, h, "CreateSession", map[string]any{
		"Id":   "sess-1",
		"Role": "arn:aws:iam::123456789012:role/GlueRole",
		"Command": map[string]any{
			"Name":          "glueetl",
			"PythonVersion": "3",
		},
	})
	sess := out["Session"].(map[string]any)
	if sess["Id"] != "sess-1" {
		t.Errorf("session ID mismatch: %v", sess["Id"])
	}
	if sess["Status"] != "PROVISIONING" {
		t.Errorf("status mismatch: %v", sess["Status"])
	}

	// GetSession
	out2 := dispatchNewOp(t, h, "GetSession", map[string]any{"Id": "sess-1"})
	sess2 := out2["Session"].(map[string]any)
	if sess2["Id"] != "sess-1" {
		t.Errorf("GetSession ID mismatch: %v", sess2)
	}

	// RunStatement
	out3 := dispatchNewOp(t, h, "RunStatement", map[string]any{
		"SessionId": "sess-1",
		"Code":      "print('hello')",
	})
	if out3["Id"] == nil {
		t.Errorf("expected statement ID in response")
	}
	stmtID := int32(out3["Id"].(float64))

	// GetStatement
	out4 := dispatchNewOp(t, h, "GetStatement", map[string]any{
		"SessionId": "sess-1",
		"Id":        stmtID,
	})
	stmt := out4["Statement"].(map[string]any)
	if stmt["Code"] != "print('hello')" {
		t.Errorf("statement code mismatch: %v", stmt["Code"])
	}

	// ListStatements
	out5 := dispatchNewOp(t, h, "ListStatements", map[string]any{"SessionId": "sess-1"})
	stmts, _ := out5["Statements"].([]any)
	if len(stmts) != 1 {
		t.Errorf("expected 1 statement, got %d", len(stmts))
	}

	// CancelStatement
	dispatchNewOp(t, h, "CancelStatement", map[string]any{
		"SessionId": "sess-1",
		"Id":        stmtID,
	})

	// ListSessions
	out6 := dispatchNewOp(t, h, "ListSessions", map[string]any{})
	sessions, _ := out6["Sessions"].([]any)
	if len(sessions) != 1 {
		t.Errorf("expected 1 session, got %d", len(sessions))
	}

	// StopSession
	dispatchNewOp(t, h, "StopSession", map[string]any{"Id": "sess-1"})

	// DeleteSession
	dispatchNewOp(t, h, "DeleteSession", map[string]any{"Id": "sess-1"})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetSession", map[string]any{"Id": "sess-1"})
}

// TestCancelStatement_Validation verifies SessionId is required.
func TestCancelStatement_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{name: "missing_session_id_returns_400", input: map[string]any{}, wantCode: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CancelStatement", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

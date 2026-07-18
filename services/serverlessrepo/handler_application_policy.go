package serverlessrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// policyStatementRequest represents a policy statement in a PutApplicationPolicy request.
type policyStatementRequest struct {
	StatementID     string   `json:"statementId"`
	Actions         []string `json:"actions"`
	Principals      []string `json:"principals"`
	PrincipalOrgIDs []string `json:"principalOrgIDs"`
}

// putApplicationPolicyRequest is the request body for PutApplicationPolicy.
type putApplicationPolicyRequest struct {
	Statements []policyStatementRequest `json:"statements"`
}

func (h *Handler) handleGetApplicationPolicy(req *http.Request) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	stmts, backendErr := h.Backend.GetApplicationPolicy(appName)
	if backendErr != nil {
		return nil, backendErr
	}

	return json.Marshal(map[string]any{"statements": toPolicyStatementsResponse(stmts)})
}

func (h *Handler) handlePutApplicationPolicy(ctx context.Context, req *http.Request, body []byte) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	var putReq putApplicationPolicyRequest
	if jsonErr := json.Unmarshal(body, &putReq); jsonErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	stmts := make([]*ApplicationPolicyStatement, 0, len(putReq.Statements))
	for _, s := range putReq.Statements {
		stmts = append(stmts, &ApplicationPolicyStatement{
			Actions:         s.Actions,
			Principals:      s.Principals,
			PrincipalOrgIDs: s.PrincipalOrgIDs,
			StatementID:     s.StatementID,
		})
	}

	result, backendErr := h.Backend.PutApplicationPolicy(appName, stmts)
	if backendErr != nil {
		return nil, backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: updated application policy", "app", appName)

	return json.Marshal(map[string]any{"statements": toPolicyStatementsResponse(result)})
}

func toPolicyStatementsResponse(stmts []*ApplicationPolicyStatement) []map[string]any {
	out := make([]map[string]any, 0, len(stmts))

	for _, s := range stmts {
		out = append(out, map[string]any{
			"actions":         nonNilStringSlice(s.Actions),
			"principals":      nonNilStringSlice(s.Principals),
			"principalOrgIDs": nonNilStringSlice(s.PrincipalOrgIDs),
			"statementId":     s.StatementID,
		})
	}

	return out
}

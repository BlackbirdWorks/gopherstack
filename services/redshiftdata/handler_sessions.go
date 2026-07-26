package redshiftdata

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleListSessions handles ListSessions. Field names verified against
// aws-sdk-go-v2/service/redshiftdata@v1.43.0's
// awsAwsjson11_serializeOpDocumentListSessionsInput (serializers.go) and
// awsAwsjson11_deserializeDocumentSessionData (deserializers.go).
func (h *Handler) handleListSessions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		RoleLevel         *bool  `json:"RoleLevel"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		Database          string `json:"Database"`
		NextToken         string `json:"NextToken"`
		SessionID         string `json:"SessionId"`
		Status            string `json:"Status"`
		WorkgroupName     string `json:"WorkgroupName"`
		MaxResults        int    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := ValidateListSessionsRequest(
		req.SessionID, req.Status, req.ClusterIdentifier, req.WorkgroupName, req.Database,
	); err != nil {
		return nil, err
	}

	// RoleLevel is accepted on the wire but not applied as a filter, matching the
	// identical, already-documented gap on ListStatements' RoleLevel (see
	// PARITY.md gaps): this mock has no per-caller-identity or per-IAM-session
	// model of session ownership to filter on.
	sessions, nextToken, err := h.Backend.ListSessions(ctx, ListSessionsFilter{
		ClusterIdentifier: req.ClusterIdentifier,
		WorkgroupName:     req.WorkgroupName,
		Database:          req.Database,
		SessionID:         req.SessionID,
		Status:            req.Status,
		NextToken:         req.NextToken,
		MaxResults:        req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(sessions))
	for _, sess := range sessions {
		items = append(items, sessionToListItem(sess))
	}

	resp := map[string]any{
		"Sessions": items,
	}

	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

// sessionToListItem converts a derived SessionData to the SessionData wire shape
// (aws-sdk-go-v2/service/redshiftdata/types.SessionData). CreatedAt/UpdatedAt use
// epochSeconds like every other timestamp field in this package's AWS JSON 1.1
// responses (see epochSeconds in handler_statements.go). ClusterIdentifier/
// WorkgroupName/Database/DbUser are conditionally included since real AWS omits
// whichever of ClusterIdentifier/WorkgroupName doesn't apply to the connection type,
// and Database/DbUser are themselves optional members.
func sessionToListItem(sess *SessionData) map[string]any {
	item := map[string]any{
		"SessionId":    sess.SessionID,
		keyStatusField: sess.Status,
		keyCreatedAt:   epochSeconds(sess.CreatedAt),
		keyUpdatedAt:   epochSeconds(sess.UpdatedAt),
	}

	if sess.ClusterIdentifier != "" {
		item["ClusterIdentifier"] = sess.ClusterIdentifier
	}

	if sess.WorkgroupName != "" {
		item["WorkgroupName"] = sess.WorkgroupName
	}

	if sess.Database != "" {
		item["Database"] = sess.Database
	}

	if sess.DBUser != "" {
		item["DbUser"] = sess.DBUser
	}

	return item
}

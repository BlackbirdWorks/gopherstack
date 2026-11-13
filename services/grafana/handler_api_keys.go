package grafana

import (
	"context"
	"encoding/json/v2"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// createWorkspaceAPIKeyRequest mirrors CreateWorkspaceApiKeyInput's JSON body.
type createWorkspaceAPIKeyRequest struct {
	KeyName       string `json:"keyName"`
	KeyRole       string `json:"keyRole"`
	SecondsToLive int32  `json:"secondsToLive"`
}

func (h *Handler) handleCreateWorkspaceAPIKey(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req createWorkspaceAPIKeyRequest
	if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil {
		return nil, validationError("invalid request body: " + err.Error())
	}

	key, err := h.Backend.CreateWorkspaceAPIKey(segs[1], req.KeyName, req.KeyRole, req.SecondsToLive)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"key":          key.Key,
		"keyName":      key.KeyName,
		keyWorkspaceID: key.WorkspaceID,
	}, jsonx.V1())
}

func (h *Handler) handleDeleteWorkspaceAPIKey(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	workspaceID, keyName := segs[1], segs[3]

	if err := h.Backend.DeleteWorkspaceAPIKey(workspaceID, keyName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"keyName":      keyName,
		keyWorkspaceID: workspaceID,
	}, jsonx.V1())
}

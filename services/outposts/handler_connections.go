package outposts

import (
	"context"
	"net/http"
)

func (h *Handler) handleStartConnection(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startConnectionRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.StartConnection(&req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(startConnectionResponse{
		ConnectionId:      c.ID,
		UnderlayIpAddress: c.UnderlayIPAddress,
	})
}

func (h *Handler) handleGetConnection(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	c, err := h.Backend.GetConnection(segs[1])
	if err != nil {
		return nil, err
	}

	return marshalResponse(getConnectionResponse{
		ConnectionDetails: toConnectionDetailsWire(c),
		ConnectionId:      c.ID,
	})
}

package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleStartProtectedJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobParameters        map[string]any `json:"jobParameters"`
		ResultConfiguration  map[string]any `json:"resultConfiguration"`
		MembershipIdentifier string         `json:"membershipIdentifier"`
		Type                 string         `json:"type"`
	}
	_ = json.Unmarshal(body, &req)
	j, err := h.Backend.StartProtectedJob(
		req.MembershipIdentifier,
		req.Type,
		req.JobParameters,
		req.ResultConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedJob: j}), nil
}

func (h *Handler) handleGetProtectedJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier   string `json:"membershipIdentifier"`
		ProtectedJobIdentifier string `json:"protectedJobIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	j, err := h.Backend.GetProtectedJob(req.MembershipIdentifier, req.ProtectedJobIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedJob: j}), nil
}

func (h *Handler) handleListProtectedJobs(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListProtectedJobs(
		req.MembershipIdentifier,
		qp(c, "status"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"protectedJobs": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateProtectedJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier   string `json:"membershipIdentifier"`
		ProtectedJobIdentifier string `json:"protectedJobIdentifier"`
		TargetStatus           string `json:"targetStatus"`
	}
	_ = json.Unmarshal(body, &req)
	j, err := h.Backend.UpdateProtectedJob(
		req.MembershipIdentifier,
		req.ProtectedJobIdentifier,
		req.TargetStatus,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedJob: j}), nil
}

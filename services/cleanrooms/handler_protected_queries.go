package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleStartProtectedQuery(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		SQLParameters        map[string]any `json:"sqlParameters"`
		ResultConfiguration  map[string]any `json:"resultConfiguration"`
		ComputeConfiguration map[string]any `json:"computeConfiguration"`
		MembershipIdentifier string         `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	var sqlText string
	if req.SQLParameters != nil {
		if v, ok := req.SQLParameters["queryString"].(string); ok {
			sqlText = v
		}
	}
	q, err := h.Backend.StartProtectedQuery(
		req.MembershipIdentifier,
		sqlText,
		req.ResultConfiguration,
		req.ComputeConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedQuery: q}), nil
}

func (h *Handler) handleGetProtectedQuery(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		ProtectedQueryIdentifier string `json:"protectedQueryIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	q, err := h.Backend.GetProtectedQuery(req.MembershipIdentifier, req.ProtectedQueryIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedQuery: q}), nil
}

func (h *Handler) handleListProtectedQueries(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListProtectedQueries(
		req.MembershipIdentifier,
		qp(c, "status"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"protectedQueries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateProtectedQuery(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		ProtectedQueryIdentifier string `json:"protectedQueryIdentifier"`
		TargetStatus             string `json:"targetStatus"`
	}
	_ = json.Unmarshal(body, &req)
	q, err := h.Backend.UpdateProtectedQuery(
		req.MembershipIdentifier,
		req.ProtectedQueryIdentifier,
		req.TargetStatus,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedQuery: q}), nil
}

// buildProtectedComputeHandlers wires both ProtectedQuery and ProtectedJob
// ops. The two op-maps have near-identical shape (Start/Get/List/Update),
// so they are grouped into one function rather than kept as two
// structurally-identical map literals in separate files, which previously
// tripped the dupl linter (see handler_protected_jobs.go, where the
// ProtectedJob handle* functions still live).
func (h *Handler) buildProtectedComputeHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		// ProtectedQuery
		opStartProtectedQuery: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleStartProtectedQuery(ctx, body)
		},
		opGetProtectedQuery: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetProtectedQuery(ctx, body)
		},
		opListProtectedQueries: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListProtectedQueries(ctx, body, ec)
		},
		opUpdateProtectedQuery: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateProtectedQuery(ctx, body)
		},
		// ProtectedJob
		opStartProtectedJob: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleStartProtectedJob(ctx, body)
		},
		opGetProtectedJob: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetProtectedJob(ctx, body)
		},
		opListProtectedJobs: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListProtectedJobs(ctx, body, ec)
		},
		opUpdateProtectedJob: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateProtectedJob(ctx, body)
		},
	}
}

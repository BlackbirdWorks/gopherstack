package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type rootObject struct {
	ID          string             `json:"Id"`
	ARN         string             `json:"Arn"`
	Name        string             `json:"Name"`
	PolicyTypes []policyTypeObject `json:"PolicyTypes"`
}

type policyTypeObject struct {
	Type   string `json:"Type"`
	Status string `json:"Status"`
}

type listRootsResponse struct {
	NextToken string       `json:"NextToken,omitempty"`
	Roots     []rootObject `json:"Roots"`
}

// dispatchRoot handles root operations.
func (h *Handler) dispatchRoot(c *echo.Context, op string, body []byte) (bool, error) {
	if op == "ListRoots" {
		return true, h.handleListRoots(c, body)
	}

	return false, nil
}

type listRootsRequest struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListRoots(c *echo.Context, body []byte) error {
	var req listRootsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
		}
	}

	if rejected, err := h.checkPaging(c, req.MaxResults, req.NextToken); rejected {
		return err
	}

	roots, err := h.Backend.ListRoots()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]rootObject, 0, len(roots))
	for _, r := range roots {
		objs = append(objs, toRootObject(r))
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listRootsResponse{Roots: p.Data, NextToken: p.Next})
}

func toRootObject(r *Root) rootObject {
	pts := make([]policyTypeObject, 0, len(r.PolicyTypes))
	for _, pt := range r.PolicyTypes {
		pts = append(pts, policyTypeObject(pt))
	}

	return rootObject{
		ID:          r.ID,
		ARN:         r.ARN,
		Name:        r.Name,
		PolicyTypes: pts,
	}
}

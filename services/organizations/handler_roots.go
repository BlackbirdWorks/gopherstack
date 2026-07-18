package organizations

import (
	"net/http"

	"github.com/labstack/echo/v5"
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

func (h *Handler) handleListRoots(c *echo.Context, _ []byte) error {
	roots, err := h.Backend.ListRoots()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]rootObject, 0, len(roots))
	for _, r := range roots {
		objs = append(objs, toRootObject(r))
	}

	return c.JSON(http.StatusOK, listRootsResponse{Roots: objs})
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

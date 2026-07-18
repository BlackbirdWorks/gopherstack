package mediaconvert

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func parsePolicyRoute(method string) mcRoute {
	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetPolicy}
	case http.MethodPut:
		return mcRoute{operation: opPutPolicy}
	case http.MethodDelete:
		return mcRoute{operation: opDeletePolicy}
	}

	return mcRoute{operation: opUnknown}
}

// --- Policy handlers ---

type policyWrapper struct {
	Policy *Policy `json:"policy"`
}

func (h *Handler) handleGetPolicy(c *echo.Context) error {
	p, err := h.Backend.GetPolicy()
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, policyWrapper{Policy: p})
}

func (h *Handler) handleDeletePolicy(c *echo.Context) error {
	if err := h.Backend.DeletePolicy(); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type putPolicyInput struct {
	Policy *policyInputEntry `json:"policy"`
}

type policyInputEntry struct {
	HTTPInputs  string `json:"httpInputs,omitempty"`
	HTTPSInputs string `json:"httpsInputs,omitempty"`
	S3Inputs    string `json:"s3Inputs,omitempty"`
}

func (h *Handler) handlePutPolicy(c *echo.Context, body []byte) error {
	var in putPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	var httpInputs, httpsInputs, s3Inputs string
	if in.Policy != nil {
		httpInputs = in.Policy.HTTPInputs
		httpsInputs = in.Policy.HTTPSInputs
		s3Inputs = in.Policy.S3Inputs
	}

	p := h.Backend.PutPolicy(httpInputs, httpsInputs, s3Inputs)

	return c.JSON(http.StatusOK, policyWrapper{Policy: p})
}

package iotwireless

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type createDestinationRequest struct {
	Name           string    `json:"Name"`
	Expression     string    `json:"Expression"`
	ExpressionType string    `json:"ExpressionType"`
	RoleArn        string    `json:"RoleArn"`
	Description    string    `json:"Description"`
	Tags           []tags.KV `json:"Tags"`
}

type destinationEntry struct {
	Arn            string `json:"Arn"`
	Name           string `json:"Name"`
	Expression     string `json:"Expression"`
	ExpressionType string `json:"ExpressionType"`
	RoleArn        string `json:"RoleArn"`
	Description    string `json:"Description"`
}

type listDestinationsResponse struct {
	DestinationList []destinationEntry `json:"DestinationList"`
}

// --- Destination handlers ---

func (h *Handler) createDestination(c *echo.Context, body []byte) error {
	var req createDestinationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	dest, err := h.Backend.CreateDestination(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Expression, req.ExpressionType, req.RoleArn, req.Description, tagKVsToMap(req.Tags),
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, destinationEntry{
		Arn:            dest.ARN,
		Name:           dest.Name,
		Expression:     dest.Expression,
		ExpressionType: dest.ExpressionType,
		RoleArn:        dest.RoleArn,
		Description:    dest.Description,
	})
}

func (h *Handler) getDestination(c *echo.Context, name string) error {
	dest, err := h.Backend.GetDestination(h.AccountID, h.DefaultRegion, name)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, destinationEntry{
		Arn:            dest.ARN,
		Name:           dest.Name,
		Expression:     dest.Expression,
		ExpressionType: dest.ExpressionType,
		RoleArn:        dest.RoleArn,
		Description:    dest.Description,
	})
}

func (h *Handler) listDestinations(c *echo.Context) error {
	dests := h.Backend.ListDestinations(h.AccountID, h.DefaultRegion)
	entries := make([]destinationEntry, 0, len(dests))

	for _, dest := range dests {
		entries = append(entries, destinationEntry{
			Arn:            dest.ARN,
			Name:           dest.Name,
			Expression:     dest.Expression,
			ExpressionType: dest.ExpressionType,
			RoleArn:        dest.RoleArn,
			Description:    dest.Description,
		})
	}

	return writeJSON(c, http.StatusOK, listDestinationsResponse{DestinationList: entries})
}

func (h *Handler) deleteDestination(c *echo.Context, name string) error {
	err := h.Backend.DeleteDestination(h.AccountID, h.DefaultRegion, name)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateDestination(c *echo.Context, name string) error {
	var req struct {
		Expression     string `json:"Expression"`
		ExpressionType string `json:"ExpressionType"`
		RoleArn        string `json:"RoleArn"`
		Description    string `json:"Description"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateDestination(
		h.AccountID, h.DefaultRegion, name,
		req.Expression, req.ExpressionType, req.RoleArn, req.Description,
	); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

package mediaconvert

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func parseQueueRoute(method, suffix string) mcRoute {
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: opListQueues}
		case http.MethodPost:
			return mcRoute{operation: opCreateQueue}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetQueue, resource: name}
	case http.MethodPut:
		return mcRoute{operation: opUpdateQueue, resource: name}
	case http.MethodDelete:
		return mcRoute{operation: opDeleteQueue, resource: name}
	}

	return mcRoute{operation: opUnknown}
}

// --- Queue handlers ---

type createQueueInput struct {
	// ReservationPlanSettings is the wire field name the real MediaConvert
	// API uses on CreateQueueInput/UpdateQueueInput (it becomes
	// ReservationPlan on the Queue output resource -- the request and
	// response field names differ).
	ReservationPlanSettings *ReservationPlan  `json:"reservationPlanSettings,omitempty"`
	ServiceOverrides        map[string]any    `json:"serviceOverrides,omitempty"`
	Tags                    map[string]string `json:"tags,omitempty"`
	Name                    string            `json:"name"`
	Description             string            `json:"description,omitempty"`
	PricingPlan             string            `json:"pricingPlan,omitempty"`
	Status                  string            `json:"status,omitempty"`
	ConcurrentJobs          int               `json:"concurrentJobs,omitempty"`
}

type queueWrapper struct {
	Queue *Queue `json:"queue"`
}

type queuesListOutput struct {
	Queues []*Queue `json:"queues"`
}

func (h *Handler) handleCreateQueue(c *echo.Context, body []byte) error {
	var in createQueueInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	q, err := h.Backend.CreateQueueFull(
		in.Name, in.Description, in.PricingPlan, in.Status,
		in.Tags, in.ConcurrentJobs, in.ReservationPlanSettings, in.ServiceOverrides,
	)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, queueWrapper{Queue: q})
}

func (h *Handler) handleGetQueue(c *echo.Context, name string) error {
	q, err := h.Backend.GetQueue(name)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, queueWrapper{Queue: q})
}

func (h *Handler) handleListQueues(c *echo.Context) error {
	queues := h.Backend.ListQueues()
	if queues == nil {
		queues = []*Queue{}
	}

	q := c.Request().URL.Query()

	if q.Get("order") == orderDescending {
		reverseSlice(queues)
	}

	return c.JSON(http.StatusOK, queuesListOutput{Queues: limitSlice(queues, parseMaxResults(q.Get("maxResults")))})
}

type updateQueueInput struct {
	ReservationPlanSettings *ReservationPlan `json:"reservationPlanSettings,omitempty"`
	ConcurrentJobs          *int             `json:"concurrentJobs,omitempty"`
	Description             string           `json:"description,omitempty"`
	Status                  string           `json:"status,omitempty"`
}

func (h *Handler) handleUpdateQueue(c *echo.Context, name string, body []byte) error {
	var in updateQueueInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	q, err := h.Backend.UpdateQueue(name, in.Description, in.Status, in.ConcurrentJobs, in.ReservationPlanSettings)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, queueWrapper{Queue: q})
}

func (h *Handler) handleDeleteQueue(c *echo.Context, name string) error {
	if err := h.Backend.DeleteQueue(name); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

package databrew

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func parseScheduleOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateSchedule
		}
	case http.MethodGet:
		if name == "" {
			return opListSchedules
		}

		return opDescribeSchedule
	case http.MethodPut:

		return opUpdateSchedule
	case http.MethodDelete:

		return opDeleteSchedule
	}

	return opUnknown
}

func (h *Handler) dispatchSchedule(ctx context.Context, action string, body []byte) ([]byte, bool, error) {
	switch action {
	case opCreateSchedule:
		r, e := h.handleCreateSchedule(ctx, body)

		return r, true, e
	case opDescribeSchedule:
		r, e := h.handleDescribeSchedule(ctx, body)

		return r, true, e
	case opListSchedules:
		r, e := h.handleListSchedules(ctx, body)

		return r, true, e
	case opUpdateSchedule:
		r, e := h.handleUpdateSchedule(ctx, body)

		return r, true, e
	case opDeleteSchedule:
		r, e := h.handleDeleteSchedule(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) handleCreateSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags           map[string]string `json:"Tags"`
		Name           string            `json:"Name"`
		CronExpression string            `json:"CronExpression"`
		JobNames       []string          `json:"JobNames"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	sc, err := h.Backend.CreateSchedule(ctx, req.Name, req.JobNames, req.CronExpression, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: sc.Name})
}

func (h *Handler) handleDescribeSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	sc, err := h.Backend.DescribeSchedule(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(sc)
}

func (h *Handler) handleListSchedules(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	schedules, next := h.Backend.ListSchedules(ctx, maxResults, req.NextToken)

	return json.Marshal(map[string]any{"Schedules": schedules, nextTokenKey: next})
}

func (h *Handler) handleUpdateSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name           string   `json:"Name"`
		CronExpression string   `json:"CronExpression"`
		JobNames       []string `json:"JobNames"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateSchedule(ctx, req.Name, req.JobNames, req.CronExpression); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteSchedule(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

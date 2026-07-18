package databrew

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func parseDatasetOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateDataset
		}
	case http.MethodGet:
		if name == "" {
			return opListDatasets
		}

		return opDescribeDataset
	case http.MethodPut:
		if name != "" {
			return opUpdateDataset
		}
	case http.MethodDelete:
		if name != "" {
			return opDeleteDataset
		}
	}

	return opUnknown
}

func (h *Handler) dispatchDataset(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateDataset:
		r, e := h.handleCreateDataset(ctx, body)

		return r, true, e
	case opDescribeDataset:
		r, e := h.handleDescribeDataset(ctx, body)

		return r, true, e
	case opListDatasets:
		r, e := h.handleListDatasets(ctx, body)

		return r, true, e
	case opUpdateDataset:
		r, e := h.handleUpdateDataset(ctx, body)

		return r, true, e
	case opDeleteDataset:
		r, e := h.handleDeleteDataset(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) handleCreateDataset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FormatOptions DatasetFormatOptions `json:"FormatOptions"`
		Input         DatasetInput         `json:"Input"`
		Tags          map[string]string    `json:"Tags"`
		Name          string               `json:"Name"`
		Format        string               `json:"Format"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	ds, err := h.Backend.CreateDataset(ctx, req.Name, req.Format, req.Input, req.FormatOptions, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: ds.Name})
}

func (h *Handler) handleDescribeDataset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	ds, err := h.Backend.DescribeDataset(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(ds)
}

func (h *Handler) handleListDatasets(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	datasets, next := h.Backend.ListDatasets(ctx, maxResults, req.NextToken)

	return json.Marshal(map[string]any{"Datasets": datasets, nextTokenKey: next})
}

func (h *Handler) handleUpdateDataset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FormatOptions DatasetFormatOptions `json:"FormatOptions"`
		Input         DatasetInput         `json:"Input"`
		Name          string               `json:"Name"`
		Format        string               `json:"Format"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateDataset(ctx, req.Name, req.Format, req.Input, req.FormatOptions); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteDataset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteDataset(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

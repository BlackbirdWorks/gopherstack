package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// Domain handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDomain(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainName string      `json:"DomainName"`
		AuthMode   string      `json:"AuthMode"`
		Tags       []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", errInvalidRequest)
	}

	d, err := h.Backend.CreateDomain(ctx, req.DomainName, req.AuthMode, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created domain", "name", d.DomainName, "id", d.DomainID)

	return json.Marshal(
		map[string]string{keyDomainArn: d.DomainArn, keyDomainID: d.DomainID, keyURL: d.URL},
	)
}

func (h *Handler) handleDescribeDomain(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID string `json:"DomainId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	d, err := h.Backend.DescribeDomain(ctx, req.DomainID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyDomainID:         d.DomainID,
		keyDomainArn:        d.DomainArn,
		"DomainName":        d.DomainName,
		keyStatus:           d.Status,
		"AuthMode":          d.AuthMode,
		"Url":               d.URL,
		keyCreationTime:     epochSeconds(d.CreationTime),
		keyLastModifiedTime: epochSeconds(d.LastModifiedTime),
	})
}

type domainSummary struct {
	DomainID     string  `json:"DomainId"`
	DomainArn    string  `json:"DomainArn"`
	DomainName   string  `json:"DomainName"`
	Status       string  `json:"Status"`
	CreationTime float64 `json:"CreationTime"`
}

func (h *Handler) handleListDomains(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	domains, nextToken := h.Backend.ListDomains(ctx, req.NextToken)
	summaries := make([]domainSummary, 0, len(domains))

	for _, d := range domains {
		summaries = append(summaries, domainSummary{
			DomainID:     d.DomainID,
			DomainArn:    d.DomainArn,
			DomainName:   d.DomainName,
			Status:       d.Status,
			CreationTime: epochSeconds(d.CreationTime),
		})
	}

	resp := map[string]any{"Domains": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteDomain(ctx context.Context, body []byte) error {
	var req struct {
		DomainID string `json:"DomainId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteDomain(ctx, req.DomainID); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted domain", "id", req.DomainID)

	return nil
}

func (h *Handler) handleUpdateDomain(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID string `json:"DomainId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	d, err := h.Backend.UpdateDomain(ctx, req.DomainID)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated domain", "id", req.DomainID)

	return json.Marshal(map[string]string{keyDomainArn: d.DomainArn})
}

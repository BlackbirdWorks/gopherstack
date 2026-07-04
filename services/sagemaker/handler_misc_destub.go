package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Dispatch for the miscellaneous de-stubbed operations grouped in
// backend_misc_destub.go: Service Catalog portfolio toggle, ResourceCatalogs,
// TrialComponent association extras, image alias listing, and UpdateProject.
// ---------------------------------------------------------------------------

// miscDestubOpsSupported returns the operations dispatched by dispatchMiscDestubOps.
func miscDestubOpsSupported() []string {
	return []string{
		"EnableSagemakerServicecatalogPortfolio",
		"DisableSagemakerServicecatalogPortfolio",
		"GetSagemakerServicecatalogPortfolioStatus",
		"ListResourceCatalogs",
		"DisassociateTrialComponent",
		"ListTrialComponents",
		"ListAliases",
		"UpdateProject",
	}
}

func (h *Handler) dispatchMiscDestubOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "EnableSagemakerServicecatalogPortfolio":
		h.Backend.EnableSagemakerServicecatalogPortfolio(ctx)

		return []byte("{}"), true, nil
	case "DisableSagemakerServicecatalogPortfolio":
		h.Backend.DisableSagemakerServicecatalogPortfolio(ctx)

		return []byte("{}"), true, nil
	case "GetSagemakerServicecatalogPortfolioStatus":
		status := h.Backend.GetSagemakerServicecatalogPortfolioStatus(ctx)
		r, err := json.Marshal(map[string]string{keyStatus: status})

		return r, true, err
	case "ListResourceCatalogs":
		r, err := json.Marshal(map[string]any{"ResourceCatalogs": h.Backend.ListResourceCatalogs()})

		return r, true, err
	case "DisassociateTrialComponent":
		r, err := h.handleDisassociateTrialComponent(ctx, body)

		return r, true, err
	case "ListTrialComponents":
		r, err := h.handleListTrialComponents(ctx, body)

		return r, true, err
	case "ListAliases":
		r, err := h.handleListAliases(ctx, body)

		return r, true, err
	case "UpdateProject":
		r, err := h.handleUpdateProject(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) handleDisassociateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrialComponentName string `json:"TrialComponentName"`
		TrialName          string `json:"TrialName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	trialArn, trialComponentArn, err := h.Backend.DisassociateTrialComponent(
		ctx, req.TrialName, req.TrialComponentName,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{
		keyTrialArn:          trialArn,
		keyTrialComponentArn: trialComponentArn,
	})
}

func (h *Handler) handleListTrialComponents(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ExperimentName string `json:"ExperimentName,omitempty"`
		TrialName      string `json:"TrialName,omitempty"`
		NextToken      string `json:"NextToken,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, nextToken := h.Backend.ListTrialComponents(ctx, req.ExperimentName, req.TrialName, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))

	for _, tc := range items {
		summary := map[string]any{
			keyTrialComponentArn: tc.TrialComponentArn,
			"TrialComponentName": tc.TrialComponentName,
			keyCreationTime:      epochSeconds(tc.CreationTime),
			keyLastModifiedTime:  epochSeconds(tc.LastModifiedTime),
		}
		if tc.DisplayName != "" {
			summary["DisplayName"] = tc.DisplayName
		}
		if tc.Status != "" {
			summary["Status"] = map[string]string{"PrimaryStatus": tc.Status}
		}
		if tc.StartTime != nil {
			summary["StartTime"] = epochSeconds(*tc.StartTime)
		}
		if tc.EndTime != nil {
			summary["EndTime"] = epochSeconds(*tc.EndTime)
		}

		summaries = append(summaries, summary)
	}

	return listResp("TrialComponentSummaries", summaries, nextToken)
}

func (h *Handler) handleListAliases(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
		NextToken string `json:"NextToken,omitempty"`
		Version   int32  `json:"Version,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	aliases, nextToken, err := h.Backend.ListImageAliases(ctx, req.ImageName, req.Version, req.NextToken)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{"SageMakerImageVersionAliases": aliases}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleUpdateProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ProjectName        string      `json:"ProjectName"`
		ProjectDescription string      `json:"ProjectDescription,omitempty"`
		Tags               []tagObject `json:"Tags,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdateProject(ctx, req.ProjectName, req.ProjectDescription, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyProjectArn: p.ProjectArn})
}

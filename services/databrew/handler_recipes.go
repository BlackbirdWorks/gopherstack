package databrew

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

func parseRecipeSubOp(method, subOp string) string {
	switch {
	case subOp == "publishRecipe":
		return opPublishRecipe
	// Convenience aliases: /recipes/{Name}/recipeVersions is not a real AWS
	// path (the real ListRecipeVersions op is GET /recipeVersions?name=...,
	// handled by mapResourceOp's segRecipeVersions case, and the real
	// BatchDeleteRecipeVersion op is POST
	// /recipes/{Name}/batchDeleteRecipeVersion below), but both GET and POST
	// forms are kept here so callers using the /databrew/v1/ nested
	// convenience path keep working.
	case subOp == segRecipeVersions && method == http.MethodGet:
		return opListRecipeVersions
	case subOp == segRecipeVersions && method == http.MethodPost:
		return opBatchDeleteRecipeVersion
	// Real AWS path: POST /recipes/{Name}/batchDeleteRecipeVersion.
	case subOp == "batchDeleteRecipeVersion" && method == http.MethodPost:
		return opBatchDeleteRecipeVersion
	case strings.HasPrefix(subOp, "recipeVersion/") && method == http.MethodDelete:
		return opDeleteRecipeVersion
	}

	return ""
}

func parseRecipeOp(method, name, subOp string) string {
	if op := parseRecipeSubOp(method, subOp); op != "" {
		return op
	}
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateRecipe
		}
	case http.MethodGet:
		if name == "" {
			return opListRecipes
		}

		return opDescribeRecipe
	case http.MethodPut:
		if name != "" {
			return opUpdateRecipe
		}
	case http.MethodDelete:
		if name != "" {
			return opDeleteRecipe
		}
	}

	return opUnknown
}

func (h *Handler) dispatchRecipe(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateRecipe:
		r, e := h.handleCreateRecipe(ctx, body)

		return r, true, e
	case opDescribeRecipe:
		r, e := h.handleDescribeRecipe(ctx, body)

		return r, true, e
	case opListRecipes:
		r, e := h.handleListRecipes(ctx, body)

		return r, true, e
	case opPublishRecipe:
		r, e := h.handlePublishRecipe(ctx, body)

		return r, true, e
	case opUpdateRecipe:
		r, e := h.handleUpdateRecipe(ctx, body)

		return r, true, e
	case opDeleteRecipe:
		r, e := h.handleDeleteRecipe(ctx, body)

		return r, true, e
	case opBatchDeleteRecipeVersion:
		r, e := h.handleBatchDeleteRecipeVersion(ctx, body)

		return r, true, e
	case opDeleteRecipeVersion:
		r, e := h.handleDeleteRecipeVersion(ctx, body)

		return r, true, e
	case opListRecipeVersions:
		r, e := h.handleListRecipeVersions(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) handleCreateRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
		Steps       []RecipeStep      `json:"Steps"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	r, err := h.Backend.CreateRecipe(ctx, req.Name, req.Description, req.Steps, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: r.Name})
}

func (h *Handler) handleDescribeRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name          string `json:"Name"`
		RecipeVersion string `json:"RecipeVersion"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	r, err := h.Backend.DescribeRecipe(ctx, req.Name, req.RecipeVersion)
	if err != nil {
		return nil, err
	}

	return json.Marshal(r)
}

func (h *Handler) handleListRecipes(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults    string `json:"MaxResults"`
		NextToken     string `json:"NextToken"`
		RecipeVersion string `json:"RecipeVersion"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	recipes, next := h.Backend.ListRecipes(ctx, maxResults, req.NextToken, req.RecipeVersion)

	return json.Marshal(map[string]any{"Recipes": recipes, nextTokenKey: next})
}

func (h *Handler) handlePublishRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.PublishRecipe(ctx, req.Name, req.Description); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleUpdateRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string       `json:"Name"`
		Description string       `json:"Description"`
		Steps       []RecipeStep `json:"Steps"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateRecipe(ctx, req.Name, req.Description, req.Steps); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteRecipe(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleBatchDeleteRecipeVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name           string   `json:"Name"`
		RecipeVersions []string `json:"RecipeVersions"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	errs, err := h.Backend.BatchDeleteRecipeVersion(ctx, req.Name, req.RecipeVersions)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyName: req.Name, "Errors": errs})
}

func (h *Handler) handleDeleteRecipeVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name          string `json:"Name"`
		RecipeVersion string `json:"RecipeVersion"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteRecipeVersion(ctx, req.Name, req.RecipeVersion); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name, "RecipeVersion": req.RecipeVersion})
}

func (h *Handler) handleListRecipeVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name       string `json:"Name"`
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	maxResults, _ := strconv.Atoi(req.MaxResults)

	recipes, next, err := h.Backend.ListRecipeVersions(ctx, req.Name, maxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Recipes": recipes, nextTokenKey: next})
}

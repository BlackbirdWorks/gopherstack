package sagemaker

import (
	"context"
	"encoding/json"
)

// ---------------------------------------------------------------------------
// Dispatch for the Service Catalog portfolio toggle and ResourceCatalogs ops.
// ---------------------------------------------------------------------------

// servicecatalogOpsSupported returns the operations dispatched by dispatchServicecatalogOps.
func servicecatalogOpsSupported() []string {
	return []string{
		"EnableSagemakerServicecatalogPortfolio",
		"DisableSagemakerServicecatalogPortfolio",
		"GetSagemakerServicecatalogPortfolioStatus",
		"ListResourceCatalogs",
	}
}

func (h *Handler) dispatchServicecatalogOps(
	ctx context.Context,
	op string,
	_ []byte,
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
	}

	return nil, false, nil
}

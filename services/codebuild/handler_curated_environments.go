package codebuild

import "context"

type listCuratedEnvironmentImagesInput struct{}

type listCuratedEnvironmentImagesOutput struct {
	Platforms []map[string]any `json:"platforms"`
}

func (h *Handler) handleListCuratedEnvironmentImages(
	_ context.Context,
	_ *listCuratedEnvironmentImagesInput,
) (*listCuratedEnvironmentImagesOutput, error) {
	return &listCuratedEnvironmentImagesOutput{Platforms: h.Backend.ListCuratedEnvironmentImages()}, nil
}

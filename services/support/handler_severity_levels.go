package support

import (
	"context"
	"fmt"
)

type describeSeverityLevelsInput struct {
	Language string `json:"language"`
}

type severityLevelView struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

type describeSeverityLevelsOutput struct {
	SeverityLevels []severityLevelView `json:"severityLevels"`
}

func (h *Handler) handleDescribeSeverityLevels(
	_ context.Context,
	in *describeSeverityLevelsInput,
) (*describeSeverityLevelsOutput, error) {
	if in.Language != "" && !validLanguage(in.Language) {
		return nil, fmt.Errorf("%w: invalid language", ErrValidation)
	}
	levels := h.Backend.DescribeSeverityLevels(in.Language)

	views := make([]severityLevelView, 0, len(levels))
	for _, l := range levels {
		views = append(views, severityLevelView(l))
	}

	return &describeSeverityLevelsOutput{SeverityLevels: views}, nil
}

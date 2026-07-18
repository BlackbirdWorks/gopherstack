package support

import (
	"context"
	"fmt"
)

type describeSupportedLanguagesInput struct {
	IssueType    string `json:"issueType"`
	ServiceCode  string `json:"serviceCode"`
	CategoryCode string `json:"categoryCode"`
}

type supportedLanguageView struct {
	Code     string `json:"code"`
	Display  string `json:"display"`
	Language string `json:"language"`
}

type describeSupportedLanguagesOutput struct {
	SupportedLanguages []supportedLanguageView `json:"supportedLanguages"`
}

func (h *Handler) handleDescribeSupportedLanguages(
	_ context.Context,
	in *describeSupportedLanguagesInput,
) (*describeSupportedLanguagesOutput, error) {
	if !validIssueType(in.IssueType) || in.ServiceCode == "" || in.CategoryCode == "" {
		return nil, fmt.Errorf("%w: issueType, serviceCode, and categoryCode are required", ErrValidation)
	}
	langs := h.Backend.DescribeSupportedLanguages(in.IssueType, in.ServiceCode, in.CategoryCode)

	views := make([]supportedLanguageView, 0, len(langs))
	for _, l := range langs {
		views = append(views, supportedLanguageView(l))
	}

	return &describeSupportedLanguagesOutput{SupportedLanguages: views}, nil
}

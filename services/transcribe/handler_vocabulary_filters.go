package transcribe

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- CreateVocabularyFilter ---

type createVocabularyFilterInput struct {
	Tags                    []transcribeTag `json:"Tags"`
	VocabularyFilterName    string          `json:"VocabularyFilterName"`
	LanguageCode            string          `json:"LanguageCode"`
	VocabularyFilterFileURI string          `json:"VocabularyFilterFileUri"`
	Words                   []string        `json:"Words"`
}

type createVocabularyFilterOutput struct {
	LastModifiedTime     *float64 `json:"LastModifiedTime,omitempty"`
	VocabularyFilterName string   `json:"VocabularyFilterName"`
	LanguageCode         string   `json:"LanguageCode"`
}

func (h *Handler) handleCreateVocabularyFilter(
	_ context.Context,
	in *createVocabularyFilterInput,
) (*createVocabularyFilterOutput, error) {
	f, err := h.Backend.CreateVocabularyFilter(&VocabularyFilter{
		VocabularyFilterName:    in.VocabularyFilterName,
		LanguageCode:            in.LanguageCode,
		Words:                   in.Words,
		VocabularyFilterFileURI: in.VocabularyFilterFileURI,
		Tags:                    tagsToMap(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return buildCreateVocabularyFilterOutput(f), nil
}

func buildCreateVocabularyFilterOutput(f *VocabularyFilter) *createVocabularyFilterOutput {
	out := &createVocabularyFilterOutput{
		VocabularyFilterName: f.VocabularyFilterName,
		LanguageCode:         f.LanguageCode,
	}

	if !f.LastModifiedTime.IsZero() {
		t := awstime.Epoch(f.LastModifiedTime)
		out.LastModifiedTime = &t
	}

	return out
}

// --- GetVocabularyFilter ---

type getVocabularyFilterInput struct {
	VocabularyFilterName string `json:"VocabularyFilterName"`
}

type vocabularyFilterOutput struct {
	LastModifiedTime     *float64 `json:"LastModifiedTime,omitempty"`
	VocabularyFilterName string   `json:"VocabularyFilterName"`
	LanguageCode         string   `json:"LanguageCode"`
}

type getVocabularyFilterOutput struct {
	LastModifiedTime     *float64 `json:"LastModifiedTime,omitempty"`
	VocabularyFilterName string   `json:"VocabularyFilterName"`
	LanguageCode         string   `json:"LanguageCode"`
	DownloadURI          string   `json:"DownloadUri,omitempty"`
}

func (h *Handler) handleGetVocabularyFilter(
	_ context.Context,
	in *getVocabularyFilterInput,
) (*getVocabularyFilterOutput, error) {
	f, err := h.Backend.GetVocabularyFilter(in.VocabularyFilterName)
	if err != nil {
		return nil, err
	}

	out := &getVocabularyFilterOutput{
		VocabularyFilterName: f.VocabularyFilterName,
		LanguageCode:         f.LanguageCode,
		DownloadURI:          f.VocabularyFilterFileURI,
	}

	if !f.LastModifiedTime.IsZero() {
		t := awstime.Epoch(f.LastModifiedTime)
		out.LastModifiedTime = &t
	}

	return out, nil
}

// --- UpdateVocabularyFilter ---

type updateVocabularyFilterInput struct {
	VocabularyFilterName    string   `json:"VocabularyFilterName"`
	LanguageCode            string   `json:"LanguageCode"`
	VocabularyFilterFileURI string   `json:"VocabularyFilterFileUri"`
	Words                   []string `json:"Words"`
}

func (h *Handler) handleUpdateVocabularyFilter(
	_ context.Context,
	in *updateVocabularyFilterInput,
) (*vocabularyFilterOutput, error) {
	f, err := h.Backend.UpdateVocabularyFilter(&VocabularyFilter{
		VocabularyFilterName:    in.VocabularyFilterName,
		LanguageCode:            in.LanguageCode,
		Words:                   in.Words,
		VocabularyFilterFileURI: in.VocabularyFilterFileURI,
	})
	if err != nil {
		return nil, err
	}

	return buildVocabularyFilterOutput(f), nil
}

func buildVocabularyFilterOutput(f *VocabularyFilter) *vocabularyFilterOutput {
	out := &vocabularyFilterOutput{
		VocabularyFilterName: f.VocabularyFilterName,
		LanguageCode:         f.LanguageCode,
	}

	if !f.LastModifiedTime.IsZero() {
		t := awstime.Epoch(f.LastModifiedTime)
		out.LastModifiedTime = &t
	}

	return out
}

// --- DeleteVocabularyFilter ---

type deleteVocabularyFilterInput struct {
	VocabularyFilterName string `json:"VocabularyFilterName"`
}

func (h *Handler) handleDeleteVocabularyFilter(
	_ context.Context,
	in *deleteVocabularyFilterInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteVocabularyFilter(in.VocabularyFilterName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- ListVocabularyFilters ---

type listVocabularyFiltersInput struct {
	NameContains string `json:"NameContains"`
	NextToken    string `json:"NextToken"`
}

type listVocabularyFiltersOutput struct {
	NextToken         string                   `json:"NextToken,omitempty"`
	VocabularyFilters []vocabularyFilterOutput `json:"VocabularyFilters"`
}

func (h *Handler) handleListVocabularyFilters(
	_ context.Context,
	in *listVocabularyFiltersInput,
) (*listVocabularyFiltersOutput, error) {
	filters, nextToken := h.Backend.ListVocabularyFilters(in.NameContains, in.NextToken)

	result := make([]vocabularyFilterOutput, 0, len(filters))
	for i := range filters {
		result = append(result, *buildVocabularyFilterOutput(&filters[i]))
	}

	return &listVocabularyFiltersOutput{
		VocabularyFilters: result,
		NextToken:         nextToken,
	}, nil
}

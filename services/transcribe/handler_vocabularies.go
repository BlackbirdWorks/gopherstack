package transcribe

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- CreateVocabulary ---

type createVocabularyInput struct {
	Tags              []transcribeTag `json:"Tags"`
	VocabularyName    string          `json:"VocabularyName"`
	LanguageCode      string          `json:"LanguageCode"`
	VocabularyFileURI string          `json:"VocabularyFileUri"`
	Phrases           []string        `json:"Phrases"`
}

type createVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleCreateVocabulary(
	_ context.Context,
	in *createVocabularyInput,
) (*createVocabularyOutput, error) {
	v, err := h.Backend.CreateVocabulary(&Vocabulary{
		VocabularyName:    in.VocabularyName,
		LanguageCode:      in.LanguageCode,
		Phrases:           in.Phrases,
		VocabularyFileURI: in.VocabularyFileURI,
		Tags:              tagsToMap(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return &createVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- GetVocabulary ---

type getVocabularyInput struct {
	VocabularyName string `json:"VocabularyName"`
}

type getVocabularyOutput struct {
	LastModifiedTime *float64 `json:"LastModifiedTime,omitempty"`
	VocabularyName   string   `json:"VocabularyName"`
	LanguageCode     string   `json:"LanguageCode"`
	VocabularyState  string   `json:"VocabularyState"`
	DownloadURI      string   `json:"DownloadUri,omitempty"`
}

func (h *Handler) handleGetVocabulary(
	_ context.Context,
	in *getVocabularyInput,
) (*getVocabularyOutput, error) {
	v, err := h.Backend.GetVocabulary(in.VocabularyName)
	if err != nil {
		return nil, err
	}

	out := &getVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
		DownloadURI:     v.VocabularyFileURI,
	}

	if !v.LastModifiedTime.IsZero() {
		t := awstime.Epoch(v.LastModifiedTime)
		out.LastModifiedTime = &t
	}

	return out, nil
}

// --- UpdateVocabulary ---

type updateVocabularyInput struct {
	VocabularyName    string   `json:"VocabularyName"`
	LanguageCode      string   `json:"LanguageCode"`
	VocabularyFileURI string   `json:"VocabularyFileUri"`
	Phrases           []string `json:"Phrases"`
}

type updateVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleUpdateVocabulary(
	_ context.Context,
	in *updateVocabularyInput,
) (*updateVocabularyOutput, error) {
	v, err := h.Backend.UpdateVocabulary(&Vocabulary{
		VocabularyName:    in.VocabularyName,
		LanguageCode:      in.LanguageCode,
		Phrases:           in.Phrases,
		VocabularyFileURI: in.VocabularyFileURI,
	})
	if err != nil {
		return nil, err
	}

	return &updateVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- DeleteVocabulary ---

type deleteVocabularyInput struct {
	VocabularyName string `json:"VocabularyName"`
}

func (h *Handler) handleDeleteVocabulary(
	_ context.Context,
	in *deleteVocabularyInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteVocabulary(in.VocabularyName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- ListVocabularies ---

type listVocabulariesInput struct {
	StateEquals string `json:"StateEquals"`
	NextToken   string `json:"NextToken"`
}

type vocabularySummary struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

type listVocabulariesOutput struct {
	NextToken    string              `json:"NextToken,omitempty"`
	Vocabularies []vocabularySummary `json:"Vocabularies"`
}

func (h *Handler) handleListVocabularies(
	_ context.Context,
	in *listVocabulariesInput,
) (*listVocabulariesOutput, error) {
	vocabs, nextToken := h.Backend.ListVocabularies(in.StateEquals, in.NextToken)

	result := make([]vocabularySummary, 0, len(vocabs))
	for _, v := range vocabs {
		result = append(result, vocabularySummary{
			VocabularyName:  v.VocabularyName,
			LanguageCode:    v.LanguageCode,
			VocabularyState: v.VocabularyState,
		})
	}

	return &listVocabulariesOutput{
		Vocabularies: result,
		NextToken:    nextToken,
	}, nil
}

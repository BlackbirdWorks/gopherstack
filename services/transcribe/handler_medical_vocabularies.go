package transcribe

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- CreateMedicalVocabulary ---

type createMedicalVocabularyInput struct {
	VocabularyName    string          `json:"VocabularyName"`
	LanguageCode      string          `json:"LanguageCode"`
	VocabularyFileURI string          `json:"VocabularyFileUri"`
	Tags              []transcribeTag `json:"Tags"`
}

type createMedicalVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleCreateMedicalVocabulary(
	_ context.Context,
	in *createMedicalVocabularyInput,
) (*createMedicalVocabularyOutput, error) {
	v, err := h.Backend.CreateMedicalVocabulary(
		in.VocabularyName, in.LanguageCode, in.VocabularyFileURI, tagsToMap(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createMedicalVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- GetMedicalVocabulary ---

type getMedicalVocabularyInput struct {
	VocabularyName string `json:"VocabularyName"`
}

type getMedicalVocabularyOutput struct {
	LastModifiedTime *float64 `json:"LastModifiedTime,omitempty"`
	VocabularyName   string   `json:"VocabularyName"`
	LanguageCode     string   `json:"LanguageCode"`
	VocabularyState  string   `json:"VocabularyState"`
	DownloadURI      string   `json:"DownloadUri,omitempty"`
}

func (h *Handler) handleGetMedicalVocabulary(
	_ context.Context,
	in *getMedicalVocabularyInput,
) (*getMedicalVocabularyOutput, error) {
	v, err := h.Backend.GetMedicalVocabulary(in.VocabularyName)
	if err != nil {
		return nil, err
	}

	out := &getMedicalVocabularyOutput{
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

// --- UpdateMedicalVocabulary ---

type updateMedicalVocabularyInput struct {
	VocabularyName    string `json:"VocabularyName"`
	LanguageCode      string `json:"LanguageCode"`
	VocabularyFileURI string `json:"VocabularyFileUri"`
}

type updateMedicalVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleUpdateMedicalVocabulary(
	_ context.Context,
	in *updateMedicalVocabularyInput,
) (*updateMedicalVocabularyOutput, error) {
	v, err := h.Backend.UpdateMedicalVocabulary(
		in.VocabularyName,
		in.LanguageCode,
		in.VocabularyFileURI,
	)
	if err != nil {
		return nil, err
	}

	return &updateMedicalVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- DeleteMedicalVocabulary ---

type deleteMedicalVocabularyInput struct {
	VocabularyName string `json:"VocabularyName"`
}

func (h *Handler) handleDeleteMedicalVocabulary(
	_ context.Context,
	in *deleteMedicalVocabularyInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteMedicalVocabulary(in.VocabularyName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- ListMedicalVocabularies ---

type listMedicalVocabulariesInput struct {
	StateEquals string `json:"StateEquals"`
	NextToken   string `json:"NextToken"`
}

type medicalVocabularySummary struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

type listMedicalVocabulariesOutput struct {
	NextToken    string                     `json:"NextToken,omitempty"`
	Vocabularies []medicalVocabularySummary `json:"Vocabularies"`
}

func (h *Handler) handleListMedicalVocabularies(
	_ context.Context,
	in *listMedicalVocabulariesInput,
) (*listMedicalVocabulariesOutput, error) {
	vocabs, nextToken := h.Backend.ListMedicalVocabularies(in.StateEquals, in.NextToken)

	result := make([]medicalVocabularySummary, 0, len(vocabs))
	for _, v := range vocabs {
		result = append(result, medicalVocabularySummary{
			VocabularyName:  v.VocabularyName,
			LanguageCode:    v.LanguageCode,
			VocabularyState: v.VocabularyState,
		})
	}

	return &listMedicalVocabulariesOutput{
		Vocabularies: result,
		NextToken:    nextToken,
	}, nil
}

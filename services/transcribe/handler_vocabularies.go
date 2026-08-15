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
	LastModifiedTime *float64 `json:"LastModifiedTime,omitempty"`
	VocabularyName   string   `json:"VocabularyName"`
	LanguageCode     string   `json:"LanguageCode"`
	VocabularyState  string   `json:"VocabularyState"`
	FailureReason    string   `json:"FailureReason,omitempty"`
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

	out := &createVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
		FailureReason:   v.FailureReason,
	}

	if !v.LastModifiedTime.IsZero() {
		t := awstime.Epoch(v.LastModifiedTime)
		out.LastModifiedTime = &t
	}

	return out, nil
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
	FailureReason    string   `json:"FailureReason,omitempty"`
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
		FailureReason:   v.FailureReason,
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
	LastModifiedTime *float64 `json:"LastModifiedTime,omitempty"`
	VocabularyName   string   `json:"VocabularyName"`
	LanguageCode     string   `json:"LanguageCode"`
	VocabularyState  string   `json:"VocabularyState"`
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

	out := &updateVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}

	if !v.LastModifiedTime.IsZero() {
		t := awstime.Epoch(v.LastModifiedTime)
		out.LastModifiedTime = &t
	}

	return out, nil
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
	StateEquals  string `json:"StateEquals"`
	NameContains string `json:"NameContains"`
	NextToken    string `json:"NextToken"`
	MaxResults   int32  `json:"MaxResults"`
}

type vocabularySummary struct {
	LastModifiedTime *float64 `json:"LastModifiedTime,omitempty"`
	VocabularyName   string   `json:"VocabularyName"`
	LanguageCode     string   `json:"LanguageCode"`
	VocabularyState  string   `json:"VocabularyState"`
}

type listVocabulariesOutput struct {
	Status       string              `json:"Status,omitempty"`
	NextToken    string              `json:"NextToken,omitempty"`
	Vocabularies []vocabularySummary `json:"Vocabularies"`
}

func (h *Handler) handleListVocabularies(
	_ context.Context,
	in *listVocabulariesInput,
) (*listVocabulariesOutput, error) {
	vocabs, nextToken := h.Backend.ListVocabularies(in.StateEquals, in.NameContains, in.NextToken, in.MaxResults)

	result := make([]vocabularySummary, 0, len(vocabs))
	for _, v := range vocabs {
		s := vocabularySummary{
			VocabularyName:  v.VocabularyName,
			LanguageCode:    v.LanguageCode,
			VocabularyState: v.VocabularyState,
		}
		if !v.LastModifiedTime.IsZero() {
			t := awstime.Epoch(v.LastModifiedTime)
			s.LastModifiedTime = &t
		}

		result = append(result, s)
	}

	return &listVocabulariesOutput{
		Vocabularies: result,
		NextToken:    nextToken,
		Status:       in.StateEquals,
	}, nil
}

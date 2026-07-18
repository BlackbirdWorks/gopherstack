package textract

import (
	"context"
	"fmt"
)

// createAdapterVersionInput is the input for CreateAdapterVersion.
type createAdapterVersionInput struct {
	Tags               map[string]string `json:"Tags"`
	DatasetConfig      *DatasetConfig    `json:"DatasetConfig"`
	OutputConfig       *OutputConfig     `json:"OutputConfig"`
	AdapterID          string            `json:"AdapterId"`
	ClientRequestToken string            `json:"ClientRequestToken"`
	//nolint:revive,staticcheck // KMSKeyId: AWS SDK field name convention
	KMSKeyId string `json:"KMSKeyId"`
}

// createAdapterVersionResponse is the response for CreateAdapterVersion.
type createAdapterVersionResponse struct {
	AdapterID      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

func (h *Handler) handleCreateAdapterVersion(
	ctx context.Context,
	in *createAdapterVersionInput,
) (*createAdapterVersionResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	var av *AdapterVersion
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		av, err = b.CreateAdapterVersionWithOptions(
			ctx,
			in.AdapterID, in.Tags,
			in.DatasetConfig, in.OutputConfig,
			in.KMSKeyId, in.ClientRequestToken,
		)
	} else {
		av, err = h.Backend.CreateAdapterVersion(ctx, in.AdapterID, in.Tags)
	}

	if err != nil {
		return nil, err
	}

	return &createAdapterVersionResponse{
		AdapterID:      av.AdapterID,
		AdapterVersion: av.AdapterVersion,
	}, nil
}

// getAdapterVersionInput is the input for GetAdapterVersion.
type getAdapterVersionInput struct {
	AdapterID      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

// getAdapterVersionResponse is the response for GetAdapterVersion.
type getAdapterVersionResponse struct {
	Tags              map[string]string  `json:"Tags"`
	DatasetConfig     *DatasetConfig     `json:"DatasetConfig,omitempty"`
	OutputConfig      *OutputConfig      `json:"OutputConfig,omitempty"`
	EvaluationMetrics *EvaluationMetrics `json:"EvaluationMetrics,omitempty"`
	AdapterID         string             `json:"AdapterId"`
	AdapterVersion    string             `json:"AdapterVersion"`
	CreationTime      string             `json:"CreationTime"`
	Status            string             `json:"Status"`
	StatusMessage     string             `json:"StatusMessage"`
	//nolint:revive,staticcheck // KMSKeyId: AWS SDK field name convention
	KMSKeyId     string   `json:"KMSKeyId,omitempty"`
	FeatureTypes []string `json:"FeatureTypes"`
}

func (h *Handler) handleGetAdapterVersion(
	ctx context.Context,
	in *getAdapterVersionInput,
) (*getAdapterVersionResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	if in.AdapterVersion == "" {
		return nil, fmt.Errorf("%w: AdapterVersion is required", errInvalidRequest)
	}

	av, err := h.Backend.GetAdapterVersion(ctx, in.AdapterID, in.AdapterVersion)
	if err != nil {
		return nil, err
	}

	return &getAdapterVersionResponse{
		AdapterID:         av.AdapterID,
		AdapterVersion:    av.AdapterVersion,
		CreationTime:      av.CreationTime.Format("2006-01-02T15:04:05Z"),
		FeatureTypes:      av.FeatureTypes,
		Status:            av.Status,
		StatusMessage:     av.StatusMessage,
		Tags:              av.Tags,
		DatasetConfig:     av.DatasetConfig,
		OutputConfig:      av.OutputConfig,
		KMSKeyId:          av.KMSKeyId,
		EvaluationMetrics: av.EvaluationMetrics,
	}, nil
}

// listAdapterVersionsInput is the input for ListAdapterVersions.
type listAdapterVersionsInput struct {
	AdapterID string `json:"AdapterId"`
}

// listAdapterVersionsResponse is the response for ListAdapterVersions.
type listAdapterVersionsResponse struct {
	AdapterID       string                  `json:"AdapterId"`
	AdapterVersions []adapterVersionSummary `json:"AdapterVersions"`
}

type adapterVersionSummary struct {
	AdapterVersion string   `json:"AdapterVersion"`
	CreationTime   string   `json:"CreationTime"`
	Status         string   `json:"Status"`
	StatusMessage  string   `json:"StatusMessage,omitempty"`
	FeatureTypes   []string `json:"FeatureTypes"`
}

func (h *Handler) handleListAdapterVersions(
	ctx context.Context,
	in *listAdapterVersionsInput,
) (*listAdapterVersionsResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	versions, err := h.Backend.ListAdapterVersions(ctx, in.AdapterID)
	if err != nil {
		return nil, err
	}

	summaries := make([]adapterVersionSummary, 0, len(versions))
	for _, av := range versions {
		summaries = append(summaries, adapterVersionSummary{
			AdapterVersion: av.AdapterVersion,
			CreationTime:   av.CreationTime.Format("2006-01-02T15:04:05Z"),
			FeatureTypes:   av.FeatureTypes,
			Status:         av.Status,
			StatusMessage:  av.StatusMessage,
		})
	}

	return &listAdapterVersionsResponse{
		AdapterID:       in.AdapterID,
		AdapterVersions: summaries,
	}, nil
}

// deleteAdapterVersionInput is the input for DeleteAdapterVersion.
type deleteAdapterVersionInput struct {
	AdapterID      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

func (h *Handler) handleDeleteAdapterVersion(
	ctx context.Context,
	in *deleteAdapterVersionInput,
) (*emptyResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	if in.AdapterVersion == "" {
		return nil, fmt.Errorf("%w: AdapterVersion is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAdapterVersion(ctx, in.AdapterID, in.AdapterVersion); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

package textract

import (
	"context"
	"fmt"
)

// createAdapterInput is the input for CreateAdapter.
type createAdapterInput struct {
	Tags               map[string]string `json:"Tags"`
	AdapterName        string            `json:"AdapterName"`
	AutoUpdate         string            `json:"AutoUpdate"`
	Description        string            `json:"Description"`
	ClientRequestToken string            `json:"ClientRequestToken"`
	FeatureTypes       []string          `json:"FeatureTypes"`
}

// createAdapterResponse is the response for CreateAdapter.
type createAdapterResponse struct {
	AdapterID string `json:"AdapterId"`
}

func (h *Handler) handleCreateAdapter(
	ctx context.Context,
	in *createAdapterInput,
) (*createAdapterResponse, error) {
	if in.AdapterName == "" {
		return nil, fmt.Errorf("%w: AdapterName is required", errInvalidRequest)
	}

	var adapter *Adapter
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		adapter, err = b.CreateAdapterWithToken(
			ctx,
			in.AdapterName, in.Description, in.AutoUpdate,
			in.FeatureTypes, in.Tags, in.ClientRequestToken,
		)
	} else {
		adapter, err = h.Backend.CreateAdapter(
			ctx, in.AdapterName, in.Description, in.AutoUpdate, in.FeatureTypes, in.Tags,
		)
	}

	if err != nil {
		return nil, err
	}

	return &createAdapterResponse{AdapterID: adapter.AdapterID}, nil
}

// getAdapterInput is the input for GetAdapter.
type getAdapterInput struct {
	AdapterID string `json:"AdapterId"`
}

// getAdapterResponse is the response for GetAdapter.
type getAdapterResponse struct {
	Tags         map[string]string `json:"Tags"`
	AdapterID    string            `json:"AdapterId"`
	AdapterName  string            `json:"AdapterName"`
	AutoUpdate   string            `json:"AutoUpdate"`
	CreationTime string            `json:"CreationTime"`
	Description  string            `json:"Description"`
	FeatureTypes []string          `json:"FeatureTypes"`
}

func (h *Handler) handleGetAdapter(
	ctx context.Context,
	in *getAdapterInput,
) (*getAdapterResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	adapter, err := h.Backend.GetAdapter(ctx, in.AdapterID)
	if err != nil {
		return nil, err
	}

	return &getAdapterResponse{
		AdapterID:    adapter.AdapterID,
		AdapterName:  adapter.AdapterName,
		AutoUpdate:   adapter.AutoUpdate,
		CreationTime: adapter.CreationTime.Format("2006-01-02T15:04:05Z"),
		Description:  adapter.Description,
		FeatureTypes: adapter.FeatureTypes,
		Tags:         adapter.Tags,
	}, nil
}

// updateAdapterInput is the input for UpdateAdapter.
type updateAdapterInput struct {
	AdapterID   string `json:"AdapterId"`
	AutoUpdate  string `json:"AutoUpdate"`
	Description string `json:"Description"`
}

// updateAdapterResponse is the response for UpdateAdapter.
type updateAdapterResponse struct {
	Tags         map[string]string `json:"Tags"`
	AdapterID    string            `json:"AdapterId"`
	AdapterName  string            `json:"AdapterName"`
	AutoUpdate   string            `json:"AutoUpdate"`
	CreationTime string            `json:"CreationTime"`
	Description  string            `json:"Description"`
	FeatureTypes []string          `json:"FeatureTypes"`
}

func (h *Handler) handleUpdateAdapter(
	ctx context.Context,
	in *updateAdapterInput,
) (*updateAdapterResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	adapter, err := h.Backend.UpdateAdapter(ctx, in.AdapterID, in.Description, in.AutoUpdate)
	if err != nil {
		return nil, err
	}

	return &updateAdapterResponse{
		AdapterID:    adapter.AdapterID,
		AdapterName:  adapter.AdapterName,
		AutoUpdate:   adapter.AutoUpdate,
		CreationTime: adapter.CreationTime.Format("2006-01-02T15:04:05Z"),
		Description:  adapter.Description,
		FeatureTypes: adapter.FeatureTypes,
		Tags:         adapter.Tags,
	}, nil
}

// listAdaptersInput is the input for ListAdapters.
type listAdaptersInput struct{}

// listAdaptersResponse is the response for ListAdapters.
type listAdaptersResponse struct {
	Adapters []adapterSummary `json:"Adapters"`
}

type adapterSummary struct {
	AdapterID    string   `json:"AdapterId"`
	AdapterName  string   `json:"AdapterName"`
	CreationTime string   `json:"CreationTime"`
	FeatureTypes []string `json:"FeatureTypes"`
}

func (h *Handler) handleListAdapters(
	ctx context.Context,
	_ *listAdaptersInput,
) (*listAdaptersResponse, error) {
	adapters := h.Backend.ListAdapters(ctx)
	summaries := make([]adapterSummary, 0, len(adapters))

	for _, a := range adapters {
		summaries = append(summaries, adapterSummary{
			AdapterID:    a.AdapterID,
			AdapterName:  a.AdapterName,
			CreationTime: a.CreationTime.Format("2006-01-02T15:04:05Z"),
			FeatureTypes: a.FeatureTypes,
		})
	}

	return &listAdaptersResponse{Adapters: summaries}, nil
}

// deleteAdapterInput is the input for DeleteAdapter.
type deleteAdapterInput struct {
	AdapterID string `json:"AdapterId"`
}

func (h *Handler) handleDeleteAdapter(
	ctx context.Context,
	in *deleteAdapterInput,
) (*emptyResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAdapter(ctx, in.AdapterID); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

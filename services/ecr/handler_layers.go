package ecr

import (
	"context"
)

// batchCheckLayerAvailabilityInput is the request body for BatchCheckLayerAvailability.
type batchCheckLayerAvailabilityInput struct {
	RepositoryName string   `json:"repositoryName"`
	RegistryID     string   `json:"registryId,omitempty"`
	LayerDigests   []string `json:"layerDigests"`
}

type batchCheckLayerAvailabilityOutput struct {
	Layers   []LayerAvailability `json:"layers"`
	Failures []LayerFailure      `json:"failures"`
}

func (h *Handler) handleBatchCheckLayerAvailability(
	ctx context.Context,
	in *batchCheckLayerAvailabilityInput,
) (*batchCheckLayerAvailabilityOutput, error) {
	layers, failures, err := h.Backend.BatchCheckLayerAvailability(
		ctx,
		in.RepositoryName,
		in.LayerDigests,
	)
	if err != nil {
		return nil, err
	}

	if layers == nil {
		layers = []LayerAvailability{}
	}

	if failures == nil {
		failures = []LayerFailure{}
	}

	return &batchCheckLayerAvailabilityOutput{Layers: layers, Failures: failures}, nil
}

// completeLayerUploadInput is the request body for CompleteLayerUpload.
type completeLayerUploadInput struct {
	RepositoryName string   `json:"repositoryName"`
	UploadID       string   `json:"uploadId"`
	RegistryID     string   `json:"registryId,omitempty"`
	LayerDigests   []string `json:"layerDigests"`
}

func (h *Handler) handleCompleteLayerUpload(
	ctx context.Context,
	in *completeLayerUploadInput,
) (*CompleteLayerUploadResult, error) {
	result, err := h.Backend.CompleteLayerUpload(
		ctx,
		in.RepositoryName,
		in.UploadID,
		in.LayerDigests,
	)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type getDownloadURLForLayerInput struct {
	LayerDigest    string `json:"layerDigest"`
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

type getDownloadURLForLayerOutput struct {
	DownloadURL string `json:"downloadUrl"`
	LayerDigest string `json:"layerDigest"`
}

func (h *Handler) handleGetDownloadURLForLayer(
	ctx context.Context,
	in *getDownloadURLForLayerInput,
) (*getDownloadURLForLayerOutput, error) {
	url, err := h.Backend.GetDownloadURLForLayer(ctx, in.RepositoryName, in.LayerDigest)
	if err != nil {
		return nil, err
	}

	return &getDownloadURLForLayerOutput{DownloadURL: url, LayerDigest: in.LayerDigest}, nil
}

type initiateLayerUploadInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

type initiateLayerUploadOutput struct {
	UploadID string `json:"uploadId"`
	PartSize int64  `json:"partSize"`
}

func (h *Handler) handleInitiateLayerUpload(
	ctx context.Context,
	in *initiateLayerUploadInput,
) (*initiateLayerUploadOutput, error) {
	result, err := h.Backend.InitiateLayerUpload(ctx, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return &initiateLayerUploadOutput{PartSize: result.PartSize, UploadID: result.UploadID}, nil
}

type uploadLayerPartInput struct {
	RepositoryName string `json:"repositoryName"`
	UploadID       string `json:"uploadId"`
	RegistryID     string `json:"registryId,omitempty"`
	LayerPartBlob  []byte `json:"layerPartBlob"`
	PartFirstByte  int64  `json:"partFirstByte"`
	PartLastByte   int64  `json:"partLastByte"`
}

func (h *Handler) handleUploadLayerPart(
	ctx context.Context,
	in *uploadLayerPartInput,
) (*LayerUploadPartResult, error) {
	return h.Backend.UploadLayerPart(
		ctx,
		in.RepositoryName,
		in.UploadID,
		in.PartFirstByte,
		in.PartLastByte,
		in.LayerPartBlob,
	)
}

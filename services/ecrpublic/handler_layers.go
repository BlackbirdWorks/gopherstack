package ecrpublic

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) buildLayerOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"BatchCheckLayerAvailability": service.WrapOp(h.handleBatchCheckLayerAvailability),
		"InitiateLayerUpload":         service.WrapOp(h.handleInitiateLayerUpload),
		"UploadLayerPart":             service.WrapOp(h.handleUploadLayerPart),
		"CompleteLayerUpload":         service.WrapOp(h.handleCompleteLayerUpload),
	}
}

type batchCheckLayerAvailabilityInput struct {
	RegistryID     string   `json:"registryId,omitempty"`
	RepositoryName string   `json:"repositoryName"`
	LayerDigests   []string `json:"layerDigests"`
}

type batchCheckLayerAvailabilityOutput struct {
	Failures []LayerFailureWire `json:"failures,omitempty"`
	Layers   []LayerWire        `json:"layers"`
}

func (h *Handler) handleBatchCheckLayerAvailability(
	_ context.Context, in *batchCheckLayerAvailabilityInput,
) (*batchCheckLayerAvailabilityOutput, error) {
	layers, failures, err := h.Backend.BatchCheckLayerAvailability(in.RegistryID, in.RepositoryName, in.LayerDigests)
	if err != nil {
		return nil, err
	}

	outLayers := make([]LayerWire, 0, len(layers))
	for _, l := range layers {
		outLayers = append(outLayers, toLayerWire(l))
	}

	outFailures := make([]LayerFailureWire, 0, len(failures))
	for _, f := range failures {
		outFailures = append(outFailures, toLayerFailureWire(f))
	}

	return &batchCheckLayerAvailabilityOutput{Failures: outFailures, Layers: outLayers}, nil
}

type initiateLayerUploadInput struct {
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName"`
}

type initiateLayerUploadOutput struct {
	UploadID string `json:"uploadId,omitempty"`
	PartSize int64  `json:"partSize,omitempty"`
}

func (h *Handler) handleInitiateLayerUpload(
	_ context.Context, in *initiateLayerUploadInput,
) (*initiateLayerUploadOutput, error) {
	uploadID, partSize, err := h.Backend.InitiateLayerUpload(in.RegistryID, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return &initiateLayerUploadOutput{UploadID: uploadID, PartSize: partSize}, nil
}

type uploadLayerPartInput struct {
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName"`
	UploadID       string `json:"uploadId"`
	LayerPartBlob  []byte `json:"layerPartBlob"`
	PartFirstByte  int64  `json:"partFirstByte"`
	PartLastByte   int64  `json:"partLastByte"`
}

type uploadLayerPartOutput struct {
	RegistryID       string `json:"registryId,omitempty"`
	RepositoryName   string `json:"repositoryName,omitempty"`
	UploadID         string `json:"uploadId,omitempty"`
	LastByteReceived int64  `json:"lastByteReceived,omitempty"`
}

func (h *Handler) handleUploadLayerPart(
	_ context.Context, in *uploadLayerPartInput,
) (*uploadLayerPartOutput, error) {
	lastByteReceived, err := h.Backend.UploadLayerPart(
		in.RegistryID, in.RepositoryName, in.UploadID, in.PartFirstByte, in.PartLastByte, in.LayerPartBlob,
	)
	if err != nil {
		return nil, err
	}

	return &uploadLayerPartOutput{
		LastByteReceived: lastByteReceived,
		RegistryID:       h.registryIDOrDefault(in.RegistryID),
		RepositoryName:   in.RepositoryName,
		UploadID:         in.UploadID,
	}, nil
}

type completeLayerUploadInput struct {
	RegistryID     string   `json:"registryId,omitempty"`
	RepositoryName string   `json:"repositoryName"`
	UploadID       string   `json:"uploadId"`
	LayerDigests   []string `json:"layerDigests"`
}

type completeLayerUploadOutput struct {
	LayerDigest    string `json:"layerDigest,omitempty"`
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName,omitempty"`
	UploadID       string `json:"uploadId,omitempty"`
}

func (h *Handler) handleCompleteLayerUpload(
	_ context.Context, in *completeLayerUploadInput,
) (*completeLayerUploadOutput, error) {
	digest, err := h.Backend.CompleteLayerUpload(in.RegistryID, in.RepositoryName, in.UploadID, in.LayerDigests)
	if err != nil {
		return nil, err
	}

	return &completeLayerUploadOutput{
		LayerDigest:    digest,
		RegistryID:     h.registryIDOrDefault(in.RegistryID),
		RepositoryName: in.RepositoryName,
		UploadID:       in.UploadID,
	}, nil
}

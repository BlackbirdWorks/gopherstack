package datasync

import (
	"context"
	"fmt"
)

// --- AzureBlob location ---

type azureBlobSasConfigInput struct {
	Token string `json:"Token"`
}

type createLocationAzureBlobInput struct {
	SasConfiguration   *azureBlobSasConfigInput `json:"SasConfiguration"`
	ContainerURL       string                   `json:"ContainerUrl"`
	Subdirectory       string                   `json:"Subdirectory,omitempty"`
	BlobType           string                   `json:"BlobType,omitempty"`
	AccessTier         string                   `json:"AccessTier,omitempty"`
	AuthenticationType string                   `json:"AuthenticationType"`
	AgentArns          []string                 `json:"AgentArns"`
	Tags               []tagInput               `json:"Tags"`
}

type createLocationAzureBlobOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationAzureBlob(
	_ context.Context,
	in *createLocationAzureBlobInput,
) (*createLocationAzureBlobOutput, error) {
	if in.ContainerURL == "" {
		return nil, fmt.Errorf("%w: ContainerUrl is required", errInvalidRequest)
	}

	if in.AuthenticationType == "" {
		return nil, fmt.Errorf("%w: AuthenticationType is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	var sasConfig *SasConfiguration
	if in.SasConfiguration != nil {
		sasConfig = &SasConfiguration{Token: in.SasConfiguration.Token}
	}

	l, err := h.Backend.CreateLocationAzureBlob(
		in.ContainerURL, in.Subdirectory, in.BlobType, in.AccessTier, in.AuthenticationType,
		sasConfig, in.AgentArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationAzureBlobOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationAzureBlobInput struct {
	LocationArn string `json:"LocationArn"`
}

// describeLocationAzureBlobOutput intentionally has no SasConfiguration,
// Subdirectory, or ContainerUrl field: the real DescribeLocationAzureBlobOutput
// never returns the SAS token (AWS never echoes back access credentials on a
// Describe call), has no separate Subdirectory member -- the path is folded
// into LocationUri only -- and has no separate ContainerUrl member either
// (LocationUri's own doc string is literally "The URL of the Azure Blob
// Storage container involved in your transfer"). It does have
// AuthenticationType.
type describeLocationAzureBlobOutput struct {
	LocationArn        string   `json:"LocationArn"`
	LocationURI        string   `json:"LocationUri"`
	BlobType           string   `json:"BlobType,omitempty"`
	AccessTier         string   `json:"AccessTier,omitempty"`
	AuthenticationType string   `json:"AuthenticationType,omitempty"`
	AgentArns          []string `json:"AgentArns,omitempty"`
	CreationTime       int64    `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationAzureBlob(
	_ context.Context,
	in *describeLocationAzureBlobInput,
) (*describeLocationAzureBlobOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationAzureBlob(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationAzureBlobOutput{
		LocationArn:        l.LocationArn,
		LocationURI:        l.LocationURI,
		BlobType:           l.BlobType,
		AccessTier:         l.AccessTier,
		AuthenticationType: l.AuthenticationType,
		AgentArns:          l.AgentArns,
		CreationTime:       l.CreationTime.Unix(),
	}, nil
}

type updateLocationAzureBlobInput struct {
	SasConfiguration   *azureBlobSasConfigInput `json:"SasConfiguration"`
	LocationArn        string                   `json:"LocationArn"`
	ContainerURL       string                   `json:"ContainerUrl,omitempty"`
	Subdirectory       string                   `json:"Subdirectory,omitempty"`
	BlobType           string                   `json:"BlobType,omitempty"`
	AccessTier         string                   `json:"AccessTier,omitempty"`
	AuthenticationType string                   `json:"AuthenticationType,omitempty"`
	AgentArns          []string                 `json:"AgentArns"`
}

type updateLocationAzureBlobOutput struct{}

func (h *Handler) handleUpdateLocationAzureBlob(
	_ context.Context,
	in *updateLocationAzureBlobInput,
) (*updateLocationAzureBlobOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var sasConfig *SasConfiguration
	if in.SasConfiguration != nil {
		sasConfig = &SasConfiguration{Token: in.SasConfiguration.Token}
	}

	if err := h.Backend.UpdateLocationAzureBlob(
		in.LocationArn, in.ContainerURL, in.Subdirectory, in.BlobType, in.AccessTier, in.AuthenticationType,
		sasConfig, in.AgentArns,
	); err != nil {
		return nil, err
	}

	return &updateLocationAzureBlobOutput{}, nil
}

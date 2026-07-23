package datasync

import (
	"context"
	"fmt"
)

// --- ObjectStorage location ---

type createLocationObjectStorageInput struct {
	ServerHostname string     `json:"ServerHostname"`
	BucketName     string     `json:"BucketName"`
	Subdirectory   string     `json:"Subdirectory,omitempty"`
	AccessKey      string     `json:"AccessKey,omitempty"`
	SecretKey      string     `json:"SecretKey,omitempty"`
	ServerProtocol string     `json:"ServerProtocol,omitempty"`
	AgentArns      []string   `json:"AgentArns"`
	Tags           []tagInput `json:"Tags"`
	ServerPort     int32      `json:"ServerPort,omitempty"`
}

type createLocationObjectStorageOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationObjectStorage(
	_ context.Context,
	in *createLocationObjectStorageInput,
) (*createLocationObjectStorageOutput, error) {
	if in.ServerHostname == "" {
		return nil, fmt.Errorf("%w: ServerHostname is required", errInvalidRequest)
	}

	if in.BucketName == "" {
		return nil, fmt.Errorf("%w: BucketName is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationObjectStorage(
		in.ServerHostname, in.ServerProtocol, in.BucketName, in.Subdirectory,
		in.AccessKey, in.SecretKey, in.ServerPort, in.AgentArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationObjectStorageOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationObjectStorageInput struct {
	LocationArn string `json:"LocationArn"`
}

// describeLocationObjectStorageOutput intentionally has no ServerHostname,
// BucketName, or Subdirectory field: the real
// DescribeLocationObjectStorageOutput has none of them -- confirmed against
// aws-sdk-go-v2 v1.59.2: AccessKey, AgentArns, CmkSecretConfig, CreationTime,
// CustomSecretConfig, LocationArn, LocationUri, ManagedSecretConfig,
// ServerCertificate, ServerPort, ServerProtocol (host/bucket/path are folded
// into LocationUri).
type describeLocationObjectStorageOutput struct {
	LocationArn    string   `json:"LocationArn"`
	LocationURI    string   `json:"LocationUri"`
	AccessKey      string   `json:"AccessKey,omitempty"`
	ServerProtocol string   `json:"ServerProtocol,omitempty"`
	AgentArns      []string `json:"AgentArns,omitempty"`
	CreationTime   int64    `json:"CreationTime"`
	ServerPort     int32    `json:"ServerPort,omitempty"`
}

func (h *Handler) handleDescribeLocationObjectStorage(
	_ context.Context,
	in *describeLocationObjectStorageInput,
) (*describeLocationObjectStorageOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationObjectStorage(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationObjectStorageOutput{
		LocationArn:    l.LocationArn,
		LocationURI:    l.LocationURI,
		AccessKey:      l.AccessKey,
		ServerProtocol: l.ServerProtocol,
		ServerPort:     l.ServerPort,
		AgentArns:      l.AgentArns,
		CreationTime:   l.CreationTime.Unix(),
	}, nil
}

type updateLocationObjectStorageInput struct {
	LocationArn    string   `json:"LocationArn"`
	Subdirectory   string   `json:"Subdirectory,omitempty"`
	AccessKey      string   `json:"AccessKey,omitempty"`
	SecretKey      string   `json:"SecretKey,omitempty"`
	ServerProtocol string   `json:"ServerProtocol,omitempty"`
	AgentArns      []string `json:"AgentArns"`
	ServerPort     int32    `json:"ServerPort,omitempty"`
}

type updateLocationObjectStorageOutput struct{}

func (h *Handler) handleUpdateLocationObjectStorage(
	_ context.Context,
	in *updateLocationObjectStorageInput,
) (*updateLocationObjectStorageOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationObjectStorage(
		in.LocationArn, in.ServerProtocol, in.Subdirectory,
		in.AccessKey, in.SecretKey, in.ServerPort, in.AgentArns,
	); err != nil {
		return nil, err
	}

	return &updateLocationObjectStorageOutput{}, nil
}

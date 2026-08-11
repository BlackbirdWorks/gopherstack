package datasync

import (
	"context"
	"fmt"
)

// --- ObjectStorage location ---

type createLocationObjectStorageInput struct {
	CmkSecretConfig    *cmkSecretConfigWire    `json:"CmkSecretConfig"`
	CustomSecretConfig *customSecretConfigWire `json:"CustomSecretConfig"`
	ServerHostname     string                  `json:"ServerHostname"`
	BucketName         string                  `json:"BucketName"`
	Subdirectory       string                  `json:"Subdirectory,omitempty"`
	AccessKey          string                  `json:"AccessKey,omitempty"`
	SecretKey          string                  `json:"SecretKey,omitempty"`
	ServerProtocol     string                  `json:"ServerProtocol,omitempty"`
	AgentArns          []string                `json:"AgentArns"`
	Tags               []tagInput              `json:"Tags"`
	ServerPort         int32                   `json:"ServerPort,omitempty"`
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

	if err := validateSecretConfig(in.CmkSecretConfig, in.CustomSecretConfig); err != nil {
		return nil, err
	}

	tags := tagsFromInput(in.Tags)

	secretConfig := SecretConfig{
		Cmk:    cmkSecretConfigFromWire(in.CmkSecretConfig),
		Custom: customSecretConfigFromWire(in.CustomSecretConfig),
	}

	l, err := h.Backend.CreateLocationObjectStorage(
		in.ServerHostname, in.ServerProtocol, in.BucketName, in.Subdirectory,
		in.AccessKey, in.SecretKey, in.ServerPort, in.AgentArns, tags, secretConfig,
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
// aws-sdk-go-v2 v1.61.4: AccessKey, AgentArns, CmkSecretConfig, CreationTime,
// CustomSecretConfig, LocationArn, LocationUri, ManagedSecretConfig,
// ServerCertificate, ServerPort, ServerProtocol (host/bucket/path are folded
// into LocationUri). CmkSecretConfig/CustomSecretConfig are echoed below;
// ManagedSecretConfig stays absent -- it's documented ReadOnly/AWS-populated
// and gopherstack doesn't provision a Secrets Manager secret (see
// PARITY.md).
type describeLocationObjectStorageOutput struct {
	CmkSecretConfig    *cmkSecretConfigWire    `json:"CmkSecretConfig,omitempty"`
	CustomSecretConfig *customSecretConfigWire `json:"CustomSecretConfig,omitempty"`
	LocationArn        string                  `json:"LocationArn"`
	LocationURI        string                  `json:"LocationUri"`
	AccessKey          string                  `json:"AccessKey,omitempty"`
	ServerProtocol     string                  `json:"ServerProtocol,omitempty"`
	AgentArns          []string                `json:"AgentArns,omitempty"`
	CreationTime       int64                   `json:"CreationTime"`
	ServerPort         int32                   `json:"ServerPort,omitempty"`
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
		LocationArn:        l.LocationArn,
		LocationURI:        l.LocationURI,
		AccessKey:          l.AccessKey,
		ServerProtocol:     l.ServerProtocol,
		ServerPort:         l.ServerPort,
		AgentArns:          l.AgentArns,
		CreationTime:       l.CreationTime.Unix(),
		CmkSecretConfig:    cmkSecretConfigToWire(l.CmkSecretConfig),
		CustomSecretConfig: customSecretConfigToWire(l.CustomSecretConfig),
	}, nil
}

type updateLocationObjectStorageInput struct {
	CmkSecretConfig    *cmkSecretConfigWire    `json:"CmkSecretConfig"`
	CustomSecretConfig *customSecretConfigWire `json:"CustomSecretConfig"`
	LocationArn        string                  `json:"LocationArn"`
	Subdirectory       string                  `json:"Subdirectory,omitempty"`
	AccessKey          string                  `json:"AccessKey,omitempty"`
	SecretKey          string                  `json:"SecretKey,omitempty"`
	ServerProtocol     string                  `json:"ServerProtocol,omitempty"`
	AgentArns          []string                `json:"AgentArns"`
	ServerPort         int32                   `json:"ServerPort,omitempty"`
}

type updateLocationObjectStorageOutput struct{}

func (h *Handler) handleUpdateLocationObjectStorage(
	_ context.Context,
	in *updateLocationObjectStorageInput,
) (*updateLocationObjectStorageOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := validateSecretConfig(in.CmkSecretConfig, in.CustomSecretConfig); err != nil {
		return nil, err
	}

	secretConfig := SecretConfig{
		Cmk:    cmkSecretConfigFromWire(in.CmkSecretConfig),
		Custom: customSecretConfigFromWire(in.CustomSecretConfig),
	}

	if err := h.Backend.UpdateLocationObjectStorage(
		in.LocationArn, in.ServerProtocol, in.Subdirectory,
		in.AccessKey, in.SecretKey, in.ServerPort, in.AgentArns, secretConfig,
	); err != nil {
		return nil, err
	}

	return &updateLocationObjectStorageOutput{}, nil
}

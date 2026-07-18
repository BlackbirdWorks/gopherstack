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

type describeLocationObjectStorageOutput struct {
	LocationArn    string   `json:"LocationArn"`
	LocationURI    string   `json:"LocationUri"`
	ServerHostname string   `json:"ServerHostname,omitempty"`
	BucketName     string   `json:"BucketName,omitempty"`
	Subdirectory   string   `json:"Subdirectory,omitempty"`
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
		ServerHostname: l.ServerHostname,
		BucketName:     l.BucketName,
		Subdirectory:   l.Subdirectory,
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

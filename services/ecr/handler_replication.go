package ecr

import (
	"context"
)

type replicationConfigurationInput struct {
	ReplicationConfiguration *ReplicationConfig `json:"replicationConfiguration"`
}

func (h *Handler) handlePutReplicationConfiguration(
	ctx context.Context,
	in *replicationConfigurationInput,
) (*replicationConfigurationInput, error) {
	cfg, err := h.Backend.PutReplicationConfiguration(ctx, in.ReplicationConfiguration)
	if err != nil {
		return nil, err
	}

	return &replicationConfigurationInput{ReplicationConfiguration: cfg}, nil
}

type describeImageReplicationStatusOutput struct {
	ImageID             ImageIdentifier          `json:"imageId"`
	RepositoryName      string                   `json:"repositoryName"`
	ReplicationStatuses []imageReplicationStatus `json:"replicationStatuses"`
}

type imageReplicationStatus struct {
	Region        string `json:"region,omitempty"`
	RegistryID    string `json:"registryId,omitempty"`
	Status        string `json:"status"`
	FailureCode   string `json:"failureCode,omitempty"`
	FailureReason string `json:"failureReason,omitempty"`
}

func (h *Handler) handleDescribeImageReplicationStatus(
	ctx context.Context,
	in *imageInput,
) (*describeImageReplicationStatusOutput, error) {
	result, err := h.Backend.DescribeImageReplicationStatus(ctx, in.RepositoryName, in.ImageID)
	if err != nil {
		return nil, err
	}

	statuses := make([]imageReplicationStatus, 0, len(result.ReplicationStatuses))
	for _, s := range result.ReplicationStatuses {
		statuses = append(statuses, imageReplicationStatus(s))
	}

	return &describeImageReplicationStatusOutput{
		ImageID:             result.ImageID,
		ReplicationStatuses: statuses,
		RepositoryName:      result.RepositoryName,
	}, nil
}

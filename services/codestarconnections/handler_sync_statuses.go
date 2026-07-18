package codestarconnections

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type getRepositorySyncStatusInput struct {
	Branch           string `json:"Branch"`
	RepositoryLinkID string `json:"RepositoryLinkId"`
	SyncType         string `json:"SyncType"`
}

type syncEventItem struct {
	Event      string  `json:"Event"`
	ExternalID string  `json:"ExternalId,omitempty"`
	Type       string  `json:"Type"`
	Time       float64 `json:"Time"`
}

type repositorySyncAttemptItem struct {
	Status    string          `json:"Status"`
	Events    []syncEventItem `json:"Events"`
	StartedAt float64         `json:"StartedAt"`
}

type getRepositorySyncStatusOutput struct {
	LatestSync repositorySyncAttemptItem `json:"LatestSync"`
}

func (h *Handler) handleGetRepositorySyncStatus(
	ctx context.Context,
	in *getRepositorySyncStatusInput,
) (*getRepositorySyncStatusOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	if in.Branch == "" {
		return nil, fmt.Errorf("%w: Branch is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	status, err := h.Backend.GetRepositorySyncStatus(ctx, in.RepositoryLinkID, in.Branch, in.SyncType)
	if err != nil {
		return nil, err
	}

	events := buildSyncEventItems(status.Events)

	return &getRepositorySyncStatusOutput{
		LatestSync: repositorySyncAttemptItem{
			StartedAt: awstime.Epoch(status.StartedAt),
			Status:    status.Status,
			Events:    events,
		},
	}, nil
}

type getResourceSyncStatusInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

type resourceSyncAttemptItem struct {
	Status    string          `json:"Status"`
	Events    []syncEventItem `json:"Events"`
	StartedAt float64         `json:"StartedAt"`
}

type getResourceSyncStatusOutput struct {
	LatestSync resourceSyncAttemptItem `json:"LatestSync"`
}

func (h *Handler) handleGetResourceSyncStatus(
	ctx context.Context,
	in *getResourceSyncStatusInput,
) (*getResourceSyncStatusOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	status, err := h.Backend.GetResourceSyncStatus(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	events := buildSyncEventItems(status.Events)

	return &getResourceSyncStatusOutput{
		LatestSync: resourceSyncAttemptItem{
			StartedAt: awstime.Epoch(status.StartedAt),
			Status:    status.Status,
			Events:    events,
		},
	}, nil
}

func buildSyncEventItems(evts []SyncEvent) []syncEventItem {
	out := make([]syncEventItem, len(evts))

	for i, e := range evts {
		out[i] = syncEventItem{
			Event:      e.Event,
			Time:       awstime.Epoch(e.Time),
			Type:       e.Type,
			ExternalID: e.ExternalID,
		}
	}

	return out
}

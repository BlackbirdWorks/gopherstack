package codeconnections

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

// syncEventItem is the wire shape of a sync event. Time is an epoch-seconds
// JSON number on the wire (see smithytime.ParseEpochSeconds in the real SDK
// deserializer), not an RFC3339 string.
type syncEventItem struct {
	Event      string  `json:"Event"`
	ExternalID string  `json:"ExternalId,omitempty"`
	Type       string  `json:"Type"`
	Time       float64 `json:"Time"`
}

// repositorySyncAttemptItem is the wire shape of a repository sync attempt.
// StartedAt is an epoch-seconds JSON number on the wire, not an RFC3339 string.
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
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	if in.Branch == "" {
		return nil, fmt.Errorf("%w: Branch is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	status, err := h.Backend.GetRepositorySyncStatus(
		ctx,
		in.RepositoryLinkID,
		in.Branch,
		in.SyncType,
	)
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

// resourceSyncAttemptItem is the wire shape of a resource sync attempt.
// StartedAt is an epoch-seconds JSON number on the wire, not an RFC3339 string.
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
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
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

// buildSyncEventItems converts backend SyncEvents to handler response items.
func buildSyncEventItems(evts []SyncEvent) []syncEventItem {
	out := make([]syncEventItem, 0, len(evts))

	for _, e := range evts {
		out = append(out, syncEventItem{
			Event:      e.Event,
			Time:       awstime.Epoch(e.Time),
			Type:       e.Type,
			ExternalID: e.ExternalID,
		})
	}

	return out
}

type listRepositorySyncDefinitionsInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
	SyncType         string `json:"SyncType"`
}

type repositorySyncDefinitionItem struct {
	Branch    string `json:"Branch"`
	Directory string `json:"Directory"`
	Parent    string `json:"Parent,omitempty"`
	Target    string `json:"Target"`
}

type listRepositorySyncDefinitionsOutput struct {
	RepositorySyncDefinitions []repositorySyncDefinitionItem `json:"RepositorySyncDefinitions"`
}

func (h *Handler) handleListRepositorySyncDefinitions(
	ctx context.Context,
	in *listRepositorySyncDefinitionsInput,
) (*listRepositorySyncDefinitionsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	defs, err := h.Backend.ListRepositorySyncDefinitions(ctx, in.RepositoryLinkID, in.SyncType)
	if err != nil {
		return nil, err
	}

	items := make([]repositorySyncDefinitionItem, len(defs))
	for i, d := range defs {
		items[i] = repositorySyncDefinitionItem(d)
	}

	return &listRepositorySyncDefinitionsOutput{RepositorySyncDefinitions: items}, nil
}

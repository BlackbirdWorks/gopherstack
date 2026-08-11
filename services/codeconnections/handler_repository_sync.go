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

// revisionItem is the wire shape of a Revision (aws-sdk-go-v2/service/
// codeconnections@v1.13.4 types.Revision -- all six members are required).
type revisionItem struct {
	Branch         string `json:"Branch"`
	Directory      string `json:"Directory"`
	OwnerID        string `json:"OwnerId"`
	ProviderType   string `json:"ProviderType"`
	RepositoryName string `json:"RepositoryName"`
	Sha            string `json:"Sha"`
}

// revisionToItem converts a backend Revision to its wire shape. Struct tags
// are ignored by Go's conversion rules, so this is a plain type conversion
// as long as revisionItem's field names/types/order keep matching Revision's.
func revisionToItem(r Revision) revisionItem {
	return revisionItem(r)
}

// resourceSyncAttemptItem is the wire shape of a resource sync attempt
// (aws-sdk-go-v2/service/codeconnections@v1.13.4 types.ResourceSyncAttempt).
// StartedAt is an epoch-seconds JSON number on the wire, not an RFC3339
// string. Events/InitialRevision/StartedAt/Status/Target/TargetRevision are
// all required wire members -- InitialRevision/Target/TargetRevision were
// previously missing entirely from this response shape.
type resourceSyncAttemptItem struct {
	InitialRevision revisionItem    `json:"InitialRevision"`
	TargetRevision  revisionItem    `json:"TargetRevision"`
	Status          string          `json:"Status"`
	Target          string          `json:"Target"`
	Events          []syncEventItem `json:"Events"`
	StartedAt       float64         `json:"StartedAt"`
}

func resourceSyncAttemptToItem(a ResourceSyncAttempt) resourceSyncAttemptItem {
	return resourceSyncAttemptItem{
		StartedAt:       awstime.Epoch(a.StartedAt),
		Status:          a.Status,
		Target:          a.Target,
		InitialRevision: revisionToItem(a.InitialRevision),
		TargetRevision:  revisionToItem(a.TargetRevision),
		Events:          buildSyncEventItems(a.Events),
	}
}

// getResourceSyncStatusOutput is the GetResourceSyncStatusOutput wire shape.
// DesiredState/LatestSuccessfulSync are optional in the real shape but this
// backend always has them once a sync configuration exists (see
// GetResourceSyncStatus), so they are populated whenever LatestSync is.
type getResourceSyncStatusOutput struct {
	DesiredState         *revisionItem            `json:"DesiredState,omitempty"`
	LatestSuccessfulSync *resourceSyncAttemptItem `json:"LatestSuccessfulSync,omitempty"`
	LatestSync           resourceSyncAttemptItem  `json:"LatestSync"`
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

	desiredState := revisionToItem(status.DesiredState)
	out := &getResourceSyncStatusOutput{
		LatestSync:   resourceSyncAttemptToItem(status.LatestSync),
		DesiredState: &desiredState,
	}

	if status.LatestSuccessfulSync != nil {
		item := resourceSyncAttemptToItem(*status.LatestSuccessfulSync)
		out.LatestSuccessfulSync = &item
	}

	return out, nil
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

// listRepositorySyncDefinitionsOutput is the ListRepositorySyncDefinitionsOutput
// wire shape. The real output has an optional NextToken member, but the real
// input (ListRepositorySyncDefinitionsInput) has NO NextToken/MaxResults
// member at all (confirmed against aws-sdk-go-v2/service/codeconnections@
// v1.13.4's ListRepositorySyncDefinitionsInput struct and botocore's
// paginators-1.json, which has an empty pagination config for this op) --
// a real client has no way to ever request a further page, so this
// emulation always returns every definition in one response and NextToken
// stays nil/omitted.
type listRepositorySyncDefinitionsOutput struct {
	NextToken                 *string                        `json:"NextToken,omitempty"`
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

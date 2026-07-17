package codestarconnections

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type getSyncBlockerSummaryInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

type syncBlockerItem struct {
	ID             string  `json:"Id"`
	Type           string  `json:"Type"`
	Status         string  `json:"Status"`
	CreatedReason  string  `json:"CreatedReason"`
	ResolvedReason string  `json:"ResolvedReason,omitempty"`
	CreatedAt      float64 `json:"CreatedAt"`
	ResolvedAt     float64 `json:"ResolvedAt,omitempty"`
}

// syncBlockerToItem converts a backend SyncBlocker to its wire shape.
// CreatedAt/ResolvedAt are epoch-seconds numbers on the wire (see
// awsAwsjson10_deserializeDocumentSyncBlocker in the real SDK), not RFC3339
// strings.
func syncBlockerToItem(b SyncBlocker) syncBlockerItem {
	item := syncBlockerItem{
		ID:            b.ID,
		Type:          b.Type,
		Status:        b.Status,
		CreatedAt:     awstime.Epoch(b.CreatedAt),
		CreatedReason: b.CreatedReason,
	}

	if b.ResolvedAt != nil {
		item.ResolvedAt = awstime.Epoch(*b.ResolvedAt)
		item.ResolvedReason = b.ResolvedReason
	}

	return item
}

type syncBlockerSummaryItem struct {
	ResourceName       string            `json:"ResourceName"`
	ParentResourceName string            `json:"ParentResourceName,omitempty"`
	LatestBlockers     []syncBlockerItem `json:"LatestBlockers"`
}

type getSyncBlockerSummaryOutput struct {
	SyncBlockerSummary syncBlockerSummaryItem `json:"SyncBlockerSummary"`
}

func (h *Handler) handleGetSyncBlockerSummary(
	ctx context.Context,
	in *getSyncBlockerSummaryInput,
) (*getSyncBlockerSummaryOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	summary, err := h.Backend.GetSyncBlockerSummary(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	blockers := make([]syncBlockerItem, len(summary.LatestBlockers))
	for i, b := range summary.LatestBlockers {
		blockers[i] = syncBlockerToItem(b)
	}

	return &getSyncBlockerSummaryOutput{
		SyncBlockerSummary: syncBlockerSummaryItem{
			ResourceName:       summary.ResourceName,
			ParentResourceName: summary.ParentResourceName,
			LatestBlockers:     blockers,
		},
	}, nil
}

type updateSyncBlockerInput struct {
	ID             string `json:"Id"`
	ResolvedReason string `json:"ResolvedReason"`
	ResourceName   string `json:"ResourceName"`
	SyncType       string `json:"SyncType"`
}

// updateSyncBlockerOutput is the UpdateSyncBlocker response shape. The real
// operation returns the single updated SyncBlocker object under the
// "SyncBlocker" key -- NOT a "SyncBlockerSummary" list (confirmed against
// aws-sdk-go-v2's awsAwsjson10_deserializeOpDocumentUpdateSyncBlockerOutput,
// which only recognizes ResourceName/ParentResourceName/SyncBlocker).
type updateSyncBlockerOutput struct {
	ResourceName       string          `json:"ResourceName"`
	ParentResourceName string          `json:"ParentResourceName,omitempty"`
	SyncBlocker        syncBlockerItem `json:"SyncBlocker"`
}

func (h *Handler) handleUpdateSyncBlocker(
	ctx context.Context,
	in *updateSyncBlockerInput,
) (*updateSyncBlockerOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	summary, err := h.Backend.UpdateSyncBlocker(ctx, in.ID, in.ResolvedReason)
	if err != nil {
		return nil, err
	}

	// The backend returns every blocker for the owning resource; pick out the
	// one that was just resolved (backend.UpdateSyncBlocker only succeeds when
	// in.ID exists, so it is always present here).
	var resolved SyncBlocker

	for _, b := range summary.LatestBlockers {
		if b.ID == in.ID {
			resolved = b

			break
		}
	}

	return &updateSyncBlockerOutput{
		ResourceName:       summary.ResourceName,
		ParentResourceName: summary.ParentResourceName,
		SyncBlocker:        syncBlockerToItem(resolved),
	}, nil
}

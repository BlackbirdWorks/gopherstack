package wafv2

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// getManagedRuleSetRequest is the request body for GetManagedRuleSet.
type getManagedRuleSetRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
}

// handleGetManagedRuleSet returns the stored managed rule set.
func (h *Handler) handleGetManagedRuleSet(ctx context.Context, body []byte) ([]byte, error) {
	var req getManagedRuleSetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	ms, err := h.Backend.GetManagedRuleSet(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ManagedRuleSet": map[string]any{
			"Id":                 ms.ID,
			keyName:              ms.Name,
			keyARN:               ms.ARN,
			keyLockToken:         ms.LockToken,
			"PublishedVersions":  ms.PublishedVersions,
			"RecommendedVersion": ms.RecommendedVersion,
		},
		keyLockToken: ms.LockToken,
	})
}

// listManagedRuleSetsRequest is the request body for ListManagedRuleSets.
type listManagedRuleSetsRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

// handleListManagedRuleSets lists all stored managed rule sets, filtered by scope.
func (h *Handler) handleListManagedRuleSets(ctx context.Context, body []byte) ([]byte, error) {
	var req listManagedRuleSetsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	sets := h.Backend.ListManagedRuleSets(ctx, req.Scope)

	items, nextMarker := paginateByName(
		sets,
		func(ms *ManagedRuleSet) string { return ms.Name },
		req.NextMarker,
		req.Limit,
	)

	summaries := make([]map[string]any, 0, len(items))

	for _, ms := range items {
		summaries = append(summaries, map[string]any{
			"Id":         ms.ID,
			keyName:      ms.Name,
			keyARN:       ms.ARN,
			keyLockToken: ms.LockToken,
		})
	}

	resp := map[string]any{"ManagedRuleSets": summaries}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return json.Marshal(resp)
}

// putManagedRuleSetVersionsRequest is the request body for PutManagedRuleSetVersions.
type putManagedRuleSetVersionsRequest struct {
	VersionsToPublish  map[string]any `json:"VersionsToPublish"`
	ID                 string         `json:"Id"`
	Name               string         `json:"Name"`
	Scope              string         `json:"Scope"`
	LockToken          string         `json:"LockToken"`
	RecommendedVersion string         `json:"RecommendedVersion"`
}

func (h *Handler) handlePutManagedRuleSetVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req putManagedRuleSetVersionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	ms, err := h.Backend.PutManagedRuleSetVersions(
		ctx,
		req.ID,
		req.Name,
		req.Scope,
		req.LockToken,
		req.RecommendedVersion,
		req.VersionsToPublish,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: put managed rule set versions", "id", req.ID)

	return json.Marshal(map[string]string{keyNextLockToken: ms.LockToken})
}

// updateManagedRuleSetVersionExpiryDateRequest is the request body for UpdateManagedRuleSetVersionExpiryDate.
type updateManagedRuleSetVersionExpiryDateRequest struct {
	ExpiryTimestamp *int64 `json:"ExpiryTimestamp"`
	ID              string `json:"Id"`
	Name            string `json:"Name"`
	Scope           string `json:"Scope"`
	LockToken       string `json:"LockToken"`
	VersionToExpire string `json:"VersionToExpire"`
}

func (h *Handler) handleUpdateManagedRuleSetVersionExpiryDate(ctx context.Context, body []byte) ([]byte, error) {
	var req updateManagedRuleSetVersionExpiryDateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	ms, err := h.Backend.UpdateManagedRuleSetVersionExpiryDate(
		ctx,
		req.ID,
		req.LockToken,
		req.VersionToExpire,
		req.ExpiryTimestamp,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated managed rule set version expiry date", "id", req.ID)

	return json.Marshal(map[string]any{
		keyNextLockToken:  ms.LockToken,
		"ExpiringVersion": req.VersionToExpire,
		"ExpiryTimestamp": req.ExpiryTimestamp,
	})
}

// managedRuleSetDispatchOps returns the ManagedRuleSet-family operation dispatch entries.
// Each entry is a bound method value -- handleGetManagedRuleSet et al. already match the
// dispatchFn signature, so no wrapper closure is needed.
func (h *Handler) managedRuleSetDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"GetManagedRuleSet":                     h.handleGetManagedRuleSet,
		"ListManagedRuleSets":                   h.handleListManagedRuleSets,
		"PutManagedRuleSetVersions":             h.handlePutManagedRuleSetVersions,
		"UpdateManagedRuleSetVersionExpiryDate": h.handleUpdateManagedRuleSetVersionExpiryDate,
	}
}

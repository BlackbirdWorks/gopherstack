package cloudwatchlogs

import (
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// storageTierPolicyWireShape maps a StorageTierPolicy to the real
// GetStorageTierPolicyOutput/PutStorageTierPolicyOutput shape
// (storageTier/lastUpdatedTime). LastUpdatedTime is omitted when zero,
// matching its nilable *int64 real-SDK type (never explicitly set yet).
func storageTierPolicyWireShape(p *StorageTierPolicy) map[string]any {
	shape := map[string]any{"storageTier": p.StorageTier}
	if p.LastUpdatedTime != 0 {
		shape[keyLastUpdatedTime] = p.LastUpdatedTime
	}

	return shape
}

func (h *Handler) handleGetStorageTierPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	b := cwlBackend(h)
	if b == nil {
		return storageTierPolicyWireShape(&StorageTierPolicy{StorageTier: StorageTierStandard}), nil
	}

	p := b.GetStorageTierPolicy()

	return storageTierPolicyWireShape(&p), nil
}

type putStorageTierPolicyInput struct {
	StorageTier string `json:"storageTier"`
}

func (h *Handler) handlePutStorageTierPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putStorageTierPolicyInput
	if err := json.Unmarshal(body, &in, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	b := cwlBackend(h)
	if b == nil {
		return storageTierPolicyWireShape(&StorageTierPolicy{StorageTier: in.StorageTier}), nil
	}

	p, err := b.PutStorageTierPolicy(in.StorageTier)
	if err != nil {
		return nil, err
	}

	return storageTierPolicyWireShape(p), nil
}

package transfer

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// hostKeyARN builds the ARN for a Transfer host key.
func hostKeyARN(accountID, region, serverID, hostKeyID string) string {
	return arn.Build("transfer", region, accountID, "host-key/"+serverID+"/"+hostKeyID)
}

type importHostKeyInput struct {
	ServerID    string              `json:"ServerId"`
	HostKeyBody string              `json:"HostKeyBody"`
	Description string              `json:"Description"`
	Tags        []map[string]string `json:"Tags"`
}

type importHostKeyOutput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

func (h *Handler) handleImportHostKey(
	_ context.Context,
	in *importHostKeyInput,
) (*importHostKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	hk, err := h.Backend.ImportHostKey(in.ServerID, in.HostKeyBody, in.Description, tags)
	if err != nil {
		return nil, err
	}

	return &importHostKeyOutput{ServerID: hk.ServerID, HostKeyID: hk.HostKeyID}, nil
}

type deleteHostKeyInput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

func (h *Handler) handleDeleteHostKey(
	_ context.Context,
	in *deleteHostKeyInput,
) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.HostKeyID == "" {
		return nil, fmt.Errorf("%w: HostKeyId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHostKey(in.ServerID, in.HostKeyID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeHostKeyInput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

type describeHostKeyOutput struct {
	HostKey  map[string]any `json:"HostKey"`
	ServerID string         `json:"ServerId"`
}

func (h *Handler) handleDescribeHostKey(
	_ context.Context,
	in *describeHostKeyInput,
) (*describeHostKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.HostKeyID == "" {
		return nil, fmt.Errorf("%w: HostKeyId is required", errInvalidRequest)
	}

	hk, err := h.Backend.DescribeHostKey(in.ServerID, in.HostKeyID)
	if err != nil {
		return nil, err
	}

	hkMap := map[string]any{
		"HostKeyId":    hk.HostKeyID,
		keyDescription: hk.Description,
		keyStepType:    hk.Type,
		"DateImported": awstime.Epoch(hk.CreatedAt),
		keyArn:         hostKeyARN(hk.AccountID, hk.Region, hk.ServerID, hk.HostKeyID),
		keyTags:        tagsToList(hk.Tags),
	}

	if hk.Fingerprint != "" {
		hkMap["HostKeyFingerprint"] = hk.Fingerprint
	}

	return &describeHostKeyOutput{
		ServerID: hk.ServerID,
		HostKey:  hkMap,
	}, nil
}

type listHostKeysInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listHostKeysOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	ServerID  string           `json:"ServerId"`
	HostKeys  []map[string]any `json:"HostKeys"`
}

func (h *Handler) handleListHostKeys(
	_ context.Context,
	in *listHostKeysInput,
) (*listHostKeysOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListHostKeys(in.ServerID)
	if err != nil {
		return nil, err
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, hk := range page {
		item := map[string]any{
			"HostKeyId":    hk.HostKeyID,
			keyDescription: hk.Description,
			keyStepType:    hk.Type,
			"DateImported": awstime.Epoch(hk.CreatedAt),
			keyArn:         hostKeyARN(hk.AccountID, hk.Region, hk.ServerID, hk.HostKeyID),
		}
		if hk.Fingerprint != "" {
			// Real ListedHostKey's member is "Fingerprint" -- DescribedHostKey
			// (DescribeHostKey, above) is the one that uses "HostKeyFingerprint".
			item["Fingerprint"] = hk.Fingerprint
		}
		out[i] = item
	}

	return &listHostKeysOutput{HostKeys: out, NextToken: next, ServerID: in.ServerID}, nil
}

type updateHostKeyInput struct {
	ServerID    string `json:"ServerId"`
	HostKeyID   string `json:"HostKeyId"`
	Description string `json:"Description"`
}

type updateHostKeyOutput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

func (h *Handler) handleUpdateHostKey(
	_ context.Context,
	in *updateHostKeyInput,
) (*updateHostKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.HostKeyID == "" {
		return nil, fmt.Errorf("%w: HostKeyId is required", errInvalidRequest)
	}

	hk, err := h.Backend.UpdateHostKey(in.ServerID, in.HostKeyID, in.Description)
	if err != nil {
		return nil, err
	}

	return &updateHostKeyOutput{ServerID: hk.ServerID, HostKeyID: hk.HostKeyID}, nil
}

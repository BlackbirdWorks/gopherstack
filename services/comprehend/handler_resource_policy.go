package comprehend

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func (h *Handler) deleteResourcePolicy(input map[string]any) (map[string]any, error) {
	err := h.Backend.DeleteResourcePolicy(
		stringValue(input, "ResourceArn", ""),
		stringValue(input, "PolicyRevisionId", ""),
	)

	return map[string]any{}, err
}

func (h *Handler) describeResourcePolicy(input map[string]any) (map[string]any, error) {
	policy, revision, createdAt, modifiedAt, err := h.Backend.GetResourcePolicy(stringValue(input, "ResourceArn", ""))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ResourcePolicy":   policy,
		"CreationTime":     awstime.Epoch(createdAt),
		"LastModifiedTime": awstime.Epoch(modifiedAt),
		"PolicyRevisionId": revision,
	}, nil
}

func (h *Handler) putResourcePolicy(input map[string]any) (map[string]any, error) {
	revision, err := h.Backend.PutResourcePolicy(
		stringValue(input, "ResourceArn", ""),
		stringValue(input, "ResourcePolicy", ""),
		stringValue(input, "PolicyRevisionId", ""),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"PolicyRevisionId": revision,
	}, nil
}

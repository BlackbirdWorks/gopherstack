package acmpca

import (
	"context"
	"encoding/json"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type tagCertificateAuthorityInput struct {
	CertificateAuthorityArn string       `json:"CertificateAuthorityArn"`
	Tags                    []svcTags.KV `json:"Tags"`
}

type tagCertificateAuthorityOutput struct{}

type acmpcaTagKey struct {
	Key string `json:"Key"`
}

type untagCertificateAuthorityInput struct {
	CertificateAuthorityArn string         `json:"CertificateAuthorityArn"`
	Tags                    []acmpcaTagKey `json:"Tags"`
}

type untagCertificateAuthorityOutput struct{}

type listTagsInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type listTagsOutput struct {
	Tags []map[string]string `json:"Tags"`
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()

	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("acmpca." + resourceID + ".tags")
	}

	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()

	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) cleanupTags(resourceID string) {
	h.tagsMu.Lock("cleanupTags")
	defer h.tagsMu.Unlock()

	if t, ok := h.tags[resourceID]; ok {
		t.Close()
		delete(h.tags, resourceID)
	}
}

func (h *Handler) getTags(resourceID string) []map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()

	if t == nil {
		return []map[string]string{}
	}

	tagMap := t.Clone()
	result := make([]map[string]string, 0, len(tagMap))

	for k, v := range tagMap {
		result = append(result, map[string]string{"Key": k, "Value": v})
	}

	return result
}

// SetTagsForTest is a test helper that sets tags for a resource by ARN.
func (h *Handler) SetTagsForTest(resourceID string, kv map[string]string) {
	h.setTags(resourceID, kv)
}

// GetTagsForTest is a test helper that returns all tags for a resource by ARN.
func (h *Handler) GetTagsForTest(resourceID string) []map[string]string {
	return h.getTags(resourceID)
}

func (h *Handler) jsonTagCA(ctx context.Context, body []byte) (any, error) {
	var input tagCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.verifyCertificateAuthorityActive(ctx, input.CertificateAuthorityArn); err != nil {
		return nil, err
	}

	kv := make(map[string]string, len(input.Tags))
	for _, t := range input.Tags {
		kv[t.Key] = t.Value
	}

	h.setTags(input.CertificateAuthorityArn, kv)

	return &tagCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonUntagCA(ctx context.Context, body []byte) (any, error) {
	var input untagCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.verifyCertificateAuthorityActive(ctx, input.CertificateAuthorityArn); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(input.Tags))
	for _, t := range input.Tags {
		keys = append(keys, t.Key)
	}

	h.removeTags(input.CertificateAuthorityArn, keys)

	return &untagCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonListTags(ctx context.Context, body []byte) (any, error) {
	var input listTagsInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.verifyCertificateAuthorityActive(ctx, input.CertificateAuthorityArn); err != nil {
		return nil, err
	}

	return &listTagsOutput{Tags: h.getTags(input.CertificateAuthorityArn)}, nil
}

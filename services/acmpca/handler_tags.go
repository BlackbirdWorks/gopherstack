package acmpca

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// maxTagsPerCA is the real API's documented per-CA tag limit ("You can
// associate up to 50 tags with a private CA" -- see aws-sdk-go-v2's
// CreateCertificateAuthorityInput.Tags / TagCertificateAuthorityInput.Tags doc
// comments). Exceeding it returns TooManyTagsException.
const maxTagsPerCA = 50

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
	NextToken               string `json:"NextToken"`
	MaxResults              int    `json:"MaxResults"`
}

type listTagsOutput struct {
	NextToken string              `json:"NextToken,omitempty"`
	Tags      []map[string]string `json:"Tags"`
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()

	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("acmpca." + resourceID + ".tags")
	}

	h.tags[resourceID].Merge(kv)
}

// tagCountAfterMerge returns how many distinct tag keys resourceID would carry
// if kv were merged in, without mutating any state -- used to enforce
// maxTagsPerCA before committing a TagCertificateAuthority call.
func (h *Handler) tagCountAfterMerge(resourceID string, kv map[string]string) int {
	merged := h.getTagsMap(resourceID)
	maps.Copy(merged, kv)

	return len(merged)
}

// getTagsMap returns a plain map[string]string copy of resourceID's tags (empty,
// never nil, when untagged).
func (h *Handler) getTagsMap(resourceID string) map[string]string {
	h.tagsMu.RLock("getTagsMap")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()

	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
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
	tagMap := h.getTagsMap(resourceID)
	result := make([]map[string]string, 0, len(tagMap))

	for k, v := range tagMap {
		result = append(result, map[string]string{"Key": k, "Value": v})
	}

	sort.Slice(result, func(i, j int) bool { return result[i]["Key"] < result[j]["Key"] })

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

	if count := h.tagCountAfterMerge(input.CertificateAuthorityArn, kv); count > maxTagsPerCA {
		return nil, fmt.Errorf("%w: a certificate authority may have at most %d tags (would have %d)",
			ErrTooManyTags, maxTagsPerCA, count)
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

	all := h.getTags(input.CertificateAuthorityArn)
	p := page.New(all, input.NextToken, input.MaxResults, defaultMaxItems)

	return &listTagsOutput{Tags: p.Data, NextToken: p.Next}, nil
}

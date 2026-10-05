package xray

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	NextToken   string `json:"NextToken"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, body []byte) ([]byte, error) {
	var in listTagsForResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	pg := page.New(tags, in.NextToken, 0, defaultTagsPageSize)

	return json.Marshal(map[string]any{
		"Tags":       pg.Data,
		keyNextToken: pg.Next,
	})
}

// tagWire mirrors types.Tag (xray@v1.39.4): a {Key, Value} object, one per
// list element -- NOT a JSON map. TagResourceInput.Tags serializes as this
// list shape (serializers.go's awsRestjson1_serializeDocumentTagList), so a
// real client's request body is always a JSON array here.
type tagWire struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type tagResourceInput struct {
	ResourceARN string    `json:"ResourceARN"`
	Tags        []tagWire `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) ([]byte, error) {
	var in tagResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceARN, tagsToMap(in.Tags)); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte) ([]byte, error) {
	var in untagResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

const (
	defaultTagsPageSize = 50
)

func tagsToMap(in []tagWire) map[string]string {
	tags := make(map[string]string, len(in))
	for _, t := range in {
		tags[t.Key] = t.Value
	}

	return tags
}

// checkCreateTags rejects an over-limit tag list before the resource exists.
func checkCreateTags(in []tagWire) error {
	if n := len(tagsToMap(in)); n > maxTagsPerResource {
		return fmt.Errorf("%w: at most %d tags allowed, got %d", ErrTooManyTags, maxTagsPerResource, n)
	}

	return nil
}

func (h *Handler) applyCreateTags(arn string, in []tagWire) error {
	if len(in) == 0 {
		return nil
	}

	return h.Backend.TagResource(arn, tagsToMap(in))
}

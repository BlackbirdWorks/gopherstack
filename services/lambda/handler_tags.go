package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type lambdaTagsInput struct {
	Tags *tags.Tags `json:"Tags"`
}

type getTagsOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = tags.New("lambda." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	var t *tags.Tags

	func() {
		h.tagsMu.RLock("removeTags")
		defer h.tagsMu.RUnlock()

		t = h.tags[resourceID]
	}()

	if t != nil {
		t.DeleteKeys(keys)
	}
}

// deleteTags removes the tags entry for a resource and releases its resources.
func (h *Handler) deleteTags(resourceID string) {
	var t *tags.Tags

	func() {
		h.tagsMu.Lock("deleteTags")
		defer h.tagsMu.Unlock()

		t = h.tags[resourceID]
		delete(h.tags, resourceID)
	}()

	if t != nil {
		t.Close()
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	var t *tags.Tags

	func() {
		h.tagsMu.RLock("getTags")
		defer h.tagsMu.RUnlock()

		t = h.tags[resourceID]
	}()

	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// handleTagsRoute handles GET/POST/DELETE /2015-03-31/tags/{arn}.
func (h *Handler) handleTagsRoute(c *echo.Context, method string) error {
	resourceARN := strings.TrimPrefix(c.Request().URL.Path, lambdaTagsPathPrefix+"/")
	fnName := functionNameFromARN(resourceARN)

	switch method {
	case http.MethodGet:
		if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok && fnName != "" {
			fn, err := lambdaBk.GetFunction(fnName)
			if err == nil {
				return c.JSON(http.StatusOK, &getTagsOutput{Tags: fn.Tags})
			}
		}

		return c.JSON(http.StatusOK, &getTagsOutput{Tags: h.getTags(resourceARN)})
	case http.MethodPost:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}
		var input lambdaTagsInput
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid body")
		}
		var kv map[string]string
		if input.Tags != nil {
			kv = input.Tags.Clone()
		}
		if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok && fnName != "" {
			_ = lambdaBk.TagResource(fnName, kv)
		}
		h.setTags(resourceARN, kv)
		c.Response().WriteHeader(http.StatusNoContent)

		return nil
	case http.MethodDelete:
		keys := c.Request().URL.Query()["tagKeys"]
		if lambdaBk, ok := h.Backend.(*InMemoryBackend); ok && fnName != "" {
			_ = lambdaBk.UntagResource(fnName, keys)
		}
		h.removeTags(resourceARN, keys)
		c.Response().WriteHeader(http.StatusNoContent)

		return nil
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// TaggedFunctionInfo contains a Lambda function's ARN and tag snapshot.
// Used by the Resource Groups Tagging API cross-service listing.
type TaggedFunctionInfo struct {
	Tags map[string]string
	ARN  string
}

// TaggedFunctions returns a snapshot of all Lambda functions with their ARNs and tags.
// Intended for use by the Resource Groups Tagging API provider.
func (h *Handler) TaggedFunctions(_ context.Context) []TaggedFunctionInfo {
	p := h.Backend.ListFunctions("", 0)
	fns := p.Data

	h.tagsMu.RLock("TaggedFunctions")
	defer h.tagsMu.RUnlock()

	result := make([]TaggedFunctionInfo, 0, len(fns))

	for _, fn := range fns {
		var tagMap map[string]string
		if t := h.tags[fn.FunctionArn]; t != nil {
			tagMap = t.Clone()
		}

		result = append(result, TaggedFunctionInfo{ARN: fn.FunctionArn, Tags: tagMap})
	}

	return result
}

// TagFunctionByARN applies tags to the Lambda function identified by its ARN.
func (h *Handler) TagFunctionByARN(_ context.Context, fnARN string, newTags map[string]string) error {
	p := h.Backend.ListFunctions("", 0)
	fns := p.Data

	for _, fn := range fns {
		if fn.FunctionArn == fnARN {
			h.setTags(fn.FunctionArn, newTags)

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrFunctionNotFound, fnARN)
}

// UntagFunctionByARN removes the specified tag keys from the Lambda function identified by its ARN.
func (h *Handler) UntagFunctionByARN(_ context.Context, fnARN string, tagKeys []string) error {
	p := h.Backend.ListFunctions("", 0)
	fns := p.Data

	for _, fn := range fns {
		if fn.FunctionArn == fnARN {
			h.removeTags(fn.FunctionArn, tagKeys)

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrFunctionNotFound, fnARN)
}

// Purge removes all resources older than the given cutoff time.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Purge(ctx, cutoff)
	}

	// Build the set of function ARNs that still exist in the backend after purge.
	remaining := make(map[string]struct{})
	pg := h.Backend.ListFunctions("", -1)
	for _, fn := range pg.Data {
		remaining[fn.FunctionArn] = struct{}{}
	}

	// Remove tags for any function that no longer exists.
	h.tagsMu.Lock("Purge")
	defer h.tagsMu.Unlock()

	for arn, t := range h.tags {
		if _, ok := remaining[arn]; !ok {
			if t != nil {
				t.Close()
			}
			delete(h.tags, arn)
		}
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}

	// Close and clear the handler-level tag store.
	h.tagsMu.Lock("Reset")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		if t != nil {
			t.Close()
		}
	}

	h.tags = make(map[string]*tags.Tags)
}

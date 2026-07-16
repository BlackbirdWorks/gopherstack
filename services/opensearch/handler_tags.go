package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// handleTagRoutes processes /2021-01-01/tags and /2021-01-01/tags-removal requests.
// Returns true if the request was handled.
func (h *Handler) handleTagRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	switch {
	case path == openSearchTagsPath && r.Method == http.MethodGet:
		h.handleListTags(w, r)

		return true
	case path == openSearchTagsPath && r.Method == http.MethodPost:
		h.handleAddTags(w, r)

		return true
	case path == openSearchTagsRemoval && r.Method == http.MethodPost:
		h.handleRemoveTags(w, r)

		return true
	}

	return false
}

type listTagsOutput struct {
	TagList []svcTags.KV `json:"TagList"`
}

func (h *Handler) handleListTags(w http.ResponseWriter, r *http.Request) {
	domainARN := r.URL.Query().Get("arn")

	tags, err := h.Backend.ListTags(domainARN)
	if err != nil {
		h.writeJSON(r, w, &listTagsOutput{TagList: []svcTags.KV{}})

		return
	}

	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	h.writeJSON(r, w, &listTagsOutput{TagList: tagList})
}

type addTagsInput struct {
	ARN     string       `json:"ARN"`
	TagList []svcTags.KV `json:"TagList"`
}

func (h *Handler) handleAddTags(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addTagsInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	tagMap := make(map[string]string, len(req.TagList))
	for _, t := range req.TagList {
		tagMap[t.Key] = t.Value
	}

	if addErr := h.Backend.AddTags(req.ARN, tagMap); addErr != nil {
		if errors.Is(addErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", addErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

	w.WriteHeader(http.StatusOK)
}

type removeTagsInput struct {
	ARN     string   `json:"ARN"`
	TagKeys []string `json:"TagKeys"`
}

func (h *Handler) handleRemoveTags(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req removeTagsInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	if removeErr := h.Backend.RemoveTags(req.ARN, req.TagKeys); removeErr != nil {
		if errors.Is(removeErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", removeErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", removeErr.Error())
		}

		return
	}

	w.WriteHeader(http.StatusOK)
}

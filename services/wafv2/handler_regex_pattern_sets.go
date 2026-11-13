package wafv2

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// createRegexPatternSetRequest is the request body for CreateRegexPatternSet.
// RegularExpressionList accepts both []string and []{RegexString:...} shapes.
type createRegexPatternSetRequest struct {
	Name                  string         `json:"Name"`
	Scope                 string         `json:"Scope"`
	Description           string         `json:"Description"`
	RegularExpressionList jsontext.Value `json:"RegularExpressionList"`
	Tags                  []tags.KV      `json:"Tags"`
}

func (h *Handler) handleCreateRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req createRegexPatternSetRequest
	if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateResourceName(req.Name); err != nil {
		return nil, err
	}

	if err := validateDescription(req.Description); err != nil {
		return nil, err
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if !validScope(req.Scope) {
		return nil, fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	entries, err := parseRegexEntries(req.RegularExpressionList)
	if err != nil {
		return nil, err
	}

	if validateErr := validateRegexEntries(entries); validateErr != nil {
		return nil, validateErr
	}

	tags := tags.MapFromKV(req.Tags)
	if tagsErr := validateTags(tags); tagsErr != nil {
		return nil, tagsErr
	}

	rps, err := h.Backend.CreateRegexPatternSet(
		ctx,
		req.Name,
		req.Scope,
		req.Description,
		entries,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created regex pattern set", "name", rps.Name, "id", rps.ID)

	arnStr := rps.ARN

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":           rps.ID,
			keyName:        rps.Name,
			keyARN:         arnStr,
			keyLockToken:   rps.LockToken,
			keyDescription: rps.Description,
		},
	}, jsonx.V1())
}

// parseRegexEntries accepts either []RegexEntry or []string and normalises to []RegexEntry.
func parseRegexEntries(raw jsontext.Value) ([]RegexEntry, error) {
	if len(raw) == 0 {
		return []RegexEntry{}, nil
	}

	// Try object form first: [{"RegexString": "..."}]
	var entries []RegexEntry
	if err := json.Unmarshal(raw, &entries, jsonx.V1()); err == nil {
		return entries, nil
	}

	// Fall back to plain string form: ["...", "..."]
	var strs []string
	if err := json.Unmarshal(raw, &strs, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: RegularExpressionList must be an array of objects or strings", errInvalidRequest)
	}

	result := make([]RegexEntry, len(strs))
	for i, s := range strs {
		result[i] = RegexEntry{RegexString: s}
	}

	return result, nil
}

// deleteRegexPatternSetRequest is the request body for DeleteRegexPatternSet.
type deleteRegexPatternSetRequest struct {
	ID        string `json:"Id"`
	Name      string `json:"Name"`
	Scope     string `json:"Scope"`
	LockToken string `json:"LockToken"`
}

func (h *Handler) handleDeleteRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteRegexPatternSetRequest
	if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := requireIDNameScopeLockToken(req.ID, req.Name, req.Scope, req.LockToken); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteRegexPatternSet(ctx, req.ID, req.LockToken); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted regex pattern set", "id", req.ID)

	return nil, nil
}

// getRegexPatternSetRequest is the request body for GetRegexPatternSet.
type getRegexPatternSetRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
}

func (h *Handler) handleGetRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req getRegexPatternSetRequest
	if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	r, err := h.Backend.GetRegexPatternSet(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if req.Scope != "" && r.Scope != req.Scope {
		return nil, fmt.Errorf(
			"%w: regex pattern set %q has scope %s, not %s",
			ErrRegexPatternSetNotFound,
			req.ID,
			r.Scope,
			req.Scope,
		)
	}

	arnStr := r.ARN

	return json.Marshal(map[string]any{
		"RegexPatternSet": map[string]any{
			"Id":                    r.ID,
			keyName:                 r.Name,
			keyARN:                  arnStr,
			keyDescription:          r.Description,
			"RegularExpressionList": r.RegularExpressionList,
		},
		keyLockToken: r.LockToken,
	}, jsonx.V1())
}

func (h *Handler) handleListRegexPatternSets(ctx context.Context, body []byte) ([]byte, error) {
	return handleListResourceFamily(
		body,
		h.Backend.ListRegexPatternSets(ctx),
		"RegexPatternSets",
		func(r *RegexPatternSet) string { return r.Scope },
		func(r *RegexPatternSet) string { return r.Name },
		func(r *RegexPatternSet) map[string]string {
			return map[string]string{
				"Id":           r.ID,
				keyName:        r.Name,
				keyARN:         r.ARN,
				keyLockToken:   r.LockToken,
				keyDescription: r.Description,
			}
		},
	)
}

// updateRegexPatternSetRequest is the request body for UpdateRegexPatternSet.
type updateRegexPatternSetRequest struct {
	ID                    string         `json:"Id"`
	Name                  string         `json:"Name"`
	Scope                 string         `json:"Scope"`
	LockToken             string         `json:"LockToken"`
	Description           string         `json:"Description"`
	RegularExpressionList jsontext.Value `json:"RegularExpressionList"`
}

func (h *Handler) handleUpdateRegexPatternSet(ctx context.Context, body []byte) ([]byte, error) {
	var req updateRegexPatternSetRequest
	if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.LockToken == "" {
		return nil, fmt.Errorf("%w: LockToken is required", errInvalidRequest)
	}

	entries, err := parseRegexEntries(req.RegularExpressionList)
	if err != nil {
		return nil, err
	}

	if validateErr := validateRegexEntries(entries); validateErr != nil {
		return nil, validateErr
	}

	r, err := h.Backend.UpdateRegexPatternSet(ctx, req.ID, req.Description, req.LockToken, entries)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated regex pattern set", "id", req.ID)

	return json.Marshal(map[string]string{keyNextLockToken: r.LockToken}, jsonx.V1())
}

// regexPatternSetDispatchOps returns the RegexPatternSet-family operation dispatch
// entries. Each entry is a bound method value -- handleCreateRegexPatternSet et al.
// already match the dispatchFn signature, so no wrapper closure is needed.
func (h *Handler) regexPatternSetDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"CreateRegexPatternSet": h.handleCreateRegexPatternSet,
		"DeleteRegexPatternSet": h.handleDeleteRegexPatternSet,
		"GetRegexPatternSet":    h.handleGetRegexPatternSet,
		"ListRegexPatternSets":  h.handleListRegexPatternSets,
		"UpdateRegexPatternSet": h.handleUpdateRegexPatternSet,
	}
}

package azurearm

import (
	_ "embed"
	"encoding/json/v2"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	srchLinksType        = "sharedPrivateLinkResources"
	srchLinkStatePending = "Pending"
	srchLinkStateOK      = "Succeeded"
	srchFieldGroupID     = "groupId"
	srchFieldRequestMsg  = "requestMessage"
	srchFieldLinkTarget  = "privateLinkResourceId"
	srchLinkListLen      = srchServiceLen + 1
	srchLinkItemLen      = srchServiceLen + 2
)

type storedSearchLink struct {
	name           string
	groupID        string
	resourceID     string
	requestMsg     string
	resourceRegion string
	status         string
}

func (p *SearchProvider) linkBody(id ResourceID, l *storedSearchLink) map[string]any {
	props := map[string]any{
		srchFieldGroupID: l.groupID, srchFieldLinkTarget: l.resourceID, srchFieldRequestMsg: l.requestMsg,
		"resourceRegion": nil, srchFieldStatus: l.status, fieldProvisioningState: srchLinkStateOK,
	}
	if l.resourceRegion != "" {
		props["resourceRegion"] = l.resourceRegion
	}

	return map[string]any{
		"id": id.ARMID() + "/" + srchLinksType + "/" + l.name, fieldName: l.name,
		fieldType: namespaceMicrosoftSearch + "/" + srchServicesType + "/" + srchLinksType, fieldProperties: props,
	}
}

func (p *SearchProvider) linkBodies(id ResourceID, rec *storedSearch) []any {
	names := make([]string, 0, len(rec.links))
	for k := range rec.links {
		names = append(names, k)
	}

	slices.Sort(names)

	out := make([]any, 0, len(names))
	for _, k := range names {
		out = append(out, p.linkBody(id, rec.links[k]))
	}

	return out
}

func srchLinkString(props map[string]any, key string) string {
	v, _ := props[key].(string)

	return strings.TrimSpace(v)
}

// PutSharedLink creates or updates a shared private link resource. A new link starts Pending: approval
// happens on the target resource, which this emulator does not do.
func (p *SearchProvider) PutSharedLink(id ResourceID, name string, body map[string]any) (map[string]any, error) {
	props, _ := body[fieldProperties].(map[string]any)

	l := &storedSearchLink{
		name:           name,
		groupID:        srchLinkString(props, srchFieldGroupID),
		resourceID:     srchLinkString(props, srchFieldLinkTarget),
		requestMsg:     srchLinkString(props, srchFieldRequestMsg),
		resourceRegion: srchLinkString(props, "resourceRegion"),
		status:         srchLinkStatePending,
	}

	if l.groupID == "" || l.resourceID == "" {
		return nil, badSearchRequest("properties.groupId and properties.privateLinkResourceId are required")
	}

	p.mu.Lock("PutSharedLink")
	defer p.mu.Unlock()

	rec, err := p.service(id)
	if err != nil {
		return nil, err
	}

	if rec.links == nil {
		rec.links = map[string]*storedSearchLink{}
	}

	key := strs.Fold(name)
	if old, ok := rec.links[key]; ok {
		l.status = old.status
	}

	rec.links[key] = l

	return p.linkBody(id, l), nil
}

// GetSharedLink returns one shared private link resource.
func (p *SearchProvider) GetSharedLink(id ResourceID, name string) (map[string]any, error) {
	p.mu.RLock("GetSharedLink")
	defer p.mu.RUnlock()

	rec, err := p.service(id)
	if err != nil {
		return nil, err
	}

	l, ok := rec.links[strs.Fold(name)]
	if !ok {
		return nil, ErrSearchLinkNotFound
	}

	return p.linkBody(id, l), nil
}

// ListSharedLinks lists a service's shared private link resources.
func (p *SearchProvider) ListSharedLinks(id ResourceID) ([]any, error) {
	p.mu.RLock("ListSharedLinks")
	defer p.mu.RUnlock()

	rec, err := p.service(id)
	if err != nil {
		return nil, err
	}

	return p.linkBodies(id, rec), nil
}

// DeleteSharedLink removes a shared private link resource.
func (p *SearchProvider) DeleteSharedLink(id ResourceID, name string) error {
	p.mu.Lock("DeleteSharedLink")
	defer p.mu.Unlock()

	rec, err := p.service(id)
	if err != nil {
		return err
	}

	key := strs.Fold(name)
	if _, ok := rec.links[key]; !ok {
		return ErrSearchLinkNotFound
	}

	delete(rec.links, key)

	return nil
}

func isSearchLinks(segs []string, r *http.Request, method string, n int) bool {
	return r.Method == method && len(segs) == n && isSearchService(segs) &&
		strings.EqualFold(segs[srchActionIdx], srchLinksType)
}

func (h *Handler) handleSearchLinkList(c *echo.Context, segs []string) error {
	p, id, err := h.searchOrNotFound(c, segs)
	if p == nil {
		return err
	}

	out, err := p.ListSharedLinks(id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: out})
}

func (h *Handler) handleSearchLinkGet(c *echo.Context, segs []string) error {
	p, id, err := h.searchOrNotFound(c, segs)
	if p == nil {
		return err
	}

	out, err := p.GetSharedLink(id, segs[len(segs)-1])
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleSearchLinkPut(c *echo.Context, segs []string) error {
	p, id, err := h.searchOrNotFound(c, segs)
	if p == nil {
		return err
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.PutSharedLink(id, segs[len(segs)-1], body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleSearchLinkDelete(c *echo.Context, segs []string) error {
	p, id, err := h.searchOrNotFound(c, segs)
	if p == nil {
		return err
	}

	if err = p.DeleteSharedLink(id, segs[len(segs)-1]); err != nil {
		return h.writeAPIError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleSearchOperations(c *echo.Context) error {
	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: searchOperations()})
}

//go:embed searchcatalog/operations.json
var searchOperationsJSON []byte

// searchOperations is the Operations_List example of azure-rest-api-specs
// (Microsoft.Search/Search/stable/2025-05-01/examples/SearchListOperations.json).
func searchOperations() []any {
	var doc struct {
		Value []any `json:"value"`
	}

	if err := json.Unmarshal(searchOperationsJSON, &doc, jsonx.V1()); err != nil {
		return []any{}
	}

	return doc.Value
}

func isSearchOperations(segs []string, r *http.Request) bool {
	return r.Method == http.MethodGet && len(segs) == srchOperationsLen &&
		segAt(segs, srchOpsProvidersIdx, providersSegment) &&
		segAt(segs, srchOpsNamespaceIdx, namespaceMicrosoftSearch) && segAt(segs, srchOpsNameIdx, "operations")
}

const (
	srchOperationsLen   = 3
	srchOpsProvidersIdx = 0
	srchOpsNamespaceIdx = 1
	srchOpsNameIdx      = 2
)

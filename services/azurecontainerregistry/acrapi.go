package azurecontainerregistry

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

const (
	defaultPageSize = 100
	maxPageSize     = 1000
	orderTimeAsc    = "timeasc"
	orderTimeDesc   = "timedesc"
	segTags         = "/_tags"
	segManifests    = "/_manifests"
	quarantinePass  = "Passed"
	kindCatalog     = "catalog"
	keyCreated      = "createdTime"
	keyUpdated      = "lastUpdateTime"
	keyChangeable   = "changeableAttributes"
	keyDigest       = "digest"
	keyRegistry     = "registry"
	keyImageName    = "imageName"
)

type changeable struct {
	DeleteEnabled bool `json:"deleteEnabled"`
	WriteEnabled  bool `json:"writeEnabled"`
	ReadEnabled   bool `json:"readEnabled"`
	ListEnabled   bool `json:"listEnabled"`
}

func wireAttrs(a attrs) changeable {
	return changeable{
		DeleteEnabled: !a.DeleteDisabled, WriteEnabled: !a.WriteDisabled,
		ReadEnabled: !a.ReadDisabled, ListEnabled: !a.ListDisabled,
	}
}

type manifestChangeable struct {
	QuarantineState   string `json:"quarantineState"`
	QuarantineDetails string `json:"quarantineDetails"`
	changeable
}

func stamp(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.0000000Z") }

// acrRoute is a parsed /acr/v1 path.
type acrRoute struct {
	repo  string
	kind  string
	item  string
	scope string
}

func parseACRPath(path string) acrRoute {
	rest := strings.TrimPrefix(path, prefixACR)
	if rest == "_catalog" {
		return acrRoute{kind: kindCatalog, scope: "registry:catalog:*"}
	}

	for _, seg := range []string{segTags, segManifests} {
		if i := strings.Index(rest, seg+"/"); i > 0 {
			return acrRoute{repo: rest[:i], kind: seg[2:], item: rest[i+len(seg)+1:]}
		}

		if before, ok := strings.CutSuffix(rest, seg); ok {
			return acrRoute{repo: before, kind: seg[2:]}
		}
	}

	return acrRoute{repo: rest, kind: "repo"}
}

func (h *Handler) serveACR(w http.ResponseWriter, r *http.Request) {
	reg, ok := h.resolve(r)
	if !ok {
		h.unknownRegistry(w)

		return
	}

	rt := parseACRPath(r.URL.Path)
	if rt.repo == "" && rt.kind != kindCatalog {
		writeRegistryError(w, http.StatusNotFound, "NOT_FOUND", "The requested resource was not found.")

		return
	}

	if _, authed := h.authenticate(r, reg); !authed {
		scope := rt.scope
		if scope == "" {
			scope = "repository:" + rt.repo + ":metadata_read"
		}

		challenge(w, r, scope)

		return
	}

	h.dispatchACR(w, r, reg, rt)
}

func (h *Handler) dispatchACR(w http.ResponseWriter, r *http.Request, reg string, rt acrRoute) {
	switch rt.kind {
	case kindCatalog:
		h.acrCatalog(w, r, reg)
	case "repo":
		h.acrRepo(w, r, reg, rt)
	case "tags":
		h.acrTags(w, r, reg, rt)
	case "manifests":
		h.acrManifests(w, r, reg, rt)
	}
}

func methodNotAllowed(w http.ResponseWriter) {
	writeRegistryError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "The operation is not supported.")
}

func notFound(w http.ResponseWriter, what string) {
	writeRegistryError(w, http.StatusNotFound, what, "The requested resource was not found.")
}

type page struct {
	last  string
	order string
	n     int
}

func readPage(r *http.Request) page {
	q := r.URL.Query()
	p := page{n: defaultPageSize, last: q.Get("last"), order: strings.ToLower(q.Get("orderby"))}

	if n, err := strconv.Atoi(q.Get("n")); err == nil && n > 0 {
		p.n = min(n, maxPageSize)
	}

	return p
}

// cut returns the slice of names after p.last (at most p.n) and the cursor for the next page, if any.
func (p page) cut(names []string) ([]string, string) {
	start := 0
	if p.last != "" {
		start = sort.Search(len(names), func(i int) bool { return names[i] > p.last })
	}

	end := min(start+p.n, len(names))
	out := names[start:end]

	next := ""
	if end < len(names) && len(out) > 0 {
		next = out[len(out)-1]
	}

	return out, next
}

func setNextLink(w http.ResponseWriter, r *http.Request, p page, next string) {
	if next == "" {
		return
	}

	q := url.Values{"last": {next}, "n": {strconv.Itoa(p.n)}}
	if p.order != "" {
		q.Set("orderby", p.order)
	}

	w.Header().Set("Link", "<"+r.URL.Path+"?"+q.Encode()+`>; rel="next"`)
}

func (h *Handler) acrCatalog(w http.ResponseWriter, r *http.Request, reg string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)

		return
	}

	views := h.Backend.Repositories(reg)
	names := make([]string, 0, len(views))

	for _, v := range views {
		if !v.Attrs.ListDisabled {
			names = append(names, v.Name)
		}
	}

	p := readPage(r)
	out, next := p.cut(names)
	setNextLink(w, r, p, next)

	if out == nil {
		out = []string{}
	}

	writeJSON(w, http.StatusOK, map[string]any{"repositories": out})
}

func (h *Handler) acrRepo(w http.ResponseWriter, r *http.Request, reg string, rt acrRoute) {
	v, ok := h.Backend.Repository(reg, rt.repo)
	if !ok {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			keyRegistry: r.Host, keyImageName: rt.repo, keyCreated: stamp(v.Created),
			keyUpdated: stamp(v.Updated), "manifestCount": v.ManifestCount, "tagCount": v.TagCount,
			keyChangeable: wireAttrs(v.Attrs),
		})
	case http.MethodPatch:
		h.patchAttrs(w, r, func(p AttrPatch) error { return h.Backend.PatchRepository(reg, rt.repo, p) })
	case http.MethodDelete:
		h.deleteRepo(w, r, reg, rt.repo)
	default:
		methodNotAllowed(w)
	}
}

func (h *Handler) deleteRepo(w http.ResponseWriter, r *http.Request, reg, repo string) {
	app, ok := h.Backend.app(reg)
	if !ok {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	gone, found := h.Backend.ForgetRepository(reg, repo)
	if !found {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	c := ociregistry.NewClient(app)
	for _, d := range gone.Digests {
		_ = c.DeleteManifest(r.Context(), repo, d)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{"manifestsDeleted": gone.Digests, "tagsDeleted": gone.Tags})
}

type attrPatchBody struct {
	Value struct {
		DeleteEnabled *bool `json:"deleteEnabled"`
		WriteEnabled  *bool `json:"writeEnabled"`
		ReadEnabled   *bool `json:"readEnabled"`
		ListEnabled   *bool `json:"listEnabled"`
	} `json:"value"`
}

func (h *Handler) patchAttrs(w http.ResponseWriter, r *http.Request, apply func(AttrPatch) error) {
	var body attrPatchBody
	if !decodeBody(w, r, &body) {
		return
	}

	v := body.Value

	err := apply(AttrPatch{Delete: v.DeleteEnabled, Write: v.WriteEnabled, Read: v.ReadEnabled, List: v.ListEnabled})
	if err != nil {
		notFound(w, "NOT_FOUND")

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) acrTags(w http.ResponseWriter, r *http.Request, reg string, rt acrRoute) {
	tags, ok := h.Backend.Tags(reg, rt.repo)
	if !ok {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	if rt.item == "" {
		h.listTags(w, r, reg, rt, tags)

		return
	}

	h.tagItem(w, r, reg, rt, tags)
}

func tagWire(t TagView) map[string]any {
	return map[string]any{
		"name": t.Name, keyDigest: t.Digest, keyCreated: stamp(t.Created), keyUpdated: stamp(t.Updated),
		"signed": false, keyChangeable: wireAttrs(t.Attrs),
	}
}

func orderTags(tags []TagView, order string) []TagView {
	if order != orderTimeAsc && order != orderTimeDesc {
		return tags
	}

	sorted := append([]TagView(nil), tags...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if order == orderTimeAsc {
			return sorted[i].Updated.Before(sorted[j].Updated)
		}

		return sorted[i].Updated.After(sorted[j].Updated)
	})

	return sorted
}

func (h *Handler) listTags(w http.ResponseWriter, r *http.Request, _ string, rt acrRoute, tags []TagView) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)

		return
	}

	p := readPage(r)
	digest := r.URL.Query().Get(keyDigest)
	ordered := orderTags(tags, p.order)
	filtered := make([]TagView, 0, len(ordered))

	for _, t := range ordered {
		if digest == "" || t.Digest == digest {
			filtered = append(filtered, t)
		}
	}

	page, next := pageItems(p, filtered, func(t TagView) string { return t.Name })
	items := make([]map[string]any, 0, len(page))

	for _, t := range page {
		items = append(items, tagWire(t))
	}

	setNextLink(w, r, p, next)
	writeJSON(w, http.StatusOK, map[string]any{keyRegistry: r.Host, keyImageName: rt.repo, "tags": items})
}

// pageItems applies the last/n cursor over items whose cursor key is key(item), preserving item order.
func pageItems[T any](p page, items []T, key func(T) string) ([]T, string) {
	start := 0

	if p.last != "" {
		start = len(items)

		for i, it := range items {
			if key(it) == p.last {
				start = i + 1

				break
			}
		}
	}

	end := min(start+p.n, len(items))
	out := append([]T{}, items[start:end]...)

	next := ""
	if end < len(items) && len(out) > 0 {
		next = key(out[len(out)-1])
	}

	return out, next
}

func (h *Handler) tagItem(w http.ResponseWriter, r *http.Request, reg string, rt acrRoute, tags []TagView) {
	var found *TagView

	for i := range tags {
		if tags[i].Name == rt.item {
			found = &tags[i]
		}
	}

	if found == nil {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{keyRegistry: r.Host, keyImageName: rt.repo, "tag": tagWire(*found)})
	case http.MethodPatch:
		h.patchAttrs(w, r, func(p AttrPatch) error { return h.Backend.PatchTag(reg, rt.repo, rt.item, p) })
	case http.MethodDelete:
		h.deleteTag(w, r, reg, rt)
	default:
		methodNotAllowed(w)
	}
}

func (h *Handler) deleteTag(w http.ResponseWriter, r *http.Request, reg string, rt acrRoute) {
	if !h.Backend.Allowed(reg, rt.repo, rt.item, ociregistry.OpDelete) {
		writeRegistryError(w, http.StatusForbidden, "DENIED", "The operation is disabled.")

		return
	}

	app, ok := h.Backend.app(reg)
	if !ok {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	if err := ociregistry.NewClient(app).DeleteManifest(r.Context(), rt.repo, rt.item); err != nil {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	h.Backend.RecordManifestDelete(reg, rt.repo, rt.item)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) acrManifests(w http.ResponseWriter, r *http.Request, reg string, rt acrRoute) {
	list, ok := h.Backend.Manifests(reg, rt.repo)
	if !ok {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	if rt.item == "" {
		h.listManifests(w, r, reg, rt, list)

		return
	}

	h.manifestItem(w, r, reg, rt, list)
}

func (h *Handler) describe(ctx context.Context, reg, repo string, m ManifestView) map[string]any {
	out := map[string]any{
		keyDigest: m.Digest, keyCreated: stamp(m.Created), keyUpdated: stamp(m.Updated),
		keyChangeable: manifestChangeable{QuarantineState: quarantinePass, changeable: wireAttrs(m.Attrs)},
	}

	if len(m.Tags) > 0 {
		out["tags"] = m.Tags
	}

	app, ok := h.Backend.app(reg)
	if !ok {
		return out
	}

	c := ociregistry.NewClient(app)

	stored, err := c.GetManifest(ctx, repo, m.Digest)
	if err != nil {
		return out
	}

	info := c.Describe(ctx, repo, stored)
	out["mediaType"] = info.MediaType

	if info.Index {
		out["references"] = h.references(ctx, c, repo, info)

		return out
	}

	out["imageSize"], out["configMediaType"] = info.Size, info.ConfigMediaType
	out["architecture"], out["os"] = info.Architecture, info.OS

	return out
}

func (h *Handler) references(ctx context.Context, c ociregistry.Client, repo string, info ociregistry.Info) []any {
	refs := make([]any, 0, len(info.Children))

	for _, child := range info.Children {
		ref := map[string]any{keyDigest: child.Digest}

		if m, err := c.GetManifest(ctx, repo, child.Digest); err == nil {
			ci := c.Describe(ctx, repo, m)
			ref["architecture"], ref["os"] = ci.Architecture, ci.OS
		}

		refs = append(refs, ref)
	}

	return refs
}

func orderManifests(list []ManifestView, order string) []ManifestView {
	if order != orderTimeAsc && order != orderTimeDesc {
		return list
	}

	sorted := append([]ManifestView(nil), list...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if order == orderTimeAsc {
			return sorted[i].Updated.Before(sorted[j].Updated)
		}

		return sorted[i].Updated.After(sorted[j].Updated)
	})

	return sorted
}

func (h *Handler) listManifests(w http.ResponseWriter, r *http.Request, reg string, rt acrRoute, list []ManifestView) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)

		return
	}

	p := readPage(r)
	items, next := pageItems(p, orderManifests(list, p.order), func(m ManifestView) string { return m.Digest })
	out := make([]map[string]any, 0, len(items))

	for _, m := range items {
		out = append(out, h.describe(r.Context(), reg, rt.repo, m))
	}

	setNextLink(w, r, p, next)
	writeJSON(w, http.StatusOK, map[string]any{keyRegistry: r.Host, keyImageName: rt.repo, "manifests": out})
}

func (h *Handler) manifestItem(w http.ResponseWriter, r *http.Request, reg string, rt acrRoute, list []ManifestView) {
	var found *ManifestView

	for i := range list {
		if list[i].Digest == rt.item {
			found = &list[i]
		}
	}

	if found == nil {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			keyRegistry: r.Host, keyImageName: rt.repo, "manifest": h.describe(r.Context(), reg, rt.repo, *found),
		})
	case http.MethodPatch:
		h.patchAttrs(w, r, func(p AttrPatch) error { return h.Backend.PatchManifest(reg, rt.repo, rt.item, p) })
	case http.MethodDelete:
		h.deleteManifest(w, r, reg, rt)
	default:
		methodNotAllowed(w)
	}
}

func (h *Handler) deleteManifest(w http.ResponseWriter, r *http.Request, reg string, rt acrRoute) {
	if !h.Backend.Allowed(reg, rt.repo, rt.item, ociregistry.OpDelete) {
		writeRegistryError(w, http.StatusForbidden, "DENIED", "The operation is disabled.")

		return
	}

	app, ok := h.Backend.app(reg)
	if !ok {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	if err := ociregistry.NewClient(app).DeleteManifest(r.Context(), rt.repo, rt.item); err != nil {
		notFound(w, "NAME_UNKNOWN")

		return
	}

	h.Backend.RecordManifestDelete(reg, rt.repo, rt.item)
	w.WriteHeader(http.StatusAccepted)
}

package azurearm

import (
	"maps"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	kidExtensions  = "extensions"
	kidRunCommands = "runCommands"
	kidImages      = "images"
	kidVersions    = "images/versions"

	codeParentNotFound = "ParentResourceNotFound"
	codeCannotDelete   = "CannotDeleteResource"
)

// cmpKid is a child resource stored inside its parent's cmpRes.
type cmpKid struct {
	Created  time.Time         `json:"created"`
	Tags     map[string]string `json:"tags,omitempty"`
	Props    map[string]any    `json:"props,omitempty"`
	Type     string            `json:"type"`
	Location string            `json:"location,omitempty"`
	Etag     string            `json:"etag"`
	Names    []string          `json:"names"`
}

type kidSpec struct {
	put    func(p *ComputeProvider, parent *cmpRes, k, old *cmpKid, body map[string]any) error
	render func(p *ComputeProvider, parent *cmpRes, k *cmpKid, expand string) map[string]any
	delete func(p *ComputeProvider, parent *cmpRes, k *cmpKid) error
	parent string
	rel    string
	proxy  bool
}

func kidSpecs() []kidSpec {
	return []kidSpec{
		{parent: cmpVMs, rel: kidExtensions, put: putExtension, render: renderExtension},
		{parent: cmpVMSS, rel: kidExtensions, put: putExtension, render: renderExtension, proxy: true},
		{parent: cmpVMs, rel: kidRunCommands, put: putRunCommand, render: renderRunCommand},
		{parent: cmpGalleries, rel: kidImages, put: putGalleryImage, render: renderGalleryImage,
			delete: deleteGalleryImage},
		{parent: cmpGalleries, rel: kidVersions, put: putGalleryVersion, render: renderGalleryVersion,
			delete: deleteGalleryVersion},
	}
}

type kidRef struct {
	parent *cmpRes
	spec   kidSpec
	key    string
	names  []string
}

// kidRefOf resolves a child resource id; ok is false when the type path is not a modelled child. parent is nil when
// the owning resource does not exist.
func (p *ComputeProvider) kidRefOf(id ResourceID) (kidRef, bool) {
	typ := canonicalCmpType(firstType(id))
	if typ == "" || len(id.Types) < 2 || len(id.Names) < 1 || len(id.Names) > len(id.Types) {
		return kidRef{}, false
	}

	rel := strings.Join(id.Types[1:], "/")

	i := slices.IndexFunc(kidSpecs(), func(s kidSpec) bool {
		return s.parent == typ && strings.EqualFold(s.rel, rel)
	})
	if i < 0 {
		return kidRef{}, false
	}

	spec := kidSpecs()[i]
	ref := kidRef{spec: spec, names: id.Names[1:]}
	ref.parent, _ = p.top(typ, id.ResourceGroup, id.Names[0])

	if len(id.Names) == len(id.Types) {
		ref.key = kidKey(spec.rel, ref.names)
	}

	return ref, true
}

func kidKey(rel string, names []string) string {
	types := strings.Split(rel, "/")

	var b strings.Builder

	for i, n := range names {
		if i > 0 {
			b.WriteByte('/')
		}

		b.WriteString(types[i] + "/" + n)
	}

	return strings.ToLower(b.String())
}

func parentMissing(id ResourceID) error {
	return netErr(http.StatusNotFound, codeParentNotFound,
		"Can not perform requested operation on nested resource. Parent resource '%s' not found.", id.Names[0])
}

func (k *cmpKid) leaf() string { return k.Names[len(k.Names)-1] }

func (k *cmpKid) armID(parent *cmpRes) string {
	types := strings.Split(k.Type, "/")

	var b strings.Builder

	b.WriteString(parent.armID())

	for i, n := range k.Names {
		b.WriteString("/" + types[i] + "/" + n)
	}

	return b.String()
}

func (p *ComputeProvider) kidBase(parent *cmpRes, k *cmpKid, props map[string]any) map[string]any {
	out := map[string]any{
		cmpFieldID: k.armID(parent), fieldName: k.leaf(),
		fieldType: namespaceMicrosoftCompute + "/" + parent.Type + "/" + k.Type, fieldProperties: props,
	}

	if k.Location != "" {
		out[fieldLocation], out[fieldTags] = k.Location, tagsOrEmpty(k.Tags)
	}

	return out
}

func (p *ComputeProvider) kidGet(id ResourceID, expand string) (map[string]any, error) {
	ref, ok := p.kidRefOf(id)
	if !ok || ref.parent == nil || ref.key == "" {
		return nil, ErrResourceNotFound
	}

	k := ref.parent.Kids[ref.key]
	if k == nil {
		return nil, ErrResourceNotFound
	}

	return ref.spec.render(p, ref.parent, k, expand), nil
}

// GetKidExpanded serves GET on a child resource with a $expand option.
func (p *ComputeProvider) GetKidExpanded(id ResourceID, expand string) (map[string]any, error) {
	p.mu.RLock("GetKidExpanded")
	defer p.mu.RUnlock()

	if _, ok := p.kidRefOf(id); !ok {
		return p.getLocked(id, false)
	}

	return p.kidGet(id, expand)
}

func (p *ComputeProvider) kidList(id ResourceID) ([]map[string]any, error) {
	ref, ok := p.kidRefOf(id)
	if !ok || ref.parent == nil {
		return nil, ErrResourceNotFound
	}

	prefix := ""
	if len(ref.names) > 0 {
		prefix = kidKey(strings.Join(strings.Split(ref.spec.rel, "/")[:len(ref.names)], "/"), ref.names) + "/"
	}

	keys := make([]string, 0, len(ref.parent.Kids))

	for key, k := range ref.parent.Kids {
		if k.Type == ref.spec.rel && strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)

	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, ref.spec.render(p, ref.parent, ref.parent.Kids[key], ""))
	}

	return out, nil
}

func (p *ComputeProvider) putKid(id ResourceID, body map[string]any) (map[string]any, error) {
	ref, ok := p.kidRefOf(id)
	if !ok || ref.key == "" {
		return nil, ErrResourceNotFound
	}

	if ref.parent == nil {
		return nil, parentMissing(id)
	}

	old := ref.parent.Kids[ref.key]
	loc := normLocation(strOf(body, fieldLocation))

	k := &cmpKid{
		Type: ref.spec.rel, Names: ref.names, Etag: newEtag(), Created: time.Now().UTC(), Location: loc,
		Tags: stringTags(body[fieldTags]),
	}

	switch {
	case ref.spec.proxy:
		k.Location, k.Tags = "", nil
	case old == nil && loc == "":
		return nil, netErr(
			http.StatusBadRequest,
			netCodeLocation,
			"The location property is required for this definition.",
		)
	case old != nil:
		k.Created = old.Created

		if loc == "" {
			k.Location = old.Location
		}

		if k.Location != old.Location {
			return nil, cmpChanged(fieldLocation)
		}
	}

	if err := ref.spec.put(p, ref.parent, k, old, body); err != nil {
		return nil, err
	}

	if ref.parent.Kids == nil {
		ref.parent.Kids = map[string]*cmpKid{}
	}

	ref.parent.Kids[ref.key] = k

	return ref.spec.render(p, ref.parent, k, ""), nil
}

func (p *ComputeProvider) deleteKid(id ResourceID) error {
	ref, ok := p.kidRefOf(id)
	if !ok || ref.parent == nil || ref.key == "" {
		return ErrResourceNotFound
	}

	k := ref.parent.Kids[ref.key]
	if k == nil {
		return ErrResourceNotFound
	}

	if ref.spec.delete != nil {
		if err := ref.spec.delete(p, ref.parent, k); err != nil {
			return err
		}
	}

	delete(ref.parent.Kids, ref.key)

	return nil
}

func (p *ComputeProvider) patchKid(id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.RLock("patchKid")

	cur, err := p.kidGet(id, "")
	p.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	for _, key := range []string{fieldTags} {
		if v, found := body[key]; found {
			cur[key] = v
		}
	}

	maps.Copy(mapOf(cur[fieldProperties]), mapOf(body[fieldProperties]))

	p.mu.Lock("patchKid")
	defer p.mu.Unlock()

	return p.putKid(id, cur)
}

// kidsOf returns the children of parent with relative type rel in key order.
func kidsOf(parent *cmpRes, rel string) []*cmpKid {
	keys := make([]string, 0, len(parent.Kids))

	for key, k := range parent.Kids {
		if k.Type == rel {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)

	out := make([]*cmpKid, 0, len(keys))
	for _, key := range keys {
		out = append(out, parent.Kids[key])
	}

	return out
}

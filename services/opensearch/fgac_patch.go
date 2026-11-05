package opensearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

var errPatch = errors.New("invalid JSON patch")

// patchOp is one RFC 6902 operation.
type patchOp struct {
	Value any    `json:"value"`
	Op    string `json:"op"`
	Path  string `json:"path"`
}

func patchSegments(path string) []string {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return nil
	}

	segs := strings.Split(path, "/")
	for i, s := range segs {
		s = strings.ReplaceAll(s, "~1", "/")
		segs[i] = strings.ReplaceAll(s, "~0", "~")
	}

	return segs
}

func patchMapStep(n map[string]any, segs []string, op string, value any) (any, error) {
	seg := segs[0]
	child, exists := n[seg]

	if len(segs) == 1 {
		switch {
		case op == patchAdd:
			n[seg] = value
		case !exists:
			return nil, fmt.Errorf("%w: path %q does not exist", errPatch, seg)
		case op == patchReplace:
			n[seg] = value
		default:
			delete(n, seg)
		}

		return n, nil
	}

	if !exists {
		return nil, fmt.Errorf("%w: path %q does not exist", errPatch, seg)
	}

	nc, err := patchApply(child, segs[1:], op, value)
	if err != nil {
		return nil, err
	}

	n[seg] = nc

	return n, nil
}

func patchSliceStep(n []any, segs []string, op string, value any) (any, error) {
	seg := segs[0]

	if seg == "-" && len(segs) == 1 && op == patchAdd {
		return append(n, value), nil
	}

	i, err := strconv.Atoi(seg)
	if err != nil || i < 0 || i > len(n) || (i == len(n) && op != patchAdd) {
		return nil, fmt.Errorf("%w: bad array index %q", errPatch, seg)
	}

	if len(segs) > 1 {
		nc, perr := patchApply(n[i], segs[1:], op, value)
		if perr != nil {
			return nil, perr
		}

		n[i] = nc

		return n, nil
	}

	switch op {
	case patchAdd:
		return slices.Insert(n, i, value), nil
	case patchReplace:
		n[i] = value

		return n, nil
	default:
		return slices.Delete(n, i, i+1), nil
	}
}

func patchApply(node any, segs []string, op string, value any) (any, error) {
	if len(segs) == 0 {
		if op == patchRemove {
			return nil, fmt.Errorf("%w: cannot remove the document root", errPatch)
		}

		return value, nil
	}

	switch n := node.(type) {
	case map[string]any:
		return patchMapStep(n, segs, op, value)
	case []any:
		return patchSliceStep(n, segs, op, value)
	default:
		return nil, fmt.Errorf("%w: cannot descend into a scalar", errPatch)
	}
}

// applyPatchOps applies add / replace / remove operations to a JSON document.
func applyPatchOps(doc any, ops []patchOp) (any, error) {
	for _, o := range ops {
		switch o.Op {
		case patchAdd, patchReplace, patchRemove:
		default:
			return nil, fmt.Errorf("%w: unsupported op %q", errPatch, o.Op)
		}

		var err error
		if doc, err = patchApply(doc, patchSegments(o.Path), o.Op, o.Value); err != nil {
			return nil, err
		}
	}

	return doc, nil
}

func storedDoc(cfg *SecurityConfig, kind securityKind, name string) (map[string]any, bool) {
	var v any

	switch kind {
	case kindInternalUsers:
		u, ok := cfg.Users[name]
		if !ok {
			return nil, false
		}

		v = u
	case kindRoles:
		r, ok := cfg.Roles[name]
		if !ok {
			return nil, false
		}

		v = roleView(r)
	case kindRolesMapping:
		m, ok := cfg.RolesMapping[name]
		if !ok {
			return nil, false
		}

		v = mappingView(m)
	case kindActionGroups:
		g, ok := cfg.ActionGroups[name]
		if !ok {
			return nil, false
		}

		v = g
	case kindTenants:
		t, ok := cfg.Tenants[name]
		if !ok {
			return nil, false
		}

		v = t
	}

	raw, err := json.Marshal(v)
	if err != nil {
		return nil, false
	}

	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		return nil, false
	}

	for _, k := range []string{"reserved", "hidden", "static"} {
		delete(out, k)
	}

	return out, true
}

func docToBody(doc map[string]any) []byte {
	b, _ := json.Marshal(doc)

	return b
}

// SecurityPatch applies a JSON patch to one resource, or to the whole collection when name is empty.
func (b *InMemoryBackend) SecurityPatch(domain string, kind securityKind, name string, ops []patchOp) *secError {
	b.mu.Lock("SecurityPatch")
	defer b.mu.Unlock()

	if !b.domains.Has(domain) {
		return secDomainMissing()
	}

	cfg := b.ensureSecurityConfigLocked(domain)

	if name != "" {
		return patchOne(cfg, kind, name, ops)
	}

	return patchCollection(cfg, kind, ops)
}

func patchOne(cfg *SecurityConfig, kind securityKind, name string, ops []patchOp) *secError {
	if err := guardReadOnly(cfg, kind, name); err != nil {
		return err
	}

	doc, ok := storedDoc(cfg, kind, name)
	if !ok {
		return secNotFound(name)
	}

	patched, err := applyPatchOps(doc, ops)
	if err != nil {
		return secBadRequest(err.Error())
	}

	m, isMap := patched.(map[string]any)
	if !isMap {
		return secBadRequest("Patched resource must be an object.")
	}

	_, perr := putResource(cfg, kind, name, docToBody(m))

	return perr
}

func patchCollection(cfg *SecurityConfig, kind securityKind, ops []patchOp) *secError {
	collection := map[string]any{}

	for _, n := range securityNames(cfg, kind) {
		if d, ok := storedDoc(cfg, kind, n); ok {
			collection[n] = d
		}
	}

	var working any

	if raw, merr := json.Marshal(collection); merr != nil || json.Unmarshal(raw, &working) != nil {
		return secBadRequest("Could not read the current resources.")
	}

	patched, err := applyPatchOps(working, ops)
	if err != nil {
		return secBadRequest(err.Error())
	}

	next, ok := patched.(map[string]any)
	if !ok {
		return secBadRequest("Patched collection must be an object.")
	}

	if gerr := removeMissing(cfg, kind, collection, next); gerr != nil {
		return gerr
	}

	return upsertChanged(cfg, kind, collection, next)
}

func removeMissing(cfg *SecurityConfig, kind securityKind, before, after map[string]any) *secError {
	for n := range before {
		if _, still := after[n]; still {
			continue
		}

		if gerr := guardReadOnly(cfg, kind, n); gerr != nil {
			return gerr
		}

		deleteResource(cfg, kind, n)
	}

	return nil
}

func upsertChanged(cfg *SecurityConfig, kind securityKind, before, after map[string]any) *secError {
	for n, v := range after {
		doc, isMap := v.(map[string]any)
		if !isMap {
			return secBadRequest("Patched resources must be objects.")
		}

		if existing, had := before[n]; had && jsonEqual(existing, doc) {
			continue
		}

		if gerr := guardReadOnly(cfg, kind, n); gerr != nil {
			return gerr
		}

		if _, perr := putResource(cfg, kind, n, docToBody(doc)); perr != nil {
			return perr
		}
	}

	return nil
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)

	return string(x) == string(y)
}

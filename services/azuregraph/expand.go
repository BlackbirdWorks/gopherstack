package azuregraph

import (
	"slices"
	"strconv"
	"strings"
)

// maxExpanded is the number of related objects Graph returns for an expanded navigation property.
const maxExpanded = 20

type expandSpec struct {
	name    string
	selects []string
	levels  int
}

const (
	maxLevels = 1000
	levelsMax = -1
)

// parseExpand reads $expand=owners,members($select=id,displayName).
func parseExpand(spec string) ([]expandSpec, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}

	var (
		out   []expandSpec
		depth int
		start int
	)

	parts := []string{}

	for i, r := range spec {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, spec[start:i])
				start = i + 1
			}
		}
	}

	parts = append(parts, spec[start:])

	for _, p := range parts {
		name, opts, nested := strings.Cut(strings.TrimSpace(p), "(")
		e := expandSpec{name: name}

		if nested {
			opts = strings.TrimSuffix(opts, ")")

			if err := e.parseOptions(opts); err != nil {
				return nil, err
			}
		}

		out = append(out, e)
	}

	return out, nil
}

func (e *expandSpec) parseOptions(opts string) error {
	for opt := range strings.SplitSeq(opts, ";") {
		switch {
		case strings.HasPrefix(opt, "$select="):
			e.selects = strings.Split(strings.TrimPrefix(opt, "$select="), ",")
		case strings.HasPrefix(opt, "$levels="):
			v := strings.TrimPrefix(opt, "$levels=")

			n, err := strconv.Atoi(v)

			switch {
			case v == "max":
				e.levels = levelsMax
			case err != nil || n < 1 || n > maxLevels:
				return badRequest("Invalid value '%s' for the $levels option; use max or a number between 1 and %d.",
					v, maxLevels)
			default:
				e.levels = n
			}
		default:
			return unsupported("Only $select and $levels are supported inside $expand.")
		}
	}

	return nil
}

func kindOfResource(resource string) string {
	switch resource {
	case typeApplication:
		return kindApplication
	case "ServicePrincipal":
		return kindServicePrincipal
	case typeUser:
		return kindUser
	case "Group":
		return kindGroup
	}

	return ""
}

func (b *Backend) expandRelation(name, id, kind string) ([]Object, bool, error) {
	allowed := func(kinds ...string) bool { return slices.Contains(kinds, kind) }

	switch strings.ToLower(name) {
	case propOwners:
		if allowed(kindApplication, kindServicePrincipal, kindGroup) {
			objs, err := b.Owners(id)

			return objs, true, err
		}
	case propMembers:
		if allowed(kindGroup) {
			objs, err := b.Members(id)

			return objs, true, err
		}
	case "directreports":
		if allowed(kindUser) {
			objs, err := b.DirectReports(id)

			return objs, true, err
		}
	case "transitivemembers":
		if allowed(kindGroup) {
			objs, err := b.TransitiveMembers(id)

			return objs, true, err
		}
	case "memberof", "transitivememberof":
		if allowed(kindUser, kindGroup, kindServicePrincipal) {
			objs, err := b.MemberOf(id, strings.EqualFold(name, "transitiveMemberOf"))

			return objs, true, err
		}
	}

	return nil, false, nil
}

// expandAll adds the requested navigation properties to rendered entities of resource.
func (h *Handler) expandAll(q *listQuery, resource string, items ...map[string]any) error {
	if len(q.expand) == 0 {
		return nil
	}

	kind := kindOfResource(resource)
	if kind == "" {
		return unsupported("Expanding relationships with $expand is not supported on this resource.")
	}

	for _, e := range q.expand {
		if strings.EqualFold(e.name, propManager) {
			if err := h.expandManager(q, e, kind, items); err != nil {
				return err
			}

			continue
		}

		for _, it := range items {
			if err := h.expandRelationOn(e, kind, it); err != nil {
				return err
			}
		}
	}

	return nil
}

func (h *Handler) expandRelationOn(e expandSpec, kind string, it map[string]any) error {
	id, _ := it["id"].(string)

	objs, ok, err := h.Backend.expandRelation(e.name, id, kind)
	if !ok {
		return badRequest("Property '%s' on entity type 'microsoft.graph.%s' is not a valid expandable "+
			"navigation property.", e.name, kind)
	}

	if err != nil {
		return err
	}

	objs = objs[:min(len(objs), maxExpanded)]
	rendered := typedObjects(objs)

	if len(e.selects) > 0 {
		sub := &listQuery{selects: e.selects}
		for i := range rendered {
			rendered[i] = sub.project(rendered[i])
		}
	}

	it[e.name] = rendered

	return nil
}

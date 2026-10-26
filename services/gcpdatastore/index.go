package gcpdatastore

import (
	"slices"
	"strings"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
)

// indexSignature is the composite index a query needs.
type indexSignature struct {
	eq       []string
	sorts    []order
	ancestor bool
}

// signature collects the equality properties and the sort list a query's index must cover.
func (p *queryPlan) signature() (indexSignature, int) {
	sig := indexSignature{ancestor: p.ancestor != nil}

	for n := range p.eqProps {
		sig.eq = append(sig.eq, n)
	}

	slices.Sort(sig.eq)

	for _, o := range p.explicit {
		_, eq := p.eqProps[o.name]
		if (eq && o.name != keyProperty) || (o.name == keyProperty && !o.desc) {
			continue
		}

		sig.sorts = append(sig.sorts, o)
	}

	explicit := len(sig.sorts)

	if explicit == 0 {
		for _, n := range p.ineq {
			sig.sorts = append(sig.sorts, order{name: n})
		}
	}

	return sig, explicit
}

// compositeNeeded applies the documented rules for which queries built-in indexes cannot serve.
func (p *queryPlan) compositeNeeded() (indexSignature, bool) {
	sig, explicit := p.signature()

	hasEq := len(sig.eq) > 0
	hasIneq := len(p.ineq) > 0
	onIneq := explicit == 1 && len(p.ineq) == 1 && sig.sorts[0].name == p.ineq[0] && !sig.sorts[0].desc
	descKey := slices.ContainsFunc(sig.sorts, func(o order) bool { return o.name == keyProperty && o.desc })

	switch {
	case p.ancestor != nil && hasIneq, hasIneq && hasEq, descKey, explicit > 1:
		return sig, true
	case explicit == 1 && !onIneq:
		return sig, hasEq || hasIneq || p.ancestor != nil
	default:
		return sig, false
	}
}

func (b *InMemoryBackend) checkIndex(p *queryPlan) error {
	if !b.enforceIndexes || isReserved(p.kind) {
		return nil
	}

	sig, need := p.compositeNeeded()
	if !need {
		return nil
	}

	for _, idx := range b.indexes {
		if idx.GetProjectId() == p.part.project && idx.GetKind() == p.kind && indexServes(idx, sig) {
			return nil
		}
	}

	return errPrecondition("no matching index found. recommended index is:\n%s", recommendedIndex(p.kind, sig))
}

func indexServes(idx *adminpb.Index, sig indexSignature) bool {
	if sig.ancestor && idx.GetAncestor() != adminpb.Index_ALL_ANCESTORS {
		return false
	}

	var rest []order

	eqSeen := map[string]struct{}{}

	for _, ip := range idx.GetProperties() {
		if slices.Contains(sig.eq, ip.GetName()) {
			eqSeen[ip.GetName()] = struct{}{}

			continue
		}

		rest = append(rest, order{name: ip.GetName(), desc: ip.GetDirection() == adminpb.Index_DESCENDING})
	}

	if len(eqSeen) != len(sig.eq) || len(rest) != len(sig.sorts) {
		return false
	}

	for i, o := range rest {
		if o != sig.sorts[i] {
			return false
		}
	}

	return true
}

func recommendedIndex(kind string, sig indexSignature) string {
	var sb strings.Builder

	sb.WriteString("- kind: " + kind + "\n")

	if sig.ancestor {
		sb.WriteString("  ancestor: yes\n")
	}

	sb.WriteString("  properties:\n")

	for _, n := range sig.eq {
		sb.WriteString("  - name: " + n + "\n")
	}

	for _, o := range sig.sorts {
		sb.WriteString("  - name: " + o.name + "\n")

		if o.desc {
			sb.WriteString("    direction: desc\n")
		}
	}

	return sb.String()
}

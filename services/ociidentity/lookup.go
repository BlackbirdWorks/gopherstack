package ociidentity

import (
	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// CompartmentActive reports whether id is the tenancy or an ACTIVE compartment.
func (b *InMemoryBackend) CompartmentActive(id string) bool {
	b.mu.RLock("CompartmentActive")
	defer b.mu.RUnlock()

	if id == TenancyID {
		return true
	}

	c, ok := b.compartment[id]

	return ok && c.LifecycleState == stateActive
}

// CompartmentWithin reports whether id equals ancestor or is nested below it; the tenancy contains everything.
func (b *InMemoryBackend) CompartmentWithin(id, ancestor string) bool {
	b.mu.RLock("CompartmentWithin")
	defer b.mu.RUnlock()

	if ancestor == TenancyID {
		return true
	}

	for hops := 0; id != "" && hops <= maxCompartmentHops; hops++ {
		if id == ancestor {
			return true
		}

		c, ok := b.compartment[id]
		if !ok {
			return false
		}

		id = c.CompartmentID
	}

	return false
}

const maxCompartmentHops = 64

// TagDefinition resolves a defined tag namespace and key (both case-insensitive) for tag validation in other services.
func (b *InMemoryBackend) TagDefinition(namespace, key string) (ocitags.Def, bool) {
	b.mu.RLock("TagDefinition")
	defer b.mu.RUnlock()

	for _, n := range b.tagNS {
		if n.LifecycleState == stateDeleted || !strs.Equal(n.Name, namespace) {
			continue
		}

		for _, d := range b.tagDefs {
			if d.NamespaceID == n.ID && d.LifecycleState != stateDeleted && strs.Equal(d.Name, key) {
				return tagDef(n, d), true
			}
		}
	}

	return ocitags.Def{}, false
}

func tagDef(n *TagNamespace, d *TagDef) ocitags.Def {
	def := ocitags.Def{Retired: n.IsRetired || d.IsRetired}

	if kind, _ := d.Validator["validatorType"].(string); strs.Equal(kind, "ENUM") {
		def.HasEnum = true

		vals, _ := d.Validator["values"].([]any)
		for _, v := range vals {
			if s, ok := v.(string); ok {
				def.EnumValues = append(def.EnumValues, s)
			}
		}
	}

	return def
}

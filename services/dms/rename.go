package dms

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// rekey moves v to newName in t (the table keys on the name), rejecting a
// name already in use. A blank or unchanged newName is a no-op. Caller holds b.mu.
func rekey[V any](
	t *store.Table[V], region, oldName, newName, kind string, v *V, setName func(string),
) error {
	if newName == "" || newName == oldName {
		return nil
	}

	if t.Has(regionKey(region, newName)) {
		return fmt.Errorf("%w: %s %s already exists", ErrAlreadyExists, kind, newName)
	}

	t.Delete(regionKey(region, oldName))
	setName(newName)
	t.Put(v)

	return nil
}

package ociobjectstorage

import "slices"

// HoldsInCompartment names a bucket held in the compartment, or returns "".
func (b *InMemoryBackend) HoldsInCompartment(compartmentID string) string {
	b.mu.RLock("HoldsInCompartment")
	defer b.mu.RUnlock()

	var names []string

	for _, bk := range b.buckets {
		if bk.CompartmentID == compartmentID {
			names = append(names, bk.Name)
		}
	}

	if len(names) == 0 {
		return ""
	}

	return "Object Storage bucket " + slices.Min(names)
}

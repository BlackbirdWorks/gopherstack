package gcpidentitytoolkit

import "slices"

// ProjectIDs lists the project IDs with stored state, always including the default project.
func (b *InMemoryBackend) ProjectIDs() []string {
	b.mu.RLock("ProjectIDs")
	defer b.mu.RUnlock()

	ids := []string{b.defaultProject}

	for id := range b.projects {
		if id != b.defaultProject {
			ids = append(ids, id)
		}
	}

	slices.Sort(ids)

	return ids
}

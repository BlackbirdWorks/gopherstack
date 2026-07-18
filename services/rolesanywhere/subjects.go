package rolesanywhere

import "context"

// GetSubject returns a subject by ID.
func (b *InMemoryBackend) GetSubject(ctx context.Context, id string) (*Subject, error) {
	b.mu.RLock("GetSubject")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	s, exists := b.subjects.Get(regionKey(region, id))
	if !exists {
		return nil, ErrSubjectNotFound
	}

	return copySubject(s), nil
}

// ListSubjects returns all subjects with optional pagination.
func (b *InMemoryBackend) ListSubjects(
	ctx context.Context,
	pageToken string,
	maxResults int,
) ([]*Subject, string, error) {
	b.mu.RLock("ListSubjects")
	defer b.mu.RUnlock()

	items, token := listByRegionIndex(
		b.subjectsByRegion,
		getRegion(ctx, b.defaultRegion),
		copySubject,
		func(s *Subject) string { return s.SubjectID },
		func(s *Subject) string { return s.SubjectID },
		pageToken,
		maxResults,
	)

	return items, token, nil
}

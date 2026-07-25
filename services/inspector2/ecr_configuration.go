package inspector2

// GetClustersForImage returns the ECS/EKS clusters running the given ECR
// image resource. Real GetClustersForImageInput's "filter.resourceId" member
// is required (ValidationException if empty); gopherstack has no ECS/EKS
// cluster-membership tracking to join against, so a validated request
// always returns an empty (but correctly-shaped) cluster list rather than
// fabricating cluster ARNs.
func (b *InMemoryBackend) GetClustersForImage(resourceID string) (map[string]any, error) {
	if resourceID == "" {
		return nil, ErrValidation
	}

	return map[string]any{
		"cluster": []any{},
	}, nil
}

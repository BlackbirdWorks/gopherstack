package inspector2

// GetClustersForImage returns clusters associated with a container image (stub).
func (b *InMemoryBackend) GetClustersForImage(_ map[string]any) (map[string]any, error) {
	return map[string]any{
		"clusters": []any{},
	}, nil
}

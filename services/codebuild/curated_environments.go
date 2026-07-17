package codebuild

// ListCuratedEnvironmentImages returns a minimal hardcoded list of curated images.
func (b *InMemoryBackend) ListCuratedEnvironmentImages() []map[string]any {
	return []map[string]any{
		{
			"platform": "UBUNTU",
			"languages": []map[string]any{
				{
					"language": "PYTHON",
					"images": []map[string]any{
						{"name": "aws/codebuild/standard:7.0"},
					},
				},
			},
		},
	}
}

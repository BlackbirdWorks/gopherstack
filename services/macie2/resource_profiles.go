package macie2

// GetResourceProfile returns a bucket's sensitivity profile.
func (b *InMemoryBackend) GetResourceProfile(resourceARN string) (*ResourceProfile, error) {
	b.mu.RLock("GetResourceProfile")
	defer b.mu.RUnlock()

	if p, ok := b.resourceProfiles.Get(resourceARN); ok {
		cp := *p

		return &cp, nil
	}

	return &ResourceProfile{
		ResourceArn:      resourceARN,
		SensitivityScore: 0,
		Statistics:       &ResourceStatistics{},
	}, nil
}

// UpdateResourceProfile sets a bucket's sensitivity score override.
func (b *InMemoryBackend) UpdateResourceProfile(resourceARN string, sensitivityScore int32) error {
	b.mu.Lock("UpdateResourceProfile")
	defer b.mu.Unlock()

	if p, ok := b.resourceProfiles.Get(resourceARN); ok {
		p.SensitivityScore = sensitivityScore
		p.SensitivityScoreOverride = true
	} else {
		b.resourceProfiles.Put(&ResourceProfile{
			ResourceArn:              resourceARN,
			SensitivityScore:         sensitivityScore,
			SensitivityScoreOverride: true,
			Statistics:               &ResourceStatistics{},
		})
	}

	return nil
}

// ListResourceProfileArtifacts returns classified artifacts for a bucket (always empty).
func (b *InMemoryBackend) ListResourceProfileArtifacts(_ string) ([]ResourceProfileArtifact, error) {
	return []ResourceProfileArtifact{}, nil
}

// ListResourceProfileDetections returns data identifier detections for a bucket.
func (b *InMemoryBackend) ListResourceProfileDetections(resourceARN string) ([]ResourceProfileDetection, error) {
	b.mu.RLock("ListResourceProfileDetections")
	defer b.mu.RUnlock()

	detections := b.resourceDetections[resourceARN]
	if len(detections) == 0 {
		return []ResourceProfileDetection{}, nil
	}

	cp := make([]ResourceProfileDetection, len(detections))
	copy(cp, detections)

	return cp, nil
}

// UpdateResourceProfileDetections updates suppression status of detections.
func (b *InMemoryBackend) UpdateResourceProfileDetections(
	resourceARN string, suppressDataIdentifiers []map[string]any,
) error {
	b.mu.Lock("UpdateResourceProfileDetections")
	defer b.mu.Unlock()

	suppress := make(map[string]bool, len(suppressDataIdentifiers))
	for _, s := range suppressDataIdentifiers {
		if id, ok := s["id"].(string); ok {
			suppress[id] = true
		}
	}

	for i := range b.resourceDetections[resourceARN] {
		d := &b.resourceDetections[resourceARN][i]
		if suppress[d.ID] {
			d.Suppressed = true
		}
	}

	return nil
}

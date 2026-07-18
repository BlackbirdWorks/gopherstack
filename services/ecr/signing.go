package ecr

import (
	"context"
	"fmt"
)

// GetSigningConfiguration returns the current registry signing configuration.
func (b *InMemoryBackend) GetSigningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
) (*SigningSettings, error) {
	b.mu.RLock("GetSigningConfiguration")
	defer b.mu.RUnlock()

	return copySigningSettings(b.signingConfig), nil
}

// PutSigningConfiguration updates the registry signing configuration.
func (b *InMemoryBackend) PutSigningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	settings *SigningSettings,
) (*SigningSettings, error) {
	b.mu.Lock("PutSigningConfiguration")
	defer b.mu.Unlock()

	b.signingConfig = copySigningSettings(settings)

	return copySigningSettings(b.signingConfig), nil
}

// DeleteSigningConfiguration removes the registry signing configuration.
func (b *InMemoryBackend) DeleteSigningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
) (*SigningSettings, error) {
	b.mu.Lock("DeleteSigningConfiguration")
	defer b.mu.Unlock()

	settings := copySigningSettings(b.signingConfig)
	b.signingConfig = nil

	return settings, nil
}

// DescribeImageSigningStatus returns signing status for an image.
func (b *InMemoryBackend) DescribeImageSigningStatus(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageID ImageIdentifier,
) (*ImageSigningStatusResult, error) {
	b.mu.RLock("DescribeImageSigningStatus")
	defer b.mu.RUnlock()

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	if _, ok := findImageLocked(b.images, b.imagesByRepo, repositoryName, b.tagIndex[repositoryName], imageID); !ok {
		return nil, fmt.Errorf("%w: image not found", ErrRepositoryNotFound)
	}

	out := []ImageSigningStatusRecord{{Status: scanStatusComplete}}
	if b.signingConfig != nil && len(b.signingConfig.Rules) > 0 {
		out[0].SigningProfileArn = b.signingConfig.Rules[0].SigningProfileArn
	}

	return &ImageSigningStatusResult{
		ImageID:         imageID,
		RegistryID:      b.accountID,
		RepositoryName:  repositoryName,
		SigningStatuses: out,
	}, nil
}

func copySigningSettings(in *SigningSettings) *SigningSettings {
	if in == nil {
		return &SigningSettings{}
	}

	out := &SigningSettings{Rules: make([]SigningRule, len(in.Rules))}
	for i, rule := range in.Rules {
		out.Rules[i] = SigningRule{
			SigningProfileArn: rule.SigningProfileArn,
			RepositoryFilters: append([]RepositoryFilter(nil), rule.RepositoryFilters...),
		}
	}

	return out
}

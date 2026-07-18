package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// newInferenceProfileID generates a unique inference profile ID.
func (b *InMemoryBackend) newInferenceProfileID() string {
	b.inferenceProfileCounter++

	return fmt.Sprintf("ip-%07d", b.inferenceProfileCounter)
}

// CreateInferenceProfile creates a new inference profile.
func (b *InMemoryBackend) CreateInferenceProfile(
	name, description string,
	tags []Tag,
) (*InferenceProfile, error) {
	b.mu.Lock("CreateInferenceProfile")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: inferenceProfileName is required", ErrValidation)
	}

	if _, exists := b.inferenceProfilesByName[name]; exists {
		return nil, fmt.Errorf("%w: inference profile %s already exists", ErrAlreadyExists, name)
	}

	id := b.newInferenceProfileID()
	profileARN := arn.Build("bedrock", b.region, b.accountID, "inference-profile/"+id)
	now := time.Now().UTC()

	profile := &InferenceProfile{
		InferenceProfileArn:  profileARN,
		InferenceProfileID:   id,
		InferenceProfileName: name,
		Description:          description,
		Status:               "ACTIVE",
		Type:                 "APPLICATION",
		CreatedAt:            now,
		UpdatedAt:            now,
		Tags:                 copyTags(tags),
	}
	b.inferenceProfiles.Put(profile)
	b.inferenceProfilesByName[name] = profileARN
	cp := *profile
	cp.Tags = copyTags(profile.Tags)

	return &cp, nil
}

// findInferenceProfileARN resolves a profile ID or name to its ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findInferenceProfileARN(idOrARN string) (string, bool) {
	if _, ok := b.inferenceProfiles.Get(idOrARN); ok {
		return idOrARN, true
	}

	if a := b.inferenceProfilesByName[idOrARN]; a != "" {
		return a, true
	}

	return "", false
}

// GetInferenceProfile returns an inference profile by ARN or name.
func (b *InMemoryBackend) GetInferenceProfile(idOrARN string) (*InferenceProfile, error) {
	b.mu.RLock("GetInferenceProfile")
	defer b.mu.RUnlock()

	profileARN, ok := b.findInferenceProfileARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: inference profile %s not found", ErrNotFound, idOrARN)
	}

	p, _ := b.inferenceProfiles.Get(profileARN)
	cp := *p
	cp.Tags = copyTags(p.Tags)

	return &cp, nil
}

// ListInferenceProfiles returns all inference profiles with optional pagination.
func (b *InMemoryBackend) ListInferenceProfiles(nextToken string) ([]*InferenceProfile, string) {
	b.mu.RLock("ListInferenceProfiles")
	defer b.mu.RUnlock()

	list := make([]*InferenceProfile, 0, b.inferenceProfiles.Len())

	for _, p := range b.inferenceProfiles.All() {
		cp := *p
		cp.Tags = copyTags(p.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].InferenceProfileArn < list[j].InferenceProfileArn
	})

	return paginateBedrockSlice(list, nextToken)
}

// DeleteInferenceProfile removes an inference profile by ARN or name.
func (b *InMemoryBackend) DeleteInferenceProfile(idOrARN string) error {
	b.mu.Lock("DeleteInferenceProfile")
	defer b.mu.Unlock()

	profileARN, ok := b.findInferenceProfileARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: inference profile %s not found", ErrNotFound, idOrARN)
	}

	p, _ := b.inferenceProfiles.Get(profileARN)
	delete(b.inferenceProfilesByName, p.InferenceProfileName)
	b.inferenceProfiles.Delete(profileARN)

	return nil
}

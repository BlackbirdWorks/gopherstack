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

// CreateInferenceProfile creates a new inference profile. modelSource is the
// required ModelSource member (api_op_CreateInferenceProfile.go:48), the
// CopyFrom ARN of the foundation model or system-defined inference profile
// this profile tracks (types.InferenceProfileModelSourceMemberCopyFrom, the
// union's only member).
func (b *InMemoryBackend) CreateInferenceProfile(
	name, description, modelSource string,
	tags []Tag,
) (*InferenceProfile, error) {
	b.mu.Lock("CreateInferenceProfile")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: inferenceProfileName is required", ErrValidation)
	}

	if modelSource == "" {
		return nil, fmt.Errorf("%w: modelSource is required", ErrValidation)
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
		ModelSource:          modelSource,
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

// ListInferenceProfiles returns inference profiles matching typeEquals (real
// query param "type", aws-sdk-go-v2 serializers.go:6752-6754), with optional
// pagination. An empty typeEquals matches every profile.
func (b *InMemoryBackend) ListInferenceProfiles(nextToken, typeEquals string) ([]*InferenceProfile, string) {
	b.mu.RLock("ListInferenceProfiles")
	defer b.mu.RUnlock()

	list := make([]*InferenceProfile, 0, b.inferenceProfiles.Len())

	for _, p := range b.inferenceProfiles.All() {
		if typeEquals != "" && p.Type != typeEquals {
			continue
		}

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

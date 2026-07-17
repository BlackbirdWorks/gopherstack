package emr

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) studioGet(region, id string) (*Studio, bool) {
	return b.studios.Get(regionKey(region, id))
}

func (b *InMemoryBackend) studioPut(v *Studio) { b.studios.Put(v) }

func (b *InMemoryBackend) studioDelete(region, id string) { b.studios.Delete(regionKey(region, id)) }

func (b *InMemoryBackend) studiosInRegion(region string) []*Studio {
	return b.studiosByRegion.Get(region)
}

func (b *InMemoryBackend) studioSessionMappingGet(region, key string) (*StudioSessionMapping, bool) {
	return b.studioSessionMappings.Get(regionKey(region, key))
}

func (b *InMemoryBackend) studioSessionMappingPut(v *StudioSessionMapping) {
	b.studioSessionMappings.Put(v)
}

func (b *InMemoryBackend) studioSessionMappingDelete(region, key string) {
	b.studioSessionMappings.Delete(regionKey(region, key))
}

func (b *InMemoryBackend) studioSessionMappingsInRegion(region string) []*StudioSessionMapping {
	return b.studioSessionMappingsByRegion.Get(region)
}

// DescribeStudio returns an EMR Studio by ID.
func (b *InMemoryBackend) DescribeStudio(ctx context.Context, studioID string) (*Studio, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeStudio")
	defer b.mu.RUnlock()

	studio, ok := b.studioGet(region, studioID)
	if !ok {
		return nil, fmt.Errorf("%w: studio %s not found", ErrNotFound, studioID)
	}

	cp := *studio

	return &cp, nil
}

// ListStudios returns all studios as summaries, sorted by name.
func (b *InMemoryBackend) ListStudios(ctx context.Context, marker string) ([]StudioSummary, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListStudios")
	defer b.mu.RUnlock()

	studios := b.studiosInRegion(region)
	summaries := make([]StudioSummary, 0, len(studios))

	for _, s := range studios {
		summaries = append(summaries, StudioSummary{
			StudioID:          s.StudioID,
			StudioArn:         s.StudioArn,
			Name:              s.Name,
			VpcID:             s.VpcID,
			DefaultS3Location: s.DefaultS3Location,
			AuthMode:          s.AuthMode,
			URL:               s.URL,
			CreationTime:      s.CreationTime,
			Description:       s.Description,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})

	p := page.New(summaries, marker, listStudiosPageSize, listStudiosPageSize)

	return p.Data, p.Next
}

// UpdateStudio updates mutable fields on an EMR Studio.
func (b *InMemoryBackend) UpdateStudio(
	ctx context.Context,
	studioID, name, description, defaultS3Location, subnetIDsJSON string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateStudio")
	defer b.mu.Unlock()

	studio, ok := b.studioGet(region, studioID)
	if !ok {
		return fmt.Errorf("%w: studio %s not found", ErrNotFound, studioID)
	}

	if name != "" {
		studio.Name = name
	}

	if description != "" {
		studio.Description = description
	}

	if defaultS3Location != "" {
		studio.DefaultS3Location = defaultS3Location
	}

	_ = subnetIDsJSON

	return nil
}

// GetStudioSessionMapping returns a session mapping for a studio.
func (b *InMemoryBackend) GetStudioSessionMapping(
	ctx context.Context,
	studioID, identityType, identityID, identityName string,
) (*StudioSessionMapping, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetStudioSessionMapping")
	defer b.mu.RUnlock()

	key := studioSessionKey(studioID, identityType, identityID, identityName)

	mapping, ok := b.studioSessionMappingGet(region, key)
	if !ok {
		return nil, fmt.Errorf("%w: session mapping not found for studio %s", ErrNotFound, studioID)
	}

	cp := *mapping

	return &cp, nil
}

// ListStudioSessionMappings returns session mappings for a studio, optionally filtered by identity type.
func (b *InMemoryBackend) ListStudioSessionMappings(
	ctx context.Context,
	studioID, identityType string,
) []StudioSessionMapping {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListStudioSessionMappings")
	defer b.mu.RUnlock()

	result := make([]StudioSessionMapping, 0)

	for _, m := range b.studioSessionMappingsInRegion(region) {
		if m.StudioID != studioID {
			continue
		}

		if identityType != "" && m.IdentityType != identityType {
			continue
		}

		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].IdentityID < result[j].IdentityID
	})

	return result
}

// UpdateStudioSessionMapping changes the SessionPolicyArn on a mapping.
func (b *InMemoryBackend) UpdateStudioSessionMapping(
	ctx context.Context,
	studioID, identityType, identityID, identityName, sessionPolicyArn string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateStudioSessionMapping")
	defer b.mu.Unlock()

	key := studioSessionKey(studioID, identityType, identityID, identityName)

	mapping, ok := b.studioSessionMappingGet(region, key)
	if !ok {
		return fmt.Errorf("%w: session mapping not found for studio %s", ErrNotFound, studioID)
	}

	mapping.SessionPolicyArn = sessionPolicyArn
	mapping.LastModifiedTime = awstime.Epoch(time.Now())

	return nil
}

// CreateStudio creates a new EMR Studio.
func (b *InMemoryBackend) CreateStudio(
	ctx context.Context,
	name, authMode, defaultS3Location, engineSGID, serviceRole, vpcID, workspaceSGID string,
	subnetIDs []string, tags []Tag,
) (*Studio, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateStudio")
	defer b.mu.Unlock()

	for _, s := range b.studiosInRegion(region) {
		if s.Name == name {
			return nil, fmt.Errorf("%w: studio with name %s already exists", ErrAlreadyExists, name)
		}
	}

	id := b.nextStudioID()
	studioARN := arn.Build("elasticmapreduce", region, b.accountID, "studio/"+id)

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	subnetCopy := make([]string, len(subnetIDs))
	copy(subnetCopy, subnetIDs)

	studio := &Studio{
		StudioID:                 id,
		StudioArn:                studioARN,
		Name:                     name,
		AuthMode:                 authMode,
		DefaultS3Location:        defaultS3Location,
		EngineSecurityGroupID:    engineSGID,
		ServiceRole:              serviceRole,
		VpcID:                    vpcID,
		WorkspaceSecurityGroupID: workspaceSGID,
		SubnetIDs:                subnetCopy,
		Tags:                     tagsCopy,
		CreationTime:             awstime.Epoch(time.Now()),
		URL:                      "https://studio." + id + ".emrstudio-prod." + region + ".amazonaws.com",
		region:                   region,
	}

	b.studioPut(studio)

	cp := *studio

	return &cp, nil
}

// DeleteStudio deletes an EMR Studio by ID.
func (b *InMemoryBackend) DeleteStudio(ctx context.Context, studioID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteStudio")
	defer b.mu.Unlock()

	if _, ok := b.studioGet(region, studioID); !ok {
		return fmt.Errorf("%w: studio %s not found", ErrNotFound, studioID)
	}

	b.studioDelete(region, studioID)

	// Clone before deleting: studioSessionMappingDelete mutates the byRegion
	// index in place, which would otherwise invalidate this range mid-loop
	// (same rationale as services/neptune's cluster-cleanup loops).
	for _, m := range slices.Clone(b.studioSessionMappingsInRegion(region)) {
		if m.StudioID == studioID {
			b.studioSessionMappingDelete(
				region, studioSessionKey(m.StudioID, m.IdentityType, m.IdentityID, m.IdentityName),
			)
		}
	}

	return nil
}

// studioSessionKey returns the composite key for a session mapping.
func studioSessionKey(studioID, identityType, identityID, identityName string) string {
	if identityID != "" {
		return studioID + "|" + identityType + "|id:" + identityID
	}

	return studioID + "|" + identityType + "|name:" + identityName
}

// CreateStudioSessionMapping maps a user or group to an EMR Studio.
func (b *InMemoryBackend) CreateStudioSessionMapping(
	ctx context.Context,
	studioID, identityType, identityID, identityName, sessionPolicyArn string,
) error {
	if studioID == "" {
		return fmt.Errorf("%w: StudioId is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateStudioSessionMapping")
	defer b.mu.Unlock()

	if _, ok := b.studioGet(region, studioID); !ok {
		return fmt.Errorf("%w: studio %s not found", ErrNotFound, studioID)
	}

	nowEpoch := awstime.Epoch(time.Now())

	b.studioSessionMappingPut(&StudioSessionMapping{
		StudioID:         studioID,
		IdentityType:     identityType,
		IdentityID:       identityID,
		IdentityName:     identityName,
		SessionPolicyArn: sessionPolicyArn,
		CreationTime:     nowEpoch,
		LastModifiedTime: nowEpoch,
		region:           region,
	})

	return nil
}

// DeleteStudioSessionMapping removes a user or group from an EMR Studio.
func (b *InMemoryBackend) DeleteStudioSessionMapping(
	ctx context.Context,
	studioID, identityType, identityID, identityName string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteStudioSessionMapping")
	defer b.mu.Unlock()

	key := studioSessionKey(studioID, identityType, identityID, identityName)
	if _, ok := b.studioSessionMappingGet(region, key); !ok {
		return fmt.Errorf("%w: session mapping not found for studio %s", ErrNotFound, studioID)
	}

	b.studioSessionMappingDelete(region, key)

	return nil
}

// AddStudioInternal seeds a studio directly into the backend for testing.
func (b *InMemoryBackend) AddStudioInternal(ctx context.Context, studio Studio) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddStudioInternal")
	defer b.mu.Unlock()

	cp := studio
	cp.region = region
	b.studioPut(&cp)
}

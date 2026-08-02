package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file backs family M (13 ops, all under /network-migration/):
// CreateNetworkMigrationDefinition, GetNetworkMigrationDefinition,
// UpdateNetworkMigrationDefinition, DeleteNetworkMigrationDefinition,
// ListNetworkMigrationDefinitions, GetNetworkMigrationMapperSegmentConstruct,
// ListNetworkMigrationMapperSegmentConstructs,
// ListNetworkMigrationMapperSegments, UpdateNetworkMigrationMapperSegment,
// ListNetworkMigrationMappings, ListNetworkMigrationMappingUpdates,
// StartNetworkMigrationMapping, StartNetworkMigrationMappingUpdate.
//
// # Mapper segments/constructs are never populated
//
// No op in this 95-op surface creates a NetworkMigrationMapperSegment or
// NetworkMigrationMapperSegmentConstruct either -- they are conceptually
// produced by the (unbuildable, per PARITY.md) network-analysis engine as a
// side effect of a MAPPING job succeeding. Unlike SourceServer/VcenterClient
// (which this package resolves with an explicit SeedX non-SDK convenience,
// since they back the primary 70-op replication surface this service exists
// to emulate), mapper segments back only bookkeeping display within the
// already-honest-gapped Network Migration analysis sub-feature -- this
// package deliberately takes the OTHER option this task's instructions
// explicitly weigh: "leave the families genuinely empty and record it as a
// gap," rather than adding a second synthetic seeding seam whose payoff is
// far smaller. ListNetworkMigrationMapperSegments/
// ListNetworkMigrationMapperSegmentConstructs therefore always return empty
// (after validating the (definition, execution) scope exists);
// GetNetworkMigrationMapperSegmentConstruct/UpdateNetworkMigrationMapperSegment
// always 404, since no segment ever exists to address.

// CreateNetworkMigrationDefinitionInput mirrors
// CreateNetworkMigrationDefinitionInput.
type CreateNetworkMigrationDefinitionInput struct {
	TargetNetwork         *TargetNetwork
	TargetS3Configuration *TargetS3Configuration
	ScopeTags             map[string]string
	Tags                  map[string]string
	Name                  string
	Description           string
	TargetDeployment      string
	SourceConfigurations  []SourceConfiguration
}

// CreateNetworkMigrationDefinition creates a new NetworkMigrationDefinition.
func (b *InMemoryBackend) CreateNetworkMigrationDefinition(
	in CreateNetworkMigrationDefinitionInput,
) (*NetworkMigrationDefinition, error) {
	b.mu.Lock("CreateNetworkMigrationDefinition")
	defer b.mu.Unlock()

	if in.Name == "" {
		return nil, validationError("name is required")
	}

	if in.TargetNetwork == nil {
		return nil, validationError("targetNetwork is required")
	}

	if in.TargetS3Configuration == nil {
		return nil, validationError("targetS3Configuration is required")
	}

	id := newNMDefinitionID()
	now := nowUTC()
	t := tags.New("mgn.nmdefinition." + id + ".tags")
	t.Merge(in.Tags)

	d := &NetworkMigrationDefinition{
		NetworkMigrationDefinitionID: id,
		Arn:                          b.nmDefinitionARN(id),
		Name:                         in.Name,
		Description:                  in.Description,
		TargetNetwork:                in.TargetNetwork,
		TargetS3Configuration:        in.TargetS3Configuration,
		ScopeTags:                    cloneStrMap(in.ScopeTags),
		SourceConfigurations:         append([]SourceConfiguration(nil), in.SourceConfigurations...),
		TargetDeployment:             in.TargetDeployment,
		Tags:                         t,
		CreatedAt:                    now,
		UpdatedAt:                    now,
	}
	b.nmDefinitions.Put(d)

	return d.clone(), nil
}

// GetNetworkMigrationDefinition returns the full detail of a
// NetworkMigrationDefinition.
func (b *InMemoryBackend) GetNetworkMigrationDefinition(id string) (*NetworkMigrationDefinition, error) {
	b.mu.RLock("GetNetworkMigrationDefinition")
	defer b.mu.RUnlock()

	d, ok := b.resolveNMDefinitionLocked(id)
	if !ok {
		return nil, notFoundError(resourceNMDefinition, id)
	}

	return d.clone(), nil
}

// UpdateNetworkMigrationDefinitionInput mirrors
// UpdateNetworkMigrationDefinitionInput -- everything but
// NetworkMigrationDefinitionID is optional.
type UpdateNetworkMigrationDefinitionInput struct {
	TargetNetworkUpdate   *TargetNetwork
	TargetS3Configuration *TargetS3Configuration
	ScopeTags             map[string]string
	Name                  *string
	Description           *string
	TargetDeployment      *string
	SourceConfigurations  []SourceConfiguration
}

// UpdateNetworkMigrationDefinition applies a partial update to id.
func (b *InMemoryBackend) UpdateNetworkMigrationDefinition(
	id string,
	in UpdateNetworkMigrationDefinitionInput,
) (*NetworkMigrationDefinition, error) {
	b.mu.Lock("UpdateNetworkMigrationDefinition")
	defer b.mu.Unlock()

	d, ok := b.resolveNMDefinitionLocked(id)
	if !ok {
		return nil, notFoundError(resourceNMDefinition, id)
	}

	applyNMDefinitionUpdate(d, in)
	d.UpdatedAt = nowUTC()

	return d.clone(), nil
}

func applyNMDefinitionUpdate(d *NetworkMigrationDefinition, in UpdateNetworkMigrationDefinitionInput) {
	if in.Name != nil {
		d.Name = *in.Name
	}

	if in.Description != nil {
		d.Description = *in.Description
	}

	if in.TargetDeployment != nil {
		d.TargetDeployment = *in.TargetDeployment
	}

	if in.TargetNetworkUpdate != nil {
		d.TargetNetwork = in.TargetNetworkUpdate
	}

	if in.TargetS3Configuration != nil {
		d.TargetS3Configuration = in.TargetS3Configuration
	}

	if in.ScopeTags != nil {
		d.ScopeTags = cloneStrMap(in.ScopeTags)
	}

	if in.SourceConfigurations != nil {
		d.SourceConfigurations = append([]SourceConfiguration(nil), in.SourceConfigurations...)
	}
}

// DeleteNetworkMigrationDefinition deletes a NetworkMigrationDefinition.
// Rejected (ConflictException) if it still has executions.
func (b *InMemoryBackend) DeleteNetworkMigrationDefinition(id string) error {
	b.mu.Lock("DeleteNetworkMigrationDefinition")
	defer b.mu.Unlock()

	d, ok := b.resolveNMDefinitionLocked(id)
	if !ok {
		return notFoundError(resourceNMDefinition, id)
	}

	if len(b.nmExecutionsByDef.Get(id)) > 0 {
		return conflictErrorWithResource(
			resourceNMDefinition,
			id,
			"network migration definition still has executions: "+id,
		)
	}

	if d.Tags != nil {
		d.Tags.Close()
	}

	b.nmDefinitions.Delete(id)

	return nil
}

// ListNetworkMigrationDefinitionsFilters mirrors
// types.ListNetworkMigrationDefinitionsRequestFilters.
type ListNetworkMigrationDefinitionsFilters struct {
	NetworkMigrationDefinitionIDs []string
}

// ListNetworkMigrationDefinitions returns a page of
// NetworkMigrationDefinitionSummary-shaped entries (converted in
// wire_convert.go) matching f. This is the single op in the entire service
// with a one-member error set (AccessDeniedException alone, PARITY.md
// wire-trap #8) -- it never itself returns ResourceNotFoundException or
// ValidationException, which this method's signature reflects by never
// producing either.
func (b *InMemoryBackend) ListNetworkMigrationDefinitions(
	f ListNetworkMigrationDefinitionsFilters,
	token string,
	limit int,
) page.Page[*NetworkMigrationDefinition] {
	b.mu.RLock("ListNetworkMigrationDefinitions")
	defer b.mu.RUnlock()

	all := b.nmDefinitions.Snapshot()
	filtered := make([]*NetworkMigrationDefinition, 0, len(all))

	for _, d := range all {
		if len(f.NetworkMigrationDefinitionIDs) == 0 ||
			containsStr(f.NetworkMigrationDefinitionIDs, d.NetworkMigrationDefinitionID) {
			filtered = append(filtered, d.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit)
}

// GetNetworkMigrationMapperSegmentConstruct always returns
// ResourceNotFoundException -- see this file's doc comment: no construct is
// ever created.
func (b *InMemoryBackend) GetNetworkMigrationMapperSegmentConstruct(
	definitionID, executionID, segmentID, constructID string,
) error {
	b.mu.RLock("GetNetworkMigrationMapperSegmentConstruct")
	defer b.mu.RUnlock()

	if err := b.requireNMScopeExistsLocked(definitionID, executionID); err != nil {
		return err
	}
	// segmentID is accepted for wire-shape completeness (a required scoping
	// key on the real Input) but, since no segment ever exists (this file's
	// doc comment), it is never independently validated against a stored
	// segment -- matching ListNetworkMigrationMapperSegmentConstructs' own
	// identical treatment below.
	_ = segmentID

	return notFoundError(resourceNMConstruct, constructID)
}

// ListNetworkMigrationMapperSegmentConstructs always returns an empty list
// -- see this file's doc comment.
func (b *InMemoryBackend) ListNetworkMigrationMapperSegmentConstructs(
	definitionID, executionID, segmentID string,
) error {
	if err := b.requireNMScopeExists(definitionID, executionID); err != nil {
		return err
	}

	b.mu.RLock("ListNetworkMigrationMapperSegmentConstructs")
	defer b.mu.RUnlock()

	if _, ok := b.nmDefinitions.Get(definitionID); !ok {
		return notFoundError(resourceNMDefinition, definitionID)
	}
	// segmentID is accepted for wire-shape completeness (a required scoping
	// key on the real Input) but, since no segment ever exists (this file's
	// doc comment), it is never independently validated against a stored
	// segment.
	_ = segmentID

	return nil
}

// ListNetworkMigrationMapperSegments always returns an empty list -- see
// this file's doc comment.
func (b *InMemoryBackend) ListNetworkMigrationMapperSegments(definitionID, executionID string) error {
	return b.requireNMScopeExists(definitionID, executionID)
}

// UpdateNetworkMigrationMapperSegment always returns
// ResourceNotFoundException -- see this file's doc comment: no segment is
// ever created for this to update. The real Input's only mutable field is
// ScopeTags (confirmed by direct SDK read -- PARITY.md's family-M table
// over-stated this as also including TargetAccount, a real correction this
// implementation pass found; see this package's PARITY.md revision notes).
func (b *InMemoryBackend) UpdateNetworkMigrationMapperSegment(definitionID, executionID, segmentID string) error {
	if err := b.requireNMScopeExists(definitionID, executionID); err != nil {
		return err
	}

	return notFoundError(resourceNMSegment, segmentID)
}

// StartNetworkMigrationMapping starts a new mapping job (family M, but
// shares family N's job-bookkeeping engine -- networkmigrationjobs.go).
func (b *InMemoryBackend) StartNetworkMigrationMapping(definitionID, executionID string) (string, error) {
	b.mu.Lock("StartNetworkMigrationMapping")
	defer b.mu.Unlock()

	return b.createAndScheduleNMJobLocked(definitionID, executionID, StageMapping)
}

// StartNetworkMigrationMappingUpdate starts a new mapping-update job. The
// real Input's Constructs/Segments edit-script (OperationUnion of Delete/
// Merge/Split/Update) is accepted but not applied to any stored construct --
// see this file's doc comment: no construct exists to apply it to.
func (b *InMemoryBackend) StartNetworkMigrationMappingUpdate(definitionID, executionID string) (string, error) {
	b.mu.Lock("StartNetworkMigrationMappingUpdate")
	defer b.mu.Unlock()

	return b.createAndScheduleNMJobLocked(definitionID, executionID, StageMappingUpdate)
}

// ListNetworkMigrationMappings returns a page of mapping job details.
func (b *InMemoryBackend) ListNetworkMigrationMappings(
	definitionID, executionID, token string,
	limit int,
) (page.Page[*NetworkMigrationJob], error) {
	return b.listNMJobs(definitionID, executionID, StageMapping, token, limit)
}

// ListNetworkMigrationMappingUpdates returns a page of mapping-update job
// details.
func (b *InMemoryBackend) ListNetworkMigrationMappingUpdates(
	definitionID, executionID, token string,
	limit int,
) (page.Page[*NetworkMigrationJob], error) {
	return b.listNMJobs(definitionID, executionID, StageMappingUpdate, token, limit)
}

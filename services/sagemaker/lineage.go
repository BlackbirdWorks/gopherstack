package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// defaultLineageGroupName is the name of the lineage group SageMaker
// auto-provisions for every account/region (there is no CreateLineageGroup
// operation in the real API).
const defaultLineageGroupName = "sagemaker-default-lineage-group"

var (
	// ErrActionNotFound is returned when an action does not exist.
	ErrActionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrActionAlreadyExists is returned when an action already exists.
	ErrActionAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrArtifactNotFound is returned when a lineage artifact does not exist.
	ErrArtifactNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrArtifactAlreadyExists is returned when a lineage artifact already exists.
	ErrArtifactAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrContextNotFound is returned when a lineage context does not exist.
	ErrContextNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrContextAlreadyExists is returned when a lineage context already exists.
	ErrContextAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrLineageGroupNotFound is returned when a lineage group does not exist.
	ErrLineageGroupNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrLineageGroupPolicyNotFound is returned when a lineage group has no resource policy attached.
	ErrLineageGroupPolicyNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
)

// ArtifactSourceType is the ID and ID type of an artifact source.
type ArtifactSourceType struct {
	SourceIDType string `json:"SourceIdType"`
	Value        string `json:"Value"`
}

// ArtifactSource represents the source of a SageMaker artifact.
type ArtifactSource struct {
	SourceURI   string               `json:"SourceUri"`
	SourceTypes []ArtifactSourceType `json:"SourceTypes,omitempty"`
}

// Artifact represents a SageMaker ML lineage artifact.
type Artifact struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Properties       map[string]string `json:"Properties,omitempty"`
	ArtifactName     string            `json:"ArtifactName,omitempty"`
	ArtifactArn      string            `json:"ArtifactArn"`
	ArtifactType     string            `json:"ArtifactType"`
	Source           ArtifactSource    `json:"Source"`
}

// cloneArtifact returns a deep copy of ar.
func cloneArtifact(ar *Artifact) *Artifact {
	cp := *ar
	cp.Tags = maps.Clone(ar.Tags)
	cp.Properties = maps.Clone(ar.Properties)
	cp.Source.SourceTypes = append([]ArtifactSourceType(nil), ar.Source.SourceTypes...)

	return &cp
}

// ContextSource represents the source of a SageMaker context.
type ContextSource struct {
	SourceURI  string `json:"SourceUri"`
	SourceID   string `json:"SourceId,omitempty"`
	SourceType string `json:"SourceType,omitempty"`
}

// Context represents a SageMaker ML lineage context.
type Context struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Properties       map[string]string `json:"Properties,omitempty"`
	Source           ContextSource     `json:"Source"`
	ContextName      string            `json:"ContextName"`
	ContextArn       string            `json:"ContextArn"`
	ContextType      string            `json:"ContextType"`
	Description      string            `json:"Description,omitempty"`
}

// cloneContext returns a deep copy of c.
func cloneContext(c *Context) *Context {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)
	cp.Properties = maps.Clone(c.Properties)

	return &cp
}

// Vertex represents a lineage entity reached by a QueryLineage traversal.
type Vertex struct {
	Arn         string `json:"Arn"`
	Type        string `json:"Type,omitempty"`
	LineageType string `json:"LineageType,omitempty"`
}

// Edge represents a directed association traversed by QueryLineage.
type Edge struct {
	SourceArn       string `json:"SourceArn"`
	DestinationArn  string `json:"DestinationArn"`
	AssociationType string `json:"AssociationType,omitempty"`
}

func (b *InMemoryBackend) artifactsStore(r string) *store.Table[Artifact] {
	if b.artifacts[r] == nil {
		b.artifacts[r] = store.Register(
			b.registry,
			"artifacts:"+r,
			store.New(func(v *Artifact) string { return v.ArtifactArn }),
		)
	}

	return b.artifacts[r]
}

// artifactsStoreRO returns the region-scoped artifacts table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) artifactsStoreRO(r string) *store.Table[Artifact] {
	if v := b.artifacts[r]; v != nil {
		return v
	}

	return store.New(func(v *Artifact) string { return v.ArtifactArn })
}

func (b *InMemoryBackend) contextsStore(r string) *store.Table[Context] {
	if b.contexts[r] == nil {
		b.contexts[r] = store.Register(
			b.registry,
			"contexts:"+r,
			store.New(func(v *Context) string { return v.ContextName }),
		)
	}

	return b.contexts[r]
}

// contextsStoreRO returns the region-scoped contexts table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) contextsStoreRO(r string) *store.Table[Context] {
	if v := b.contexts[r]; v != nil {
		return v
	}

	return store.New(func(v *Context) string { return v.ContextName })
}

func (b *InMemoryBackend) contextARNIndexStore(r string) map[string]string {
	if b.contextARNIndex[r] == nil {
		b.contextARNIndex[r] = make(map[string]string)
	}

	return b.contextARNIndex[r]
}

// contextARNIndexStoreRO returns the region-scoped contextARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) contextARNIndexStoreRO(r string) map[string]string {
	if v := b.contextARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

// ---------------------------------------------------------------------------
// Backend methods — Artifact
// ---------------------------------------------------------------------------

// CreateArtifact creates a new ML lineage artifact.
func (b *InMemoryBackend) CreateArtifact(
	ctx context.Context,
	name, artifactType string,
	source ArtifactSource,
	properties map[string]string,
	tags map[string]string,
) (*Artifact, error) {
	b.mu.Lock("CreateArtifact")
	defer b.mu.Unlock()

	if artifactType == "" {
		return nil, fmt.Errorf("%w: ArtifactType is required", ErrValidation)
	}

	if source.SourceURI == "" {
		return nil, fmt.Errorf("%w: Source.SourceUri is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	store := b.artifactsStore(region)

	slug := name
	if slug == "" {
		slug = generateID()
	}

	artifactARN := arn.Build("sagemaker", region, b.accountID, "artifact/"+slug)
	if _, ok := store.Get(artifactARN); ok {
		return nil, fmt.Errorf("%w: artifact %q already exists", ErrArtifactAlreadyExists, artifactARN)
	}

	now := time.Now()
	ar := &Artifact{
		ArtifactName:     name,
		ArtifactArn:      artifactARN,
		ArtifactType:     artifactType,
		Source:           source,
		Properties:       maps.Clone(properties),
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	store.Put(ar)

	return cloneArtifact(ar), nil
}

// DescribeArtifact returns an artifact by ARN.
func (b *InMemoryBackend) DescribeArtifact(ctx context.Context, artifactArn string) (*Artifact, error) {
	b.mu.RLock("DescribeArtifact")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	ar, ok := b.artifactsStoreRO(region).Get(artifactArn)
	if !ok {
		return nil, fmt.Errorf("%w: artifact %q not found", ErrArtifactNotFound, artifactArn)
	}

	return cloneArtifact(ar), nil
}

// UpdateArtifact updates an artifact's name and properties.
func (b *InMemoryBackend) UpdateArtifact(
	ctx context.Context,
	artifactArn, name string,
	properties map[string]string,
	propertiesToRemove []string,
) (*Artifact, error) {
	b.mu.Lock("UpdateArtifact")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.artifactsStore(region)

	ar, ok := store.Get(artifactArn)
	if !ok {
		return nil, fmt.Errorf("%w: artifact %q not found", ErrArtifactNotFound, artifactArn)
	}

	if name != "" {
		ar.ArtifactName = name
	}

	if ar.Properties == nil {
		ar.Properties = make(map[string]string)
	}

	maps.Copy(ar.Properties, properties)

	for _, k := range propertiesToRemove {
		delete(ar.Properties, k)
	}

	ar.LastModifiedTime = time.Now()

	return cloneArtifact(ar), nil
}

// DeleteArtifact deletes an artifact by ARN.
func (b *InMemoryBackend) DeleteArtifact(ctx context.Context, artifactArn string) (*Artifact, error) {
	b.mu.Lock("DeleteArtifact")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.artifactsStore(region)

	ar, ok := store.Get(artifactArn)
	if !ok {
		return nil, fmt.Errorf("%w: artifact %q not found", ErrArtifactNotFound, artifactArn)
	}

	cp := cloneArtifact(ar)
	store.Delete(artifactArn)

	return cp, nil
}

// ListArtifacts returns artifacts, optionally filtered by type or source URI.
func (b *InMemoryBackend) ListArtifacts(
	ctx context.Context,
	artifactType, sourceURI, nextToken string,
) ([]*Artifact, string) {
	b.mu.RLock("ListArtifacts")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.artifactsStoreRO(region)

	filtered := make(map[string]*Artifact, store.Len())

	for _, ar := range store.All() {
		if artifactType != "" && ar.ArtifactType != artifactType {
			continue
		}

		if sourceURI != "" && ar.Source.SourceURI != sourceURI {
			continue
		}

		filtered[ar.ArtifactArn] = ar
	}

	return sagemakerListPagedMap(filtered, nextToken, cloneArtifact,
		func(a, b *Artifact) bool { return a.ArtifactArn < b.ArtifactArn })
}

// ---------------------------------------------------------------------------
// Backend methods — Context
// ---------------------------------------------------------------------------

// CreateContext creates a new ML lineage context.
func (b *InMemoryBackend) CreateContext(
	ctx context.Context,
	name, contextType, description string,
	source ContextSource,
	properties map[string]string,
	tags map[string]string,
) (*Context, error) {
	b.mu.Lock("CreateContext")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ContextName is required", ErrValidation)
	}

	if contextType == "" {
		return nil, fmt.Errorf("%w: ContextType is required", ErrValidation)
	}

	if source.SourceURI == "" {
		return nil, fmt.Errorf("%w: Source.SourceUri is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	store := b.contextsStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: context %q already exists", ErrContextAlreadyExists, name)
	}

	contextARN := arn.Build("sagemaker", region, b.accountID, "context/"+name)
	now := time.Now()
	c := &Context{
		ContextName:      name,
		ContextArn:       contextARN,
		ContextType:      contextType,
		Description:      description,
		Source:           source,
		Properties:       maps.Clone(properties),
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	store.Put(c)
	b.contextARNIndexStore(region)[contextARN] = name

	return cloneContext(c), nil
}

// DescribeContext returns a context by name.
func (b *InMemoryBackend) DescribeContext(ctx context.Context, name string) (*Context, error) {
	b.mu.RLock("DescribeContext")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, ok := b.contextsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: context %q not found", ErrContextNotFound, name)
	}

	return cloneContext(c), nil
}

// UpdateContext updates a context's description and properties.
func (b *InMemoryBackend) UpdateContext(
	ctx context.Context,
	name, description string,
	properties map[string]string,
	propertiesToRemove []string,
) (*Context, error) {
	b.mu.Lock("UpdateContext")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.contextsStore(region)

	c, ok := store.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: context %q not found", ErrContextNotFound, name)
	}

	if description != "" {
		c.Description = description
	}

	if c.Properties == nil {
		c.Properties = make(map[string]string)
	}

	maps.Copy(c.Properties, properties)

	for _, k := range propertiesToRemove {
		delete(c.Properties, k)
	}

	c.LastModifiedTime = time.Now()

	return cloneContext(c), nil
}

// DeleteContext deletes a context by name.
func (b *InMemoryBackend) DeleteContext(ctx context.Context, name string) (*Context, error) {
	b.mu.Lock("DeleteContext")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.contextsStore(region)

	c, ok := store.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: context %q not found", ErrContextNotFound, name)
	}

	cp := cloneContext(c)
	store.Delete(name)
	delete(b.contextARNIndexStore(region), c.ContextArn)

	return cp, nil
}

// ListContexts returns contexts, optionally filtered by type or source URI.
func (b *InMemoryBackend) ListContexts(
	ctx context.Context,
	contextType, sourceURI, nextToken string,
) ([]*Context, string) {
	b.mu.RLock("ListContexts")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.contextsStoreRO(region)

	filtered := make(map[string]*Context, store.Len())

	for _, c := range store.All() {
		if contextType != "" && c.ContextType != contextType {
			continue
		}

		if sourceURI != "" && c.Source.SourceURI != sourceURI {
			continue
		}

		filtered[c.ContextName] = c
	}

	return sagemakerListPagedMap(filtered, nextToken, cloneContext,
		func(a, b *Context) bool { return a.ContextName < b.ContextName })
}

// ---------------------------------------------------------------------------
// Backend methods — Action (Describe/Update/Delete/List; Create lives in backend.go)
// ---------------------------------------------------------------------------

// DescribeAction returns an action by name.
func (b *InMemoryBackend) DescribeAction(ctx context.Context, name string) (*Action, error) {
	b.mu.RLock("DescribeAction")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	a, ok := b.actionsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: action %q not found", ErrActionNotFound, name)
	}

	return cloneAction(a), nil
}

// UpdateAction updates an action's description, status and properties.
func (b *InMemoryBackend) UpdateAction(
	ctx context.Context,
	name, description, status string,
	properties map[string]string,
	propertiesToRemove []string,
) (*Action, error) {
	b.mu.Lock("UpdateAction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.actionsStore(region)

	a, ok := store.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: action %q not found", ErrActionNotFound, name)
	}

	if description != "" {
		a.Description = description
	}

	if status != "" {
		a.Status = status
	}

	if a.Properties == nil {
		a.Properties = make(map[string]string)
	}

	maps.Copy(a.Properties, properties)

	for _, k := range propertiesToRemove {
		delete(a.Properties, k)
	}

	a.LastModifiedTime = time.Now()

	return cloneAction(a), nil
}

// DeleteAction deletes an action by name.
func (b *InMemoryBackend) DeleteAction(ctx context.Context, name string) (*Action, error) {
	b.mu.Lock("DeleteAction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.actionsStore(region)

	a, ok := store.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: action %q not found", ErrActionNotFound, name)
	}

	cp := cloneAction(a)
	store.Delete(name)
	delete(b.actionARNIndexStore(region), a.ActionArn)

	return cp, nil
}

// ListActions returns actions, optionally filtered by type or source URI.
func (b *InMemoryBackend) ListActions(
	ctx context.Context,
	actionType, sourceURI, nextToken string,
) ([]*Action, string) {
	b.mu.RLock("ListActions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.actionsStoreRO(region)

	filtered := make(map[string]*Action, store.Len())

	for _, a := range store.All() {
		if actionType != "" && a.ActionType != actionType {
			continue
		}

		if sourceURI != "" && a.Source.SourceURI != sourceURI {
			continue
		}

		filtered[a.ActionName] = a
	}

	return sagemakerListPagedMap(filtered, nextToken, cloneAction,
		func(a, b *Action) bool { return a.ActionName < b.ActionName })
}

// ---------------------------------------------------------------------------
// Backend methods — Association (Delete/List; AddAssociation lives in backend.go)
// ---------------------------------------------------------------------------

// DeleteAssociation removes an association between a source and destination entity.
func (b *InMemoryBackend) DeleteAssociation(ctx context.Context, sourceArn, destinationArn string) error {
	b.mu.Lock("DeleteAssociation")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.associationsStore(region)

	key := associationKey(sourceArn, destinationArn)
	if _, ok := store.Get(key); !ok {
		return fmt.Errorf(
			"%w: association between %s and %s not found",
			ErrAssociationNotFound,
			sourceArn,
			destinationArn,
		)
	}

	store.Delete(key)

	return nil
}

// Enum values for ListAssociations' SortBy (aws-sdk-go-v2/service/sagemaker
// types.SortAssociationsBy).
const (
	sortAssociationsBySourceArn       = "SourceArn"
	sortAssociationsByDestinationArn  = "DestinationArn"
	sortAssociationsBySourceType      = "SourceType"
	sortAssociationsByDestinationType = "DestinationType"
	sortAssociationsByCreationTime    = "CreationTime"
)

// ListAssociationsParams bundles the filter/sort criteria for ListAssociations.
type ListAssociationsParams struct {
	CreatedAfter    *time.Time
	CreatedBefore   *time.Time
	SourceArn       string
	DestinationArn  string
	SourceType      string
	DestinationType string
	AssociationType string
	SortBy          string
	SortOrder       string
	NextToken       string
	MaxResults      int32
}

// ListAssociations returns associations, optionally filtered by source/destination
// ARN, source/destination entity type, association type, and creation-time window,
// sorted per params.SortBy/SortOrder (real default: CreationTime, Descending).
func (b *InMemoryBackend) ListAssociations(
	ctx context.Context, params ListAssociationsParams,
) ([]*Association, string) {
	b.mu.RLock("ListAssociations")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.associationsStoreRO(region)

	needsType := params.SourceType != "" || params.DestinationType != "" ||
		params.SortBy == sortAssociationsBySourceType || params.SortBy == sortAssociationsByDestinationType

	list := make([]*Association, 0, store.Len())

	for _, a := range store.All() {
		if !associationMatchesFilters(a, params) {
			continue
		}

		if needsType && !b.associationMatchesTypeFilters(region, a, params) {
			continue
		}

		list = append(list, cloneAssociation(a))
	}

	sortAssociations(list, region, b, params.SortBy, params.SortOrder)

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// associationMatchesFilters reports whether a passes every ListAssociations
// filter that doesn't require resolving the source/destination entity type
// (SourceArn/DestinationArn/AssociationType/CreatedAfter/CreatedBefore).
func associationMatchesFilters(a *Association, params ListAssociationsParams) bool {
	if params.SourceArn != "" && a.SourceArn != params.SourceArn {
		return false
	}

	if params.DestinationArn != "" && a.DestinationArn != params.DestinationArn {
		return false
	}

	if params.AssociationType != "" && a.AssociationType != params.AssociationType {
		return false
	}

	if params.CreatedAfter != nil && !a.CreationTime.After(*params.CreatedAfter) {
		return false
	}

	if params.CreatedBefore != nil && !a.CreationTime.Before(*params.CreatedBefore) {
		return false
	}

	return true
}

// associationMatchesTypeFilters reports whether a passes the SourceType/
// DestinationType filters, resolving both entity types via lineageEntityLookup.
func (b *InMemoryBackend) associationMatchesTypeFilters(
	region string, a *Association, params ListAssociationsParams,
) bool {
	_, srcType, _, _ := b.lineageEntityLookup(region, a.SourceArn)
	_, dstType, _, _ := b.lineageEntityLookup(region, a.DestinationArn)

	if params.SourceType != "" && srcType != params.SourceType {
		return false
	}

	if params.DestinationType != "" && dstType != params.DestinationType {
		return false
	}

	return true
}

// sortAssociations sorts in place per sortBy (default CreationTime) and
// sortOrder (default Descending, matching real AWS's ListAssociations default).
func sortAssociations(list []*Association, region string, b *InMemoryBackend, sortBy, sortOrder string) {
	desc := !strings.EqualFold(sortOrder, "Ascending")

	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch sortBy {
		case sortAssociationsBySourceArn:
			less = list[i].SourceArn < list[j].SourceArn
		case sortAssociationsByDestinationArn:
			less = list[i].DestinationArn < list[j].DestinationArn
		case sortAssociationsBySourceType:
			_, iType, _, _ := b.lineageEntityLookup(region, list[i].SourceArn)
			_, jType, _, _ := b.lineageEntityLookup(region, list[j].SourceArn)
			less = iType < jType
		case sortAssociationsByDestinationType:
			_, iType, _, _ := b.lineageEntityLookup(region, list[i].DestinationArn)
			_, jType, _, _ := b.lineageEntityLookup(region, list[j].DestinationArn)
			less = iType < jType
		default:
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})
}

// ---------------------------------------------------------------------------
// Backend methods — LineageGroup (read-only; SageMaker auto-provisions one
// default lineage group per account/region, there is no Create/Delete op)
// ---------------------------------------------------------------------------

// DescribeLineageGroup returns the default lineage group if name matches it.
func (b *InMemoryBackend) DescribeLineageGroup(ctx context.Context, name string) (string, time.Time, error) {
	if name != defaultLineageGroupName {
		return "", time.Time{}, fmt.Errorf("%w: lineage group %q not found", ErrLineageGroupNotFound, name)
	}

	region := getRegion(ctx, b.region)
	lineageGroupARN := arn.Build("sagemaker", region, b.accountID, "lineage-group/"+defaultLineageGroupName)

	return lineageGroupARN, b.lifecycleEpoch(), nil
}

// ListLineageGroups returns the account's single auto-provisioned lineage group.
func (b *InMemoryBackend) ListLineageGroups(ctx context.Context) (string, time.Time) {
	region := getRegion(ctx, b.region)
	lineageGroupARN := arn.Build("sagemaker", region, b.accountID, "lineage-group/"+defaultLineageGroupName)

	return lineageGroupARN, b.lifecycleEpoch()
}

// lifecycleEpoch returns a stable creation timestamp for backend-scoped singleton
// resources (there is no explicit create call to timestamp against).
func (b *InMemoryBackend) lifecycleEpoch() time.Time {
	return time.Unix(0, 0).UTC()
}

// GetLineageGroupPolicy returns the resource policy attached to a lineage group.
// nameOrArn may be either the lineage group's name or its ARN. No
// policy-attachment operation exists in this batch, so a lineage group never
// has a policy attached and this always reports not-found for a valid group.
func (b *InMemoryBackend) GetLineageGroupPolicy(ctx context.Context, nameOrArn string) (string, error) {
	region := getRegion(ctx, b.region)
	lineageGroupARN := arn.Build("sagemaker", region, b.accountID, "lineage-group/"+defaultLineageGroupName)

	if nameOrArn != defaultLineageGroupName && nameOrArn != lineageGroupARN {
		return "", fmt.Errorf("%w: lineage group %q not found", ErrLineageGroupNotFound, nameOrArn)
	}

	return "", fmt.Errorf("%w: lineage group %q has no resource policy attached",
		ErrLineageGroupPolicyNotFound, nameOrArn)
}

// ---------------------------------------------------------------------------
// QueryLineage — traverses the association graph from a set of start ARNs.
// ---------------------------------------------------------------------------

// lineageEntityLookup resolves an ARN to its lineage entity name, type and
// LineageType. Returns ok=false when the ARN is not a known Action/Artifact/
// Context (it may still be a valid association endpoint, e.g. a TrainingJob
// or Model ARN).
func (b *InMemoryBackend) lineageEntityLookup(
	region, entityArn string,
) (string, string, string, bool) {
	if actionName, found := b.actionARNIndexStoreRO(region)[entityArn]; found {
		if a, exists := b.actionsStoreRO(region).Get(actionName); exists {
			return a.ActionName, a.ActionType, "Action", true
		}
	}

	if ar, found := b.artifactsStoreRO(region).Get(entityArn); found {
		return ar.ArtifactName, ar.ArtifactType, "Artifact", true
	}

	if contextName, found := b.contextARNIndexStoreRO(region)[entityArn]; found {
		if c, exists := b.contextsStoreRO(region).Get(contextName); exists {
			return c.ContextName, c.ContextType, "Context", true
		}
	}

	return "", "", "", false
}

// LineageEntityInfo resolves an ARN to its lineage entity name, type and
// LineageType, for use by handlers enriching association/lineage responses.
func (b *InMemoryBackend) LineageEntityInfo(
	ctx context.Context, entityArn string,
) (string, string, string, bool) {
	b.mu.RLock("LineageEntityInfo")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return b.lineageEntityLookup(region, entityArn)
}

// QueryLineage traverses the association graph starting from startArns, following
// edges in the given direction up to maxDepth hops, and returns the reached
// vertices plus (if includeEdges) the edges traversed.
func (b *InMemoryBackend) QueryLineage(
	ctx context.Context,
	startArns []string,
	direction string,
	maxDepth int,
	includeEdges bool,
) ([]Vertex, []Edge, error) {
	b.mu.RLock("QueryLineage")
	defer b.mu.RUnlock()

	if len(startArns) == 0 {
		return nil, nil, fmt.Errorf("%w: StartArns is required", ErrValidation)
	}

	if maxDepth <= 0 {
		maxDepth = 10
	}

	region := getRegion(ctx, b.region)

	fwd, back := b.buildLineageAdjacency(region)

	visited := map[string]bool{}
	var edges []Edge

	type queued struct {
		arnStr string
		depth  int
	}

	queue := make([]queued, 0, len(startArns))

	for _, s := range startArns {
		if !visited[s] {
			visited[s] = true
			queue = append(queue, queued{arnStr: s, depth: 0})
		}
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		if cur.depth >= maxDepth {
			continue
		}

		neighbors := lineageNeighbors(direction, cur.arnStr, fwd, back)
		for _, e := range neighbors {
			edges = append(edges, e)

			next := e.DestinationArn
			if next == cur.arnStr {
				next = e.SourceArn
			}

			if !visited[next] {
				visited[next] = true
				queue = append(queue, queued{arnStr: next, depth: cur.depth + 1})
			}
		}
	}

	vertices := make([]Vertex, 0, len(visited))

	arns := make([]string, 0, len(visited))
	for a := range visited {
		arns = append(arns, a)
	}

	sort.Strings(arns)

	for _, a := range arns {
		_, entityType, lineageType, _ := b.lineageEntityLookup(region, a)
		vertices = append(vertices, Vertex{Arn: a, Type: entityType, LineageType: lineageType})
	}

	if !includeEdges {
		edges = nil
	}

	return vertices, edges, nil
}

// buildLineageAdjacency builds forward (source->[]edge) and backward
// (destination->[]edge) adjacency lists from the association store.
func (b *InMemoryBackend) buildLineageAdjacency(region string) (map[string][]Edge, map[string][]Edge) {
	fwd := make(map[string][]Edge)
	back := make(map[string][]Edge)

	for _, a := range b.associationsStoreRO(region).All() {
		e := Edge{SourceArn: a.SourceArn, DestinationArn: a.DestinationArn, AssociationType: a.AssociationType}
		fwd[a.SourceArn] = append(fwd[a.SourceArn], e)
		back[a.DestinationArn] = append(back[a.DestinationArn], e)
	}

	return fwd, back
}

// lineageNeighbors returns the edges reachable from arnStr in the given direction.
// "Descendants" follows source->destination edges, "Ascendants" follows them in
// reverse, and "Both" (the default) follows both.
func lineageNeighbors(direction, arnStr string, fwd, back map[string][]Edge) []Edge {
	var out []Edge

	if direction != "Ascendants" {
		out = append(out, fwd[arnStr]...)
	}

	if direction != "Descendants" {
		out = append(out, back[arnStr]...)
	}

	return out
}

// AddActionInternal adds an action directly for seeding tests.
func (b *InMemoryBackend) AddActionInternal(ctx context.Context, name, actionType string) *Action {
	b.mu.Lock("AddActionInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	actionARN := arn.Build("sagemaker", region, b.accountID, "action/"+name)
	now := time.Now()
	a := &Action{
		ActionName:       name,
		ActionArn:        actionARN,
		ActionType:       actionType,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             make(map[string]string),
	}
	b.actionsStore(region).Put(a)
	b.actionARNIndexStore(region)[actionARN] = name

	return cloneAction(a)
}

// CreateAction creates a SageMaker ML lineage action.
func (b *InMemoryBackend) CreateAction(
	ctx context.Context,
	name, actionType, description, status string,
	source ActionSource,
	properties map[string]string,
	tags map[string]string,
) (*Action, error) {
	b.mu.Lock("CreateAction")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ActionName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	actionsStore := b.actionsStore(region)

	if _, ok := actionsStore.Get(name); ok {
		return nil, fmt.Errorf("%w: action %q already exists", ErrActionAlreadyExists, name)
	}

	actionARN := arn.Build("sagemaker", region, b.accountID, "action/"+name)
	now := time.Now()

	a := &Action{
		ActionName:       name,
		ActionArn:        actionARN,
		ActionType:       actionType,
		Description:      description,
		Status:           status,
		Source:           source,
		Properties:       maps.Clone(properties),
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	actionsStore.Put(a)
	b.actionARNIndexStore(region)[actionARN] = name

	return cloneAction(a), nil
}

package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"slices"
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

// MetadataProperties tracks commit/repo provenance for an Action or Artifact,
// per CreateActionInput.MetadataProperties / CreateArtifactInput.MetadataProperties
// (aws-sdk-go-v2/service/sagemaker types/types.go:13617, flat 4-string struct).
type MetadataProperties struct {
	CommitID    string `json:"CommitId,omitempty"`
	GeneratedBy string `json:"GeneratedBy,omitempty"`
	ProjectID   string `json:"ProjectId,omitempty"`
	Repository  string `json:"Repository,omitempty"`
}

// Artifact represents a SageMaker ML lineage artifact.
type Artifact struct {
	CreationTime       time.Time           `json:"CreationTime"`
	LastModifiedTime   time.Time           `json:"LastModifiedTime"`
	Tags               map[string]string   `json:"Tags,omitempty"`
	Properties         map[string]string   `json:"Properties,omitempty"`
	MetadataProperties *MetadataProperties `json:"MetadataProperties,omitempty"`
	ArtifactName       string              `json:"ArtifactName,omitempty"`
	ArtifactArn        string              `json:"ArtifactArn"`
	ArtifactType       string              `json:"ArtifactType"`
	Source             ArtifactSource      `json:"Source"`
}

// cloneArtifact returns a deep copy of ar.
func cloneArtifact(ar *Artifact) *Artifact {
	cp := *ar
	cp.Tags = maps.Clone(ar.Tags)
	cp.Properties = maps.Clone(ar.Properties)
	cp.Source.SourceTypes = append([]ArtifactSourceType(nil), ar.Source.SourceTypes...)

	if ar.MetadataProperties != nil {
		mp := *ar.MetadataProperties
		cp.MetadataProperties = &mp
	}

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
	metadataProperties *MetadataProperties,
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
		ArtifactName:       name,
		ArtifactArn:        artifactARN,
		ArtifactType:       artifactType,
		Source:             source,
		Properties:         maps.Clone(properties),
		Tags:               mergeTags(nil, tags),
		MetadataProperties: metadataProperties,
		CreationTime:       now,
		LastModifiedTime:   now,
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

// DeleteArtifact deletes an artifact identified either by ARN or, per the
// real DeleteArtifactInput ("Either ArtifactArn or Source must be
// specified" — docs.aws.amazon.com/sagemaker/latest/APIReference/API_DeleteArtifact.html,
// since neither field is marked required on the Go SDK struct itself), by
// Source.SourceUri. When multiple artifacts share a SourceUri (the real API
// does not document a tie-break), the lowest ArtifactArn is deleted, matching
// this emulator's other deterministic-by-ARN tie-break conventions.
func (b *InMemoryBackend) DeleteArtifact(
	ctx context.Context, artifactArn string, source *ArtifactSource,
) (*Artifact, error) {
	b.mu.Lock("DeleteArtifact")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.artifactsStore(region)

	resolvedArn := artifactArn
	if resolvedArn == "" && source != nil && source.SourceURI != "" {
		resolvedArn = b.artifactArnBySourceURI(store, source.SourceURI)
	}

	ar, ok := store.Get(resolvedArn)
	if resolvedArn == "" || !ok {
		return nil, fmt.Errorf("%w: artifact %q not found", ErrArtifactNotFound, artifactArn)
	}

	cp := cloneArtifact(ar)
	store.Delete(resolvedArn)

	return cp, nil
}

// artifactArnBySourceURI returns the lowest ArtifactArn among artifacts whose
// Source.SourceUri matches sourceURI, or "" if none match.
func (b *InMemoryBackend) artifactArnBySourceURI(store *store.Table[Artifact], sourceURI string) string {
	best := ""

	for _, ar := range store.All() {
		if ar.Source.SourceURI == sourceURI && (best == "" || ar.ArtifactArn < best) {
			best = ar.ArtifactArn
		}
	}

	return best
}

// ListArtifactsParams bundles the filter/sort criteria for ListArtifacts.
type ListArtifactsParams struct {
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	ArtifactType  string
	SourceURI     string
	NextToken     string
	SortBy        string
	SortOrder     string
	MaxResults    int32
}

// ListArtifacts returns artifacts, optionally filtered by type, source URI, and
// creation-time window, sorted per params.SortBy/SortOrder (real default:
// CreationTime, Descending).
func (b *InMemoryBackend) ListArtifacts(ctx context.Context, params ListArtifactsParams) ([]*Artifact, string) {
	b.mu.RLock("ListArtifacts")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.artifactsStoreRO(region)

	list := make([]*Artifact, 0, store.Len())

	for _, ar := range store.All() {
		if params.ArtifactType != "" && ar.ArtifactType != params.ArtifactType {
			continue
		}

		if params.SourceURI != "" && ar.Source.SourceURI != params.SourceURI {
			continue
		}

		if params.CreatedAfter != nil && !ar.CreationTime.After(*params.CreatedAfter) {
			continue
		}

		if params.CreatedBefore != nil && !ar.CreationTime.Before(*params.CreatedBefore) {
			continue
		}

		list = append(list, cloneArtifact(ar))
	}

	// SortArtifactsBy has a single enum value, CreationTime (types/enums.go:9056-9061)
	// — unlike ListContexts/ListActions' Name|CreationTime, there is no other sort key.
	desc := !strings.EqualFold(params.SortOrder, "Ascending")
	sort.Slice(list, func(i, j int) bool {
		less := list[i].CreationTime.Before(list[j].CreationTime)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
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

// ListContextsParams bundles the filter/sort criteria for ListContexts.
type ListContextsParams struct {
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	ContextType   string
	SourceURI     string
	NextToken     string
	SortBy        string
	SortOrder     string
	MaxResults    int32
}

// ListContexts returns contexts, optionally filtered by type, source URI, and
// creation-time window, sorted per params.SortBy/SortOrder (real default:
// CreationTime, Descending).
// filterSortPaginateByNameOrTime for a distinct type, which already dedupes the real logic
//
//nolint:dupl // structurally mirrors ListActions below; both are thin adapters onto
func (b *InMemoryBackend) ListContexts(ctx context.Context, params ListContextsParams) ([]*Context, string) {
	b.mu.RLock("ListContexts")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.contextsStoreRO(region).All()

	return filterSortPaginateByNameOrTime(items, nameOrTimeSortParams{
		CreatedAfter:  params.CreatedAfter,
		CreatedBefore: params.CreatedBefore,
		TypeFilter:    params.ContextType,
		SourceURI:     params.SourceURI,
		NextToken:     params.NextToken,
		SortBy:        params.SortBy,
		SortOrder:     params.SortOrder,
		MaxResults:    params.MaxResults,
	},
		func(c *Context) string { return c.ContextType },
		func(c *Context) string { return c.Source.SourceURI },
		func(c *Context) string { return c.ContextName },
		func(c *Context) time.Time { return c.CreationTime },
		cloneContext,
	)
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

// ListActionsParams bundles the filter/sort criteria for ListActions.
type ListActionsParams struct {
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	ActionType    string
	SourceURI     string
	NextToken     string
	SortBy        string
	SortOrder     string
	MaxResults    int32
}

// ListActions returns actions, optionally filtered by type, source URI, and
// creation-time window, sorted per params.SortBy/SortOrder (real default:
// CreationTime, Descending).
// filterSortPaginateByNameOrTime for a distinct type, which already dedupes the real logic
//
//nolint:dupl // structurally mirrors ListContexts above; both are thin adapters onto
func (b *InMemoryBackend) ListActions(ctx context.Context, params ListActionsParams) ([]*Action, string) {
	b.mu.RLock("ListActions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.actionsStoreRO(region).All()

	return filterSortPaginateByNameOrTime(items, nameOrTimeSortParams{
		CreatedAfter:  params.CreatedAfter,
		CreatedBefore: params.CreatedBefore,
		TypeFilter:    params.ActionType,
		SourceURI:     params.SourceURI,
		NextToken:     params.NextToken,
		SortBy:        params.SortBy,
		SortOrder:     params.SortOrder,
		MaxResults:    params.MaxResults,
	},
		func(a *Action) string { return a.ActionType },
		func(a *Action) string { return a.Source.SourceURI },
		func(a *Action) string { return a.ActionName },
		func(a *Action) time.Time { return a.CreationTime },
		cloneAction,
	)
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

// ListLineageGroupsParams bundles the filter/sort/pagination criteria for
// ListLineageGroups. SortBy/SortOrder are accepted but are a genuine no-op:
// there is at most one lineage group per account/region (see the type
// comment above), so no ordering of a 0-or-1-element list is observable.
type ListLineageGroupsParams struct {
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	NextToken     string
	SortBy        string
	SortOrder     string
	MaxResults    int32
}

// LineageGroupInfo is a single lineage group's ARN and creation time.
type LineageGroupInfo struct {
	CreationTime    time.Time
	LineageGroupArn string
}

// ListLineageGroups returns the account's single auto-provisioned lineage
// group, filtered by params.CreatedAfter/CreatedBefore — a group outside the
// requested window is correctly excluded, even though there is only ever one.
func (b *InMemoryBackend) ListLineageGroups(
	ctx context.Context, params ListLineageGroupsParams,
) ([]LineageGroupInfo, string) {
	region := getRegion(ctx, b.region)
	lineageGroupARN := arn.Build("sagemaker", region, b.accountID, "lineage-group/"+defaultLineageGroupName)
	createdAt := b.lifecycleEpoch()

	list := []LineageGroupInfo{}

	inWindow := (params.CreatedAfter == nil || createdAt.After(*params.CreatedAfter)) &&
		(params.CreatedBefore == nil || createdAt.Before(*params.CreatedBefore))
	if inWindow {
		list = append(list, LineageGroupInfo{LineageGroupArn: lineageGroupARN, CreationTime: createdAt})
	}

	return paginateSlice(list, params.NextToken, params.MaxResults)
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

// lineageEntityDetail holds the metadata QueryLineage's Filters need about a
// vertex that resolves to a tracked Action/Artifact/Context.
type lineageEntityDetail struct {
	CreationTime     time.Time
	LastModifiedTime time.Time
	Properties       map[string]string
	Name             string
	EntityType       string
	LineageType      string
}

// lineageEntityDetailLookup resolves an ARN to its full lineage entity
// detail. Returns ok=false when the ARN is not a known Action/Artifact/
// Context (it may still be a valid association endpoint, e.g. a TrainingJob
// or Model ARN, which this backend does not track timestamps/properties for).
func (b *InMemoryBackend) lineageEntityDetailLookup(region, entityArn string) (lineageEntityDetail, bool) {
	if actionName, found := b.actionARNIndexStoreRO(region)[entityArn]; found {
		if a, exists := b.actionsStoreRO(region).Get(actionName); exists {
			return lineageEntityDetail{
				Name: a.ActionName, EntityType: a.ActionType, LineageType: "Action",
				CreationTime: a.CreationTime, LastModifiedTime: a.LastModifiedTime, Properties: a.Properties,
			}, true
		}
	}

	if ar, found := b.artifactsStoreRO(region).Get(entityArn); found {
		return lineageEntityDetail{
			Name: ar.ArtifactName, EntityType: ar.ArtifactType, LineageType: "Artifact",
			CreationTime: ar.CreationTime, LastModifiedTime: ar.LastModifiedTime, Properties: ar.Properties,
		}, true
	}

	if contextName, found := b.contextARNIndexStoreRO(region)[entityArn]; found {
		if c, exists := b.contextsStoreRO(region).Get(contextName); exists {
			return lineageEntityDetail{
				Name: c.ContextName, EntityType: c.ContextType, LineageType: "Context",
				CreationTime: c.CreationTime, LastModifiedTime: c.LastModifiedTime, Properties: c.Properties,
			}, true
		}
	}

	return lineageEntityDetail{}, false
}

// lineageEntityLookup resolves an ARN to its lineage entity name, type and
// LineageType. Returns ok=false when the ARN is not a known Action/Artifact/
// Context (it may still be a valid association endpoint, e.g. a TrainingJob
// or Model ARN).
func (b *InMemoryBackend) lineageEntityLookup(
	region, entityArn string,
) (string, string, string, bool) {
	d, ok := b.lineageEntityDetailLookup(region, entityArn)
	if !ok {
		return "", "", "", false
	}

	return d.Name, d.EntityType, d.LineageType, true
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

// QueryLineageFilters narrows a QueryLineage traversal's result vertices, per
// QueryFilters (aws-sdk-go-v2/service/sagemaker types/types.go:19078). Types
// (matching non-lineage-tracked entities like TrainingJob/Model/Endpoint by
// their AWS resource type) is deliberately not modeled here — see
// QueryLineage's doc comment.
type QueryLineageFilters struct {
	CreatedAfter   *time.Time
	CreatedBefore  *time.Time
	ModifiedAfter  *time.Time
	ModifiedBefore *time.Time
	Properties     map[string]string
	LineageTypes   []string
}

// QueryLineageParams bundles QueryLineage's traversal and filter/pagination criteria.
type QueryLineageParams struct {
	Filters      *QueryLineageFilters
	Direction    string
	NextToken    string
	StartArns    []string
	MaxDepth     int
	MaxResults   int32
	IncludeEdges bool
}

// QueryLineage traverses the association graph starting from StartArns, following
// edges in the given direction up to MaxDepth hops. The reached vertices are
// narrowed by Filters, then paginated by MaxResults/NextToken (real docs
// describe both as bounding "the number of vertices", not edges — see
// api_op_QueryLineage.go:34,38 — so Edges is the full, unpaginated edge set
// between vertices that survive filtering).
//
// Filters.Types (entity-type match for non-lineage-tracked vertices such as
// TrainingJob/Model/Endpoint ARNs) is accepted but NOT enforced: this
// backend has no per-service entity-type resolver for arbitrary ARNs outside
// Action/Artifact/Context, and building one is out of this pass's scope.
// Filters.CreatedAfter/CreatedBefore/ModifiedAfter/ModifiedBefore/Properties
// exclude any vertex that does not resolve to a tracked Action/Artifact/
// Context (an external vertex's creation/modification time or properties
// are unknown here, so it cannot honestly be said to match).
func (b *InMemoryBackend) QueryLineage(
	ctx context.Context, params QueryLineageParams,
) ([]Vertex, []Edge, string, error) {
	b.mu.RLock("QueryLineage")
	defer b.mu.RUnlock()

	if len(params.StartArns) == 0 {
		return nil, nil, "", fmt.Errorf("%w: StartArns is required", ErrValidation)
	}

	maxDepth := params.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 10
	}

	region := getRegion(ctx, b.region)
	fwd, back := b.buildLineageAdjacency(region)
	visited, allEdges := bfsLineageGraph(params.StartArns, params.Direction, maxDepth, fwd, back)

	vertices, kept := b.filterLineageVertices(region, visited, params.Filters)
	page, nextToken := paginateSlice(vertices, params.NextToken, params.MaxResults)

	var edges []Edge
	if params.IncludeEdges {
		for _, e := range allEdges {
			if kept[e.SourceArn] && kept[e.DestinationArn] {
				edges = append(edges, e)
			}
		}
	}

	return page, edges, nextToken, nil
}

// bfsLineageGraph breadth-first-traverses the association graph from
// startArns in direction, up to maxDepth hops, returning every visited ARN
// and every edge encountered along the way (before any Filters are applied).
func bfsLineageGraph(
	startArns []string, direction string, maxDepth int, fwd, back map[string][]Edge,
) (map[string]bool, []Edge) {
	visited := map[string]bool{}
	var allEdges []Edge

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

		for _, e := range lineageNeighbors(direction, cur.arnStr, fwd, back) {
			allEdges = append(allEdges, e)

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

	return visited, allEdges
}

// filterLineageVertices resolves each visited ARN's entity detail, keeps
// those matching filters, and returns both the ordered (by ARN) Vertex list
// and the set of ARNs kept (for pruning allEdges to survivors).
func (b *InMemoryBackend) filterLineageVertices(
	region string, visited map[string]bool, filters *QueryLineageFilters,
) ([]Vertex, map[string]bool) {
	arns := make([]string, 0, len(visited))
	for a := range visited {
		arns = append(arns, a)
	}

	sort.Strings(arns)

	kept := map[string]bool{}
	vertices := make([]Vertex, 0, len(arns))

	for _, a := range arns {
		detail, ok := b.lineageEntityDetailLookup(region, a)
		if !queryLineageVertexMatches(detail, ok, filters) {
			continue
		}

		kept[a] = true
		vertices = append(vertices, Vertex{Arn: a, Type: detail.EntityType, LineageType: detail.LineageType})
	}

	return vertices, kept
}

// queryLineageVertexMatches reports whether a vertex (detail/ok from
// lineageEntityDetailLookup) satisfies filters. A nil filters matches
// everything.
func queryLineageVertexMatches(detail lineageEntityDetail, ok bool, filters *QueryLineageFilters) bool {
	if filters == nil {
		return true
	}

	if filters.needsEntityDetail() && !ok {
		return false
	}

	if len(filters.LineageTypes) > 0 && !slices.Contains(filters.LineageTypes, detail.LineageType) {
		return false
	}

	if !filters.matchesTimeWindows(detail) {
		return false
	}

	return len(filters.Properties) == 0 || propertiesMatchAny(detail.Properties, filters.Properties)
}

// needsEntityDetail reports whether any of f's filters require the vertex to
// resolve to a tracked Action/Artifact/Context (as opposed to LineageTypes,
// which every vertex — resolved or not — has an answer for).
func (f *QueryLineageFilters) needsEntityDetail() bool {
	return f.CreatedAfter != nil || f.CreatedBefore != nil ||
		f.ModifiedAfter != nil || f.ModifiedBefore != nil || len(f.Properties) > 0
}

// matchesTimeWindows reports whether detail falls within every CreatedAfter/
// CreatedBefore/ModifiedAfter/ModifiedBefore window f specifies.
func (f *QueryLineageFilters) matchesTimeWindows(detail lineageEntityDetail) bool {
	if f.CreatedAfter != nil && !detail.CreationTime.After(*f.CreatedAfter) {
		return false
	}

	if f.CreatedBefore != nil && !detail.CreationTime.Before(*f.CreatedBefore) {
		return false
	}

	if f.ModifiedAfter != nil && !detail.LastModifiedTime.After(*f.ModifiedAfter) {
		return false
	}

	if f.ModifiedBefore != nil && !detail.LastModifiedTime.Before(*f.ModifiedBefore) {
		return false
	}

	return true
}

// propertiesMatchAny reports whether entity contains at least one key/value
// pair present in want — QueryFilters.Properties' real semantics ("If
// multiple pairs are provided, an entity is included in the results if it
// matches any of the provided pairs" — types/types.go:19098-19101).
func propertiesMatchAny(entity, want map[string]string) bool {
	for k, v := range want {
		if ev, ok := entity[k]; ok && ev == v {
			return true
		}
	}

	return false
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
	metadataProperties *MetadataProperties,
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
		ActionName:         name,
		ActionArn:          actionARN,
		ActionType:         actionType,
		Description:        description,
		Status:             status,
		Source:             source,
		Properties:         maps.Clone(properties),
		Tags:               mergeTags(nil, tags),
		MetadataProperties: metadataProperties,
		CreationTime:       now,
		LastModifiedTime:   now,
	}
	actionsStore.Put(a)
	b.actionARNIndexStore(region)[actionARN] = name

	return cloneAction(a), nil
}

package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrModelPackageNotFound is returned when a model package does not exist.
	ErrModelPackageNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrModelPackageGroupNotFound is returned when a model package group does not exist.
	ErrModelPackageGroupNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrModelPackageGroupHasPackages is returned when deleting a group that
	// still has packages. DeleteModelPackageGroup's only documented error is
	// ConflictException (botocore sagemaker/2017-07-24@1.43.56
	// service-2.json's DeleteModelPackageGroup.errors), not the generic
	// ResourceInUse handleError emits for awserr.ErrConflict — wrap the
	// service's ErrConflictException sentinel so handleError's special case
	// (see errors.go) picks the accurate wire type.
	ErrModelPackageGroupHasPackages = awserr.New("ConflictException", ErrConflictException)
)

// ModelPackageBatchResult holds the result of describing a single model package in a batch.
type ModelPackageBatchResult struct {
	ModelPackage *ModelPackage
	ErrorCode    string
	ErrorMessage string
}

// BatchDescribeModelPackage returns descriptions of multiple model packages by ARN.
func (b *InMemoryBackend) BatchDescribeModelPackage(
	ctx context.Context,
	modelPackageArns []string,
) map[string]ModelPackageBatchResult {
	b.mu.RLock("BatchDescribeModelPackage")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	mpStore := b.modelPackagesStoreRO(region)

	results := make(map[string]ModelPackageBatchResult, len(modelPackageArns))

	for _, arnStr := range modelPackageArns {
		mp, ok := mpStore.Get(arnStr)
		if !ok {
			results[arnStr] = ModelPackageBatchResult{
				ErrorCode:    "ValidationException",
				ErrorMessage: fmt.Sprintf("model package %q not found", arnStr),
			}

			continue
		}

		results[arnStr] = ModelPackageBatchResult{
			ModelPackage: cloneModelPackage(mp),
		}
	}

	return results
}

// AddModelPackageInternal adds a model package directly for testing.
func (b *InMemoryBackend) AddModelPackageInternal(ctx context.Context, mp *ModelPackage) {
	b.mu.Lock("AddModelPackageInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.modelPackagesStore(region).Put(mp)
	b.modelPackageARNIndexStore(region)[mp.ModelPackageArn] = mp.ModelPackageArn
}

// ---------------------------------------------------------------------------
// ModelPackageGroup
// ---------------------------------------------------------------------------

// ManagedConfiguration mirrors AWS's ManagedConfiguration
// (types/types.go:13591) — the managed storage type of a model package group.
type ManagedConfiguration struct {
	ManagedStorageType string `json:"ManagedStorageType,omitempty"`
}

// ModelPackageGroup represents a SageMaker model package group.
//
// CreatedBy (types.UserContext) is "This member is required" on
// DescribeModelPackageGroupOutput but is disclosed absent, not fabricated —
// this backend has no IAM-identity model to honestly derive it from, the same
// class-d gap as every other CreatedBy/LastModifiedBy field in this service.
type ModelPackageGroup struct {
	CreationTime                 time.Time             `json:"CreationTime"`
	Tags                         map[string]string     `json:"Tags,omitempty"`
	ManagedConfiguration         *ManagedConfiguration `json:"ManagedConfiguration,omitempty"`
	ModelPackageGroupName        string                `json:"ModelPackageGroupName"`
	ModelPackageGroupArn         string                `json:"ModelPackageGroupArn"`
	ModelPackageGroupDescription string                `json:"ModelPackageGroupDescription,omitempty"`
	ModelPackageGroupStatus      string                `json:"ModelPackageGroupStatus"`
	// ResourcePolicy is the resource policy JSON document attached via
	// PutModelPackageGroupPolicy, if any.
	ResourcePolicy string `json:"ResourcePolicy,omitempty"`
}

func cloneModelPackageGroup(g *ModelPackageGroup) *ModelPackageGroup {
	cp := *g
	cp.Tags = maps.Clone(g.Tags)

	if g.ManagedConfiguration != nil {
		mc := *g.ManagedConfiguration
		cp.ManagedConfiguration = &mc
	}

	return &cp
}

// MarshalJSON emits CreationTime as an AWS awsjson1.1 epoch-seconds number
// rather than Go's default RFC3339 string — this struct is marshaled
// directly by handleDescribeModelPackageGroup.
func (g *ModelPackageGroup) MarshalJSON() ([]byte, error) {
	type alias ModelPackageGroup

	return json.Marshal(struct {
		*alias
		CreationTime float64 `json:"CreationTime"`
	}{
		alias:        (*alias)(g),
		CreationTime: epochSeconds(g.CreationTime),
	})
}

// UnmarshalJSON is the inverse of [ModelPackageGroup.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (g *ModelPackageGroup) UnmarshalJSON(data []byte) error {
	type alias ModelPackageGroup

	aux := struct {
		*alias
		CreationTime float64 `json:"CreationTime"`
	}{alias: (*alias)(g)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	g.CreationTime = timeFromEpochSeconds(aux.CreationTime)

	return nil
}

// CreateModelPackageGroupOptions holds the fields accepted by
// CreateModelPackageGroup (api_op_CreateModelPackageGroup.go:16-40,
// sagemaker@v1.263.2).
type CreateModelPackageGroupOptions struct {
	Name               string
	Description        string
	Tags               map[string]string
	ManagedStorageType string
}

// CreateModelPackageGroup creates a new model package group.
func (b *InMemoryBackend) CreateModelPackageGroup(
	ctx context.Context,
	opts CreateModelPackageGroupOptions,
) (*ModelPackageGroup, error) {
	b.mu.Lock("CreateModelPackageGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", ErrValidation)
	}

	if _, ok := b.modelPackageGroupsStore(region).Get(opts.Name); ok {
		return nil, fmt.Errorf("%w: model package group %q already exists", ErrValidation, opts.Name)
	}

	groupARN := arn.Build("sagemaker", region, b.accountID, "model-package-group/"+opts.Name)

	g := &ModelPackageGroup{
		ModelPackageGroupName:        opts.Name,
		ModelPackageGroupArn:         groupARN,
		ModelPackageGroupDescription: opts.Description,
		ModelPackageGroupStatus:      algorithmStatusCompleted,
		Tags:                         mergeTags(nil, opts.Tags),
		CreationTime:                 time.Now(),
	}
	if opts.ManagedStorageType != "" {
		g.ManagedConfiguration = &ManagedConfiguration{ManagedStorageType: opts.ManagedStorageType}
	}
	b.modelPackageGroupsStore(region).Put(g)
	b.modelPackageGroupARNIndexStore(region)[groupARN] = opts.Name

	return cloneModelPackageGroup(g), nil
}

// DescribeModelPackageGroup returns a model package group by name.
func (b *InMemoryBackend) DescribeModelPackageGroup(ctx context.Context, name string) (*ModelPackageGroup, error) {
	b.mu.RLock("DescribeModelPackageGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	return cloneModelPackageGroup(g), nil
}

// DeleteModelPackageGroup removes a model package group by name.
func (b *InMemoryBackend) DeleteModelPackageGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteModelPackageGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	// AWS rejects deletion when model packages still exist in the group.
	for _, mp := range b.modelPackagesStore(region).All() {
		if mp.ModelPackageGroupName == name {
			return fmt.Errorf("%w: model package group %q has model packages and cannot be deleted",
				ErrModelPackageGroupHasPackages, name)
		}
	}

	store := b.modelPackageGroupsStore(region)
	store.Delete(name)
	delete(b.modelPackageGroupARNIndexStore(region), g.ModelPackageGroupArn)

	return nil
}

// ListModelPackageGroupsParams bundles ListModelPackageGroups' filter/sort/
// pagination input (api_op_ListModelPackageGroups.go:29-72,
// sagemaker@v1.263.2).
type ListModelPackageGroupsParams struct {
	CreatedAfter             *time.Time
	CreatedBefore            *time.Time
	CrossAccountFilterOption string
	NameContains             string
	NextToken                string
	SortBy                   string
	SortOrder                string
	MaxResults               int32
}

// ListModelPackageGroups returns model package groups matching params, sorted
// per params.SortBy (Name/CreationTime, default CreationTime per the real
// op's documented default) / params.SortOrder (default Ascending), capped at
// params.MaxResults. CrossAccountFilterOption is honored but this backend has
// no cross-account resource-sharing model at all (grepped repo-wide) — a
// CrossAccount request always yields an empty result, which is the correct
// answer for an account with zero shared groups, not a fabricated one.
func (b *InMemoryBackend) ListModelPackageGroups(
	ctx context.Context,
	params ListModelPackageGroupsParams,
) ([]*ModelPackageGroup, string) {
	b.mu.RLock("ListModelPackageGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if params.CrossAccountFilterOption == "CrossAccount" {
		return []*ModelPackageGroup{}, ""
	}

	tbl := b.modelPackageGroupsStoreRO(region)
	list := make([]*ModelPackageGroup, 0, tbl.Len())

	for _, g := range tbl.All() {
		if params.NameContains != "" && !strings.Contains(g.ModelPackageGroupName, params.NameContains) {
			continue
		}

		if params.CreatedAfter != nil && !g.CreationTime.After(*params.CreatedAfter) {
			continue
		}

		if params.CreatedBefore != nil && !g.CreationTime.Before(*params.CreatedBefore) {
			continue
		}

		list = append(list, cloneModelPackageGroup(g))
	}

	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		var less bool
		if params.SortBy == keyGenericName {
			less = list[i].ModelPackageGroupName < list[j].ModelPackageGroupName
		} else {
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// ErrModelPackageGroupPolicyNotFound is returned when a model package group
// has no resource policy attached.
var ErrModelPackageGroupPolicyNotFound = awserr.New("ValidationException", awserr.ErrNotFound)

// GetModelPackageGroupPolicy returns the resource policy attached to a model
// package group.
func (b *InMemoryBackend) GetModelPackageGroupPolicy(ctx context.Context, name string) (string, error) {
	b.mu.RLock("GetModelPackageGroupPolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStoreRO(region).Get(name)
	if !ok {
		return "", fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	if g.ResourcePolicy == "" {
		return "", fmt.Errorf(
			"%w: model package group %q has no resource policy attached",
			ErrModelPackageGroupPolicyNotFound, name,
		)
	}

	return g.ResourcePolicy, nil
}

// PutModelPackageGroupPolicy attaches (or replaces) the resource policy for a
// model package group.
func (b *InMemoryBackend) PutModelPackageGroupPolicy(
	ctx context.Context,
	name, policy string,
) (*ModelPackageGroup, error) {
	b.mu.Lock("PutModelPackageGroupPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	if policy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy is required", ErrValidation)
	}

	g.ResourcePolicy = policy

	return cloneModelPackageGroup(g), nil
}

// DeleteModelPackageGroupPolicy removes the resource policy from a model
// package group.
func (b *InMemoryBackend) DeleteModelPackageGroupPolicy(ctx context.Context, name string) error {
	b.mu.Lock("DeleteModelPackageGroupPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	g.ResourcePolicy = ""

	return nil
}

// ---------------------------------------------------------------------------
// ModelPackage CRUD (name/ARN dual lookup)
// ---------------------------------------------------------------------------

// CreateModelPackageOptions holds the fields accepted by CreateModelPackage
// (api_op_CreateModelPackage.go:47-171, sagemaker@v1.263.2). ClientToken (a
// pure client-side idempotency token with no server-observable effect, per a
// repo-wide grep of this convention elsewhere in the service) is deliberately
// omitted.
type CreateModelPackageOptions struct {
	Name                              string
	GroupName                         string
	Description                       string
	Domain                            string
	SamplePayloadURL                  string
	SourceURI                         string
	Task                              string
	ManagedStorageType                string
	RegistrationType                  string
	SkipModelValidation               string
	ApprovalStatus                    string
	Tags                              map[string]string
	CustomerMetadataProperties        map[string]string
	InferenceSpecification            json.RawMessage
	SourceAlgorithmSpecification      json.RawMessage
	ValidationSpecification           json.RawMessage
	DriftCheckBaselines               json.RawMessage
	ModelMetrics                      json.RawMessage
	ModelCard                         json.RawMessage
	ModelLifeCycle                    json.RawMessage
	MetadataProperties                json.RawMessage
	SecurityConfig                    json.RawMessage
	AdditionalInferenceSpecifications json.RawMessage
	CertifyForMarketplace             bool
}

// CreateModelPackage creates a model package.
func (b *InMemoryBackend) CreateModelPackage(
	ctx context.Context,
	opts CreateModelPackageOptions,
) (*ModelPackage, error) {
	b.mu.Lock("CreateModelPackage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", ErrValidation)
	}

	mpARN := arn.Build("sagemaker", region, b.accountID, "model-package/"+opts.Name)

	if _, ok := b.modelPackagesStore(region).Get(mpARN); ok {
		return nil, fmt.Errorf("%w: model package %q already exists", ErrValidation, opts.Name)
	}

	mp := &ModelPackage{
		ModelPackageName:                  opts.Name,
		ModelPackageArn:                   mpARN,
		ModelPackageGroupName:             opts.GroupName,
		ModelPackageStatus:                "Completed",
		ModelApprovalStatus:               opts.ApprovalStatus,
		ModelPackageDescription:           opts.Description,
		Domain:                            opts.Domain,
		SamplePayloadURL:                  opts.SamplePayloadURL,
		SourceURI:                         opts.SourceURI,
		Task:                              opts.Task,
		ManagedStorageType:                opts.ManagedStorageType,
		ModelPackageRegistrationType:      opts.RegistrationType,
		SkipModelValidation:               opts.SkipModelValidation,
		CertifyForMarketplace:             opts.CertifyForMarketplace,
		Tags:                              mergeTags(nil, opts.Tags),
		CustomerMetadataProperties:        mergeTags(nil, opts.CustomerMetadataProperties),
		InferenceSpecification:            opts.InferenceSpecification,
		SourceAlgorithmSpecification:      opts.SourceAlgorithmSpecification,
		ValidationSpecification:           opts.ValidationSpecification,
		DriftCheckBaselines:               opts.DriftCheckBaselines,
		ModelMetrics:                      opts.ModelMetrics,
		ModelCard:                         opts.ModelCard,
		ModelLifeCycle:                    opts.ModelLifeCycle,
		MetadataProperties:                opts.MetadataProperties,
		SecurityConfig:                    opts.SecurityConfig,
		AdditionalInferenceSpecifications: opts.AdditionalInferenceSpecifications,
		CreationTime:                      time.Now(),
		ModelPackageStatusDetails: ModelPackageStatusDetails{
			ValidationStatuses: []ModelPackageStatusItem{},
		},
	}
	b.modelPackagesStore(region).Put(mp)
	b.modelPackageARNIndexStore(region)[opts.Name] = mpARN

	return cloneModelPackage(mp), nil
}

// DescribeModelPackage returns a model package by name or ARN. includedData
// (AllData/MetadataOnly) is accepted for wire-shape fidelity but is a no-op:
// this backend has no KMS-gated ModelCard redaction to apply — ModelCard is
// carried as opaque json.RawMessage with no field-level access control
// either way, so MetadataOnly cannot honestly sanitize it further than AllData
// already does. Disclosed, not modeled.
func (b *InMemoryBackend) DescribeModelPackage(ctx context.Context, nameOrArn, _ string) (*ModelPackage, error) {
	b.mu.RLock("DescribeModelPackage")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	// Try direct ARN lookup first.
	if mp, ok := b.modelPackagesStoreRO(region).Get(nameOrArn); ok {
		return cloneModelPackage(mp), nil
	}

	// Try name → ARN index.
	if arnStr, ok := b.modelPackageARNIndexStoreRO(region)[nameOrArn]; ok {
		if mp, found := b.modelPackagesStoreRO(region).Get(arnStr); found {
			return cloneModelPackage(mp), nil
		}
	}

	return nil, fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
}

// DeleteModelPackage removes a model package by name or ARN.
func (b *InMemoryBackend) DeleteModelPackage(ctx context.Context, nameOrArn string) error {
	b.mu.Lock("DeleteModelPackage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	arnStr := nameOrArn
	if v, ok := b.modelPackageARNIndexStore(region)[nameOrArn]; ok {
		arnStr = v
	}

	if _, ok := b.modelPackagesStore(region).Get(arnStr); !ok {
		return fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
	}

	mp := tableGet(b.modelPackagesStore(region), arnStr)
	arnIdxStore := b.modelPackageARNIndexStore(region)
	delete(arnIdxStore, mp.ModelPackageName)
	mpStore := b.modelPackagesStore(region)
	mpStore.Delete(arnStr)

	return nil
}

// ListModelPackagesParams bundles ListModelPackages' filter/sort/pagination
// input (api_op_ListModelPackages.go:29-77, sagemaker@v1.263.2).
//
// ModelPackageType only applies when GroupName is empty — the real op's
// UNVERSIONED/VERSIONED/BOTH distinction only makes sense for the ungrouped
// listing; a request that already scopes to one group is listing that
// group's versions, so the type filter doesn't additionally narrow it (same
// judgment call as the doc's own framing: the type filter and the group
// filter are documented as alternative ways to use this op, not composable
// ones). "Versioned" is interpreted honestly as "has a ModelPackageGroupName"
// — the only sense in which this backend distinguishes versioned from
// unversioned models, since it doesn't implement AWS's group+version ARN
// addressing scheme (disclosed on ModelPackage, see the campaign notes).
type ListModelPackagesParams struct {
	CreatedAfter     *time.Time
	CreatedBefore    *time.Time
	ApprovalStatus   string
	GroupName        string
	ModelPackageType string
	NameContains     string
	NextToken        string
	SortBy           string
	SortOrder        string
	MaxResults       int32
}

// ListModelPackages returns model packages matching params, sorted per
// params.SortBy (Name/CreationTime, default CreationTime per the real op's
// documented default) / params.SortOrder (default Ascending), capped at
// params.MaxResults.
func (b *InMemoryBackend) ListModelPackages(
	ctx context.Context,
	params ListModelPackagesParams,
) ([]*ModelPackage, string) {
	b.mu.RLock("ListModelPackages")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*ModelPackage, 0, b.modelPackagesStoreRO(region).Len())

	for _, mp := range b.modelPackagesStoreRO(region).All() {
		if !modelPackageMatchesListParams(mp, params) {
			continue
		}

		list = append(list, cloneModelPackage(mp))
	}

	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		var less bool
		if params.SortBy == keyGenericName {
			less = list[i].ModelPackageName < list[j].ModelPackageName
		} else {
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// modelPackageMatchesListParams reports whether mp passes every filter in params.
func modelPackageMatchesListParams(mp *ModelPackage, params ListModelPackagesParams) bool {
	if !modelPackageMatchesGroupAndType(mp, params) {
		return false
	}

	if params.ApprovalStatus != "" && mp.ModelApprovalStatus != params.ApprovalStatus {
		return false
	}

	if params.NameContains != "" && !strings.Contains(mp.ModelPackageName, params.NameContains) {
		return false
	}

	if params.CreatedAfter != nil && !mp.CreationTime.After(*params.CreatedAfter) {
		return false
	}

	if params.CreatedBefore != nil && !mp.CreationTime.Before(*params.CreatedBefore) {
		return false
	}

	return true
}

// modelPackageMatchesGroupAndType applies ListModelPackages' GroupName and
// ModelPackageType filters. ModelPackageType only applies when GroupName is
// empty (see ListModelPackagesParams' doc comment). Real enum values
// (types/enums.go:6115-6122) are "Versioned" / "Unversioned" / "Both", not
// the all-caps prose in the op's own doc comment -- confirmed against the
// SDK source, not the docs.
func modelPackageMatchesGroupAndType(mp *ModelPackage, params ListModelPackagesParams) bool {
	if params.GroupName != "" {
		return mp.ModelPackageGroupName == params.GroupName
	}

	versioned := mp.ModelPackageGroupName != ""

	switch params.ModelPackageType {
	case "Versioned":
		return versioned
	case "Both":
		return true
	default: // "" or "Unversioned", the documented default
		return !versioned
	}
}

// UpdateModelPackageOptions holds the fields accepted by UpdateModelPackage
// (api_op_UpdateModelPackage.go:26-95, sagemaker@v1.263.2). Every field
// leaves the prior value unchanged when omitted (empty string / nil),
// matching every other Update op in this service. ClientToken is omitted for
// the same reason as CreateModelPackage's.
type UpdateModelPackageOptions struct {
	ApprovalStatus                         string
	ApprovalDescription                    string
	RegistrationType                       string
	SourceURI                              string
	CustomerMetadataProperties             map[string]string
	CustomerMetadataPropertiesToRemove     []string
	InferenceSpecification                 json.RawMessage
	ModelCard                              json.RawMessage
	ModelLifeCycle                         json.RawMessage
	AdditionalInferenceSpecificationsToAdd json.RawMessage
}

// UpdateModelPackage updates a model package identified by its ARN (the sole
// identifier UpdateModelPackageInput carries on the real wire — see
// handleUpdateModelPackage for why this used to be reachable by name).
func (b *InMemoryBackend) UpdateModelPackage(
	ctx context.Context,
	arnStr string,
	opts UpdateModelPackageOptions,
) (*ModelPackage, error) {
	b.mu.Lock("UpdateModelPackage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	mp, ok := b.modelPackagesStore(region).Get(arnStr)
	if !ok {
		return nil, fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, arnStr)
	}

	if opts.ApprovalStatus != "" {
		mp.ModelApprovalStatus = opts.ApprovalStatus
	}

	if opts.ApprovalDescription != "" {
		mp.ApprovalDescription = opts.ApprovalDescription
	}

	if opts.RegistrationType != "" {
		mp.ModelPackageRegistrationType = opts.RegistrationType
	}

	if opts.SourceURI != "" {
		mp.SourceURI = opts.SourceURI
	}

	if len(opts.CustomerMetadataProperties) > 0 {
		mp.CustomerMetadataProperties = mergeTags(mp.CustomerMetadataProperties, opts.CustomerMetadataProperties)
	}

	for _, k := range opts.CustomerMetadataPropertiesToRemove {
		delete(mp.CustomerMetadataProperties, k)
	}

	if len(opts.InferenceSpecification) > 0 {
		mp.InferenceSpecification = opts.InferenceSpecification
	}

	if len(opts.ModelCard) > 0 {
		mp.ModelCard = opts.ModelCard
	}

	if len(opts.ModelLifeCycle) > 0 {
		mp.ModelLifeCycle = opts.ModelLifeCycle
	}

	if len(opts.AdditionalInferenceSpecificationsToAdd) > 0 {
		mp.AdditionalInferenceSpecifications = appendInferenceSpecifications(
			mp.AdditionalInferenceSpecifications, opts.AdditionalInferenceSpecificationsToAdd,
		)
	}

	mp.LastModifiedTime = time.Now()

	return cloneModelPackage(mp), nil
}

// appendInferenceSpecifications concatenates the JSON array toAdd onto the
// end of the JSON array existing, matching UpdateModelPackageInput's
// AdditionalInferenceSpecificationsToAdd doc: "An array ... to be added to
// the existing array". Both are opaque json.RawMessage passthrough (see
// ModelPackage's own doc comment), so this manipulates them as raw JSON
// arrays rather than unmarshaling into a typed element.
func appendInferenceSpecifications(existing, toAdd json.RawMessage) json.RawMessage {
	var newItems []json.RawMessage
	if err := json.Unmarshal(toAdd, &newItems); err != nil {
		return existing
	}

	existingItems := make([]json.RawMessage, 0, len(newItems))
	if len(existing) > 0 {
		if err := json.Unmarshal(existing, &existingItems); err != nil {
			return existing
		}
	}

	existingItems = append(existingItems, newItems...)

	out, err := json.Marshal(existingItems)
	if err != nil {
		return existing
	}

	return out
}

package ssm

import (
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) activationsStore(region string) *store.Table[Activation] {
	return getOrCreateTable(b, b.activations, "activations", region, activationKeyFn)
}
func (b *InMemoryBackend) resourceDataSyncsStore(region string) *store.Table[ResourceDataSync] {
	return getOrCreateTable(b, b.resourceDataSyncs, "resourceDataSyncs", region, resourceDataSyncKeyFn)
}
func generateCode(n int) string {
	const (
		byteRange = 256
		bufMult   = 2
		maxChar   = byte(byteRange - (byteRange % len(activationCodeChars)))
	)
	result := make([]byte, 0, n)
	buf := make([]byte, n*bufMult)
	for len(result) < n {
		_, _ = rand.Read(buf)
		for _, b := range buf {
			if len(result) == n {
				break
			}
			if b < maxChar {
				result = append(result, activationCodeChars[int(b)%len(activationCodeChars)])
			}
		}
	}

	return string(result)
}

// CreateActivation creates a new activation for managed instances.
func (b *InMemoryBackend) CreateActivation(
	ctx context.Context,
	input *CreateActivationInput,
) (*CreateActivationOutput, error) {
	if input.IamRole == "" {
		return nil, fmt.Errorf("%w: IamRole is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("CreateActivation")
	defer b.mu.Unlock()

	activationID := activationIDPrefix + uuid.NewString()
	code := generateCode(activationCodeLen)

	limit := input.RegistrationLimit
	if limit <= 0 {
		limit = 1
	}

	now := UnixTimeFloat(time.Now())
	expiry := input.ExpirationDate
	if expiry == 0 {
		expiry = UnixTimeFloat(time.Now().Add(defaultActivationExpiryHrs * time.Hour))
	}

	act := Activation{
		ActivationID:        activationID,
		ActivationCode:      code,
		Description:         input.Description,
		DefaultInstanceName: input.DefaultInstanceName,
		IamRole:             input.IamRole,
		RegistrationLimit:   limit,
		RegistrationsCount:  0,
		ExpirationDate:      expiry,
		Expired:             false,
		CreatedDate:         now,
	}

	b.activationsStore(region).Put(&act)

	if len(input.Tags) > 0 {
		if b.miscResourceTags[region] == nil {
			b.miscResourceTags[region] = make(map[string]map[string]string)
		}
		miscTags := b.miscResourceTagsStore(region)
		if miscTags[activationID] == nil {
			miscTags[activationID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			miscTags[activationID][t.Key] = t.Value
		}
	}

	return &CreateActivationOutput{
		ActivationCode: code,
		ActivationID:   activationID,
	}, nil
}

// validateResourceDataSyncSource enforces ResourceDataSyncSource's own
// required members (SourceType/SourceRegions) whenever the pointer is
// non-nil, matching the real SDK's validateResourceDataSyncSource
// (validators.go:4854) exactly.
func validateResourceDataSyncSource(src *ResourceDataSyncSource) error {
	if src == nil {
		return nil
	}
	if src.SourceType == "" {
		return fmt.Errorf("%w: SyncSource.SourceType is required", ErrValidationException)
	}
	if len(src.SourceRegions) == 0 {
		return fmt.Errorf("%w: SyncSource.SourceRegions is required", ErrValidationException)
	}

	return nil
}

// validateResourceDataSyncS3Destination enforces ResourceDataSyncS3Destination's
// own required members whenever the pointer is non-nil, matching the real
// SDK's validateResourceDataSyncS3Destination (validators.go:4833) exactly.
func validateResourceDataSyncS3Destination(dst *ResourceDataSyncS3Destination) error {
	if dst == nil {
		return nil
	}
	if dst.BucketName == "" {
		return fmt.Errorf("%w: S3Destination.BucketName is required", ErrValidationException)
	}
	if dst.Region == "" {
		return fmt.Errorf("%w: S3Destination.Region is required", ErrValidationException)
	}
	if dst.SyncFormat == "" {
		return fmt.Errorf("%w: S3Destination.SyncFormat is required", ErrValidationException)
	}

	return nil
}

// CreateResourceDataSync stores a new resource data sync configuration.
// S3Destination and SyncSource are optional on the wire (neither carries
// "This member is required." in types.go), but api_op_CreateResourceDataSync.go's
// own doc comment states S3Destination "is required if the SyncType value is
// SyncToDestination" (the default when SyncType is omitted) and SyncSource
// "is required if the SyncType value is SyncFromSource" -- enforced here
// since it is the only signal that distinguishes a sync that will ever have
// a source/destination from one that silently never will.
func (b *InMemoryBackend) CreateResourceDataSync(
	ctx context.Context,
	input *CreateResourceDataSyncInput,
) (*CreateResourceDataSyncOutput, error) {
	if err := validateResourceDataSyncS3Destination(input.S3Destination); err != nil {
		return nil, err
	}
	if err := validateResourceDataSyncSource(input.SyncSource); err != nil {
		return nil, err
	}

	syncType := input.SyncType
	if syncType == "" {
		syncType = "SyncToDestination"
	}
	switch syncType {
	case "SyncToDestination":
		if input.S3Destination == nil {
			return nil, fmt.Errorf(
				"%w: S3Destination is required when SyncType is SyncToDestination",
				ErrValidationException,
			)
		}
	case "SyncFromSource":
		if input.SyncSource == nil {
			return nil, fmt.Errorf("%w: SyncSource is required when SyncType is SyncFromSource", ErrValidationException)
		}
	}

	region := getRegion(ctx)
	b.mu.Lock("CreateResourceDataSync")
	defer b.mu.Unlock()

	syncName := input.SyncName
	if syncName == "" {
		syncName = "default-sync"
	}

	syncs := b.resourceDataSyncsStore(region)
	if syncs.Has(syncName) {
		return nil, ErrResourceDataSyncExists
	}

	now := UnixTimeFloat(time.Now())
	syncs.Put(&ResourceDataSync{
		SyncName:        syncName,
		SyncType:        input.SyncType,
		S3Destination:   input.S3Destination,
		SyncSource:      input.SyncSource,
		LastStatus:      "InProgress",
		SyncCreatedTime: now,
		LastSyncTime:    now,
	})

	return &CreateResourceDataSyncOutput{}, nil
}

// DeleteResourceDataSync removes a resource data sync by name.
func (b *InMemoryBackend) DeleteResourceDataSync(
	ctx context.Context,
	input *DeleteResourceDataSyncInput,
) (*DeleteResourceDataSyncOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteResourceDataSync")
	defer b.mu.Unlock()

	if input.SyncName == "" {
		return &DeleteResourceDataSyncOutput{}, nil
	}

	syncs := b.resourceDataSyncsStore(region)
	if !syncs.Has(input.SyncName) {
		return nil, fmt.Errorf("%w: %q", ErrResourceDataSyncNotFound, input.SyncName)
	}

	syncs.Delete(input.SyncName)

	return &DeleteResourceDataSyncOutput{}, nil
}

// ListResourceDataSync returns resource data syncs, filtered by
// input.SyncType and paginated by input.MaxResults/NextToken -- real,
// optional ListResourceDataSyncInput members (api_op_ListResourceDataSync.go)
// a literal struct{} input previously discarded from every request.
func (b *InMemoryBackend) ListResourceDataSync(
	ctx context.Context,
	input *ListResourceDataSyncInput,
) (*ListResourceDataSyncOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListResourceDataSync")
	defer b.mu.RUnlock()

	syncs := b.resourceDataSyncsStore(region)
	items := make([]ResourceDataSync, 0, syncs.Len())

	for _, s := range syncs.All() {
		if input.SyncType != "" && s.SyncType != input.SyncType {
			continue
		}

		items = append(items, *s)
	}

	sort.Slice(items, func(i, k int) bool {
		return items[i].SyncName < items[k].SyncName
	})

	var maxResults int
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(items, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &ListResourceDataSyncOutputFull{ResourceDataSyncItems: page, NextToken: next}, nil
}

// UpdateResourceDataSync updates an existing resource data sync. SyncType and
// SyncSource are, along with SyncName, required UpdateResourceDataSyncInput
// members (verified against validateOpUpdateResourceDataSyncInput,
// validators.go); SyncSource's own SourceType/SourceRegions are required
// whenever SyncSource is present (validateResourceDataSyncSource). The
// pre-fix handler read only SyncName, silently no-opped (returned success)
// on an empty one instead of erroring, and never errored on an unknown sync
// name either -- this API only supports updating a SyncFromSource sync
// (doc comment on api_op_UpdateResourceDataSync.go), which is what the fix
// below now actually models.
func (b *InMemoryBackend) UpdateResourceDataSync(
	ctx context.Context,
	input *UpdateResourceDataSyncInput,
) (*UpdateResourceDataSyncOutput, error) {
	if input.SyncName == "" {
		return nil, fmt.Errorf("%w: SyncName is required", ErrValidationException)
	}

	if input.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidationException)
	}

	if input.SyncSource == nil {
		return nil, fmt.Errorf("%w: SyncSource is required", ErrValidationException)
	}

	if err := validateResourceDataSyncSource(input.SyncSource); err != nil {
		return nil, err
	}

	region := getRegion(ctx)
	b.mu.Lock("UpdateResourceDataSync")
	defer b.mu.Unlock()

	sync, exists := b.resourceDataSyncsStore(region).Get(input.SyncName)
	if !exists {
		return nil, ErrResourceDataSyncNotFound
	}

	sync.SyncType = input.SyncType
	sync.SyncSource = input.SyncSource
	sync.LastSyncTime = UnixTimeFloat(time.Now())

	return &UpdateResourceDataSyncOutput{}, nil
}

// DeregisterManagedInstance removes the activation associated with a managed instance ID.
// The InstanceID field is treated as the ActivationID in this in-memory implementation.
func (b *InMemoryBackend) DeregisterManagedInstance(
	ctx context.Context,
	input *DeregisterManagedInstanceInput,
) (*DeregisterManagedInstanceOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeregisterManagedInstance")
	defer b.mu.Unlock()

	// Try to match by ActivationID (InstanceID in the request maps to ActivationID here).
	activations := b.activationsStore(region)
	if activations.Has(input.InstanceID) {
		activations.Delete(input.InstanceID)
		delete(b.miscResourceTagsStore(region), input.InstanceID)
	}

	return &DeregisterManagedInstanceOutput{}, nil
}

// UpdateManagedInstanceRole updates the IAM role for a managed instance's activation.
func (b *InMemoryBackend) UpdateManagedInstanceRole(
	ctx context.Context,
	input *UpdateManagedInstanceRoleInput,
) (*UpdateManagedInstanceRoleOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateManagedInstanceRole")
	defer b.mu.Unlock()

	activations := b.activationsStore(region)
	if actPtr, exists := activations.Get(input.InstanceID); exists {
		act := *actPtr
		act.IamRole = input.IamRole
		activations.Put(&act)
	}

	return &UpdateManagedInstanceRoleOutput{}, nil
}

// DeleteActivation removes a stored activation by ID.
func (b *InMemoryBackend) DeleteActivation(
	ctx context.Context,
	input *DeleteActivationInput,
) (*DeleteActivationOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteActivation")
	defer b.mu.Unlock()

	activations := b.activationsStore(region)
	if !activations.Has(input.ActivationID) {
		return nil, ErrInvalidActivationID
	}

	activations.Delete(input.ActivationID)
	delete(b.miscResourceTagsStore(region), input.ActivationID)

	cleanupEmptyInnerMap(b.miscResourceTags, region)

	return &DeleteActivationOutput{}, nil
}

// matchesActivationFilter reports whether an activation satisfies a single
// DescribeActivationsFilter. FilterKey values outside the three the real API
// defines (ActivationIds/DefaultInstanceName/IamRole,
// types.DescribeActivationsFilterKeys) match every activation: this backend
// has no other attribute to filter on, and accept-and-echo mirrors the
// unknown-key handling ListNodes already established (instances.go).
func matchesActivationFilter(a Activation, f DescribeActivationsFilter) bool {
	switch f.FilterKey {
	case "ActivationIds":
		return slices.Contains(f.FilterValues, a.ActivationID)
	case "DefaultInstanceName":
		return slices.Contains(f.FilterValues, a.DefaultInstanceName)
	case "IamRole":
		return slices.Contains(f.FilterValues, a.IamRole)
	default:
		return true
	}
}

// DescribeActivations lists stored activations, filtered by input.Filters and
// paginated by input.MaxResults/NextToken -- real, optional
// DescribeActivationsInput members (api_op_DescribeActivations.go) a literal
// struct{} input previously discarded from every request.
func (b *InMemoryBackend) DescribeActivations(
	ctx context.Context,
	input *DescribeActivationsInput,
) (*DescribeActivationsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeActivations")
	defer b.mu.RUnlock()

	activations := b.activationsStore(region)
	miscTags := b.miscResourceTagsStore(region)
	list := make([]Activation, 0, activations.Len())

	for _, a := range activations.All() {
		cp := *a
		for k, v := range miscTags[a.ActivationID] {
			cp.Tags = append(cp.Tags, Tag{Key: k, Value: v})
		}
		sort.Slice(cp.Tags, func(i, j int) bool { return cp.Tags[i].Key < cp.Tags[j].Key })

		matched := true
		for _, f := range input.Filters {
			if !matchesActivationFilter(cp, f) {
				matched = false

				break
			}
		}

		if matched {
			list = append(list, cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ActivationID < list[j].ActivationID })

	var maxResults int
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(list, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &DescribeActivationsOutput{ActivationList: page, NextToken: next}, nil
}

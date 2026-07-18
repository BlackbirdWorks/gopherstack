package ssm

import (
	"context"
	"crypto/rand"
	"fmt"
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

// CreateResourceDataSync stores a new resource data sync configuration.
func (b *InMemoryBackend) CreateResourceDataSync(
	ctx context.Context,
	input *CreateResourceDataSyncInput,
) (*CreateResourceDataSyncOutput, error) {
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

	syncs.Put(&ResourceDataSync{
		SyncName:        syncName,
		SyncType:        input.SyncType,
		LastStatus:      "InProgress",
		SyncCreatedTime: time.Now().UTC(),
		LastSyncTime:    time.Now().UTC(),
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

// ListResourceDataSync returns all resource data syncs.
func (b *InMemoryBackend) ListResourceDataSync(
	ctx context.Context,
	_ *ListResourceDataSyncInput,
) (*ListResourceDataSyncOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListResourceDataSync")
	defer b.mu.RUnlock()

	syncs := b.resourceDataSyncsStore(region)
	items := make([]ResourceDataSync, 0, syncs.Len())
	for _, s := range syncs.All() {
		items = append(items, *s)
	}

	sort.Slice(items, func(i, k int) bool {
		return items[i].SyncName < items[k].SyncName
	})

	return &ListResourceDataSyncOutputFull{ResourceDataSyncItems: items}, nil
}

// UpdateResourceDataSync updates an existing resource data sync.
func (b *InMemoryBackend) UpdateResourceDataSync(
	ctx context.Context,
	input *UpdateResourceDataSyncInput,
) (*UpdateResourceDataSyncOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateResourceDataSync")
	defer b.mu.Unlock()

	if input.SyncName == "" {
		return &UpdateResourceDataSyncOutput{}, nil
	}

	if sync, exists := b.resourceDataSyncsStore(region).Get(input.SyncName); exists {
		sync.LastSyncTime = time.Now().UTC()
	}

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
		return nil, ErrActivationNotFound
	}

	activations.Delete(input.ActivationID)
	delete(b.miscResourceTagsStore(region), input.ActivationID)

	cleanupEmptyInnerMap(b.miscResourceTags, region)

	return &DeleteActivationOutput{}, nil
}

// DescribeActivations lists stored activations.
func (b *InMemoryBackend) DescribeActivations(
	ctx context.Context,
	_ *DescribeActivationsInput,
) (*DescribeActivationsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeActivations")
	defer b.mu.RUnlock()

	activations := b.activationsStore(region)
	list := make([]Activation, 0, activations.Len())
	for _, a := range activations.All() {
		list = append(list, *a)
	}

	return &DescribeActivationsOutput{ActivationList: list}, nil
}

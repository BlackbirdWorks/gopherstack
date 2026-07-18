package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotebookNotFound is returned when a notebook instance does not exist.
	ErrNotebookNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrNotebookAlreadyExists is returned when a notebook instance already exists.
	ErrNotebookAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrNotebookLifecycleConfigNotFound is returned when a lifecycle config does not exist.
	ErrNotebookLifecycleConfigNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrNotebookLifecycleConfigAlreadyExists is returned when a lifecycle config already exists.
	ErrNotebookLifecycleConfigAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

type NotebookInstance struct {
	CreationTime               time.Time         `json:"CreationTime"`
	LastModifiedTime           time.Time         `json:"LastModifiedTime"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	RootAccess                 string            `json:"RootAccess,omitempty"`
	KmsKeyID                   string            `json:"KmsKeyId,omitempty"`
	URL                        string            `json:"Url,omitempty"`
	NotebookInstanceName       string            `json:"NotebookInstanceName"`
	NotebookInstanceArn        string            `json:"NotebookInstanceArn"`
	NotebookInstanceStatus     string            `json:"NotebookInstanceStatus"`
	InstanceType               string            `json:"InstanceType,omitempty"`
	RoleArn                    string            `json:"RoleArn,omitempty"`
	SubnetID                   string            `json:"SubnetId,omitempty"`
	PlatformIdentifier         string            `json:"PlatformIdentifier,omitempty"`
	LifecycleConfigName        string            `json:"NotebookInstanceLifecycleConfigName,omitempty"`
	DirectInternetAccess       string            `json:"DirectInternetAccess,omitempty"`
	DefaultCodeRepository      string            `json:"DefaultCodeRepository,omitempty"`
	SecurityGroupIDs           []string          `json:"SecurityGroupIds,omitempty"`
	AcceleratorTypes           []string          `json:"AcceleratorTypes,omitempty"`
	AdditionalCodeRepositories []string          `json:"AdditionalCodeRepositories,omitempty"`
	VolumeSizeInGB             int32             `json:"VolumeSizeInGB,omitempty"`
}

// cloneNotebook returns a deep copy of nb.
func cloneNotebook(nb *NotebookInstance) *NotebookInstance {
	cp := *nb
	cp.Tags = maps.Clone(nb.Tags)
	cp.SecurityGroupIDs = append([]string(nil), nb.SecurityGroupIDs...)
	cp.AcceleratorTypes = append([]string(nil), nb.AcceleratorTypes...)
	cp.AdditionalCodeRepositories = append([]string(nil), nb.AdditionalCodeRepositories...)

	return &cp
}

// HyperParameterTuningJob represents a SageMaker hyperparameter tuning job.

// ---------------------------------------------------------------------------
// NotebookInstance
// ---------------------------------------------------------------------------

// CreateNotebookInstance creates a new notebook instance.
func (b *InMemoryBackend) CreateNotebookInstance(
	ctx context.Context,
	name, instanceType, roleArn string,
	tags map[string]string,
) (*NotebookInstance, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", ErrValidation)
	}

	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrValidation)
	}

	if roleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	b.mu.Lock("CreateNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.notebooksStore(region).Get(name); ok {
		return nil, fmt.Errorf(
			"%w: notebook instance %s already exists",
			ErrNotebookAlreadyExists,
			name,
		)
	}

	nbARN := arn.Build("sagemaker", region, b.accountID, "notebook-instance/"+name)
	now := time.Now()
	nb := &NotebookInstance{
		NotebookInstanceName:   name,
		NotebookInstanceArn:    nbARN,
		NotebookInstanceStatus: notebookStatusPending,
		InstanceType:           instanceType,
		RoleArn:                roleArn,
		CreationTime:           now,
		LastModifiedTime:       now,
		Tags:                   mergeTags(nil, tags),
	}
	b.notebooksStore(region).Put(nb)
	b.notebookARNIndexStore(region)[nbARN] = name

	return cloneNotebook(nb), nil
}

// DescribeNotebookInstance returns a notebook instance by name.
func (b *InMemoryBackend) DescribeNotebookInstance(ctx context.Context, name string) (*NotebookInstance, error) {
	b.mu.RLock("DescribeNotebookInstance")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	return cloneNotebook(nb), nil
}

// ListNotebookInstancesFilter narrows ListNotebookInstances results.
// Empty fields are treated as wildcards.
type ListNotebookInstancesFilter struct {
	StatusEquals string
	NameContains string
}

// ListNotebookInstances returns notebook instances sorted by name with optional pagination
// and AWS-style filters: StatusEquals (exact, case-insensitive) and NameContains
// (substring, case-insensitive).
func (b *InMemoryBackend) ListNotebookInstances(
	ctx context.Context,
	nextToken string,
	filter ListNotebookInstancesFilter,
) ([]*NotebookInstance, string) {
	b.mu.RLock("ListNotebookInstances")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	store := b.notebooksStoreRO(region)
	list := make([]*NotebookInstance, 0, store.Len())
	for _, nb := range store.All() {
		if !matchesNotebookFilter(nb, filter) {
			continue
		}

		list = append(list, cloneNotebook(nb))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].NotebookInstanceName < list[j].NotebookInstanceName
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*NotebookInstance{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string
	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// matchesNotebookFilter reports whether nb satisfies the provided filter.
func matchesNotebookFilter(nb *NotebookInstance, f ListNotebookInstancesFilter) bool {
	if f.StatusEquals != "" && !strings.EqualFold(nb.NotebookInstanceStatus, f.StatusEquals) {
		return false
	}

	if f.NameContains != "" &&
		!strings.Contains(
			strings.ToLower(nb.NotebookInstanceName),
			strings.ToLower(f.NameContains),
		) {
		return false
	}

	return true
}

// DeleteNotebookInstance removes a notebook instance from the backend.
func (b *InMemoryBackend) DeleteNotebookInstance(ctx context.Context, name string) error {
	b.mu.Lock("DeleteNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	arnIdx := b.notebookARNIndexStore(region)
	delete(arnIdx, nb.NotebookInstanceArn)
	store := b.notebooksStore(region)
	store.Delete(name)

	return nil
}

// StartNotebookInstance transitions a notebook instance to InService.
func (b *InMemoryBackend) StartNotebookInstance(ctx context.Context, name string) error {
	b.mu.Lock("StartNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	nb.NotebookInstanceStatus = statusInService
	nb.LastModifiedTime = time.Now()

	return nil
}

// StopNotebookInstance transitions a notebook instance to Stopped.
func (b *InMemoryBackend) StopNotebookInstance(ctx context.Context, name string) error {
	b.mu.Lock("StopNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	nb.NotebookInstanceStatus = notebookStatusStopped
	nb.LastModifiedTime = time.Now()

	return nil
}

// UpdateNotebookInstance updates a notebook instance's instance type.
func (b *InMemoryBackend) UpdateNotebookInstance(ctx context.Context, name, instanceType string) error {
	b.mu.Lock("UpdateNotebookInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	if instanceType != "" {
		nb.InstanceType = instanceType
	}
	nb.LastModifiedTime = time.Now()

	return nil
}

// CreatePresignedNotebookInstanceURL returns a presigned URL for a notebook instance.
func (b *InMemoryBackend) CreatePresignedNotebookInstanceURL(ctx context.Context, name string) (string, error) {
	b.mu.RLock("CreatePresignedNotebookInstanceURL")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStoreRO(region).Get(name)
	if !ok {
		return "", fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	url := "https://" + nb.NotebookInstanceArn + ".notebook.sagemaker.aws/lab"

	return url, nil
}

// ---------------------------------------------------------------------------
// NotebookInstanceLifecycleConfig (#3)
// ---------------------------------------------------------------------------

// NotebookLifecycleHook is a single lifecycle script entry.
type NotebookLifecycleHook struct {
	Content string `json:"Content,omitempty"` // base64-encoded shell script
}

// NotebookInstanceLifecycleConfig stores Create/Start lifecycle scripts.
type NotebookInstanceLifecycleConfig struct {
	CreationTime     time.Time               `json:"CreationTime"`
	LastModifiedTime time.Time               `json:"LastModifiedTime"`
	Name             string                  `json:"NotebookInstanceLifecycleConfigName"`
	ARN              string                  `json:"NotebookInstanceLifecycleConfigArn"`
	OnCreate         []NotebookLifecycleHook `json:"OnCreate,omitempty"`
	OnStart          []NotebookLifecycleHook `json:"OnStart,omitempty"`
}

// cloneNotebookLifecycleConfig returns a deep copy.
func cloneNotebookLifecycleConfig(
	lc *NotebookInstanceLifecycleConfig,
) *NotebookInstanceLifecycleConfig {
	cp := *lc
	cp.OnCreate = make([]NotebookLifecycleHook, len(lc.OnCreate))
	copy(cp.OnCreate, lc.OnCreate)
	cp.OnStart = make([]NotebookLifecycleHook, len(lc.OnStart))
	copy(cp.OnStart, lc.OnStart)

	return &cp
}

// CreateNotebookInstanceLifecycleConfig creates a new lifecycle config.
func (b *InMemoryBackend) CreateNotebookInstanceLifecycleConfig(
	ctx context.Context,
	name string,
	onCreate, onStart []NotebookLifecycleHook,
) (*NotebookInstanceLifecycleConfig, error) {
	b.mu.Lock("CreateNotebookInstanceLifecycleConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.notebookLifecycleConfigsStore(region).Get(name); ok {
		return nil, fmt.Errorf(
			"%w: notebook lifecycle config %s already exists",
			ErrNotebookLifecycleConfigAlreadyExists,
			name,
		)
	}

	lcARN := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		"notebook-instance-lifecycle-config/"+name,
	)
	now := time.Now()
	lc := &NotebookInstanceLifecycleConfig{
		Name:             name,
		ARN:              lcARN,
		OnCreate:         onCreate,
		OnStart:          onStart,
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.notebookLifecycleConfigsStore(region).Put(lc)

	return cloneNotebookLifecycleConfig(lc), nil
}

// DescribeNotebookInstanceLifecycleConfig returns a lifecycle config by name.
func (b *InMemoryBackend) DescribeNotebookInstanceLifecycleConfig(
	ctx context.Context,
	name string,
) (*NotebookInstanceLifecycleConfig, error) {
	b.mu.RLock("DescribeNotebookInstanceLifecycleConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	lc, ok := b.notebookLifecycleConfigsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: notebook lifecycle config %q not found",
			ErrNotebookLifecycleConfigNotFound,
			name,
		)
	}

	return cloneNotebookLifecycleConfig(lc), nil
}

// UpdateNotebookInstanceLifecycleConfig replaces onCreate/onStart scripts.
func (b *InMemoryBackend) UpdateNotebookInstanceLifecycleConfig(
	ctx context.Context,
	name string,
	onCreate, onStart []NotebookLifecycleHook,
) (*NotebookInstanceLifecycleConfig, error) {
	b.mu.Lock("UpdateNotebookInstanceLifecycleConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	lc, ok := b.notebookLifecycleConfigsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: notebook lifecycle config %q not found",
			ErrNotebookLifecycleConfigNotFound,
			name,
		)
	}

	if onCreate != nil {
		lc.OnCreate = onCreate
	}
	if onStart != nil {
		lc.OnStart = onStart
	}
	lc.LastModifiedTime = time.Now()

	return cloneNotebookLifecycleConfig(lc), nil
}

// DeleteNotebookInstanceLifecycleConfig removes a lifecycle config.
func (b *InMemoryBackend) DeleteNotebookInstanceLifecycleConfig(ctx context.Context, name string) error {
	b.mu.Lock("DeleteNotebookInstanceLifecycleConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.notebookLifecycleConfigsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf(
			"%w: notebook lifecycle config %q not found",
			ErrNotebookLifecycleConfigNotFound,
			name,
		)
	}

	store.Delete(name)

	return nil
}

// ListNotebookInstanceLifecycleConfigs returns lifecycle configs sorted by name.
func (b *InMemoryBackend) ListNotebookInstanceLifecycleConfigs(
	ctx context.Context,
	nextToken string,
) ([]*NotebookInstanceLifecycleConfig, string) {
	b.mu.RLock("ListNotebookInstanceLifecycleConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.notebookLifecycleConfigsStoreRO(region), nextToken, cloneNotebookLifecycleConfig,
		func(a, b *NotebookInstanceLifecycleConfig) bool { return a.Name < b.Name })
}

// ---------------------------------------------------------------------------
// Notebook lifecycle FSM simulator (#2)
// ---------------------------------------------------------------------------

// scheduleNotebookTransition asynchronously transitions a notebook to nextStatus after delay.
// Must be called while holding b.mu (runDelayed captures the lifecycle context).
func (b *InMemoryBackend) scheduleNotebookTransition(
	ctx context.Context,
	name, nextStatus string,
	delay time.Duration,
) {
	region := getRegion(ctx, b.region)
	b.runDelayed(ctx, delay, func() {
		b.mu.Lock("scheduleNotebookTransition.goroutine")
		defer b.mu.Unlock()

		if nb, ok := b.notebooksStore(region).Get(name); ok {
			nb.NotebookInstanceStatus = nextStatus
			nb.LastModifiedTime = time.Now()
		}
	})
}

// StartNotebookInstanceFSM transitions: Stopped → Pending, then Pending → InService.
func (b *InMemoryBackend) StartNotebookInstanceFSM(ctx context.Context, name string) error {
	b.mu.Lock("StartNotebookInstanceFSM")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	if nb.NotebookInstanceStatus != notebookStatusStopped {
		return fmt.Errorf(
			"%w: notebook %q is not Stopped (status=%s)",
			ErrValidation,
			name,
			nb.NotebookInstanceStatus,
		)
	}

	nb.NotebookInstanceStatus = notebookStatusPending
	nb.LastModifiedTime = time.Now()
	b.scheduleNotebookTransition(
		b.lifecycleCtx,
		name,
		statusInService,
		notebookPendingToInServiceDelay,
	)

	return nil
}

// StopNotebookInstanceFSM transitions: InService → Stopping, then Stopping → Stopped.
func (b *InMemoryBackend) StopNotebookInstanceFSM(ctx context.Context, name string) error {
	b.mu.Lock("StopNotebookInstanceFSM")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	if nb.NotebookInstanceStatus != statusInService {
		return fmt.Errorf(
			"%w: notebook %q is not InService (status=%s)",
			ErrValidation,
			name,
			nb.NotebookInstanceStatus,
		)
	}

	nb.NotebookInstanceStatus = notebookStatusStopping
	nb.LastModifiedTime = time.Now()
	b.scheduleNotebookTransition(b.lifecycleCtx, name, notebookStatusStopped, notebookStoppingToStoppedDelay)

	return nil
}

// CreateNotebookInstanceFSM creates a notebook and immediately schedules Pending → InService.
func (b *InMemoryBackend) CreateNotebookInstanceFSM(
	ctx context.Context,
	opts NotebookInstanceOptions,
) (*NotebookInstance, error) {
	b.mu.RLock("CreateNotebookInstanceFSM.ctx")
	lifecycleCtx := b.lifecycleCtx
	b.mu.RUnlock()

	nb, err := b.CreateNotebookInstanceFull(ctx, opts)
	if err != nil {
		return nil, err
	}
	b.scheduleNotebookTransition(lifecycleCtx, opts.Name, statusInService, notebookPendingToInServiceDelay)

	return nb, nil
}

// UpdateNotebookInstanceFull updates all mutable fields on a notebook.
func (b *InMemoryBackend) UpdateNotebookInstanceFull(
	ctx context.Context,
	name string,
	opts NotebookUpdateOptions,
) error {
	b.mu.Lock("UpdateNotebookInstanceFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	nb, ok := b.notebooksStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: notebook instance %q not found", ErrNotebookNotFound, name)
	}

	if nb.NotebookInstanceStatus != notebookStatusStopped {
		return fmt.Errorf(
			"%w: notebook instance %q is in %s status and cannot be updated",
			ErrValidation, name, nb.NotebookInstanceStatus,
		)
	}

	if opts.InstanceType != "" {
		nb.InstanceType = opts.InstanceType
	}
	if opts.RoleArn != "" {
		nb.RoleArn = opts.RoleArn
	}
	if opts.LifecycleConfigName != "" {
		nb.LifecycleConfigName = opts.LifecycleConfigName
	}
	if opts.DisassociateLifecycleConfig {
		nb.LifecycleConfigName = ""
	}
	if opts.VolumeSizeInGB > 0 {
		nb.VolumeSizeInGB = opts.VolumeSizeInGB
	}
	if opts.DefaultCodeRepository != "" {
		nb.DefaultCodeRepository = opts.DefaultCodeRepository
	}
	if opts.DisassociateDefaultCodeRepository {
		nb.DefaultCodeRepository = ""
	}
	if opts.AdditionalCodeRepositories != nil {
		nb.AdditionalCodeRepositories = opts.AdditionalCodeRepositories
	}
	if opts.DisassociateAdditionalCodeRepositories {
		nb.AdditionalCodeRepositories = nil
	}

	nb.LastModifiedTime = time.Now()

	return nil
}

// NotebookUpdateOptions holds mutable fields for UpdateNotebookInstance.
type NotebookUpdateOptions struct {
	InstanceType                           string
	RoleArn                                string
	LifecycleConfigName                    string
	DefaultCodeRepository                  string
	AdditionalCodeRepositories             []string
	VolumeSizeInGB                         int32
	DisassociateLifecycleConfig            bool
	DisassociateDefaultCodeRepository      bool
	DisassociateAdditionalCodeRepositories bool
}

// ---------------------------------------------------------------------------
// NotebookInstanceOptions for gap #1 (full field set)
// ---------------------------------------------------------------------------

// NotebookInstanceOptions holds all CreateNotebookInstance request fields.
type NotebookInstanceOptions struct {
	Tags                       map[string]string `json:"Tags,omitempty"`
	SubnetID                   string            `json:"SubnetId,omitempty"`
	LifecycleConfigName        string            `json:"LifecycleConfigName,omitempty"`
	Name                       string            `json:"NotebookInstanceName"`
	InstanceType               string            `json:"InstanceType"`
	RoleArn                    string            `json:"RoleArn"`
	RootAccess                 string            `json:"RootAccess,omitempty"`
	KmsKeyID                   string            `json:"KmsKeyId,omitempty"`
	DirectInternetAccess       string            `json:"DirectInternetAccess,omitempty"`
	DefaultCodeRepository      string            `json:"DefaultCodeRepository,omitempty"`
	PlatformIdentifier         string            `json:"PlatformIdentifier,omitempty"`
	AcceleratorTypes           []string          `json:"AcceleratorTypes,omitempty"`
	AdditionalCodeRepositories []string          `json:"AdditionalCodeRepositories,omitempty"`
	SecurityGroupIDs           []string          `json:"SecurityGroupIds,omitempty"`
	VolumeSizeInGB             int32             `json:"VolumeSizeInGB,omitempty"`
}

// CreateNotebookInstanceFull persists all NotebookInstanceOptions fields.
func (b *InMemoryBackend) CreateNotebookInstanceFull(
	ctx context.Context,
	opts NotebookInstanceOptions,
) (*NotebookInstance, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", ErrValidation)
	}
	if opts.InstanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrValidation)
	}
	if opts.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	b.mu.Lock("CreateNotebookInstanceFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.notebooksStore(region).Get(opts.Name); ok {
		return nil, fmt.Errorf(
			"%w: notebook instance %s already exists",
			ErrNotebookAlreadyExists,
			opts.Name,
		)
	}

	nbARN := arn.Build("sagemaker", region, b.accountID, "notebook-instance/"+opts.Name)
	now := time.Now()
	nb := &NotebookInstance{
		NotebookInstanceName:       opts.Name,
		NotebookInstanceArn:        nbARN,
		NotebookInstanceStatus:     "Pending",
		InstanceType:               opts.InstanceType,
		RoleArn:                    opts.RoleArn,
		SubnetID:                   opts.SubnetID,
		SecurityGroupIDs:           append([]string(nil), opts.SecurityGroupIDs...),
		KmsKeyID:                   opts.KmsKeyID,
		LifecycleConfigName:        opts.LifecycleConfigName,
		DirectInternetAccess:       opts.DirectInternetAccess,
		RootAccess:                 opts.RootAccess,
		AcceleratorTypes:           append([]string(nil), opts.AcceleratorTypes...),
		AdditionalCodeRepositories: append([]string(nil), opts.AdditionalCodeRepositories...),
		DefaultCodeRepository:      opts.DefaultCodeRepository,
		VolumeSizeInGB:             opts.VolumeSizeInGB,
		PlatformIdentifier:         opts.PlatformIdentifier,
		CreationTime:               now,
		LastModifiedTime:           now,
		Tags:                       mergeTags(nil, opts.Tags),
	}
	b.notebooksStore(region).Put(nb)
	b.notebookARNIndexStore(region)[nbARN] = opts.Name

	return cloneNotebook(nb), nil
}

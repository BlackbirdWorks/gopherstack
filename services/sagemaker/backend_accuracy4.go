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
)

const (
	statusCreating       = "Creating"
	deviceCompositeparts = 2
)

// ---------------------------------------------------------------------------
// DeviceFleet
// ---------------------------------------------------------------------------

var (
	// ErrDeviceFleetNotFound is returned when a device fleet does not exist.
	ErrDeviceFleetNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrDeviceFleetAlreadyExists is returned when a device fleet already exists.
	ErrDeviceFleetAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// DeviceFleet represents a SageMaker device fleet.
type DeviceFleet struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	DeviceFleetName  string            `json:"DeviceFleetName"`
	DeviceFleetArn   string            `json:"DeviceFleetArn"`
	Description      string            `json:"Description,omitempty"`
	RoleArn          string            `json:"RoleArn,omitempty"`
}

func cloneDeviceFleet(f *DeviceFleet) *DeviceFleet {
	cp := *f
	cp.Tags = maps.Clone(f.Tags)

	return &cp
}

// CreateDeviceFleetOptions holds input fields for CreateDeviceFleet.
type CreateDeviceFleetOptions struct {
	Tags            map[string]string
	DeviceFleetName string
	Description     string
	RoleArn         string
}

// CreateDeviceFleet creates a SageMaker device fleet.
func (b *InMemoryBackend) CreateDeviceFleet(ctx context.Context, opts CreateDeviceFleetOptions) (*DeviceFleet, error) {
	b.mu.Lock("CreateDeviceFleet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.DeviceFleetName == "" {
		return nil, fmt.Errorf("%w: DeviceFleetName is required", ErrValidation)
	}

	if _, ok := b.deviceFleetsStore(region)[opts.DeviceFleetName]; ok {
		return nil, fmt.Errorf("%w: device fleet %q already exists", ErrDeviceFleetAlreadyExists, opts.DeviceFleetName)
	}

	fleetARN := arn.Build("sagemaker", region, b.accountID, "device-fleet/"+opts.DeviceFleetName)
	now := time.Now()

	f := &DeviceFleet{
		DeviceFleetName:  opts.DeviceFleetName,
		DeviceFleetArn:   fleetARN,
		Description:      opts.Description,
		RoleArn:          opts.RoleArn,
		Tags:             mergeTags(nil, opts.Tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.deviceFleetsStore(region)[opts.DeviceFleetName] = f

	return cloneDeviceFleet(f), nil
}

// DescribeDeviceFleet returns a device fleet by name.
func (b *InMemoryBackend) DescribeDeviceFleet(ctx context.Context, name string) (*DeviceFleet, error) {
	b.mu.RLock("DescribeDeviceFleet")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	f, ok := b.deviceFleetsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, name)
	}

	return cloneDeviceFleet(f), nil
}

// ListDeviceFleets returns all device fleets with pagination.
func (b *InMemoryBackend) ListDeviceFleets(ctx context.Context, nextToken string) ([]*DeviceFleet, string) {
	b.mu.RLock("ListDeviceFleets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(b.deviceFleetsStore(region), nextToken, cloneDeviceFleet)
}

// UpdateDeviceFleet updates a device fleet's description or role ARN.
func (b *InMemoryBackend) UpdateDeviceFleet(ctx context.Context, name, description, roleArn string) error {
	b.mu.Lock("UpdateDeviceFleet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	f, ok := b.deviceFleetsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, name)
	}

	if description != "" {
		f.Description = description
	}

	if roleArn != "" {
		f.RoleArn = roleArn
	}

	f.LastModifiedTime = time.Now()

	return nil
}

// DeleteDeviceFleet deletes a device fleet by name.
func (b *InMemoryBackend) DeleteDeviceFleet(ctx context.Context, name string) error {
	b.mu.Lock("DeleteDeviceFleet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.deviceFleetsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// Device
// ---------------------------------------------------------------------------

// ErrDeviceNotFound is returned when a device does not exist.
var ErrDeviceNotFound = awserr.New("ValidationException", awserr.ErrNotFound)

// deviceKey uniquely identifies a device within a fleet.
type deviceKey struct {
	fleetName  string
	deviceName string
}

// Device represents a SageMaker edge device registered in a fleet.
type Device struct {
	RegistrationTime time.Time         `json:"RegistrationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	DeviceName       string            `json:"DeviceName"`
	DeviceFleetName  string            `json:"DeviceFleetName"`
	DeviceArn        string            `json:"DeviceArn"`
	Description      string            `json:"Description,omitempty"`
	IotThingName     string            `json:"IotThingName,omitempty"`
}

func cloneDevice(d *Device) *Device {
	cp := *d
	cp.Tags = maps.Clone(d.Tags)

	return &cp
}

// RegisterDeviceInput is a single device to register.
type RegisterDeviceInput struct {
	Tags         map[string]string
	DeviceName   string
	Description  string
	IotThingName string
}

// RegisterDevices registers devices to a device fleet.
func (b *InMemoryBackend) RegisterDevices(ctx context.Context, fleetName string, devices []RegisterDeviceInput) error {
	b.mu.Lock("RegisterDevices")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.deviceFleetsStore(region)[fleetName]; !ok {
		return fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, fleetName)
	}

	now := time.Now()
	devicesStore := b.devicesStore(region)
	for _, d := range devices {
		if d.DeviceName == "" {
			continue
		}

		k := deviceKey{fleetName: fleetName, deviceName: d.DeviceName}
		deviceARN := arn.Build("sagemaker", region, b.accountID, "device/"+d.DeviceName)
		devicesStore[k] = &Device{
			DeviceName:       d.DeviceName,
			DeviceFleetName:  fleetName,
			DeviceArn:        deviceARN,
			Description:      d.Description,
			IotThingName:     d.IotThingName,
			Tags:             mergeTags(nil, d.Tags),
			RegistrationTime: now,
			LastModifiedTime: now,
		}
	}

	return nil
}

// DeregisterDevices removes devices from a device fleet.
func (b *InMemoryBackend) DeregisterDevices(ctx context.Context, fleetName string, deviceNames []string) error {
	b.mu.Lock("DeregisterDevices")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.devicesStore(region)

	for _, name := range deviceNames {
		delete(store, deviceKey{fleetName: fleetName, deviceName: name})
	}

	return nil
}

// DescribeDevice returns a device by fleet and device name.
func (b *InMemoryBackend) DescribeDevice(ctx context.Context, fleetName, deviceName string) (*Device, error) {
	b.mu.RLock("DescribeDevice")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	d, ok := b.devicesStore(region)[deviceKey{fleetName: fleetName, deviceName: deviceName}]
	if !ok {
		return nil, fmt.Errorf("%w: device %q in fleet %q", ErrDeviceNotFound, deviceName, fleetName)
	}

	return cloneDevice(d), nil
}

// ListDevices returns devices, optionally filtered by fleet name.
func (b *InMemoryBackend) ListDevices(ctx context.Context, fleetFilter, nextToken string) ([]*Device, string) {
	b.mu.RLock("ListDevices")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.devicesStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
		if fleetFilter != "" && k.fleetName != fleetFilter {
			continue
		}

		keys = append(keys, k.fleetName+"/"+k.deviceName)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*Device, 0, end-start)
	for _, composite := range keys[start:end] {
		parts := strings.SplitN(composite, "/", deviceCompositeparts)
		if len(parts) != deviceCompositeparts {
			continue
		}

		if d, ok := store[deviceKey{fleetName: parts[0], deviceName: parts[1]}]; ok {
			out = append(out, cloneDevice(d))
		}
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// InferenceComponent
// ---------------------------------------------------------------------------

var (
	// ErrInferenceComponentNotFound is returned when an inference component does not exist.
	ErrInferenceComponentNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrInferenceComponentAlreadyExists is returned when an inference component already exists.
	ErrInferenceComponentAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// InferenceComponent represents a SageMaker inference component.
type InferenceComponent struct {
	CreationTime             time.Time         `json:"CreationTime"`
	LastModifiedTime         time.Time         `json:"LastModifiedTime"`
	Tags                     map[string]string `json:"Tags,omitempty"`
	InferenceComponentName   string            `json:"InferenceComponentName"`
	InferenceComponentArn    string            `json:"InferenceComponentArn"`
	EndpointName             string            `json:"EndpointName"`
	VariantName              string            `json:"VariantName,omitempty"`
	InferenceComponentStatus string            `json:"InferenceComponentStatus"`
	CopyCount                int               `json:"CopyCount,omitempty"`
	CurrentCopyCount         int               `json:"CurrentCopyCount,omitempty"`
}

func cloneInferenceComponent(c *InferenceComponent) *InferenceComponent {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// CreateInferenceComponentOptions holds input fields for CreateInferenceComponent.
type CreateInferenceComponentOptions struct {
	Tags                   map[string]string
	InferenceComponentName string
	EndpointName           string
	VariantName            string
	CopyCount              int
}

// CreateInferenceComponent creates a SageMaker inference component.
func (b *InMemoryBackend) CreateInferenceComponent(
	ctx context.Context,
	opts CreateInferenceComponentOptions,
) (*InferenceComponent, error) {
	b.mu.Lock("CreateInferenceComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.InferenceComponentName == "" {
		return nil, fmt.Errorf("%w: InferenceComponentName is required", ErrValidation)
	}

	if _, ok := b.inferenceComponentsStore(region)[opts.InferenceComponentName]; ok {
		return nil, fmt.Errorf(
			"%w: inference component %q already exists",
			ErrInferenceComponentAlreadyExists,
			opts.InferenceComponentName,
		)
	}

	compARN := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		"inference-component/"+opts.InferenceComponentName,
	)
	now := time.Now()

	c := &InferenceComponent{
		InferenceComponentName:   opts.InferenceComponentName,
		InferenceComponentArn:    compARN,
		EndpointName:             opts.EndpointName,
		VariantName:              opts.VariantName,
		InferenceComponentStatus: statusCreating,
		CopyCount:                opts.CopyCount,
		CurrentCopyCount:         0,
		Tags:                     mergeTags(nil, opts.Tags),
		CreationTime:             now,
		LastModifiedTime:         now,
	}
	b.inferenceComponentsStore(region)[opts.InferenceComponentName] = c

	return cloneInferenceComponent(c), nil
}

// DescribeInferenceComponent returns an inference component by name.
func (b *InMemoryBackend) DescribeInferenceComponent(ctx context.Context, name string) (*InferenceComponent, error) {
	b.mu.RLock("DescribeInferenceComponent")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, ok := b.inferenceComponentsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: inference component %q", ErrInferenceComponentNotFound, name)
	}

	return cloneInferenceComponent(c), nil
}

// ListInferenceComponents returns all inference components with pagination.
func (b *InMemoryBackend) ListInferenceComponents(
	ctx context.Context,
	endpointFilter, nextToken string,
) ([]*InferenceComponent, string) {
	b.mu.RLock("ListInferenceComponents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.inferenceComponentsStore(region)

	keys := make([]string, 0, len(store))
	for k, c := range store {
		if endpointFilter != "" && c.EndpointName != endpointFilter {
			continue
		}

		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*InferenceComponent, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneInferenceComponent(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// UpdateInferenceComponent updates an inference component's variant or copy count.
func (b *InMemoryBackend) UpdateInferenceComponent(ctx context.Context, name, variantName string, copyCount int) error {
	b.mu.Lock("UpdateInferenceComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, ok := b.inferenceComponentsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: inference component %q", ErrInferenceComponentNotFound, name)
	}

	if variantName != "" {
		c.VariantName = variantName
	}

	if copyCount > 0 {
		c.CopyCount = copyCount
	}

	c.LastModifiedTime = time.Now()

	return nil
}

// UpdateInferenceComponentRuntimeConfig updates the copy count for an inference component.
func (b *InMemoryBackend) UpdateInferenceComponentRuntimeConfig(ctx context.Context, name string, copyCount int) error {
	b.mu.Lock("UpdateInferenceComponentRuntimeConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, ok := b.inferenceComponentsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: inference component %q", ErrInferenceComponentNotFound, name)
	}

	c.CopyCount = copyCount
	c.CurrentCopyCount = copyCount
	c.LastModifiedTime = time.Now()

	return nil
}

// DeleteInferenceComponent deletes an inference component by name.
func (b *InMemoryBackend) DeleteInferenceComponent(ctx context.Context, name string) error {
	b.mu.Lock("DeleteInferenceComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.inferenceComponentsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: inference component %q", ErrInferenceComponentNotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// ClusterSchedulerConfig
// ---------------------------------------------------------------------------

var (
	// ErrClusterSchedulerConfigNotFound is returned when a cluster scheduler config does not exist.
	ErrClusterSchedulerConfigNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrClusterSchedulerConfigAlreadyExists is returned when a cluster scheduler config already exists.
	ErrClusterSchedulerConfigAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// ClusterSchedulerConfig represents a SageMaker cluster scheduler configuration.
type ClusterSchedulerConfig struct {
	CreationTime               time.Time         `json:"CreationTime"`
	LastModifiedTime           time.Time         `json:"LastModifiedTime"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	ClusterSchedulerConfigName string            `json:"ClusterSchedulerConfigName"`
	ClusterSchedulerConfigArn  string            `json:"ClusterSchedulerConfigArn"`
	ClusterArn                 string            `json:"ClusterArn,omitempty"`
	Status                     string            `json:"Status"`
}

func cloneClusterSchedulerConfig(c *ClusterSchedulerConfig) *ClusterSchedulerConfig {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// CreateClusterSchedulerConfigOptions holds input fields for CreateClusterSchedulerConfig.
type CreateClusterSchedulerConfigOptions struct {
	Tags                       map[string]string
	ClusterSchedulerConfigName string
	ClusterArn                 string
}

// CreateClusterSchedulerConfig creates a SageMaker cluster scheduler configuration.
func (b *InMemoryBackend) CreateClusterSchedulerConfig(
	ctx context.Context,
	opts CreateClusterSchedulerConfigOptions,
) (*ClusterSchedulerConfig, error) {
	if opts.ClusterSchedulerConfigName == "" {
		return nil, fmt.Errorf("%w: ClusterSchedulerConfigName is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateClusterSchedulerConfig", opts.ClusterSchedulerConfigName, "cluster-scheduler-config",
		b.clusterSchedulerConfigsStore,
		func(n string) error {
			return fmt.Errorf(
				"%w: cluster scheduler config %q already exists",
				ErrClusterSchedulerConfigAlreadyExists,
				n,
			)
		},
		func(arnStr string, now time.Time) *ClusterSchedulerConfig {
			return &ClusterSchedulerConfig{
				ClusterSchedulerConfigName: opts.ClusterSchedulerConfigName,
				ClusterSchedulerConfigArn:  arnStr,
				ClusterArn:                 opts.ClusterArn,
				Status:                     statusCreating,
				Tags:                       mergeTags(nil, opts.Tags),
				CreationTime:               now,
				LastModifiedTime:           now,
			}
		},
		cloneClusterSchedulerConfig,
	)
}

// DescribeClusterSchedulerConfig returns a cluster scheduler config by name.
func (b *InMemoryBackend) DescribeClusterSchedulerConfig(
	ctx context.Context,
	name string,
) (*ClusterSchedulerConfig, error) {
	b.mu.RLock("DescribeClusterSchedulerConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, ok := b.clusterSchedulerConfigsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, name)
	}

	return cloneClusterSchedulerConfig(c), nil
}

// ListClusterSchedulerConfigs returns all cluster scheduler configs with pagination.
func (b *InMemoryBackend) ListClusterSchedulerConfigs(
	ctx context.Context,
	nextToken string,
) ([]*ClusterSchedulerConfig, string) {
	b.mu.RLock("ListClusterSchedulerConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(b.clusterSchedulerConfigsStore(region), nextToken, cloneClusterSchedulerConfig)
}

// UpdateClusterSchedulerConfig updates a cluster scheduler config's cluster ARN.
func (b *InMemoryBackend) UpdateClusterSchedulerConfig(ctx context.Context, name, clusterArn string) error {
	b.mu.Lock("UpdateClusterSchedulerConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, ok := b.clusterSchedulerConfigsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, name)
	}

	if clusterArn != "" {
		c.ClusterArn = clusterArn
	}

	c.LastModifiedTime = time.Now()

	return nil
}

// DeleteClusterSchedulerConfig deletes a cluster scheduler config by name.
func (b *InMemoryBackend) DeleteClusterSchedulerConfig(ctx context.Context, name string) error {
	b.mu.Lock("DeleteClusterSchedulerConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.clusterSchedulerConfigsStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// ComputeQuota
// ---------------------------------------------------------------------------

var (
	// ErrComputeQuotaNotFound is returned when a compute quota does not exist.
	ErrComputeQuotaNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrComputeQuotaAlreadyExists is returned when a compute quota already exists.
	ErrComputeQuotaAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// ComputeQuota represents a SageMaker compute quota.
type ComputeQuota struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ComputeQuotaName string            `json:"ComputeQuotaName"`
	ComputeQuotaArn  string            `json:"ComputeQuotaArn"`
	Status           string            `json:"Status"`
	ClusterArn       string            `json:"ClusterArn,omitempty"`
}

func cloneComputeQuota(q *ComputeQuota) *ComputeQuota {
	cp := *q
	cp.Tags = maps.Clone(q.Tags)

	return &cp
}

// CreateComputeQuotaOptions holds input fields for CreateComputeQuota.
type CreateComputeQuotaOptions struct {
	Tags             map[string]string
	ComputeQuotaName string
	ClusterArn       string
}

// CreateComputeQuota creates a SageMaker compute quota.
func (b *InMemoryBackend) CreateComputeQuota(
	ctx context.Context,
	opts CreateComputeQuotaOptions,
) (*ComputeQuota, error) {
	if opts.ComputeQuotaName == "" {
		return nil, fmt.Errorf("%w: ComputeQuotaName is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateComputeQuota", opts.ComputeQuotaName, "compute-quota",
		b.computeQuotasStore,
		func(n string) error {
			return fmt.Errorf("%w: compute quota %q already exists", ErrComputeQuotaAlreadyExists, n)
		},
		func(arnStr string, now time.Time) *ComputeQuota {
			return &ComputeQuota{
				ComputeQuotaName: opts.ComputeQuotaName,
				ComputeQuotaArn:  arnStr,
				ClusterArn:       opts.ClusterArn,
				Status:           statusCreated,
				Tags:             mergeTags(nil, opts.Tags),
				CreationTime:     now,
				LastModifiedTime: now,
			}
		},
		cloneComputeQuota,
	)
}

// DescribeComputeQuota returns a compute quota by name.
func (b *InMemoryBackend) DescribeComputeQuota(ctx context.Context, name string) (*ComputeQuota, error) {
	b.mu.RLock("DescribeComputeQuota")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	q, ok := b.computeQuotasStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, name)
	}

	return cloneComputeQuota(q), nil
}

// ListComputeQuotas returns all compute quotas with pagination.
func (b *InMemoryBackend) ListComputeQuotas(ctx context.Context, nextToken string) ([]*ComputeQuota, string) {
	b.mu.RLock("ListComputeQuotas")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(b.computeQuotasStore(region), nextToken, cloneComputeQuota)
}

// UpdateComputeQuota updates a compute quota's cluster ARN.
func (b *InMemoryBackend) UpdateComputeQuota(ctx context.Context, name, clusterArn string) error {
	b.mu.Lock("UpdateComputeQuota")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	q, ok := b.computeQuotasStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, name)
	}

	if clusterArn != "" {
		q.ClusterArn = clusterArn
	}

	q.LastModifiedTime = time.Now()

	return nil
}

// DeleteComputeQuota deletes a compute quota by name.
func (b *InMemoryBackend) DeleteComputeQuota(ctx context.Context, name string) error {
	b.mu.Lock("DeleteComputeQuota")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.computeQuotasStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, name)
	}

	delete(store, name)

	return nil
}

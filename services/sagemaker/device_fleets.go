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
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const deviceCompositeparts = 2

// ---------------------------------------------------------------------------
// DeviceFleet
// ---------------------------------------------------------------------------

var (
	// ErrDeviceFleetNotFound is returned when a device fleet does not exist.
	ErrDeviceFleetNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)
	// ErrDeviceFleetAlreadyExists is returned when a device fleet already exists.
	ErrDeviceFleetAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// DeviceFleetOutputConfig holds the S3 output location for a device fleet's
// sampled data, as configured on CreateDeviceFleet/UpdateDeviceFleet
// (types.EdgeOutputConfig, types/types.go:7856-7903).
type DeviceFleetOutputConfig struct {
	S3OutputLocation       string `json:"S3OutputLocation"`
	KmsKeyID               string `json:"KmsKeyId,omitempty"`
	PresetDeploymentConfig string `json:"PresetDeploymentConfig,omitempty"`
	PresetDeploymentType   string `json:"PresetDeploymentType,omitempty"`
}

// DeviceFleet represents a SageMaker device fleet.
type DeviceFleet struct {
	CreationTime     time.Time                `json:"CreationTime"`
	LastModifiedTime time.Time                `json:"LastModifiedTime"`
	Tags             map[string]string        `json:"Tags,omitempty"`
	OutputConfig     *DeviceFleetOutputConfig `json:"OutputConfig,omitempty"`
	DeviceFleetName  string                   `json:"DeviceFleetName"`
	DeviceFleetArn   string                   `json:"DeviceFleetArn"`
	Description      string                   `json:"Description,omitempty"`
	RoleArn          string                   `json:"RoleArn,omitempty"`
	// IotRoleAlias is synthesized as "SageMakerEdge-{DeviceFleetName}" when
	// EnableIotRoleAlias is set on Create/UpdateDeviceFleet (api_op_
	// CreateDeviceFleet.go:43-48) -- this backend has no real IoT Core to
	// register the alias with, so the name is stored but never resolves to
	// an actual IoT role alias.
	IotRoleAlias string `json:"IotRoleAlias,omitempty"`
}

func cloneDeviceFleet(f *DeviceFleet) *DeviceFleet {
	cp := *f
	cp.Tags = maps.Clone(f.Tags)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeDeviceFleet.
func (f *DeviceFleet) MarshalJSON() ([]byte, error) {
	type alias DeviceFleet

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(f),
		CreationTime:     epochSeconds(f.CreationTime),
		LastModifiedTime: epochSeconds(f.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [DeviceFleet.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (f *DeviceFleet) UnmarshalJSON(data []byte) error {
	type alias DeviceFleet

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(f)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	f.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	f.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateDeviceFleetOptions holds input fields for CreateDeviceFleet.
type CreateDeviceFleetOptions struct {
	Tags               map[string]string
	OutputConfig       *DeviceFleetOutputConfig
	DeviceFleetName    string
	Description        string
	RoleArn            string
	EnableIotRoleAlias bool
}

// CreateDeviceFleet creates a SageMaker device fleet.
func (b *InMemoryBackend) CreateDeviceFleet(ctx context.Context, opts CreateDeviceFleetOptions) (*DeviceFleet, error) {
	b.mu.Lock("CreateDeviceFleet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.DeviceFleetName == "" {
		return nil, fmt.Errorf("%w: DeviceFleetName is required", ErrValidation)
	}

	if _, ok := b.deviceFleetsStore(region).Get(opts.DeviceFleetName); ok {
		return nil, fmt.Errorf("%w: device fleet %q already exists", ErrDeviceFleetAlreadyExists, opts.DeviceFleetName)
	}

	fleetARN := arn.Build("sagemaker", region, b.accountID, "device-fleet/"+opts.DeviceFleetName)
	now := time.Now()

	f := &DeviceFleet{
		DeviceFleetName:  opts.DeviceFleetName,
		DeviceFleetArn:   fleetARN,
		Description:      opts.Description,
		RoleArn:          opts.RoleArn,
		OutputConfig:     opts.OutputConfig,
		Tags:             mergeTags(nil, opts.Tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	if opts.EnableIotRoleAlias {
		f.IotRoleAlias = iotRoleAliasName(opts.DeviceFleetName)
	}
	b.deviceFleetsStore(region).Put(f)

	return cloneDeviceFleet(f), nil
}

// DescribeDeviceFleet returns a device fleet by name.
func (b *InMemoryBackend) DescribeDeviceFleet(ctx context.Context, name string) (*DeviceFleet, error) {
	b.mu.RLock("DescribeDeviceFleet")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	f, ok := b.deviceFleetsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, name)
	}

	return cloneDeviceFleet(f), nil
}

// ListDeviceFleetsParams bundles ListDeviceFleets' filter/sort/pagination
// criteria (api_op_ListDeviceFleets.go:30-61, sagemaker@v1.263.2).
type ListDeviceFleetsParams struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	NameContains           string
	NextToken              string
	SortBy                 string
	SortOrder              string
	MaxResults             int32
}

// ListDeviceFleets returns device fleets matching params, sorted by
// params.SortBy (default CreationTime; no default is documented for this op,
// per api_op_ListDeviceFleets.go, so CreationTime/Ascending is kept as the
// disclosed fallback used elsewhere in this service for undocumented
// defaults), capped at params.MaxResults.
func (b *InMemoryBackend) ListDeviceFleets(
	ctx context.Context,
	params ListDeviceFleetsParams,
) ([]*DeviceFleet, string) {
	b.mu.RLock("ListDeviceFleets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tbl := b.deviceFleetsStoreRO(region)
	list := make([]*DeviceFleet, 0, tbl.Len())

	for _, f := range tbl.All() {
		if !matchesDeviceFleetListParams(f, params) {
			continue
		}

		list = append(list, cloneDeviceFleet(f))
	}

	asc := !strings.EqualFold(params.SortOrder, "Descending")
	sort.Slice(list, func(i, j int) bool {
		less := deviceFleetSortLess(list[i], list[j], params.SortBy)
		if asc {
			return less
		}

		return !less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// matchesDeviceFleetListParams reports whether f satisfies every filter in params.
func matchesDeviceFleetListParams(f *DeviceFleet, p ListDeviceFleetsParams) bool {
	if p.NameContains != "" && !strings.Contains(f.DeviceFleetName, p.NameContains) {
		return false
	}

	if p.CreationTimeAfter != nil && !f.CreationTime.After(*p.CreationTimeAfter) {
		return false
	}

	if p.CreationTimeBefore != nil && !f.CreationTime.Before(*p.CreationTimeBefore) {
		return false
	}

	if p.LastModifiedTimeAfter != nil && !f.LastModifiedTime.After(*p.LastModifiedTimeAfter) {
		return false
	}

	if p.LastModifiedTimeBefore != nil && !f.LastModifiedTime.Before(*p.LastModifiedTimeBefore) {
		return false
	}

	return true
}

// deviceFleetSortLess orders two device fleets by sortBy -- one of
// ListDeviceFleetsSortBy's real values (NAME/CREATION_TIME/LAST_MODIFIED_TIME,
// types/enums.go:5291-5293).
func deviceFleetSortLess(a, b *DeviceFleet, sortBy string) bool {
	switch sortBy {
	case sortByName:
		if a.DeviceFleetName != b.DeviceFleetName {
			return a.DeviceFleetName < b.DeviceFleetName
		}
	case sortByLastModifiedTime:
		if !a.LastModifiedTime.Equal(b.LastModifiedTime) {
			return a.LastModifiedTime.Before(b.LastModifiedTime)
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.DeviceFleetName < b.DeviceFleetName
}

// UpdateDeviceFleet updates a device fleet's description, role ARN, output
// config, or IoT role alias enablement.
func (b *InMemoryBackend) UpdateDeviceFleet(
	ctx context.Context,
	name, description, roleArn string,
	outputConfig *DeviceFleetOutputConfig,
	enableIotRoleAlias *bool,
) error {
	b.mu.Lock("UpdateDeviceFleet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	f, ok := b.deviceFleetsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, name)
	}

	if description != "" {
		f.Description = description
	}

	if roleArn != "" {
		f.RoleArn = roleArn
	}

	if outputConfig != nil {
		f.OutputConfig = outputConfig
	}

	if enableIotRoleAlias != nil {
		if *enableIotRoleAlias {
			f.IotRoleAlias = iotRoleAliasName(name)
		} else {
			f.IotRoleAlias = ""
		}
	}

	f.LastModifiedTime = time.Now()

	return nil
}

// iotRoleAliasName is the documented alias-name pattern for
// EnableIotRoleAlias (api_op_CreateDeviceFleet.go:43-48): "the name of the
// role alias generated will match this pattern: SageMakerEdge-{DeviceFleetName}".
func iotRoleAliasName(deviceFleetName string) string {
	return "SageMakerEdge-" + deviceFleetName
}

// DeleteDeviceFleet deletes a device fleet by name.
func (b *InMemoryBackend) DeleteDeviceFleet(ctx context.Context, name string) error {
	b.mu.Lock("DeleteDeviceFleet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.deviceFleetsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ---------------------------------------------------------------------------
// Device
// ---------------------------------------------------------------------------

// ErrDeviceNotFound is returned when a device does not exist.
var ErrDeviceNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)

// deviceKey uniquely identifies a device within a fleet.
type deviceKey struct {
	fleetName  string
	deviceName string
}

// deviceKeyString flattens a deviceKey to the single delimited string used as
// the store.Table primary key for b.devices.
func deviceKeyString(k deviceKey) string {
	return k.fleetName + "|" + k.deviceName
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

// MarshalJSON emits RegistrationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeDevice.
func (d *Device) MarshalJSON() ([]byte, error) {
	type alias Device

	return json.Marshal(struct {
		*alias
		RegistrationTime float64 `json:"RegistrationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(d),
		RegistrationTime: epochSeconds(d.RegistrationTime),
		LastModifiedTime: epochSeconds(d.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [Device.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (d *Device) UnmarshalJSON(data []byte) error {
	type alias Device

	aux := struct {
		*alias
		RegistrationTime float64 `json:"RegistrationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(d)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	d.RegistrationTime = timeFromEpochSeconds(aux.RegistrationTime)
	d.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// RegisterDeviceInput is a single device to register. Unlike
// CreateDeviceFleetOptions, types.Device (the wire type for each entry in
// RegisterDevicesInput.Devices, types/types.go:7222-7236) carries no Tags
// field of its own -- tags are supplied once, at the top level of
// RegisterDevicesInput, and apply to every device in the batch (see
// RegisterDevices' tags parameter below).
type RegisterDeviceInput struct {
	DeviceName   string
	Description  string
	IotThingName string
}

// RegisterDevices registers devices to a device fleet, tagging every
// registered device with tags (RegisterDevicesInput.Tags,
// api_op_RegisterDevices.go:36-38).
func (b *InMemoryBackend) RegisterDevices(
	ctx context.Context, fleetName string, devices []RegisterDeviceInput, tags map[string]string,
) error {
	b.mu.Lock("RegisterDevices")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.deviceFleetsStore(region).Get(fleetName); !ok {
		return fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, fleetName)
	}

	now := time.Now()
	devicesStore := b.devicesStore(region)
	for _, d := range devices {
		if d.DeviceName == "" {
			continue
		}

		deviceARN := arn.Build("sagemaker", region, b.accountID, "device/"+d.DeviceName)
		devicesStore.Put(&Device{
			DeviceName:       d.DeviceName,
			DeviceFleetName:  fleetName,
			DeviceArn:        deviceARN,
			Description:      d.Description,
			IotThingName:     d.IotThingName,
			Tags:             mergeTags(nil, tags),
			RegistrationTime: now,
			LastModifiedTime: now,
		})
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
		store.Delete(deviceKeyString(deviceKey{fleetName: fleetName, deviceName: name}))
	}

	return nil
}

// UpdateDeviceInput is a single device update entry for UpdateDevices.
type UpdateDeviceInput struct {
	DeviceName   string
	Description  string
	IotThingName string
}

// UpdateDevices updates metadata (description, IoT thing name) for devices
// already registered in a fleet. Devices that are not registered are skipped,
// mirroring the lenient behavior of DeregisterDevices.
func (b *InMemoryBackend) UpdateDevices(ctx context.Context, fleetName string, devices []UpdateDeviceInput) error {
	b.mu.Lock("UpdateDevices")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.deviceFleetsStore(region).Get(fleetName); !ok {
		return fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, fleetName)
	}

	store := b.devicesStore(region)
	now := time.Now()

	for _, d := range devices {
		dev, ok := store.Get(deviceKeyString(deviceKey{fleetName: fleetName, deviceName: d.DeviceName}))
		if !ok {
			continue
		}

		if d.Description != "" {
			dev.Description = d.Description
		}

		if d.IotThingName != "" {
			dev.IotThingName = d.IotThingName
		}

		dev.LastModifiedTime = now
	}

	return nil
}

// DescribeDevice returns a device by fleet and device name.
func (b *InMemoryBackend) DescribeDevice(ctx context.Context, fleetName, deviceName string) (*Device, error) {
	b.mu.RLock("DescribeDevice")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	d, ok := b.devicesStoreRO(region).Get(deviceKeyString(deviceKey{fleetName: fleetName, deviceName: deviceName}))
	if !ok {
		return nil, fmt.Errorf("%w: device %q in fleet %q", ErrDeviceNotFound, deviceName, fleetName)
	}

	return cloneDevice(d), nil
}

// ListDevices returns devices, optionally filtered by fleet name and capped
// at maxResults (falls back to sagemakerDefaultPageSize when 0). ListDevices'
// LatestHeartbeatAfter/ModelName filters (api_op_ListDevices.go:36,42) are not
// modeled: this backend has no device-agent heartbeat/model-registration
// protocol (a separate service, SageMaker Edge Manager's device-agent API,
// not implemented here at all -- distinct from ListStageDevices'
// ExcludeDevicesDeployedInOtherStage no-op, which is a real member of this
// service that this backend simply can't evaluate).
func (b *InMemoryBackend) ListDevices(
	ctx context.Context,
	fleetFilter, nextToken string,
	maxResults int32,
) ([]*Device, string) {
	b.mu.RLock("ListDevices")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return devicesInFleetPaged(b.devicesStoreRO(region), fleetFilter, nextToken, maxResults)
}

// devicesInFleetPaged filters tbl by fleetFilter (or all devices if empty),
// sorts by "fleetName/deviceName", and paginates the result, capped at
// maxResults (if positive) or sagemakerDefaultPageSize. Caller must hold b.mu
// (read or write).
func devicesInFleetPaged(
	tbl *store.Table[Device], fleetFilter, nextToken string, maxResults int32,
) ([]*Device, string) {
	keys := make([]string, 0, tbl.Len())
	for _, d := range tbl.All() {
		if fleetFilter != "" && d.DeviceFleetName != fleetFilter {
			continue
		}

		keys = append(keys, d.DeviceFleetName+"/"+d.DeviceName)
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

	pageSize := sagemakerDefaultPageSize
	if maxResults > 0 && int(maxResults) < pageSize {
		pageSize = int(maxResults)
	}

	end := min(start+pageSize, len(keys))

	out := make([]*Device, 0, end-start)
	for _, composite := range keys[start:end] {
		parts := strings.SplitN(composite, "/", deviceCompositeparts)
		if len(parts) != deviceCompositeparts {
			continue
		}

		if d, ok := tbl.Get(deviceKeyString(deviceKey{fleetName: parts[0], deviceName: parts[1]})); ok {
			out = append(out, cloneDevice(d))
		}
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

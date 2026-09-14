package appstream

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	fleetStateRunning = "RUNNING"
	fleetStateStopped = "STOPPED"

	defaultFleetType         = "ON_DEMAND"
	defaultMaxUserDuration   = 57600 // 16 hours
	defaultDisconnectTimeout = 300   // 5 minutes

	// FleetAttribute enum values a real UpdateFleetInput.AttributesToDelete
	// may carry (appstream@v1.64.5 types/enums.go) that this backend models.
	fleetAttrVpcConfiguration                 = "VPC_CONFIGURATION"
	fleetAttrVpcConfigurationSecurityGroupIDs = "VPC_CONFIGURATION_SECURITY_GROUP_IDS"
	fleetAttrDomainJoinInfo                   = "DOMAIN_JOIN_INFO"
	fleetAttrIamRoleArn                       = "IAM_ROLE_ARN"
	fleetAttrUsbDeviceFilterStrings           = "USB_DEVICE_FILTER_STRINGS"
	fleetAttrSessionScriptS3Location          = "SESSION_SCRIPT_S3_LOCATION"
	fleetAttrMaxSessionsPerInstance           = "MAX_SESSIONS_PER_INSTANCE"
	fleetAttrVolumeConfiguration              = "VOLUME_CONFIGURATION"
)

type storedFleet struct {
	CreatedTime                 time.Time         `json:"createdTime"`
	DisableIMDSV1               *bool             `json:"disableImdsv1,omitempty"`
	RootVolumeConfig            *VolumeConfig     `json:"rootVolumeConfig,omitempty"`
	Tags                        map[string]string `json:"tags"`
	EnableDefaultInternetAccess *bool             `json:"enableDefaultInternetAccess,omitempty"`
	DomainJoinInfo              DomainJoinInfo    `json:"domainJoinInfo"`
	SessionScriptS3Location     S3Location        `json:"sessionScriptS3Location"`
	Description                 string            `json:"description"`
	Platform                    string            `json:"platform,omitempty"`
	Name                        string            `json:"name"`
	Arn                         string            `json:"arn"`
	DisplayName                 string            `json:"displayName"`
	StreamView                  string            `json:"streamView,omitempty"`
	InstanceType                string            `json:"instanceType"`
	FleetType                   string            `json:"fleetType"`
	State                       string            `json:"state"`
	ImageName                   string            `json:"imageName,omitempty"`
	ImageArn                    string            `json:"imageArn,omitempty"`
	IamRoleArn                  string            `json:"iamRoleArn,omitempty"`
	VpcConfig                   VpcConfig         `json:"vpcConfig"`
	UsbDeviceFilterStrings      []string          `json:"usbDeviceFilterStrings"`
	DesiredInstances            int               `json:"desiredInstances"`
	MaxUserDurationSecs         int               `json:"maxUserDurationSecs"`
	DisconnectTimeoutSecs       int               `json:"disconnectTimeoutSecs"`
	IdleDisconnectTimeoutSecs   int               `json:"idleDisconnectTimeoutSecs"`
	MaxSessionsPerInstance      int               `json:"maxSessionsPerInstance,omitempty"`
	MaxConcurrentSessions       int               `json:"maxConcurrentSessions,omitempty"`
}

func (f *storedFleet) toFleet() *Fleet {
	tags := make(map[string]string)
	maps.Copy(tags, f.Tags)

	fleet := &Fleet{
		EnableDefaultInternetAccess: f.EnableDefaultInternetAccess,
		DisableIMDSV1:               f.DisableIMDSV1,
		CreatedTime:                 f.CreatedTime,
		Tags:                        tags,
		VpcConfig: VpcConfig{
			SecurityGroupIDs: append([]string(nil), f.VpcConfig.SecurityGroupIDs...),
			SubnetIDs:        append([]string(nil), f.VpcConfig.SubnetIDs...),
		},
		SessionScriptS3Location:   f.SessionScriptS3Location,
		DomainJoinInfo:            f.DomainJoinInfo,
		UsbDeviceFilterStrings:    append([]string(nil), f.UsbDeviceFilterStrings...),
		Name:                      f.Name,
		Arn:                       f.Arn,
		DisplayName:               f.DisplayName,
		Description:               f.Description,
		InstanceType:              f.InstanceType,
		FleetType:                 f.FleetType,
		State:                     f.State,
		ImageName:                 f.ImageName,
		ImageArn:                  f.ImageArn,
		IamRoleArn:                f.IamRoleArn,
		StreamView:                f.StreamView,
		Platform:                  f.Platform,
		DesiredInstances:          f.DesiredInstances,
		MaxUserDurationSecs:       f.MaxUserDurationSecs,
		DisconnectTimeoutSecs:     f.DisconnectTimeoutSecs,
		IdleDisconnectTimeoutSecs: f.IdleDisconnectTimeoutSecs,
		MaxSessionsPerInstance:    f.MaxSessionsPerInstance,
		MaxConcurrentSessions:     f.MaxConcurrentSessions,
	}

	if f.RootVolumeConfig != nil {
		rvc := *f.RootVolumeConfig
		fleet.RootVolumeConfig = &rvc
	}

	return fleet
}

func (b *InMemoryBackend) fleetARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("fleet/%s", name))
}

// isValidFleetType reports whether ft is an accepted AWS fleet type.
func isValidFleetType(ft string) bool {
	switch ft {
	case "ALWAYS_ON", "ON_DEMAND", "ELASTIC":
		return true
	}

	return false
}

// CreateFleet creates a new fleet.
func (b *InMemoryBackend) CreateFleet(name string, opts CreateFleetOptions) (*Fleet, error) {
	if opts.InstanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", awserr.ErrInvalidParameter)
	}

	if opts.FleetType != "" && !isValidFleetType(opts.FleetType) {
		return nil, fmt.Errorf(
			"%w: FleetType %q is not valid; must be ALWAYS_ON, ON_DEMAND, or ELASTIC",
			awserr.ErrInvalidParameter,
			opts.FleetType,
		)
	}

	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	if b.fleets.Has(name) {
		return nil, ErrAlreadyExists
	}

	fleetArn := b.fleetARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, opts.Tags)

	ft := opts.FleetType
	if ft == "" {
		ft = defaultFleetType
	}

	mux := opts.MaxUserDurationSecs
	if mux == 0 {
		mux = defaultMaxUserDuration
	}

	dt := opts.DisconnectTimeoutSecs
	if dt == 0 {
		dt = defaultDisconnectTimeout
	}

	desired := opts.DesiredInstances
	if desired == 0 {
		desired = 1
	}

	f := &storedFleet{
		EnableDefaultInternetAccess: opts.EnableDefaultInternetAccess,
		DisableIMDSV1:               opts.DisableIMDSV1,
		RootVolumeConfig:            opts.RootVolumeConfig,
		CreatedTime:                 b.now(),
		Tags:                        storedTags,
		VpcConfig:                   opts.VpcConfig,
		SessionScriptS3Location:     opts.SessionScriptS3Location,
		DomainJoinInfo:              opts.DomainJoinInfo,
		UsbDeviceFilterStrings:      append([]string(nil), opts.UsbDeviceFilterStrings...),
		Name:                        name,
		Arn:                         fleetArn,
		DisplayName:                 opts.DisplayName,
		Description:                 opts.Description,
		InstanceType:                opts.InstanceType,
		FleetType:                   ft,
		State:                       fleetStateStopped,
		ImageName:                   opts.ImageName,
		ImageArn:                    opts.ImageArn,
		IamRoleArn:                  opts.IamRoleArn,
		StreamView:                  opts.StreamView,
		Platform:                    opts.Platform,
		DesiredInstances:            desired,
		MaxUserDurationSecs:         mux,
		DisconnectTimeoutSecs:       dt,
		IdleDisconnectTimeoutSecs:   opts.IdleDisconnectTimeoutSecs,
		MaxSessionsPerInstance:      opts.MaxSessionsPerInstance,
		MaxConcurrentSessions:       opts.MaxConcurrentSessions,
	}
	b.fleets.Put(f)
	b.tags[fleetArn] = storedTags

	return f.toFleet(), nil
}

// DescribeFleets returns fleets, optionally filtered by names.
func (b *InMemoryBackend) DescribeFleets(names []string) ([]*Fleet, error) {
	b.mu.RLock("DescribeFleets")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*Fleet

		for _, name := range names {
			f, ok := b.fleets.Get(name)
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, f.toFleet())
		}

		return result, nil
	}

	result := make([]*Fleet, 0, b.fleets.Len())
	for _, f := range b.fleets.All() {
		result = append(result, f.toFleet())
	}

	return result, nil
}

// applyFleetAttributesToDelete clears the fields named by opts.AttributesToDelete,
// applied after every set field so a delete always wins over a same-request set
// (matches UpdateThemeForStack's convention).
func applyFleetAttributesToDelete(f *storedFleet, attrs []string) {
	for _, attr := range attrs {
		switch attr {
		case fleetAttrVpcConfiguration:
			f.VpcConfig = VpcConfig{}
		case fleetAttrVpcConfigurationSecurityGroupIDs:
			f.VpcConfig.SecurityGroupIDs = nil
		case fleetAttrDomainJoinInfo:
			f.DomainJoinInfo = DomainJoinInfo{}
		case fleetAttrIamRoleArn:
			f.IamRoleArn = ""
		case fleetAttrUsbDeviceFilterStrings:
			f.UsbDeviceFilterStrings = nil
		case fleetAttrSessionScriptS3Location:
			f.SessionScriptS3Location = S3Location{}
		case fleetAttrMaxSessionsPerInstance:
			f.MaxSessionsPerInstance = 0
		case fleetAttrVolumeConfiguration:
			f.RootVolumeConfig = nil
		}
	}
}

// UpdateFleet updates mutable fields of an existing fleet.
func (b *InMemoryBackend) UpdateFleet(name string, opts UpdateFleetOptions) (*Fleet, error) {
	b.mu.Lock("UpdateFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	applyFleetCoreUpdates(f, opts)
	applyFleetExtendedUpdates(f, opts)
	applyFleetAttributesToDelete(f, opts.AttributesToDelete)

	return f.toFleet(), nil
}

// applyFleetCoreUpdates sets the fields UpdateFleet has always supported
// (name/capacity/timeouts) -- split out of UpdateFleet to keep it under
// this repo's gocognit budget.
func applyFleetCoreUpdates(f *storedFleet, opts UpdateFleetOptions) {
	if opts.DisplayName != "" {
		f.DisplayName = opts.DisplayName
	}

	if opts.Description != "" {
		f.Description = opts.Description
	}

	if opts.InstanceType != "" {
		f.InstanceType = opts.InstanceType
	}

	if opts.ImageName != "" {
		f.ImageName = opts.ImageName
	}

	if opts.ImageArn != "" {
		f.ImageArn = opts.ImageArn
	}

	if opts.DesiredInstances > 0 {
		f.DesiredInstances = opts.DesiredInstances
	}

	if opts.MaxUserDurationSecs > 0 {
		f.MaxUserDurationSecs = opts.MaxUserDurationSecs
	}

	if opts.DisconnectTimeoutSecs > 0 {
		f.DisconnectTimeoutSecs = opts.DisconnectTimeoutSecs
	}

	if opts.IdleDisconnectTimeoutSecs >= 0 && opts.IdleDisconnectTimeoutSecs != f.IdleDisconnectTimeoutSecs {
		f.IdleDisconnectTimeoutSecs = opts.IdleDisconnectTimeoutSecs
	}

	if opts.EnableDefaultInternetAccess != nil {
		f.EnableDefaultInternetAccess = opts.EnableDefaultInternetAccess
	}
}

// applyFleetExtendedUpdates sets the members this pass added (VpcConfig,
// IamRoleArn, StreamView, RootVolumeConfig, MaxSessionsPerInstance,
// UsbDeviceFilterStrings, SessionScriptS3Location, Platform, DomainJoinInfo,
// MaxConcurrentSessions, DisableIMDSV1) -- split out of UpdateFleet to keep
// it under this repo's gocognit budget.
func applyFleetExtendedUpdates(f *storedFleet, opts UpdateFleetOptions) {
	if opts.DisableIMDSV1 != nil {
		f.DisableIMDSV1 = opts.DisableIMDSV1
	}

	if len(opts.VpcConfig.SecurityGroupIDs) > 0 || len(opts.VpcConfig.SubnetIDs) > 0 {
		f.VpcConfig = VpcConfig{
			SecurityGroupIDs: slices.Clone(opts.VpcConfig.SecurityGroupIDs),
			SubnetIDs:        slices.Clone(opts.VpcConfig.SubnetIDs),
		}
	}

	if opts.IamRoleArn != "" {
		f.IamRoleArn = opts.IamRoleArn
	}

	if opts.StreamView != "" {
		f.StreamView = opts.StreamView
	}

	if opts.Platform != "" {
		f.Platform = opts.Platform
	}

	if opts.MaxSessionsPerInstance > 0 {
		f.MaxSessionsPerInstance = opts.MaxSessionsPerInstance
	}

	if opts.MaxConcurrentSessions > 0 {
		f.MaxConcurrentSessions = opts.MaxConcurrentSessions
	}

	if len(opts.UsbDeviceFilterStrings) > 0 {
		f.UsbDeviceFilterStrings = slices.Clone(opts.UsbDeviceFilterStrings)
	}

	if opts.SessionScriptS3Location.S3Bucket != "" {
		f.SessionScriptS3Location = opts.SessionScriptS3Location
	}

	if opts.DomainJoinInfo.DirectoryName != "" {
		f.DomainJoinInfo = opts.DomainJoinInfo
	}

	if opts.RootVolumeConfig != nil {
		rvc := *opts.RootVolumeConfig
		f.RootVolumeConfig = &rvc
	}
}

// DeleteFleet removes a fleet. Returns ErrResourceInUse if fleet is running.
func (b *InMemoryBackend) DeleteFleet(name string) error {
	b.mu.Lock("DeleteFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets.Get(name)
	if !ok {
		return ErrNotFound
	}

	if f.State == fleetStateRunning {
		return ErrResourceInUse
	}

	delete(b.tags, f.Arn)
	b.fleets.Delete(name)
	delete(b.associations, name)

	return nil
}

// StartFleet transitions a fleet to RUNNING.
func (b *InMemoryBackend) StartFleet(name string) error {
	b.mu.Lock("StartFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets.Get(name)
	if !ok {
		return ErrNotFound
	}

	if f.State == fleetStateRunning {
		return ErrFleetNotStopped
	}

	f.State = fleetStateRunning

	return nil
}

// StopFleet transitions a fleet to STOPPED. Idempotent: stopping an
// already-stopped fleet succeeds (real AWS's StopFleet has no state-conflict
// exception -- only ResourceNotFoundException and ConcurrentModificationException).
func (b *InMemoryBackend) StopFleet(name string) error {
	b.mu.Lock("StopFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets.Get(name)
	if !ok {
		return ErrNotFound
	}

	f.State = fleetStateStopped

	return nil
}

// AssociateFleet links a fleet and a stack.
func (b *InMemoryBackend) AssociateFleet(fleetName, stackName string) error {
	b.mu.Lock("AssociateFleet")
	defer b.mu.Unlock()

	if !b.fleets.Has(fleetName) {
		return ErrNotFound
	}

	if !b.stacks.Has(stackName) {
		return ErrNotFound
	}

	if b.associations[fleetName] == nil {
		b.associations[fleetName] = make(map[string]bool)
	}

	b.associations[fleetName][stackName] = true

	return nil
}

// DisassociateFleet unlinks a fleet and a stack.
func (b *InMemoryBackend) DisassociateFleet(fleetName, stackName string) error {
	b.mu.Lock("DisassociateFleet")
	defer b.mu.Unlock()

	if !b.fleets.Has(fleetName) {
		return ErrNotFound
	}

	if !b.stacks.Has(stackName) {
		return ErrNotFound
	}

	if b.associations[fleetName] != nil {
		delete(b.associations[fleetName], stackName)
	}

	return nil
}

// ListAssociatedFleets returns fleet names associated with a stack.
func (b *InMemoryBackend) ListAssociatedFleets(stackName string) ([]string, error) {
	b.mu.RLock("ListAssociatedFleets")
	defer b.mu.RUnlock()

	if !b.stacks.Has(stackName) {
		return nil, ErrNotFound
	}

	var fleets []string

	for fleet, stacks := range b.associations {
		if stacks[stackName] {
			fleets = append(fleets, fleet)
		}
	}

	return fleets, nil
}

// ListAssociatedStacks returns stack names associated with a fleet.
func (b *InMemoryBackend) ListAssociatedStacks(fleetName string) ([]string, error) {
	b.mu.RLock("ListAssociatedStacks")
	defer b.mu.RUnlock()

	if !b.fleets.Has(fleetName) {
		return nil, ErrNotFound
	}

	stacks := b.associations[fleetName]
	result := make([]string, 0, len(stacks))

	for stack := range stacks {
		result = append(result, stack)
	}

	return result, nil
}

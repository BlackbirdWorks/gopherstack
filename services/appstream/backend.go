package appstream

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const (
	errResourceNotFound = "ResourceNotFoundException"
	errInvalidParameter = "InvalidParameterCombinationException"
	errResourceExists   = "ResourceAlreadyExistsException"
	errFleetNotStopped  = "InvalidAccountStatusException"
	errResourceInUse    = "ResourceInUseException"

	fleetStateRunning = "RUNNING"
	fleetStateStopped = "STOPPED"

	defaultFleetType         = "ON_DEMAND"
	defaultMaxUserDuration   = 57600 // 16 hours
	defaultDisconnectTimeout = 300   // 5 minutes
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New(errResourceExists, awserr.ErrAlreadyExists)
	// ErrFleetNotStopped is returned when a fleet state transition is invalid
	// (e.g. starting a running fleet, stopping a stopped fleet, deleting a running fleet).
	ErrFleetNotStopped = awserr.New(errFleetNotStopped, awserr.ErrConflict)
	// ErrResourceInUse is returned when a resource cannot be deleted because it is in use.
	ErrResourceInUse = awserr.New(errResourceInUse, awserr.ErrConflict)
)

type storedStack struct {
	CreatedTime time.Time         `json:"createdTime"`
	Tags        map[string]string `json:"tags"`
	Name        string            `json:"name"`
	Arn         string            `json:"arn"`
	DisplayName string            `json:"displayName"`
	Description string            `json:"description"`
}

func (s *storedStack) toStack() *Stack {
	tags := make(map[string]string)
	maps.Copy(tags, s.Tags)

	return &Stack{
		CreatedTime: s.CreatedTime,
		Tags:        tags,
		Name:        s.Name,
		Arn:         s.Arn,
		DisplayName: s.DisplayName,
		Description: s.Description,
	}
}

type storedFleet struct {
	CreatedTime           time.Time         `json:"createdTime"`
	Tags                  map[string]string `json:"tags"`
	Name                  string            `json:"name"`
	Arn                   string            `json:"arn"`
	DisplayName           string            `json:"displayName"`
	Description           string            `json:"description"`
	InstanceType          string            `json:"instanceType"`
	FleetType             string            `json:"fleetType"`
	State                 string            `json:"state"`
	MaxUserDurationSecs   int               `json:"maxUserDurationSecs"`
	DisconnectTimeoutSecs int               `json:"disconnectTimeoutSecs"`
}

func (f *storedFleet) toFleet() *Fleet {
	tags := make(map[string]string)
	maps.Copy(tags, f.Tags)

	return &Fleet{
		CreatedTime:           f.CreatedTime,
		Tags:                  tags,
		Name:                  f.Name,
		Arn:                   f.Arn,
		DisplayName:           f.DisplayName,
		Description:           f.Description,
		InstanceType:          f.InstanceType,
		FleetType:             f.FleetType,
		State:                 f.State,
		MaxUserDurationSecs:   f.MaxUserDurationSecs,
		DisconnectTimeoutSecs: f.DisconnectTimeoutSecs,
	}
}

type backendSnapshot struct {
	Images               map[string]*storedImage            `json:"images"`
	AppBlockBuilderAssoc map[string]map[string]bool         `json:"appBlockBuilderAssoc"`
	Associations         map[string]map[string]bool         `json:"associations"`
	Tags                 map[string]map[string]string       `json:"tags"`
	AppBlocks            map[string]*storedAppBlock         `json:"appBlocks"`
	AppBlockBuilders     map[string]*storedAppBlockBuilder  `json:"appBlockBuilders"`
	DirectoryConfigs     map[string]*storedDirectoryConfig  `json:"directoryConfigs"`
	Applications         map[string]*storedApplication      `json:"applications"`
	AppFleetAssoc        map[string]map[string]bool         `json:"appFleetAssoc"`
	Entitlements         map[string]*storedEntitlement      `json:"entitlements"`
	Fleets               map[string]*storedFleet            `json:"fleets"`
	EntitlementApps      map[string]map[string]bool         `json:"entitlementApps"`
	ExportTasks          map[string]*storedExportImageTask  `json:"exportTasks"`
	ImagePermissions     map[string]*storedImagePermissions `json:"imagePermissions"`
	ImageBuilders        map[string]*storedImageBuilder     `json:"imageBuilders"`
	SoftwareAssoc        map[string]map[string]bool         `json:"softwareAssoc"`
	Stacks               map[string]*storedStack            `json:"stacks"`
	Themes               map[string]*storedTheme            `json:"themes"`
	Users                map[string]*storedUser             `json:"users"`
	UserStackAssoc       map[string]map[string]bool         `json:"userStackAssoc"`
	Sessions             map[string]*storedSession          `json:"sessions"`
	UsageReport          *storedUsageReportSubscription     `json:"usageReport"`
	SessionSeq           int                                `json:"sessionSeq"`
	ExportTaskSeq        int                                `json:"exportTaskSeq"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	directoryConfigs     map[string]*storedDirectoryConfig
	themes               map[string]*storedTheme
	fleets               map[string]*storedFleet
	associations         map[string]map[string]bool
	tags                 map[string]map[string]string
	appBlocks            map[string]*storedAppBlock
	appBlockBuilders     map[string]*storedAppBlockBuilder
	appBlockBuilderAssoc map[string]map[string]bool
	applications         map[string]*storedApplication
	appFleetAssoc        map[string]map[string]bool
	entitlements         map[string]*storedEntitlement
	imagePermissions     map[string]*storedImagePermissions
	stacks               map[string]*storedStack
	mu                   *lockmetrics.RWMutex
	entitlementApps      map[string]map[string]bool
	imageBuilders        map[string]*storedImageBuilder
	softwareAssoc        map[string]map[string]bool
	exportTasks          map[string]*storedExportImageTask
	images               map[string]*storedImage
	users                map[string]*storedUser
	userStackAssoc       map[string]map[string]bool
	sessions             map[string]*storedSession
	usageReport          *storedUsageReportSubscription
	accountID            string
	region               string
	sessionSeq           int
	exportTaskSeq        int
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                   lockmetrics.New("appstream"),
		stacks:               make(map[string]*storedStack),
		fleets:               make(map[string]*storedFleet),
		associations:         make(map[string]map[string]bool),
		tags:                 make(map[string]map[string]string),
		appBlocks:            make(map[string]*storedAppBlock),
		appBlockBuilders:     make(map[string]*storedAppBlockBuilder),
		appBlockBuilderAssoc: make(map[string]map[string]bool),
		applications:         make(map[string]*storedApplication),
		appFleetAssoc:        make(map[string]map[string]bool),
		entitlements:         make(map[string]*storedEntitlement),
		entitlementApps:      make(map[string]map[string]bool),
		directoryConfigs:     make(map[string]*storedDirectoryConfig),
		images:               make(map[string]*storedImage),
		imagePermissions:     make(map[string]*storedImagePermissions),
		imageBuilders:        make(map[string]*storedImageBuilder),
		softwareAssoc:        make(map[string]map[string]bool),
		exportTasks:          make(map[string]*storedExportImageTask),
		users:                make(map[string]*storedUser),
		userStackAssoc:       make(map[string]map[string]bool),
		sessions:             make(map[string]*storedSession),
		themes:               make(map[string]*storedTheme),
		accountID:            accountID,
		region:               region,
	}
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) stackARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("stack/%s", name))
}

func (b *InMemoryBackend) fleetARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("fleet/%s", name))
}

// CreateStack creates a new stack.
func (b *InMemoryBackend) CreateStack(name, displayName, description string, tags map[string]string) (*Stack, error) {
	b.mu.Lock("CreateStack")
	defer b.mu.Unlock()

	if _, ok := b.stacks[name]; ok {
		return nil, ErrAlreadyExists
	}

	arn := b.stackARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	s := &storedStack{
		CreatedTime: time.Now().UTC(),
		Tags:        storedTags,
		Name:        name,
		Arn:         arn,
		DisplayName: displayName,
		Description: description,
	}
	b.stacks[name] = s
	b.tags[arn] = storedTags

	return s.toStack(), nil
}

// DescribeStacks returns stacks, optionally filtered by names.
func (b *InMemoryBackend) DescribeStacks(names []string) ([]*Stack, error) {
	b.mu.RLock("DescribeStacks")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*Stack

		for _, name := range names {
			s, ok := b.stacks[name]
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, s.toStack())
		}

		return result, nil
	}

	result := make([]*Stack, 0, len(b.stacks))
	for _, s := range b.stacks {
		result = append(result, s.toStack())
	}

	return result, nil
}

// UpdateStack updates mutable fields of an existing stack.
func (b *InMemoryBackend) UpdateStack(name, displayName, description string) (*Stack, error) {
	b.mu.Lock("UpdateStack")
	defer b.mu.Unlock()

	s, ok := b.stacks[name]
	if !ok {
		return nil, ErrNotFound
	}

	if displayName != "" {
		s.DisplayName = displayName
	}

	if description != "" {
		s.Description = description
	}

	return s.toStack(), nil
}

// DeleteStack removes a stack. Returns ErrResourceInUse if any fleet is associated with the stack.
func (b *InMemoryBackend) DeleteStack(name string) error {
	b.mu.Lock("DeleteStack")
	defer b.mu.Unlock()

	s, ok := b.stacks[name]
	if !ok {
		return ErrNotFound
	}

	for _, stacks := range b.associations {
		if stacks[name] {
			return ErrResourceInUse
		}
	}

	delete(b.tags, s.Arn)
	delete(b.stacks, name)

	return nil
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
func (b *InMemoryBackend) CreateFleet(
	name, displayName, description, instanceType, fleetType string,
	maxUserDuration, disconnectTimeout int,
	tags map[string]string,
) (*Fleet, error) {
	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", awserr.ErrInvalidParameter)
	}

	if fleetType != "" && !isValidFleetType(fleetType) {
		return nil, fmt.Errorf(
			"%w: FleetType %q is not valid; must be ALWAYS_ON, ON_DEMAND, or ELASTIC",
			awserr.ErrInvalidParameter,
			fleetType,
		)
	}

	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	if _, ok := b.fleets[name]; ok {
		return nil, ErrAlreadyExists
	}

	arn := b.fleetARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	ft := fleetType
	if ft == "" {
		ft = defaultFleetType
	}

	mux := maxUserDuration
	if mux == 0 {
		mux = defaultMaxUserDuration
	}

	dt := disconnectTimeout
	if dt == 0 {
		dt = defaultDisconnectTimeout
	}

	f := &storedFleet{
		CreatedTime:           time.Now().UTC(),
		Tags:                  storedTags,
		Name:                  name,
		Arn:                   arn,
		DisplayName:           displayName,
		Description:           description,
		InstanceType:          instanceType,
		FleetType:             ft,
		State:                 fleetStateStopped,
		MaxUserDurationSecs:   mux,
		DisconnectTimeoutSecs: dt,
	}
	b.fleets[name] = f
	b.tags[arn] = storedTags

	return f.toFleet(), nil
}

// DescribeFleets returns fleets, optionally filtered by names.
func (b *InMemoryBackend) DescribeFleets(names []string) ([]*Fleet, error) {
	b.mu.RLock("DescribeFleets")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*Fleet

		for _, name := range names {
			f, ok := b.fleets[name]
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, f.toFleet())
		}

		return result, nil
	}

	result := make([]*Fleet, 0, len(b.fleets))
	for _, f := range b.fleets {
		result = append(result, f.toFleet())
	}

	return result, nil
}

// UpdateFleet updates mutable fields of an existing fleet.
func (b *InMemoryBackend) UpdateFleet(name, displayName, description, instanceType string,
	maxUserDuration, disconnectTimeout int,
) (*Fleet, error) {
	b.mu.Lock("UpdateFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets[name]
	if !ok {
		return nil, ErrNotFound
	}

	if displayName != "" {
		f.DisplayName = displayName
	}

	if description != "" {
		f.Description = description
	}

	if instanceType != "" {
		f.InstanceType = instanceType
	}

	if maxUserDuration > 0 {
		f.MaxUserDurationSecs = maxUserDuration
	}

	if disconnectTimeout > 0 {
		f.DisconnectTimeoutSecs = disconnectTimeout
	}

	return f.toFleet(), nil
}

// DeleteFleet removes a fleet. Returns ErrFleetNotStopped if fleet is running.
func (b *InMemoryBackend) DeleteFleet(name string) error {
	b.mu.Lock("DeleteFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets[name]
	if !ok {
		return ErrNotFound
	}

	if f.State == fleetStateRunning {
		return ErrFleetNotStopped
	}

	delete(b.tags, f.Arn)
	delete(b.fleets, name)

	for _, stacks := range b.associations {
		delete(stacks, name)
	}

	return nil
}

// StartFleet transitions a fleet to RUNNING.
func (b *InMemoryBackend) StartFleet(name string) error {
	b.mu.Lock("StartFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets[name]
	if !ok {
		return ErrNotFound
	}

	if f.State == fleetStateRunning {
		return ErrFleetNotStopped
	}

	f.State = fleetStateRunning

	return nil
}

// StopFleet transitions a fleet to STOPPED.
func (b *InMemoryBackend) StopFleet(name string) error {
	b.mu.Lock("StopFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets[name]
	if !ok {
		return ErrNotFound
	}

	if f.State == fleetStateStopped {
		return ErrFleetNotStopped
	}

	f.State = fleetStateStopped

	return nil
}

// AssociateFleet links a fleet and a stack.
func (b *InMemoryBackend) AssociateFleet(fleetName, stackName string) error {
	b.mu.Lock("AssociateFleet")
	defer b.mu.Unlock()

	if _, ok := b.fleets[fleetName]; !ok {
		return ErrNotFound
	}

	if _, ok := b.stacks[stackName]; !ok {
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

	if _, ok := b.fleets[fleetName]; !ok {
		return ErrNotFound
	}

	if _, ok := b.stacks[stackName]; !ok {
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

	if _, ok := b.stacks[stackName]; !ok {
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

	if _, ok := b.fleets[fleetName]; !ok {
		return nil, ErrNotFound
	}

	stacks := b.associations[fleetName]
	result := make([]string, 0, len(stacks))

	for stack := range stacks {
		result = append(result, stack)
	}

	return result, nil
}

// TagResource applies tags to an AppStream resource ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(arn) {
		return ErrNotFound
	}

	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}

	maps.Copy(b.tags[arn], tags)

	return nil
}

// UntagResource removes tags from an AppStream resource ARN.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(arn) {
		return ErrNotFound
	}

	for _, k := range keys {
		delete(b.tags[arn], k)
	}

	return nil
}

// ListTagsForResource returns tags for an AppStream resource ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownARN(arn) {
		return nil, ErrNotFound
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[arn])

	return result, nil
}

// isKnownARN returns true if the ARN corresponds to a known resource.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) isKnownARN(arn string) bool {
	_, ok := b.tags[arn]

	return ok
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.stacks = make(map[string]*storedStack)
	b.fleets = make(map[string]*storedFleet)
	b.associations = make(map[string]map[string]bool)
	b.tags = make(map[string]map[string]string)
	b.appBlocks = make(map[string]*storedAppBlock)
	b.appBlockBuilders = make(map[string]*storedAppBlockBuilder)
	b.appBlockBuilderAssoc = make(map[string]map[string]bool)
	b.applications = make(map[string]*storedApplication)
	b.appFleetAssoc = make(map[string]map[string]bool)
	b.entitlements = make(map[string]*storedEntitlement)
	b.entitlementApps = make(map[string]map[string]bool)
	b.directoryConfigs = make(map[string]*storedDirectoryConfig)
	b.images = make(map[string]*storedImage)
	b.imagePermissions = make(map[string]*storedImagePermissions)
	b.imageBuilders = make(map[string]*storedImageBuilder)
	b.softwareAssoc = make(map[string]map[string]bool)
	b.exportTasks = make(map[string]*storedExportImageTask)
	b.exportTaskSeq = 0
	b.users = make(map[string]*storedUser)
	b.userStackAssoc = make(map[string]map[string]bool)
	b.sessions = make(map[string]*storedSession)
	b.sessionSeq = 0
	b.usageReport = nil
	b.themes = make(map[string]*storedTheme)
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "appstream", backendSnapshot{
		Stacks:               b.stacks,
		Fleets:               b.fleets,
		Associations:         b.associations,
		Tags:                 b.tags,
		AppBlocks:            b.appBlocks,
		AppBlockBuilders:     b.appBlockBuilders,
		AppBlockBuilderAssoc: b.appBlockBuilderAssoc,
		Applications:         b.applications,
		AppFleetAssoc:        b.appFleetAssoc,
		Entitlements:         b.entitlements,
		EntitlementApps:      b.entitlementApps,
		DirectoryConfigs:     b.directoryConfigs,
		Images:               b.images,
		ImagePermissions:     b.imagePermissions,
		ImageBuilders:        b.imageBuilders,
		SoftwareAssoc:        b.softwareAssoc,
		ExportTasks:          b.exportTasks,
		ExportTaskSeq:        b.exportTaskSeq,
		Users:                b.users,
		UserStackAssoc:       b.userStackAssoc,
		Sessions:             b.sessions,
		SessionSeq:           b.sessionSeq,
		UsageReport:          b.usageReport,
		Themes:               b.themes,
	})
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore( //nolint:gocognit,cyclop,funlen // existing issue.
	ctx context.Context,
	data []byte,
) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "appstream", data, &snap); err != nil {
		return err
	}

	ifNilStack := func(m map[string]*storedStack) map[string]*storedStack {
		if m != nil {
			return m
		}

		return make(map[string]*storedStack)
	}
	ifNilFleet := func(m map[string]*storedFleet) map[string]*storedFleet {
		if m != nil {
			return m
		}

		return make(map[string]*storedFleet)
	}
	ifNilBool := func(m map[string]map[string]bool) map[string]map[string]bool {
		if m != nil {
			return m
		}

		return make(map[string]map[string]bool)
	}
	ifNilTags := func(m map[string]map[string]string) map[string]map[string]string {
		if m != nil {
			return m
		}

		return make(map[string]map[string]string)
	}

	b.stacks = ifNilStack(snap.Stacks)
	b.fleets = ifNilFleet(snap.Fleets)
	b.associations = ifNilBool(snap.Associations)
	b.tags = ifNilTags(snap.Tags)

	if snap.AppBlocks != nil {
		b.appBlocks = snap.AppBlocks
	} else {
		b.appBlocks = make(map[string]*storedAppBlock)
	}
	if snap.AppBlockBuilders != nil {
		b.appBlockBuilders = snap.AppBlockBuilders
	} else {
		b.appBlockBuilders = make(map[string]*storedAppBlockBuilder)
	}
	b.appBlockBuilderAssoc = ifNilBool(snap.AppBlockBuilderAssoc)
	if snap.Applications != nil {
		b.applications = snap.Applications
	} else {
		b.applications = make(map[string]*storedApplication)
	}
	b.appFleetAssoc = ifNilBool(snap.AppFleetAssoc)
	if snap.Entitlements != nil {
		b.entitlements = snap.Entitlements
	} else {
		b.entitlements = make(map[string]*storedEntitlement)
	}
	b.entitlementApps = ifNilBool(snap.EntitlementApps)
	if snap.DirectoryConfigs != nil {
		b.directoryConfigs = snap.DirectoryConfigs
	} else {
		b.directoryConfigs = make(map[string]*storedDirectoryConfig)
	}
	if snap.Images != nil {
		b.images = snap.Images
	} else {
		b.images = make(map[string]*storedImage)
	}
	if snap.ImagePermissions != nil {
		b.imagePermissions = snap.ImagePermissions
	} else {
		b.imagePermissions = make(map[string]*storedImagePermissions)
	}
	if snap.ImageBuilders != nil {
		b.imageBuilders = snap.ImageBuilders
	} else {
		b.imageBuilders = make(map[string]*storedImageBuilder)
	}
	b.softwareAssoc = ifNilBool(snap.SoftwareAssoc)
	if snap.ExportTasks != nil {
		b.exportTasks = snap.ExportTasks
	} else {
		b.exportTasks = make(map[string]*storedExportImageTask)
	}
	if snap.Users != nil {
		b.users = snap.Users
	} else {
		b.users = make(map[string]*storedUser)
	}
	b.userStackAssoc = ifNilBool(snap.UserStackAssoc)
	if snap.Sessions != nil {
		b.sessions = snap.Sessions
	} else {
		b.sessions = make(map[string]*storedSession)
	}
	if snap.Themes != nil {
		b.themes = snap.Themes
	} else {
		b.themes = make(map[string]*storedTheme)
	}

	b.exportTaskSeq = snap.ExportTaskSeq
	b.sessionSeq = snap.SessionSeq
	b.usageReport = snap.UsageReport

	return nil
}

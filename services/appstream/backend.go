package appstream

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
	EnableDefaultInternetAccess *bool             `json:"enableDefaultInternetAccess,omitempty"`
	CreatedTime                 time.Time         `json:"createdTime"`
	Tags                        map[string]string `json:"tags"`
	Name                        string            `json:"name"`
	Arn                         string            `json:"arn"`
	DisplayName                 string            `json:"displayName"`
	Description                 string            `json:"description"`
	InstanceType                string            `json:"instanceType"`
	FleetType                   string            `json:"fleetType"`
	State                       string            `json:"state"`
	ImageName                   string            `json:"imageName,omitempty"`
	ImageArn                    string            `json:"imageArn,omitempty"`
	DesiredInstances            int               `json:"desiredInstances"`
	MaxUserDurationSecs         int               `json:"maxUserDurationSecs"`
	DisconnectTimeoutSecs       int               `json:"disconnectTimeoutSecs"`
	IdleDisconnectTimeoutSecs   int               `json:"idleDisconnectTimeoutSecs"`
}

func (f *storedFleet) toFleet() *Fleet {
	tags := make(map[string]string)
	maps.Copy(tags, f.Tags)

	return &Fleet{
		EnableDefaultInternetAccess: f.EnableDefaultInternetAccess,
		CreatedTime:                 f.CreatedTime,
		Tags:                        tags,
		Name:                        f.Name,
		Arn:                         f.Arn,
		DisplayName:                 f.DisplayName,
		Description:                 f.Description,
		InstanceType:                f.InstanceType,
		FleetType:                   f.FleetType,
		State:                       f.State,
		ImageName:                   f.ImageName,
		ImageArn:                    f.ImageArn,
		DesiredInstances:            f.DesiredInstances,
		MaxUserDurationSecs:         f.MaxUserDurationSecs,
		DisconnectTimeoutSecs:       f.DisconnectTimeoutSecs,
		IdleDisconnectTimeoutSecs:   f.IdleDisconnectTimeoutSecs,
	}
}

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// Every resource collection that is a map[string]*T is a *store.Table[T]
// (see store_setup.go), registered once on registry so Reset/Snapshot/
// Restore collapse to one registry call each instead of one hand-written
// block per map (see pkgs/store's package doc, Phase 3.3 of the datalayer
// refactor). stacks, fleets, appBlocks, appBlockBuilders, applications,
// directoryConfigs, images, imageBuilders, exportTasks, and sessions were
// already flat and keyed by their own real identity field, so each registers
// directly with no composite key. entitlements and users were already keyed
// by a composite string (entitlementKey/userKey) built from two real fields
// each already carries (Name+StackName, UserName+AuthenticationType), so
// both also register directly -- the composite key is just their keyFn.
// themes is keyed directly by its own StackName field (one theme per stack).
// imagePermissions previously had no identity field of its own (it was
// always looked up by an external imageName); it gained a real (not hidden --
// this type is purely internal, never marshaled as an AWS wire shape) Name
// field for this purpose, see storedImagePermissions in backend_image.go.
//
// associations, appBlockBuilderAssoc, appFleetAssoc, entitlementApps,
// softwareAssoc, and userStackAssoc are many-to-many association sets
// (map[string]map[string]bool); tags is map[string]map[string]string. None
// of these have a *T value store.Table could key on, so all seven remain
// plain maps, persisted directly by persistence.go (see its doc comment).
// usageReport is a single scalar record (not a map at all), so it also stays
// a plain field.
type InMemoryBackend struct {
	directoryConfigs     *store.Table[storedDirectoryConfig]
	themes               *store.Table[storedTheme]
	fleets               *store.Table[storedFleet]
	associations         map[string]map[string]bool
	tags                 map[string]map[string]string
	appBlocks            *store.Table[storedAppBlock]
	appBlockBuilders     *store.Table[storedAppBlockBuilder]
	appBlockBuilderAssoc map[string]map[string]bool
	applications         *store.Table[storedApplication]
	appFleetAssoc        map[string]map[string]bool
	entitlements         *store.Table[storedEntitlement]
	imagePermissions     *store.Table[storedImagePermissions]
	stacks               *store.Table[storedStack]
	mu                   *lockmetrics.RWMutex
	entitlementApps      map[string]map[string]bool
	imageBuilders        *store.Table[storedImageBuilder]
	softwareAssoc        map[string]map[string]bool
	exportTasks          *store.Table[storedExportImageTask]
	images               *store.Table[storedImage]
	users                *store.Table[storedUser]
	userStackAssoc       map[string]map[string]bool
	sessions             *store.Table[storedSession]
	registry             *store.Registry
	usageReport          *storedUsageReportSubscription
	accountID            string
	region               string
	sessionSeq           int
	exportTaskSeq        int
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                   lockmetrics.New("appstream"),
		registry:             store.NewRegistry(),
		associations:         make(map[string]map[string]bool),
		tags:                 make(map[string]map[string]string),
		appBlockBuilderAssoc: make(map[string]map[string]bool),
		appFleetAssoc:        make(map[string]map[string]bool),
		entitlementApps:      make(map[string]map[string]bool),
		softwareAssoc:        make(map[string]map[string]bool),
		userStackAssoc:       make(map[string]map[string]bool),
		accountID:            accountID,
		region:               region,
	}

	registerAllTables(b)

	return b
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

	if b.stacks.Has(name) {
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
	b.stacks.Put(s)
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
			s, ok := b.stacks.Get(name)
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, s.toStack())
		}

		return result, nil
	}

	result := make([]*Stack, 0, b.stacks.Len())
	for _, s := range b.stacks.All() {
		result = append(result, s.toStack())
	}

	return result, nil
}

// UpdateStack updates mutable fields of an existing stack.
func (b *InMemoryBackend) UpdateStack(name, displayName, description string) (*Stack, error) {
	b.mu.Lock("UpdateStack")
	defer b.mu.Unlock()

	s, ok := b.stacks.Get(name)
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

	s, ok := b.stacks.Get(name)
	if !ok {
		return ErrNotFound
	}

	for _, stacks := range b.associations {
		if stacks[name] {
			return ErrResourceInUse
		}
	}

	delete(b.tags, s.Arn)
	b.stacks.Delete(name)

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
	name, displayName, description, instanceType, fleetType, imageName, imageArn string,
	desiredInstances, maxUserDuration, disconnectTimeout, idleDisconnectTimeout int,
	enableDefaultInternetAccess *bool,
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

	if b.fleets.Has(name) {
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

	desired := desiredInstances
	if desired == 0 {
		desired = 1
	}

	f := &storedFleet{
		EnableDefaultInternetAccess: enableDefaultInternetAccess,
		CreatedTime:                 time.Now().UTC(),
		Tags:                        storedTags,
		Name:                        name,
		Arn:                         arn,
		DisplayName:                 displayName,
		Description:                 description,
		InstanceType:                instanceType,
		FleetType:                   ft,
		State:                       fleetStateStopped,
		ImageName:                   imageName,
		ImageArn:                    imageArn,
		DesiredInstances:            desired,
		MaxUserDurationSecs:         mux,
		DisconnectTimeoutSecs:       dt,
		IdleDisconnectTimeoutSecs:   idleDisconnectTimeout,
	}
	b.fleets.Put(f)
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

// UpdateFleet updates mutable fields of an existing fleet.
func (b *InMemoryBackend) UpdateFleet(
	name, displayName, description, instanceType, imageName, imageArn string,
	desiredInstances, maxUserDuration, disconnectTimeout, idleDisconnectTimeout int,
	enableDefaultInternetAccess *bool,
) (*Fleet, error) {
	b.mu.Lock("UpdateFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets.Get(name)
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

	if imageName != "" {
		f.ImageName = imageName
	}

	if imageArn != "" {
		f.ImageArn = imageArn
	}

	if desiredInstances > 0 {
		f.DesiredInstances = desiredInstances
	}

	if maxUserDuration > 0 {
		f.MaxUserDurationSecs = maxUserDuration
	}

	if disconnectTimeout > 0 {
		f.DisconnectTimeoutSecs = disconnectTimeout
	}

	if idleDisconnectTimeout >= 0 && idleDisconnectTimeout != f.IdleDisconnectTimeoutSecs {
		f.IdleDisconnectTimeoutSecs = idleDisconnectTimeout
	}

	if enableDefaultInternetAccess != nil {
		f.EnableDefaultInternetAccess = enableDefaultInternetAccess
	}

	return f.toFleet(), nil
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

	for _, stacks := range b.associations {
		delete(stacks, name)
	}

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

// Reset clears all state. Snapshot/Restore (and resetRawMaps, shared with
// Restore's version-mismatch path) live in persistence.go, alongside
// backendSnapshot, since both need the same registry/raw-map inventory this
// method resets.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.resetRawMaps()
}

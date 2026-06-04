package appstream

import (
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
	Stacks       map[string]*storedStack      `json:"stacks"`
	Fleets       map[string]*storedFleet      `json:"fleets"`
	Associations map[string]map[string]bool   `json:"associations"` // fleetName → set of stackNames
	Tags         map[string]map[string]string `json:"tags"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu           *lockmetrics.RWMutex
	stacks       map[string]*storedStack      // name → stack
	fleets       map[string]*storedFleet      // name → fleet
	associations map[string]map[string]bool   // fleetName → set of stackNames
	tags         map[string]map[string]string // ARN → tags
	accountID    string
	region       string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:           lockmetrics.New("appstream"),
		stacks:       make(map[string]*storedStack),
		fleets:       make(map[string]*storedFleet),
		associations: make(map[string]map[string]bool),
		tags:         make(map[string]map[string]string),
		accountID:    accountID,
		region:       region,
	}
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) stackARN(name string) string {
	return fmt.Sprintf("arn:aws:appstream:%s:%s:stack/%s", b.region, b.accountID, name)
}

func (b *InMemoryBackend) fleetARN(name string) string {
	return fmt.Sprintf("arn:aws:appstream:%s:%s:fleet/%s", b.region, b.accountID, name)
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
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(backendSnapshot{
		Stacks:       b.stacks,
		Fleets:       b.fleets,
		Associations: b.associations,
		Tags:         b.tags,
	})

	return data
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	if snap.Stacks != nil {
		b.stacks = snap.Stacks
	} else {
		b.stacks = make(map[string]*storedStack)
	}

	if snap.Fleets != nil {
		b.fleets = snap.Fleets
	} else {
		b.fleets = make(map[string]*storedFleet)
	}

	if snap.Associations != nil {
		b.associations = snap.Associations
	} else {
		b.associations = make(map[string]map[string]bool)
	}

	if snap.Tags != nil {
		b.tags = snap.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	return nil
}

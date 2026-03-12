// Package mq provides an in-memory stub of Amazon MQ.
package mq

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
)

const (
	// BrokerStateRunning indicates an active broker.
	BrokerStateRunning = "RUNNING"
	// BrokerStateCreating indicates a broker being provisioned.
	BrokerStateCreating = "CREATION_IN_PROGRESS"
	// BrokerStateDeleting indicates a broker being removed.
	BrokerStateDeleting = "DELETION_IN_PROGRESS"

	// EngineTypeActiveMQ is the ActiveMQ engine type.
	EngineTypeActiveMQ = "ACTIVEMQ"
	// EngineTypeRabbitMQ is the RabbitMQ engine type.
	EngineTypeRabbitMQ = "RABBITMQ"
)

// BrokerInstance holds endpoint information for a broker instance.
type BrokerInstance struct {
	ConsoleURL string   `json:"consoleURL"`
	Endpoints  []string `json:"endpoints"`
}

// User represents an Amazon MQ broker user.
type User struct {
	Username string   `json:"username"`
	Password string   `json:"password,omitempty"`
	Groups   []string `json:"groups,omitempty"`
	Console  bool     `json:"consoleAccess"`
}

// UserSummary is a summary of a broker user (returned in lists).
type UserSummary struct {
	Username string `json:"username"`
	Console  bool   `json:"consoleAccess"`
}

// ConfigurationID holds a reference to a broker configuration.
type ConfigurationID struct {
	ID       string `json:"id"`
	Revision int32  `json:"revision"`
}

// Configurations holds pending and current configuration references.
type Configurations struct {
	Current *ConfigurationID  `json:"current,omitempty"`
	Pending *ConfigurationID  `json:"pending,omitempty"`
	History []ConfigurationID `json:"history,omitempty"`
}

// Broker represents an Amazon MQ broker.
type Broker struct {
	Tags                    map[string]string `json:"-"`
	Users                   map[string]*User  `json:"-"`
	Configurations          *Configurations   `json:"configurations,omitempty"`
	EngineVersion           string            `json:"engineVersion"`
	BrokerArn               string            `json:"brokerArn"`
	BrokerState             string            `json:"brokerState"`
	DeploymentMode          string            `json:"deploymentMode"`
	EngineType              string            `json:"engineType"`
	BrokerID                string            `json:"brokerId"`
	HostInstanceType        string            `json:"hostInstanceType"`
	BrokerName              string            `json:"brokerName"`
	Created                 string            `json:"created"`
	SubnetIDs               []string          `json:"subnetIds,omitempty"`
	SecurityGroups          []string          `json:"securityGroups,omitempty"`
	BrokerInstances         []BrokerInstance  `json:"brokerInstances,omitempty"`
	PubliclyAccessible      bool              `json:"publiclyAccessible"`
	AutoMinorVersionUpgrade bool              `json:"autoMinorVersionUpgrade"`
}

// ConfigurationRevision holds revision metadata for a configuration.
type ConfigurationRevision struct {
	Created     string `json:"created"`
	Description string `json:"description,omitempty"`
	Revision    int32  `json:"revision"`
}

// Configuration represents an Amazon MQ configuration.
type Configuration struct {
	Tags           map[string]string       `json:"-"`
	Data           map[int32]string        `json:"-"`
	LatestRevision *ConfigurationRevision  `json:"latestRevision"`
	Arn            string                  `json:"arn"`
	ID             string                  `json:"id"`
	Name           string                  `json:"name"`
	Description    string                  `json:"description"`
	EngineType     string                  `json:"engineType"`
	EngineVersion  string                  `json:"engineVersion"`
	Created        string                  `json:"created"`
	Revisions      []ConfigurationRevision `json:"-"`
}

// InMemoryBackend stores Amazon MQ state in memory.
type InMemoryBackend struct {
	brokers        map[string]*Broker
	configurations map[string]*Configuration
	tags           map[string]map[string]string
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new in-memory Amazon MQ backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		brokers:        make(map[string]*Broker),
		configurations: make(map[string]*Configuration),
		tags:           make(map[string]map[string]string),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("mq"),
	}
}

// Region returns the region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// --- Broker operations ---

// CreateBroker creates a new Amazon MQ broker.
func (b *InMemoryBackend) CreateBroker(
	name, deploymentMode, engineType, engineVersion, hostInstanceType string,
	publiclyAccessible, autoMinorVersionUpgrade bool,
	securityGroups, subnetIDs []string,
	users []*User,
	tags map[string]string,
) (*Broker, error) {
	b.mu.Lock("CreateBroker")
	defer b.mu.Unlock()

	// Check for duplicate by name.
	for _, br := range b.brokers {
		if br.BrokerName == name {
			return nil, fmt.Errorf("%w: broker %s already exists", ErrAlreadyExists, name)
		}
	}

	if deploymentMode == "" {
		deploymentMode = "SINGLE_INSTANCE"
	}

	if engineVersion == "" {
		if engineType == EngineTypeRabbitMQ {
			engineVersion = "3.11.20"
		} else {
			engineVersion = "5.15.14"
		}
	}

	id := uuid.NewString()
	brokerArn := arn.Build("mq", b.region, b.accountID, "broker:"+name)
	created := time.Now().UTC().Format(time.RFC3339)

	endpoint := buildEndpoint(engineType, name)
	instances := []BrokerInstance{
		{
			ConsoleURL: fmt.Sprintf("http://%s.mq.%s.amazonaws.com:8162", id, b.region),
			Endpoints:  []string{endpoint},
		},
	}

	userMap := make(map[string]*User)
	for _, u := range users {
		cp := *u
		userMap[u.Username] = &cp
	}

	tagsCopy := make(map[string]string)
	maps.Copy(tagsCopy, tags)

	br := &Broker{
		BrokerArn:               brokerArn,
		BrokerID:                id,
		BrokerName:              name,
		BrokerState:             BrokerStateRunning,
		DeploymentMode:          deploymentMode,
		EngineType:              engineType,
		EngineVersion:           engineVersion,
		HostInstanceType:        hostInstanceType,
		PubliclyAccessible:      publiclyAccessible,
		AutoMinorVersionUpgrade: autoMinorVersionUpgrade,
		SecurityGroups:          securityGroups,
		SubnetIDs:               subnetIDs,
		BrokerInstances:         instances,
		Users:                   userMap,
		Tags:                    tagsCopy,
		Created:                 created,
	}

	b.brokers[id] = br
	b.tags[brokerArn] = tagsCopy

	return b.copyBroker(br), nil
}

// buildEndpoint returns a placeholder endpoint URL for the broker.
func buildEndpoint(engineType, name string) string {
	switch engineType {
	case EngineTypeRabbitMQ:
		return fmt.Sprintf("amqps://%s.mq.amazonaws.com:5671", name)
	default:
		return fmt.Sprintf("ssl://%s.mq.amazonaws.com:61617", name)
	}
}

// DescribeBroker returns a broker by ID or name.
func (b *InMemoryBackend) DescribeBroker(brokerID string) (*Broker, error) {
	b.mu.RLock("DescribeBroker")
	defer b.mu.RUnlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	return b.copyBroker(br), nil
}

// ListBrokers returns all brokers sorted by name.
func (b *InMemoryBackend) ListBrokers() []*Broker {
	b.mu.RLock("ListBrokers")
	defer b.mu.RUnlock()

	list := make([]*Broker, 0, len(b.brokers))
	for _, br := range b.brokers {
		list = append(list, b.copyBroker(br))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].BrokerName < list[j].BrokerName })

	return list
}

// DeleteBroker removes a broker by ID or name.
func (b *InMemoryBackend) DeleteBroker(brokerID string) (*Broker, error) {
	b.mu.Lock("DeleteBroker")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	cp := b.copyBroker(br)
	delete(b.brokers, br.BrokerID)
	delete(b.tags, br.BrokerArn)

	return cp, nil
}

// RebootBroker simulates a broker reboot (no-op in the mock).
func (b *InMemoryBackend) RebootBroker(brokerID string) error {
	b.mu.Lock("RebootBroker")
	defer b.mu.Unlock()

	if b.lookupBroker(brokerID) == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	return nil
}

// UpdateBroker updates mutable broker fields.
func (b *InMemoryBackend) UpdateBroker(
	brokerID, engineVersion, hostInstanceType string,
	autoMinorVersionUpgrade *bool,
	securityGroups []string,
) (*Broker, error) {
	b.mu.Lock("UpdateBroker")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	if engineVersion != "" {
		br.EngineVersion = engineVersion
	}

	if hostInstanceType != "" {
		br.HostInstanceType = hostInstanceType
	}

	if autoMinorVersionUpgrade != nil {
		br.AutoMinorVersionUpgrade = *autoMinorVersionUpgrade
	}

	if securityGroups != nil {
		br.SecurityGroups = securityGroups
	}

	return b.copyBroker(br), nil
}

// lookupBroker finds a broker by ID or by name; caller must hold a lock.
func (b *InMemoryBackend) lookupBroker(brokerID string) *Broker {
	if br, ok := b.brokers[brokerID]; ok {
		return br
	}

	for _, br := range b.brokers {
		if br.BrokerName == brokerID {
			return br
		}
	}

	return nil
}

// copyBroker returns a shallow copy of a broker with deep-copied slices/maps.
func (b *InMemoryBackend) copyBroker(br *Broker) *Broker {
	cp := *br

	cp.Tags = make(map[string]string, len(br.Tags))
	maps.Copy(cp.Tags, br.Tags)

	cp.Users = make(map[string]*User, len(br.Users))
	for k, u := range br.Users {
		uc := *u
		cp.Users[k] = &uc
	}

	if len(br.SubnetIDs) > 0 {
		cp.SubnetIDs = append([]string{}, br.SubnetIDs...)
	}

	if len(br.SecurityGroups) > 0 {
		cp.SecurityGroups = append([]string{}, br.SecurityGroups...)
	}

	cp.BrokerInstances = append([]BrokerInstance{}, br.BrokerInstances...)

	return &cp
}

// --- User operations ---

// CreateUser creates a user on a broker.
func (b *InMemoryBackend) CreateUser(brokerID, username, password string, groups []string, console bool) error {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	if _, ok := br.Users[username]; ok {
		return fmt.Errorf("%w: user %s already exists on broker %s", ErrAlreadyExists, username, brokerID)
	}

	br.Users[username] = &User{
		Username: username,
		Password: password,
		Groups:   groups,
		Console:  console,
	}

	return nil
}

// DescribeUser returns a user from a broker.
func (b *InMemoryBackend) DescribeUser(brokerID, username string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	u, ok := br.Users[username]
	if !ok {
		return nil, fmt.Errorf("%w: user %s not found on broker %s", ErrNotFound, username, brokerID)
	}

	cp := *u

	return &cp, nil
}

// UpdateUser updates a broker user.
func (b *InMemoryBackend) UpdateUser(brokerID, username, password string, groups []string, console *bool) error {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	u, ok := br.Users[username]
	if !ok {
		return fmt.Errorf("%w: user %s not found on broker %s", ErrNotFound, username, brokerID)
	}

	if password != "" {
		u.Password = password
	}

	if groups != nil {
		u.Groups = groups
	}

	if console != nil {
		u.Console = *console
	}

	return nil
}

// DeleteUser removes a user from a broker.
func (b *InMemoryBackend) DeleteUser(brokerID, username string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	if _, ok := br.Users[username]; !ok {
		return fmt.Errorf("%w: user %s not found on broker %s", ErrNotFound, username, brokerID)
	}

	delete(br.Users, username)

	return nil
}

// ListUsers returns all users for a broker.
func (b *InMemoryBackend) ListUsers(brokerID string) ([]UserSummary, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return nil, fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	list := make([]UserSummary, 0, len(br.Users))
	for _, u := range br.Users {
		list = append(list, UserSummary{Username: u.Username, Console: u.Console})
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Username < list[j].Username })

	return list, nil
}

// --- Configuration operations ---

// CreateConfiguration creates a new Amazon MQ configuration.
func (b *InMemoryBackend) CreateConfiguration(
	name, description, engineType, engineVersion string,
	tags map[string]string,
) (*Configuration, error) {
	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	for _, c := range b.configurations {
		if c.Name == name {
			return nil, fmt.Errorf("%w: configuration %s already exists", ErrAlreadyExists, name)
		}
	}

	if engineVersion == "" {
		if engineType == EngineTypeRabbitMQ {
			engineVersion = "3.11.20"
		} else {
			engineVersion = "5.15.14"
		}
	}

	id := "c-" + uuid.NewString()[:8]
	configArn := arn.Build("mq", b.region, b.accountID, "configuration:"+id)
	now := time.Now().UTC().Format(time.RFC3339)

	rev := ConfigurationRevision{
		Created:     now,
		Description: description,
		Revision:    1,
	}

	tagsCopy := make(map[string]string)
	maps.Copy(tagsCopy, tags)

	cfg := &Configuration{
		Arn:            configArn,
		ID:             id,
		Name:           name,
		Description:    description,
		EngineType:     engineType,
		EngineVersion:  engineVersion,
		LatestRevision: &rev,
		Created:        now,
		Tags:           tagsCopy,
		Revisions:      []ConfigurationRevision{rev},
		Data:           map[int32]string{1: ""},
	}

	b.configurations[id] = cfg
	b.tags[configArn] = tagsCopy

	return b.copyConfiguration(cfg), nil
}

// DescribeConfiguration returns a configuration by ID.
func (b *InMemoryBackend) DescribeConfiguration(configID string) (*Configuration, error) {
	b.mu.RLock("DescribeConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.configurations[configID]
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	return b.copyConfiguration(cfg), nil
}

// ListConfigurations returns all configurations sorted by name.
func (b *InMemoryBackend) ListConfigurations() []*Configuration {
	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	list := make([]*Configuration, 0, len(b.configurations))
	for _, c := range b.configurations {
		list = append(list, b.copyConfiguration(c))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateConfiguration updates a configuration (creates a new revision).
func (b *InMemoryBackend) UpdateConfiguration(configID, description, data string) (*Configuration, error) {
	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.configurations[configID]
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	nextRev := int32(len(cfg.Revisions)) + 1 //nolint:gosec // len() is bounded by slice capacity, never overflows int32
	now := time.Now().UTC().Format(time.RFC3339)

	rev := ConfigurationRevision{
		Created:     now,
		Description: description,
		Revision:    nextRev,
	}

	if description != "" {
		cfg.Description = description
	}

	cfg.LatestRevision = &rev
	cfg.Revisions = append(cfg.Revisions, rev)
	cfg.Data[nextRev] = data

	return b.copyConfiguration(cfg), nil
}

// copyConfiguration returns a copy of a configuration.
func (b *InMemoryBackend) copyConfiguration(c *Configuration) *Configuration {
	cp := *c

	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	if c.LatestRevision != nil {
		rev := *c.LatestRevision
		cp.LatestRevision = &rev
	}

	cp.Revisions = append([]ConfigurationRevision{}, c.Revisions...)

	cp.Data = make(map[int32]string, len(c.Data))
	maps.Copy(cp.Data, c.Data)

	return &cp
}

// --- Tag operations ---

// ListTags returns tags for a resource ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) map[string]string {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	t := b.tags[resourceARN]
	cp := make(map[string]string, len(t))
	maps.Copy(cp, t)

	return cp
}

// CreateTags adds or updates tags for a resource ARN.
func (b *InMemoryBackend) CreateTags(resourceARN string, tags map[string]string) {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	// Keep broker/config tags in sync.
	for _, br := range b.brokers {
		if br.BrokerArn == resourceARN {
			maps.Copy(br.Tags, tags)

			break
		}
	}

	for _, cfg := range b.configurations {
		if cfg.Arn == resourceARN {
			maps.Copy(cfg.Tags, tags)

			break
		}
	}
}

// DeleteTags removes the specified tag keys from a resource ARN.
func (b *InMemoryBackend) DeleteTags(resourceARN string, tagKeys []string) {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)

		for _, br := range b.brokers {
			if br.BrokerArn == resourceARN {
				delete(br.Tags, k)
			}
		}

		for _, cfg := range b.configurations {
			if cfg.Arn == resourceARN {
				delete(cfg.Tags, k)
			}
		}
	}
}

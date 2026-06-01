package medialive

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultMaxResults = 20

	stateIdle     = "IDLE"
	stateRunning  = "RUNNING"
	stateDeleted  = "DELETED"
	stateDeleting = "DELETING"

	stateDetached = "DETACHED"

	channelClassStandard     = "STANDARD"
	inputTypeUDPPush         = "UDP_PUSH"
	inputSecurityGroupActive = "IDLE"

	resourceTypeChannel            = "channel"
	resourceTypeInput              = "input"
	resourceTypeInputSecurityGroup = "inputSecurityGroup"
)

// ErrNotFound is returned when a resource does not exist.
var ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)

// ErrConflict is returned for state conflict operations.
var ErrConflict = awserr.New("ConflictException", awserr.ErrAlreadyExists)

// ErrInvalidParameter is returned for invalid input.
var ErrInvalidParameter = awserr.New("BadRequestException", awserr.ErrInvalidParameter)

type storedChannel struct {
	Tags         map[string]string `json:"tags"`
	ARN          string            `json:"arn"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	ChannelClass string            `json:"channelClass"`
	RoleARN      string            `json:"roleArn"`
	State        string            `json:"state"`
}

func (c *storedChannel) toChannel() *Channel {
	tags := make(map[string]string, len(c.Tags))
	maps.Copy(tags, c.Tags)

	return &Channel{
		ARN:          c.ARN,
		ID:           c.ID,
		Name:         c.Name,
		ChannelClass: c.ChannelClass,
		RoleARN:      c.RoleARN,
		State:        c.State,
		Tags:         tags,
	}
}

func (c *storedChannel) toSummary() *ChannelSummary {
	return &ChannelSummary{
		ARN:          c.ARN,
		ID:           c.ID,
		Name:         c.Name,
		ChannelClass: c.ChannelClass,
		State:        c.State,
	}
}

type storedInput struct {
	Tags      map[string]string `json:"tags"`
	ARN       string            `json:"arn"`
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	InputType string            `json:"inputType"`
	RoleARN   string            `json:"roleArn"`
	State     string            `json:"state"`
}

func (i *storedInput) toInput() *Input {
	tags := make(map[string]string, len(i.Tags))
	maps.Copy(tags, i.Tags)

	return &Input{
		ARN:       i.ARN,
		ID:        i.ID,
		Name:      i.Name,
		InputType: i.InputType,
		RoleARN:   i.RoleARN,
		State:     i.State,
		Tags:      tags,
	}
}

func (i *storedInput) toSummary() *InputSummary {
	return &InputSummary{
		ARN:       i.ARN,
		ID:        i.ID,
		Name:      i.Name,
		InputType: i.InputType,
		State:     i.State,
	}
}

type storedInputSecurityGroup struct {
	Tags           map[string]string `json:"tags"`
	ARN            string            `json:"arn"`
	ID             string            `json:"id"`
	State          string            `json:"state"`
	WhitelistRules []WhitelistRule   `json:"whitelistRules"`
}

func (g *storedInputSecurityGroup) toGroup() *InputSecurityGroup {
	tags := make(map[string]string, len(g.Tags))
	maps.Copy(tags, g.Tags)

	rules := make([]WhitelistRule, len(g.WhitelistRules))
	copy(rules, g.WhitelistRules)

	return &InputSecurityGroup{
		ARN:            g.ARN,
		ID:             g.ID,
		State:          g.State,
		WhitelistRules: rules,
		Tags:           tags,
	}
}

func (g *storedInputSecurityGroup) toSummary() *InputSecurityGroupSummary {
	return &InputSecurityGroupSummary{
		ARN:   g.ARN,
		ID:    g.ID,
		State: g.State,
	}
}

type snapshot struct {
	Channels            map[string]*storedChannel            `json:"channels"`
	Inputs              map[string]*storedInput              `json:"inputs"`
	InputSecurityGroups map[string]*storedInputSecurityGroup `json:"inputSecurityGroups"`
	Tags                map[string]map[string]string         `json:"tags"`
	AccountID           string                               `json:"accountId"`
	Region              string                               `json:"region"`
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu                  *lockmetrics.RWMutex
	channels            map[string]*storedChannel
	inputs              map[string]*storedInput
	inputSecurityGroups map[string]*storedInputSecurityGroup
	tags                map[string]map[string]string
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                  lockmetrics.New("medialive"),
		channels:            make(map[string]*storedChannel),
		inputs:              make(map[string]*storedInput),
		inputSecurityGroups: make(map[string]*storedInputSecurityGroup),
		tags:                make(map[string]map[string]string),
		accountID:           accountID,
		region:              region,
	}
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.channels = make(map[string]*storedChannel)
	b.inputs = make(map[string]*storedInput)
	b.inputSecurityGroups = make(map[string]*storedInputSecurityGroup)
	b.tags = make(map[string]map[string]string)
}

// Snapshot serializes current state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	s := snapshot{
		Channels:            b.channels,
		Inputs:              b.inputs,
		InputSecurityGroups: b.inputSecurityGroups,
		Tags:                b.tags,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	data, _ := json.Marshal(s)

	return data
}

// Restore deserializes state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.channels = s.Channels
	b.inputs = s.Inputs
	b.inputSecurityGroups = s.InputSecurityGroups
	b.tags = s.Tags
	b.accountID = s.AccountID
	b.region = s.Region

	return nil
}

func (b *InMemoryBackend) channelARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeChannel+":"+id)
}

func (b *InMemoryBackend) inputARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeInput+":"+id)
}

func (b *InMemoryBackend) inputSecurityGroupARN(id string) string {
	return arn.Build("medialive", b.region, b.accountID, resourceTypeInputSecurityGroup+":"+id)
}

func newID() string {
	return uuid.New().String()[:8]
}

// --- Channel operations ---

// CreateChannel creates a new channel.
func (b *InMemoryBackend) CreateChannel(name, channelClass, roleArn string, tags map[string]string) (*Channel, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	if channelClass == "" {
		channelClass = channelClassStandard
	}

	id := newID()
	ch := &storedChannel{
		ARN:          b.channelARN(id),
		ID:           id,
		Name:         name,
		ChannelClass: channelClass,
		RoleARN:      roleArn,
		State:        stateIdle,
		Tags:         copyTags(tags),
	}

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	b.channels[id] = ch

	return ch.toChannel(), nil
}

// DescribeChannel returns a channel by ID.
func (b *InMemoryBackend) DescribeChannel(channelID string) (*Channel, error) {
	b.mu.RLock("DescribeChannel")
	defer b.mu.RUnlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	return ch.toChannel(), nil
}

// UpdateChannel updates a channel's mutable fields.
func (b *InMemoryBackend) UpdateChannel(channelID, name, roleArn string) (*Channel, error) {
	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if name != "" {
		ch.Name = name
	}

	if roleArn != "" {
		ch.RoleARN = roleArn
	}

	return ch.toChannel(), nil
}

// DeleteChannel deletes a channel.
func (b *InMemoryBackend) DeleteChannel(channelID string) (*Channel, error) {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if ch.State == stateRunning {
		return nil, fmt.Errorf("%w: channel must be idle before deleting", ErrConflict)
	}

	ch.State = stateDeleted
	delete(b.channels, channelID)

	return ch.toChannel(), nil
}

// ListChannels returns a paginated list of channels.
func (b *InMemoryBackend) ListChannels(maxResults int, nextToken string) ([]*ChannelSummary, string, error) {
	b.mu.RLock("ListChannels")
	defer b.mu.RUnlock()

	all := make([]*storedChannel, 0, len(b.channels))
	for _, ch := range b.channels {
		all = append(all, ch)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*ChannelSummary, 0, len(pg.Data))
	for _, ch := range pg.Data {
		summaries = append(summaries, ch.toSummary())
	}

	return summaries, pg.Next, nil
}

// StartChannel transitions a channel to RUNNING.
func (b *InMemoryBackend) StartChannel(channelID string) (*Channel, error) {
	b.mu.Lock("StartChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if ch.State != stateIdle {
		return nil, fmt.Errorf("%w: channel must be idle to start", ErrConflict)
	}

	ch.State = stateRunning

	return ch.toChannel(), nil
}

// StopChannel transitions a channel to IDLE.
func (b *InMemoryBackend) StopChannel(channelID string) (*Channel, error) {
	b.mu.Lock("StopChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if ch.State != stateRunning {
		return nil, fmt.Errorf("%w: channel must be running to stop", ErrConflict)
	}

	ch.State = stateIdle

	return ch.toChannel(), nil
}

// --- Input operations ---

// CreateInput creates a new input.
func (b *InMemoryBackend) CreateInput(name, inputType, roleArn string, tags map[string]string) (*Input, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	if inputType == "" {
		inputType = inputTypeUDPPush
	}

	id := newID()
	inp := &storedInput{
		ARN:       b.inputARN(id),
		ID:        id,
		Name:      name,
		InputType: inputType,
		RoleARN:   roleArn,
		State:     stateDetached,
		Tags:      copyTags(tags),
	}

	b.mu.Lock("CreateInput")
	defer b.mu.Unlock()

	b.inputs[id] = inp

	return inp.toInput(), nil
}

// DescribeInput returns an input by ID.
func (b *InMemoryBackend) DescribeInput(inputID string) (*Input, error) {
	b.mu.RLock("DescribeInput")
	defer b.mu.RUnlock()

	inp, ok := b.inputs[inputID]
	if !ok {
		return nil, fmt.Errorf("%w: input %s not found", ErrNotFound, inputID)
	}

	return inp.toInput(), nil
}

// UpdateInput updates an input's mutable fields.
func (b *InMemoryBackend) UpdateInput(inputID, name, roleArn string) (*Input, error) {
	b.mu.Lock("UpdateInput")
	defer b.mu.Unlock()

	inp, ok := b.inputs[inputID]
	if !ok {
		return nil, fmt.Errorf("%w: input %s not found", ErrNotFound, inputID)
	}

	if name != "" {
		inp.Name = name
	}

	if roleArn != "" {
		inp.RoleARN = roleArn
	}

	return inp.toInput(), nil
}

// DeleteInput deletes an input.
func (b *InMemoryBackend) DeleteInput(inputID string) error {
	b.mu.Lock("DeleteInput")
	defer b.mu.Unlock()

	if _, ok := b.inputs[inputID]; !ok {
		return fmt.Errorf("%w: input %s not found", ErrNotFound, inputID)
	}

	delete(b.inputs, inputID)

	return nil
}

// ListInputs returns a paginated list of inputs.
func (b *InMemoryBackend) ListInputs(maxResults int, nextToken string) ([]*InputSummary, string, error) {
	b.mu.RLock("ListInputs")
	defer b.mu.RUnlock()

	all := make([]*storedInput, 0, len(b.inputs))
	for _, inp := range b.inputs {
		all = append(all, inp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*InputSummary, 0, len(pg.Data))
	for _, inp := range pg.Data {
		summaries = append(summaries, inp.toSummary())
	}

	return summaries, pg.Next, nil
}

// --- InputSecurityGroup operations ---

// CreateInputSecurityGroup creates a new input security group.
func (b *InMemoryBackend) CreateInputSecurityGroup(
	whitelistRules []WhitelistRule,
	tags map[string]string,
) (*InputSecurityGroup, error) {
	id := newID()
	rules := make([]WhitelistRule, len(whitelistRules))
	copy(rules, whitelistRules)

	g := &storedInputSecurityGroup{
		ARN:            b.inputSecurityGroupARN(id),
		ID:             id,
		State:          inputSecurityGroupActive,
		WhitelistRules: rules,
		Tags:           copyTags(tags),
	}

	b.mu.Lock("CreateInputSecurityGroup")
	defer b.mu.Unlock()

	b.inputSecurityGroups[id] = g

	return g.toGroup(), nil
}

// DescribeInputSecurityGroup returns an input security group by ID.
func (b *InMemoryBackend) DescribeInputSecurityGroup(groupID string) (*InputSecurityGroup, error) {
	b.mu.RLock("DescribeInputSecurityGroup")
	defer b.mu.RUnlock()

	g, ok := b.inputSecurityGroups[groupID]
	if !ok {
		return nil, fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	return g.toGroup(), nil
}

// UpdateInputSecurityGroup updates an input security group's whitelist rules.
func (b *InMemoryBackend) UpdateInputSecurityGroup(
	groupID string,
	whitelistRules []WhitelistRule,
) (*InputSecurityGroup, error) {
	b.mu.Lock("UpdateInputSecurityGroup")
	defer b.mu.Unlock()

	g, ok := b.inputSecurityGroups[groupID]
	if !ok {
		return nil, fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	rules := make([]WhitelistRule, len(whitelistRules))
	copy(rules, whitelistRules)

	g.WhitelistRules = rules

	return g.toGroup(), nil
}

// DeleteInputSecurityGroup deletes an input security group.
func (b *InMemoryBackend) DeleteInputSecurityGroup(groupID string) error {
	b.mu.Lock("DeleteInputSecurityGroup")
	defer b.mu.Unlock()

	if _, ok := b.inputSecurityGroups[groupID]; !ok {
		return fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	delete(b.inputSecurityGroups, groupID)

	return nil
}

// ListInputSecurityGroups returns a paginated list of input security groups.
func (b *InMemoryBackend) ListInputSecurityGroups(
	maxResults int,
	nextToken string,
) ([]*InputSecurityGroupSummary, string, error) {
	b.mu.RLock("ListInputSecurityGroups")
	defer b.mu.RUnlock()

	all := make([]*storedInputSecurityGroup, 0, len(b.inputSecurityGroups))
	for _, g := range b.inputSecurityGroups {
		all = append(all, g)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*InputSecurityGroupSummary, 0, len(pg.Data))
	for _, g := range pg.Data {
		summaries = append(summaries, g.toSummary())
	}

	return summaries, pg.Next, nil
}

// --- Tag operations ---

// CreateTags adds tags to a resource.
func (b *InMemoryBackend) CreateTags(resourceARN string, tags map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// DeleteTags removes tag keys from a resource.
func (b *InMemoryBackend) DeleteTags(resourceARN string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	if existing == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing := b.tags[resourceARN]
	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return make(map[string]string)
	}

	result := make(map[string]string, len(tags))
	maps.Copy(result, tags)

	return result
}

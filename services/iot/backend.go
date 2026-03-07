package iot

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrThingNotFound is returned when a Thing does not exist.
var ErrThingNotFound = errors.New("thing not found")

// ErrRuleNotFound is returned when a TopicRule does not exist.
var ErrRuleNotFound = errors.New("topic rule not found")

// ErrPolicyNotFound is returned when a Policy does not exist.
var ErrPolicyNotFound = errors.New("policy not found")

// RuleDispatcher is implemented by the CLI wiring layer and dispatches rule actions.
type RuleDispatcher interface {
	SendToSQS(queueURL, body string) error
	InvokeLambda(ctx context.Context, functionARN string, payload []byte) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	dispatcher RuleDispatcher
	things     map[string]*Thing
	policies   map[string]*Policy
	rules      map[string]*TopicRule
	accountID  string
	region     string
	mqttPort   int
	mu         sync.RWMutex
}

// mqttDefaultPort is the default TCP port for the embedded MQTT broker.
const mqttDefaultPort = 1883

// NewInMemoryBackend creates a new InMemoryBackend with default values.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		things:    make(map[string]*Thing),
		policies:  make(map[string]*Policy),
		rules:     make(map[string]*TopicRule),
		accountID: "000000000000",
		region:    "us-east-1",
		mqttPort:  mqttDefaultPort,
	}
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	b := NewInMemoryBackend()
	b.accountID = accountID
	b.region = region

	return b
}

// SetRuleDispatcher wires the SQS/Lambda action dispatcher.
func (b *InMemoryBackend) SetRuleDispatcher(d RuleDispatcher) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.dispatcher = d
}

// GetDispatcher returns the current rule dispatcher (used by the broker hook).
func (b *InMemoryBackend) GetDispatcher() RuleDispatcher {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.dispatcher
}

// GetRules returns a snapshot of all active rules (used by the broker hook).
func (b *InMemoryBackend) GetRules() []*TopicRule {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*TopicRule, 0, len(b.rules))

	for _, r := range b.rules {
		out = append(out, r)
	}

	return out
}

// MQTTPort returns the configured TCP port for the MQTT broker.
func (b *InMemoryBackend) MQTTPort() int {
	return b.mqttPort
}

// CreateThing creates a new IoT Thing.
func (b *InMemoryBackend) CreateThing(input *CreateThingInput) (*CreateThingOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	attrs := map[string]string{}

	if input.AttributePayload != nil && input.AttributePayload.Attributes != nil {
		maps.Copy(attrs, input.AttributePayload.Attributes)
	}

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:thing/%s", b.region, b.accountID, input.ThingName)
	id := uuid.NewString()

	b.things[input.ThingName] = &Thing{
		ThingName:  input.ThingName,
		ThingType:  input.ThingTypeName,
		Attributes: attrs,
		ARN:        arn,
		Version:    1,
		CreatedAt:  time.Now(),
	}

	return &CreateThingOutput{
		ThingName: input.ThingName,
		ThingARN:  arn,
		ThingID:   id,
	}, nil
}

// DescribeThing returns an existing Thing.
func (b *InMemoryBackend) DescribeThing(thingName string) (*Thing, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.things[thingName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	return t, nil
}

// ListThings returns all Things.
func (b *InMemoryBackend) ListThings() []*Thing {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Thing, 0, len(b.things))

	for _, t := range b.things {
		out = append(out, t)
	}

	return out
}

// DeleteThing deletes a Thing by name.
func (b *InMemoryBackend) DeleteThing(thingName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.things[thingName]; !ok {
		return fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	delete(b.things, thingName)

	return nil
}

// CreateTopicRule creates a new IoT Topic Rule.
func (b *InMemoryBackend) CreateTopicRule(input *CreateTopicRuleInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	payload := input.TopicRulePayload
	if payload == nil {
		payload = &TopicRulePayload{}
	}

	b.rules[input.RuleName] = &TopicRule{
		RuleName:    input.RuleName,
		SQL:         payload.SQL,
		Description: payload.Description,
		Actions:     payload.Actions,
		Enabled:     !payload.RuleDisabled,
		CreatedAt:   time.Now(),
	}

	return nil
}

// GetTopicRule returns an existing Topic Rule.
func (b *InMemoryBackend) GetTopicRule(ruleName string) (*TopicRule, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	r, ok := b.rules[ruleName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	return r, nil
}

// ListTopicRules returns all Topic Rules.
func (b *InMemoryBackend) ListTopicRules() []*TopicRule {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*TopicRule, 0, len(b.rules))

	for _, r := range b.rules {
		out = append(out, r)
	}

	return out
}

// DeleteTopicRule deletes a Topic Rule by name.
func (b *InMemoryBackend) DeleteTopicRule(ruleName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.rules[ruleName]; !ok {
		return fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	delete(b.rules, ruleName)

	return nil
}

// CreatePolicy creates a new IoT Policy (stub).
func (b *InMemoryBackend) CreatePolicy(input *CreatePolicyInput) (*CreatePolicyOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:policy/%s", b.region, b.accountID, input.PolicyName)

	b.policies[input.PolicyName] = &Policy{
		PolicyName:     input.PolicyName,
		PolicyDocument: input.PolicyDocument,
		ARN:            arn,
	}

	return &CreatePolicyOutput{
		PolicyName:     input.PolicyName,
		PolicyARN:      arn,
		PolicyDocument: input.PolicyDocument,
	}, nil
}

// AttachPrincipalPolicy attaches a policy to a principal (stub, no-op).
func (b *InMemoryBackend) AttachPrincipalPolicy(_ *AttachPrincipalPolicyInput) error {
	return nil
}

// DescribeEndpoint returns the MQTT broker endpoint address.
func (b *InMemoryBackend) DescribeEndpoint(_ string) (*DescribeEndpointOutput, error) {
	return &DescribeEndpointOutput{
		EndpointAddress: fmt.Sprintf("mqtt.%s.amazonaws.com", b.region),
	}, nil
}

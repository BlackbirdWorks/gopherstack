package iot

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrThingNotFound is returned when a Thing does not exist.
	ErrThingNotFound = errors.New("thing not found")

	// ErrRuleNotFound is returned when a TopicRule does not exist.
	ErrRuleNotFound = errors.New("topic rule not found")

	// ErrPolicyNotFound is returned when a Policy does not exist.
	ErrPolicyNotFound = errors.New("policy not found")

	// ErrValidation is returned when an input fails validation.
	ErrValidation = errors.New("validation error")

	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("resource already exists")
)

// RuleDispatcher is implemented by the CLI wiring layer and dispatches rule actions.
type RuleDispatcher interface {
	SendToSQS(queueURL, body string) error
	InvokeLambda(ctx context.Context, functionARN string, payload []byte) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	dispatcher             RuleDispatcher
	things                 map[string]*Thing
	policies               map[string]*Policy
	rules                  map[string]*TopicRule
	certificateTransfers   map[string]string
	thingBillingGroups     map[string]string
	thingThingGroups       map[string][]string
	packageVersionSboms    map[string]*SbomDocument
	jobTargets             map[string][]string
	policyTargets          map[string][]string
	securityProfileTargets map[string][]string
	thingPrincipals        map[string][]string
	auditMitigationTasks   map[string]string
	auditTasks             map[string]string
	accountID              string
	region                 string
	mqttPort               int
	mu                     sync.RWMutex
}

// Compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// mqttDefaultPort is the default TCP port for the embedded MQTT broker.
const mqttDefaultPort = 1883

// NewInMemoryBackend creates a new InMemoryBackend with default values.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		things:                 make(map[string]*Thing),
		policies:               make(map[string]*Policy),
		rules:                  make(map[string]*TopicRule),
		certificateTransfers:   make(map[string]string),
		thingBillingGroups:     make(map[string]string),
		thingThingGroups:       make(map[string][]string),
		packageVersionSboms:    make(map[string]*SbomDocument),
		jobTargets:             make(map[string][]string),
		policyTargets:          make(map[string][]string),
		securityProfileTargets: make(map[string][]string),
		thingPrincipals:        make(map[string][]string),
		auditMitigationTasks:   make(map[string]string),
		auditTasks:             make(map[string]string),
		accountID:              "000000000000",
		region:                 "us-east-1",
		mqttPort:               mqttDefaultPort,
	}
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	b := NewInMemoryBackend()
	b.accountID = accountID
	b.region = region

	return b
}

// Reset clears all backend state. Useful for test isolation.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.things = make(map[string]*Thing)
	b.policies = make(map[string]*Policy)
	b.rules = make(map[string]*TopicRule)
	b.certificateTransfers = make(map[string]string)
	b.thingBillingGroups = make(map[string]string)
	b.thingThingGroups = make(map[string][]string)
	b.packageVersionSboms = make(map[string]*SbomDocument)
	b.jobTargets = make(map[string][]string)
	b.policyTargets = make(map[string][]string)
	b.securityProfileTargets = make(map[string][]string)
	b.thingPrincipals = make(map[string][]string)
	b.auditMitigationTasks = make(map[string]string)
	b.auditTasks = make(map[string]string)
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
		out = append(out, cloneTopicRule(r))
	}

	return out
}

// cloneThing creates a deep copy of a Thing.
func cloneThing(t *Thing) *Thing {
	attrs := make(map[string]string, len(t.Attributes))
	maps.Copy(attrs, t.Attributes)

	return &Thing{
		ThingName:     t.ThingName,
		ThingTypeName: t.ThingTypeName,
		ThingType:     t.ThingType,
		ThingID:       t.ThingID,
		ARN:           t.ARN,
		Attributes:    attrs,
		Version:       t.Version,
		CreatedAt:     t.CreatedAt,
	}
}

// cloneTopicRule creates a deep copy of a TopicRule.
func cloneTopicRule(r *TopicRule) *TopicRule {
	actions := make([]RuleAction, len(r.Actions))
	for i, action := range r.Actions {
		actions[i] = RuleAction{}
		if action.SQS != nil {
			actions[i].SQS = &SQSAction{
				QueueURL: action.SQS.QueueURL,
				RoleARN:  action.SQS.RoleARN,
			}
		}
		if action.Lambda != nil {
			actions[i].Lambda = &LambdaAction{
				FunctionARN: action.Lambda.FunctionARN,
			}
		}
	}

	return &TopicRule{
		RuleName:         r.RuleName,
		ARN:              r.ARN,
		SQL:              r.SQL,
		AWSIoTSQLVersion: r.AWSIoTSQLVersion,
		Description:      r.Description,
		Enabled:          r.Enabled,
		CreatedAt:        r.CreatedAt,
		Actions:          actions,
	}
}

// clonePolicy creates a deep copy of a Policy.
func clonePolicy(p *Policy) *Policy {
	cp := *p

	return &cp
}

// cloneSbomDocument creates a deep copy of a SbomDocument.
func cloneSbomDocument(s *SbomDocument) *SbomDocument {
	if s == nil {
		return nil
	}

	cp := SbomDocument{}

	if s.S3Location != nil {
		loc := *s.S3Location
		cp.S3Location = &loc
	}

	return &cp
}

// sortedKeys returns a sorted slice of the keys in a map.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// MQTTPort returns the configured TCP port for the MQTT broker.
func (b *InMemoryBackend) MQTTPort() int {
	return b.mqttPort
}

// CreateThing creates a new IoT Thing.
func (b *InMemoryBackend) CreateThing(input *CreateThingInput) (*CreateThingOutput, error) {
	if input.ThingName == "" {
		return nil, fmt.Errorf("%w: ThingName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.things[input.ThingName]; exists {
		return nil, fmt.Errorf("%w: thing %q already exists", ErrAlreadyExists, input.ThingName)
	}

	attrs := make(map[string]string)

	if input.AttributePayload != nil && input.AttributePayload.Attributes != nil {
		maps.Copy(attrs, input.AttributePayload.Attributes)
	}

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:thing/%s", b.region, b.accountID, input.ThingName)
	id := uuid.NewString()

	b.things[input.ThingName] = &Thing{
		ThingName:     input.ThingName,
		ThingTypeName: input.ThingTypeName,
		ThingType:     input.ThingTypeName,
		ThingID:       id,
		Attributes:    attrs,
		ARN:           arn,
		Version:       1,
		CreatedAt:     time.Now(),
	}

	return &CreateThingOutput{
		ThingName: input.ThingName,
		ThingARN:  arn,
		ThingID:   id,
	}, nil
}

// DescribeThing returns a deep copy of an existing Thing.
func (b *InMemoryBackend) DescribeThing(thingName string) (*Thing, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.things[thingName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	return cloneThing(t), nil
}

// ListThings returns all Things sorted by name.
func (b *InMemoryBackend) ListThings() []*Thing {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.things)
	out := make([]*Thing, 0, len(keys))

	for _, k := range keys {
		out = append(out, cloneThing(b.things[k]))
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
	if input.RuleName == "" {
		return fmt.Errorf("%w: RuleName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.rules[input.RuleName]; exists {
		return fmt.Errorf("%w: rule %q already exists", ErrAlreadyExists, input.RuleName)
	}

	payload := input.TopicRulePayload
	if payload == nil {
		payload = &TopicRulePayload{}
	}

	actions := payload.Actions
	if actions == nil {
		actions = []RuleAction{}
	}

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:rule/%s", b.region, b.accountID, input.RuleName)

	sqlVersion := payload.AWSIoTSQLVersion
	if sqlVersion == "" {
		sqlVersion = "2015-10-08"
	}

	b.rules[input.RuleName] = &TopicRule{
		RuleName:         input.RuleName,
		ARN:              arn,
		SQL:              payload.SQL,
		AWSIoTSQLVersion: sqlVersion,
		Description:      payload.Description,
		Actions:          actions,
		Enabled:          !payload.RuleDisabled,
		CreatedAt:        time.Now(),
	}

	return nil
}

// GetTopicRule returns a deep copy of an existing Topic Rule.
func (b *InMemoryBackend) GetTopicRule(ruleName string) (*TopicRule, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	r, ok := b.rules[ruleName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	return cloneTopicRule(r), nil
}

// ListTopicRules returns all Topic Rules sorted by name.
func (b *InMemoryBackend) ListTopicRules() []*TopicRule {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.rules)
	out := make([]*TopicRule, 0, len(keys))

	for _, k := range keys {
		out = append(out, cloneTopicRule(b.rules[k]))
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

// CreatePolicy creates a new IoT Policy.
func (b *InMemoryBackend) CreatePolicy(input *CreatePolicyInput) (*CreatePolicyOutput, error) {
	if input.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.policies[input.PolicyName]; exists {
		return nil, fmt.Errorf("%w: policy %q already exists", ErrAlreadyExists, input.PolicyName)
	}

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

// AcceptCertificateTransfer accepts a pending certificate transfer.
func (b *InMemoryBackend) AcceptCertificateTransfer(input *AcceptCertificateTransferInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.certificateTransfers[input.CertificateID] = "ACTIVE"

	return nil
}

// AddThingToBillingGroup adds a thing to a billing group.
func (b *InMemoryBackend) AddThingToBillingGroup(input *AddThingToBillingGroupInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.thingBillingGroups[thingKey(input.ThingName, input.ThingArn)] = input.BillingGroupName

	return nil
}

// AddThingToThingGroup adds a thing to a thing group.
func (b *InMemoryBackend) AddThingToThingGroup(input *AddThingToThingGroupInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := thingKey(input.ThingName, input.ThingArn)
	b.thingThingGroups[key] = append(b.thingThingGroups[key], input.ThingGroupName)

	return nil
}

// thingKey returns the canonical map key for a thing, preferring name over ARN.
func thingKey(name, arn string) string {
	if name != "" {
		return name
	}

	return arn
}

// packageVersionKey builds the composite key for a package version.
func packageVersionKey(packageName, versionName string) string {
	return packageName + "/" + versionName
}

// AssociateSbomWithPackageVersion associates an SBOM with a package version.
func (b *InMemoryBackend) AssociateSbomWithPackageVersion(
	input *AssociateSbomWithPackageVersionInput,
) (*AssociateSbomWithPackageVersionOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := packageVersionKey(input.PackageName, input.VersionName)
	b.packageVersionSboms[key] = input.Sbom

	return &AssociateSbomWithPackageVersionOutput{
		PackageName:          input.PackageName,
		VersionName:          input.VersionName,
		Sbom:                 input.Sbom,
		SbomValidationStatus: "IN_PROGRESS",
	}, nil
}

// AssociateTargetsWithJob associates targets with a continuous job.
func (b *InMemoryBackend) AssociateTargetsWithJob(
	input *AssociateTargetsWithJobInput,
) (*AssociateTargetsWithJobOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.jobTargets[input.JobID] = append(b.jobTargets[input.JobID], input.Targets...)

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:job/%s", b.region, b.accountID, input.JobID)

	return &AssociateTargetsWithJobOutput{
		JobID:  input.JobID,
		JobArn: arn,
	}, nil
}

// AttachPolicy attaches a policy to a target (thing group or certificate).
func (b *InMemoryBackend) AttachPolicy(input *AttachPolicyInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.policyTargets[input.PolicyName] = append(b.policyTargets[input.PolicyName], input.Target)

	return nil
}

// AttachSecurityProfile attaches a security profile to a target.
func (b *InMemoryBackend) AttachSecurityProfile(input *AttachSecurityProfileInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.securityProfileTargets[input.SecurityProfileName] = append(
		b.securityProfileTargets[input.SecurityProfileName],
		input.SecurityProfileTargetArn,
	)

	return nil
}

// AttachThingPrincipal attaches a principal (certificate or Cognito identity) to a thing.
func (b *InMemoryBackend) AttachThingPrincipal(input *AttachThingPrincipalInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.thingPrincipals[input.ThingName] = append(b.thingPrincipals[input.ThingName], input.Principal)

	return nil
}

// CancelAuditMitigationActionsTask cancels an audit mitigation actions task.
func (b *InMemoryBackend) CancelAuditMitigationActionsTask(input *CancelAuditMitigationActionsTaskInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.auditMitigationTasks[input.TaskID] = "CANCELED"

	return nil
}

// CancelAuditTask cancels an audit task.
func (b *InMemoryBackend) CancelAuditTask(input *CancelAuditTaskInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.auditTasks[input.AuditTaskID] = "CANCELED"

	return nil
}

// GetPolicy retrieves an existing Policy by name.
func (b *InMemoryBackend) GetPolicy(policyName string) (*GetPolicyOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	p, ok := b.policies[policyName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPolicyNotFound, policyName)
	}

	return &GetPolicyOutput{
		PolicyName:     p.PolicyName,
		PolicyARN:      p.ARN,
		PolicyDocument: p.PolicyDocument,
	}, nil
}

// DeletePolicy removes a Policy by name.
func (b *InMemoryBackend) DeletePolicy(policyName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.policies[policyName]; !ok {
		return fmt.Errorf("%w: %s", ErrPolicyNotFound, policyName)
	}

	delete(b.policies, policyName)

	return nil
}

// ListPolicies returns all policies sorted by name.
func (b *InMemoryBackend) ListPolicies() []*Policy {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.policies)
	out := make([]*Policy, 0, len(keys))

	for _, k := range keys {
		cp := *b.policies[k]
		out = append(out, &cp)
	}

	return out
}

// DisableTopicRule disables an existing topic rule.
func (b *InMemoryBackend) DisableTopicRule(ruleName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	r, ok := b.rules[ruleName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	r.Enabled = false

	return nil
}

// EnableTopicRule enables an existing topic rule.
func (b *InMemoryBackend) EnableTopicRule(ruleName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	r, ok := b.rules[ruleName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	r.Enabled = true

	return nil
}

// ReplaceTopicRule replaces the payload of an existing topic rule.
func (b *InMemoryBackend) ReplaceTopicRule(input *ReplaceTopicRuleInput) error {
	if input.RuleName == "" {
		return fmt.Errorf("%w: RuleName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	r, ok := b.rules[input.RuleName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRuleNotFound, input.RuleName)
	}

	payload := input.TopicRulePayload
	if payload == nil {
		payload = &TopicRulePayload{}
	}

	actions := payload.Actions
	if actions == nil {
		actions = []RuleAction{}
	}

	sqlVersion := payload.AWSIoTSQLVersion
	if sqlVersion == "" {
		sqlVersion = "2015-10-08"
	}

	r.SQL = payload.SQL
	r.Description = payload.Description
	r.Actions = actions
	r.AWSIoTSQLVersion = sqlVersion
	r.Enabled = !payload.RuleDisabled

	return nil
}

// UpdateThing updates attributes and/or type of an existing thing.
func (b *InMemoryBackend) UpdateThing(input *UpdateThingInput) error {
	if input.ThingName == "" {
		return fmt.Errorf("%w: ThingName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.things[input.ThingName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrThingNotFound, input.ThingName)
	}

	if input.RemoveThingType {
		t.ThingTypeName = ""
		t.ThingType = ""
	} else if input.ThingTypeName != "" {
		t.ThingTypeName = input.ThingTypeName
		t.ThingType = input.ThingTypeName
	}

	if input.AttributePayload != nil && input.AttributePayload.Attributes != nil {
		if t.Attributes == nil {
			t.Attributes = make(map[string]string)
		}

		maps.Copy(t.Attributes, input.AttributePayload.Attributes)
	}

	t.Version++

	return nil
}

// ListThingPrincipals returns principals attached to the given thing.
func (b *InMemoryBackend) ListThingPrincipals(thingName string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.things[thingName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	principals := b.thingPrincipals[thingName]
	if principals == nil {
		return []string{}, nil
	}

	out := make([]string, len(principals))
	copy(out, principals)

	return out, nil
}

// AddThingInternal seeds a Thing directly into the backend for testing.
func (b *InMemoryBackend) AddThingInternal(t Thing) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t.ThingID == "" {
		t.ThingID = uuid.NewString()
	}

	if t.ARN == "" {
		t.ARN = fmt.Sprintf("arn:aws:iot:%s:%s:thing/%s", b.region, b.accountID, t.ThingName)
	}

	if t.Attributes == nil {
		t.Attributes = make(map[string]string)
	}

	if t.Version == 0 {
		t.Version = 1
	}

	b.things[t.ThingName] = &t
}

// AddPolicyInternal seeds a Policy directly into the backend for testing.
func (b *InMemoryBackend) AddPolicyInternal(p Policy) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if p.ARN == "" {
		p.ARN = fmt.Sprintf("arn:aws:iot:%s:%s:policy/%s", b.region, b.accountID, p.PolicyName)
	}

	b.policies[p.PolicyName] = &p
}

// AddRuleInternal seeds a TopicRule directly into the backend for testing.
func (b *InMemoryBackend) AddRuleInternal(r TopicRule) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if r.ARN == "" {
		r.ARN = fmt.Sprintf("arn:aws:iot:%s:%s:rule/%s", b.region, b.accountID, r.RuleName)
	}

	if r.Actions == nil {
		r.Actions = []RuleAction{}
	}

	b.rules[r.RuleName] = &r
}

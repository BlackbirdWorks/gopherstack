package iot

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
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

	// ErrVersionConflict is returned when an optimistic-lock version check fails.
	ErrVersionConflict = errors.New("version conflict")

	// ErrDeleteConflict is returned when a resource cannot be deleted due to dependencies.
	ErrDeleteConflict = errors.New("delete conflict")
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
	thingTypes             map[string]*ThingType
	thingGroups            map[string]*ThingGroup
	thingGroupMembers      map[string][]string // thingGroupName -> []thingName
	certificates           map[string]*Certificate
	policyVersions         map[string][]*PolicyVersion // policyName -> []versions
	topicRuleDestinations  map[string]*TopicRuleDestination
	certificateProviders   map[string]*CertificateProvider
	// New stateful resources (batch 1).
	jobs                 map[string]*Job
	jobExecutions        map[string]*JobExecution // key: jobID|thingName
	jobTemplates         map[string]*JobTemplate
	roleAliases          map[string]*RoleAlias
	domainConfigs        map[string]*DomainConfiguration
	provTemplates        map[string]*ProvisioningTemplate
	provTemplateVersions map[string][]*ProvisioningTemplateVersion // templateName -> versions
	authorizers          map[string]*Authorizer
	billingGroups        map[string]*BillingGroup
	scheduledAudits      map[string]*ScheduledAudit
	mitigationActions    map[string]*MitigationAction
	securityProfiles     map[string]*SecurityProfile
	// Batch 2 resources.
	caCertificates     map[string]*CACertificate
	streams            map[string]*IoTStream
	fleetMetrics       map[string]*FleetMetric
	customMetrics      map[string]*CustomMetric
	dimensions         map[string]*Dimension
	resourceTags       map[string]map[string]string // resourceARN -> tags
	auditConfiguration *AccountAuditConfiguration
	auditTaskObjects   map[string]*AuditTask
	// Batch 3 resources.
	otaUpdates          map[string]*OTAUpdate
	iotPackages         map[string]*IoTPackage
	packageVersions2    map[string]map[string]*IoTPackageVersion // packageName -> versionName -> version
	packageConfig       *PackageConfiguration
	auditSuppressions   map[string]*AuditSuppression
	auditFindings       map[string]*AuditFinding
	v2LoggingOptions    *V2LoggingOptions
	v2LoggingLevels     map[string]*V2LoggingLevel
	loggingOptions      *LoggingOptions
	eventConfigurations *EventConfigurations
	commands            map[string]*IoTCommand
	commandExecutions   map[string]*IoTCommandExecution
	registrationCode    string
	defaultAuthorizer   string
	accountID           string
	region              string
	mqttPort            int
	mu                  sync.RWMutex
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
		thingTypes:             make(map[string]*ThingType),
		thingGroups:            make(map[string]*ThingGroup),
		thingGroupMembers:      make(map[string][]string),
		certificates:           make(map[string]*Certificate),
		policyVersions:         make(map[string][]*PolicyVersion),
		topicRuleDestinations:  make(map[string]*TopicRuleDestination),
		certificateProviders:   make(map[string]*CertificateProvider),
		jobs:                   make(map[string]*Job),
		jobExecutions:          make(map[string]*JobExecution),
		jobTemplates:           make(map[string]*JobTemplate),
		roleAliases:            make(map[string]*RoleAlias),
		domainConfigs:          make(map[string]*DomainConfiguration),
		provTemplates:          make(map[string]*ProvisioningTemplate),
		provTemplateVersions:   make(map[string][]*ProvisioningTemplateVersion),
		authorizers:            make(map[string]*Authorizer),
		billingGroups:          make(map[string]*BillingGroup),
		scheduledAudits:        make(map[string]*ScheduledAudit),
		mitigationActions:      make(map[string]*MitigationAction),
		securityProfiles:       make(map[string]*SecurityProfile),
		caCertificates:         make(map[string]*CACertificate),
		streams:                make(map[string]*IoTStream),
		fleetMetrics:           make(map[string]*FleetMetric),
		customMetrics:          make(map[string]*CustomMetric),
		dimensions:             make(map[string]*Dimension),
		resourceTags:           make(map[string]map[string]string),
		auditTaskObjects:       make(map[string]*AuditTask),
		otaUpdates:             make(map[string]*OTAUpdate),
		iotPackages:            make(map[string]*IoTPackage),
		packageVersions2:       make(map[string]map[string]*IoTPackageVersion),
		auditSuppressions:      make(map[string]*AuditSuppression),
		auditFindings:          make(map[string]*AuditFinding),
		v2LoggingLevels:        make(map[string]*V2LoggingLevel),
		commands:               make(map[string]*IoTCommand),
		commandExecutions:      make(map[string]*IoTCommandExecution),
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
	b.thingTypes = make(map[string]*ThingType)
	b.thingGroups = make(map[string]*ThingGroup)
	b.thingGroupMembers = make(map[string][]string)
	b.certificates = make(map[string]*Certificate)
	b.policyVersions = make(map[string][]*PolicyVersion)
	b.topicRuleDestinations = make(map[string]*TopicRuleDestination)
	b.certificateProviders = make(map[string]*CertificateProvider)
	b.jobs = make(map[string]*Job)
	b.jobExecutions = make(map[string]*JobExecution)
	b.jobTemplates = make(map[string]*JobTemplate)
	b.roleAliases = make(map[string]*RoleAlias)
	b.domainConfigs = make(map[string]*DomainConfiguration)
	b.provTemplates = make(map[string]*ProvisioningTemplate)
	b.provTemplateVersions = make(map[string][]*ProvisioningTemplateVersion)
	b.authorizers = make(map[string]*Authorizer)
	b.billingGroups = make(map[string]*BillingGroup)
	b.scheduledAudits = make(map[string]*ScheduledAudit)
	b.mitigationActions = make(map[string]*MitigationAction)
	b.securityProfiles = make(map[string]*SecurityProfile)
	b.caCertificates = make(map[string]*CACertificate)
	b.streams = make(map[string]*IoTStream)
	b.fleetMetrics = make(map[string]*FleetMetric)
	b.customMetrics = make(map[string]*CustomMetric)
	b.dimensions = make(map[string]*Dimension)
	b.resourceTags = make(map[string]map[string]string)
	b.auditTaskObjects = make(map[string]*AuditTask)
	b.otaUpdates = make(map[string]*OTAUpdate)
	b.iotPackages = make(map[string]*IoTPackage)
	b.packageVersions2 = make(map[string]map[string]*IoTPackageVersion)
	b.packageConfig = nil
	b.auditSuppressions = make(map[string]*AuditSuppression)
	b.auditFindings = make(map[string]*AuditFinding)
	b.v2LoggingOptions = nil
	b.v2LoggingLevels = make(map[string]*V2LoggingLevel)
	b.loggingOptions = nil
	b.eventConfigurations = nil
	b.commands = make(map[string]*IoTCommand)
	b.commandExecutions = make(map[string]*IoTCommandExecution)
	b.registrationCode = ""
	b.defaultAuthorizer = ""
	b.auditConfiguration = nil
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
	now := time.Now()

	b.policies[input.PolicyName] = &Policy{
		PolicyName:     input.PolicyName,
		PolicyDocument: input.PolicyDocument,
		ARN:            arn,
		CreatedAt:      now,
		LastModifiedAt: now,
	}

	// AWS automatically creates version "1" as the default on CreatePolicy.
	b.policyVersions[input.PolicyName] = []*PolicyVersion{
		{
			VersionID:        "1",
			PolicyDocument:   input.PolicyDocument,
			IsDefaultVersion: true,
			CreatedAt:        now,
		},
	}

	return &CreatePolicyOutput{
		PolicyName:      input.PolicyName,
		PolicyARN:       arn,
		PolicyDocument:  input.PolicyDocument,
		PolicyVersionID: "1",
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

	b.certificateTransfers[input.CertificateID] = certStatusActive

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

	// Also update the group membership index.
	groupName := input.ThingGroupName
	if groupName == "" {
		groupName = input.ThingGroupArn
	}

	thingName := thingKey(input.ThingName, input.ThingArn)
	b.addThingToGroupByName(thingName, groupName)

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

	defaultVersionID := "1"
	for _, v := range b.policyVersions[policyName] {
		if v.IsDefaultVersion {
			defaultVersionID = v.VersionID

			break
		}
	}

	return &GetPolicyOutput{
		PolicyName:       p.PolicyName,
		PolicyARN:        p.ARN,
		PolicyDocument:   p.PolicyDocument,
		CreatedAt:        p.CreatedAt,
		LastModifiedAt:   p.LastModifiedAt,
		DefaultVersionID: defaultVersionID,
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

	if input.ExpectedVersion != 0 && input.ExpectedVersion != t.Version {
		return fmt.Errorf("%w: expected version %d but current is %d",
			ErrVersionConflict, input.ExpectedVersion, t.Version)
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

// -----------------------------------------------------------
// ThingType operations
// -----------------------------------------------------------

// ErrThingTypeNotFound is returned when a ThingType does not exist.
var ErrThingTypeNotFound = errors.New("thing type not found")

// ErrThingGroupNotFound is returned when a ThingGroup does not exist.
var ErrThingGroupNotFound = errors.New("thing group not found")

// ErrCertificateNotFound is returned when a Certificate does not exist.
var ErrCertificateNotFound = errors.New("certificate not found")

// ErrCertificateProviderNotFound is returned when a CertificateProvider does not exist.
var ErrCertificateProviderNotFound = errors.New("certificate provider not found")

// ErrTopicRuleDestinationNotFound is returned when a TopicRuleDestination does not exist.
var ErrTopicRuleDestinationNotFound = errors.New("topic rule destination not found")

// ErrPolicyVersionNotFound is returned when a PolicyVersion does not exist.
var ErrPolicyVersionNotFound = errors.New("policy version not found")

// fakePEM is a minimal fake PEM certificate returned by CreateCertificateFromCsr and RegisterCertificate.
const fakePEM = `-----BEGIN CERTIFICATE-----
MIICpDCCAYwCCQDU+pQ4pHgSpDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls
b2NhbGhvc3QwHhcNMjMwMTAxMDAwMDAwWhcNMjQwMTAxMDAwMDAwWjAUMRIwEAYD
VQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC7
o4qne60TB3wolFl6qADvFVMZUDCwJJlFBMDkajIxpQFNbBgxDuAQFV8AAAAAAA==
-----END CERTIFICATE-----`

// certIDHexLen is the number of bytes (half the hex char count) for a certificate ID.
const certIDHexLen = 32 // produces a 64-char hex string

// certStatusActive is the AWS IoT certificate ACTIVE status value.
const certStatusActive = "ACTIVE"

// certStatusInactive is the AWS IoT certificate INACTIVE status value.
const certStatusInactive = "INACTIVE"

// randomHex generates a cryptographically random hex string of n bytes (2n characters).
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("iot: randomHex: crypto/rand failed: " + err.Error())
	}

	return hex.EncodeToString(b)
}

// CreateThingType creates a new IoT Thing Type.
func (b *InMemoryBackend) CreateThingType(input *CreateThingTypeInput) (*ThingType, error) {
	if input.ThingTypeName == "" {
		return nil, fmt.Errorf("%w: ThingTypeName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.thingTypes[input.ThingTypeName]; exists {
		return nil, fmt.Errorf("%w: thing type %q already exists", ErrAlreadyExists, input.ThingTypeName)
	}

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:thingtype/%s", b.region, b.accountID, input.ThingTypeName)
	tt := &ThingType{
		ThingTypeName:        input.ThingTypeName,
		ThingTypeID:          uuid.NewString(),
		ThingTypeARN:         arn,
		Description:          input.Description,
		SearchableAttributes: append([]string(nil), input.SearchableAttributes...),
		CreatedAt:            time.Now(),
	}

	b.thingTypes[input.ThingTypeName] = tt

	return tt, nil
}

// DescribeThingType returns a ThingType by name.
func (b *InMemoryBackend) DescribeThingType(thingTypeName string) (*ThingType, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tt, ok := b.thingTypes[thingTypeName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrThingTypeNotFound, thingTypeName)
	}

	cp := *tt

	return &cp, nil
}

// ListThingTypes returns all thing types sorted by name.
func (b *InMemoryBackend) ListThingTypes() []*ThingType {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.thingTypes)
	out := make([]*ThingType, 0, len(keys))

	for _, k := range keys {
		cp := *b.thingTypes[k]
		out = append(out, &cp)
	}

	return out
}

// DeprecateThingType marks a thing type as deprecated (or un-deprecates it).
func (b *InMemoryBackend) DeprecateThingType(input *DeprecateThingTypeInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tt, ok := b.thingTypes[input.ThingTypeName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrThingTypeNotFound, input.ThingTypeName)
	}

	if input.UndoDeprecate {
		tt.Deprecated = false
		tt.DeprecationDate = time.Time{}
	} else {
		tt.Deprecated = true
		tt.DeprecationDate = time.Now()
	}

	return nil
}

// DeleteThingType deletes a thing type by name. The type must be deprecated first.
func (b *InMemoryBackend) DeleteThingType(thingTypeName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tt, ok := b.thingTypes[thingTypeName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrThingTypeNotFound, thingTypeName)
	}

	if !tt.Deprecated {
		return fmt.Errorf("%w: thing type %q must be deprecated before deletion", ErrValidation, thingTypeName)
	}

	delete(b.thingTypes, thingTypeName)

	return nil
}

// -----------------------------------------------------------
// ThingGroup operations
// -----------------------------------------------------------

// CreateThingGroup creates a new IoT Thing Group.
func (b *InMemoryBackend) CreateThingGroup(input *CreateThingGroupInput) (*ThingGroup, error) {
	if input.ThingGroupName == "" {
		return nil, fmt.Errorf("%w: ThingGroupName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.thingGroups[input.ThingGroupName]; exists {
		return nil, fmt.Errorf("%w: thing group %q already exists", ErrAlreadyExists, input.ThingGroupName)
	}

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:thinggroup/%s", b.region, b.accountID, input.ThingGroupName)
	id := uuid.NewString()

	attrs := make(map[string]string)
	if input.Attributes != nil {
		maps.Copy(attrs, input.Attributes)
	}

	tg := &ThingGroup{
		ThingGroupName:  input.ThingGroupName,
		ThingGroupARN:   arn,
		ThingGroupID:    id,
		Description:     input.Description,
		ParentGroupName: input.ParentGroupName,
		Attributes:      attrs,
		Members:         []string{},
		Version:         1,
		CreatedAt:       time.Now(),
	}

	b.thingGroups[input.ThingGroupName] = tg
	b.thingGroupMembers[input.ThingGroupName] = []string{}

	return tg, nil
}

// DescribeThingGroup returns a ThingGroup by name.
func (b *InMemoryBackend) DescribeThingGroup(thingGroupName string) (*ThingGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tg, ok := b.thingGroups[thingGroupName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrThingGroupNotFound, thingGroupName)
	}

	cp := *tg
	cp.Attributes = make(map[string]string, len(tg.Attributes))
	maps.Copy(cp.Attributes, tg.Attributes)
	cp.Members = make([]string, len(tg.Members))
	copy(cp.Members, tg.Members)

	return &cp, nil
}

// ListThingGroups returns all thing groups sorted by name.
func (b *InMemoryBackend) ListThingGroups() []*ThingGroup {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.thingGroups)
	out := make([]*ThingGroup, 0, len(keys))

	for _, k := range keys {
		tg := b.thingGroups[k]
		cp := *tg
		cp.Attributes = make(map[string]string, len(tg.Attributes))
		maps.Copy(cp.Attributes, tg.Attributes)
		cp.Members = make([]string, len(tg.Members))
		copy(cp.Members, tg.Members)
		out = append(out, &cp)
	}

	return out
}

// UpdateThingGroup updates an existing thing group and returns the new version.
func (b *InMemoryBackend) UpdateThingGroup(input *UpdateThingGroupInput) (int64, error) {
	if input.ThingGroupName == "" {
		return 0, fmt.Errorf("%w: ThingGroupName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	tg, ok := b.thingGroups[input.ThingGroupName]
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrThingGroupNotFound, input.ThingGroupName)
	}

	if input.ExpectedVersion != 0 && input.ExpectedVersion != tg.Version {
		return 0, fmt.Errorf("%w: expected version %d but current is %d",
			ErrVersionConflict, input.ExpectedVersion, tg.Version)
	}

	if input.Description != "" {
		tg.Description = input.Description
	}

	if input.Attributes != nil {
		if tg.Attributes == nil {
			tg.Attributes = make(map[string]string)
		}
		maps.Copy(tg.Attributes, input.Attributes)
	}

	tg.Version++

	return tg.Version, nil
}

// DeleteThingGroup deletes a thing group by name.
func (b *InMemoryBackend) DeleteThingGroup(thingGroupName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.thingGroups[thingGroupName]; !ok {
		return fmt.Errorf("%w: %s", ErrThingGroupNotFound, thingGroupName)
	}

	delete(b.thingGroups, thingGroupName)
	delete(b.thingGroupMembers, thingGroupName)

	return nil
}

// RemoveThingFromThingGroup removes a thing from a thing group.
func (b *InMemoryBackend) RemoveThingFromThingGroup(input *RemoveThingFromThingGroupInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	groupName := input.ThingGroupName
	if groupName == "" {
		groupName = input.ThingGroupArn
	}

	thingName := input.ThingName
	if thingName == "" {
		thingName = input.ThingArn
	}

	members := b.thingGroupMembers[groupName]
	filtered := make([]string, 0, len(members))

	for _, m := range members {
		if m != thingName {
			filtered = append(filtered, m)
		}
	}

	b.thingGroupMembers[groupName] = filtered

	return nil
}

// ListThingsInThingGroup returns all things in a given thing group.
func (b *InMemoryBackend) ListThingsInThingGroup(input *ListThingsInThingGroupInput) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.thingGroups[input.ThingGroupName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrThingGroupNotFound, input.ThingGroupName)
	}

	members := b.thingGroupMembers[input.ThingGroupName]
	out := make([]string, len(members))
	copy(out, members)

	return out, nil
}

// -----------------------------------------------------------
// Certificate operations
// -----------------------------------------------------------

// newCertificate creates a new Certificate with a random 64-hex-char ID.
func (b *InMemoryBackend) newCertificate(pem, status string) *Certificate {
	certID := randomHex(certIDHexLen)
	arn := fmt.Sprintf("arn:aws:iot:%s:%s:cert/%s", b.region, b.accountID, certID)
	now := time.Now()

	return &Certificate{
		CertificateID:  certID,
		ARN:            arn,
		Status:         status,
		PEM:            pem,
		CreatedAt:      now,
		LastModifiedAt: now,
	}
}

// CreateCertificateFromCsr creates a new certificate from a CSR.
func (b *InMemoryBackend) CreateCertificateFromCsr(input *CreateCertificateFromCsrInput) (*Certificate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	status := certStatusInactive
	if input.SetAsActive {
		status = certStatusActive
	}

	cert := b.newCertificate(fakePEM, status)
	b.certificates[cert.CertificateID] = cert

	return cert, nil
}

// RegisterCertificate registers a certificate.
func (b *InMemoryBackend) RegisterCertificate(input *RegisterCertificateInput) (*Certificate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	status := input.Status
	if status == "" {
		status = certStatusInactive
	}

	pem := input.CertificatePem
	if pem == "" {
		pem = fakePEM
	}

	cert := b.newCertificate(pem, status)
	b.certificates[cert.CertificateID] = cert

	return cert, nil
}

// RegisterCertificateWithoutCA registers a certificate without a CA.
func (b *InMemoryBackend) RegisterCertificateWithoutCA(input *RegisterCertificateInput) (*Certificate, error) {
	return b.RegisterCertificate(input)
}

// DescribeCertificate returns a Certificate by ID.
func (b *InMemoryBackend) DescribeCertificate(certificateID string) (*Certificate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cert, ok := b.certificates[certificateID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCertificateNotFound, certificateID)
	}

	cp := *cert

	return &cp, nil
}

// ListCertificates returns all certificates sorted by ID.
func (b *InMemoryBackend) ListCertificates() []*Certificate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.certificates)
	out := make([]*Certificate, 0, len(keys))

	for _, k := range keys {
		cp := *b.certificates[k]
		out = append(out, &cp)
	}

	return out
}

// isValidCertStatus reports whether s is a legal AWS IoT certificate status.
func isValidCertStatus(s string) bool {
	switch s {
	case certStatusActive, certStatusInactive, "REVOKED", "PENDING_TRANSFER", "PENDING_ACTIVATION":
		return true
	}

	return false
}

// UpdateCertificate updates the status of a certificate.
func (b *InMemoryBackend) UpdateCertificate(input *UpdateCertificateInput) error {
	if input.NewStatus != "" && !isValidCertStatus(input.NewStatus) {
		return fmt.Errorf("%w: invalid certificate status %q", ErrValidation, input.NewStatus)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	cert, ok := b.certificates[input.CertificateID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrCertificateNotFound, input.CertificateID)
	}

	cert.Status = input.NewStatus
	cert.LastModifiedAt = time.Now()

	return nil
}

// DeleteCertificate deletes a certificate by ID.
func (b *InMemoryBackend) DeleteCertificate(certificateID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.certificates[certificateID]; !ok {
		return fmt.Errorf("%w: %s", ErrCertificateNotFound, certificateID)
	}

	delete(b.certificates, certificateID)

	return nil
}

// -----------------------------------------------------------
// Policy attachment operations
// -----------------------------------------------------------

// DetachPolicy detaches a policy from a target.
func (b *InMemoryBackend) DetachPolicy(input *DetachPolicyInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	targets := b.policyTargets[input.PolicyName]
	filtered := make([]string, 0, len(targets))

	for _, t := range targets {
		if t != input.Target {
			filtered = append(filtered, t)
		}
	}

	b.policyTargets[input.PolicyName] = filtered

	return nil
}

// ListAttachedPolicies returns all policies attached to a target.
func (b *InMemoryBackend) ListAttachedPolicies(input *ListAttachedPoliciesInput) ([]*Policy, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []*Policy

	for policyName, targets := range b.policyTargets {
		if slices.Contains(targets, input.Target) {
			if p, ok := b.policies[policyName]; ok {
				cp := *p
				out = append(out, &cp)
			}
		}
	}

	return out, nil
}

// -----------------------------------------------------------
// PolicyVersion operations
// -----------------------------------------------------------

// CreatePolicyVersion creates a new version of an existing policy.
func (b *InMemoryBackend) CreatePolicyVersion(input *CreatePolicyVersionInput) (*PolicyVersion, error) {
	if input.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.policies[input.PolicyName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPolicyNotFound, input.PolicyName)
	}

	versions := b.policyVersions[input.PolicyName]
	versionID := strconv.Itoa(len(versions) + 1)

	if input.SetAsDefault {
		for _, v := range versions {
			v.IsDefaultVersion = false
		}
	}

	now := time.Now()
	pv := &PolicyVersion{
		VersionID:        versionID,
		PolicyDocument:   input.PolicyDocument,
		IsDefaultVersion: input.SetAsDefault,
		CreatedAt:        now,
	}

	b.policyVersions[input.PolicyName] = append(versions, pv)
	p.LastModifiedAt = now

	return pv, nil
}

// GetPolicyVersion retrieves a specific version of a policy.
func (b *InMemoryBackend) GetPolicyVersion(policyName, versionID string) (*PolicyVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	versions := b.policyVersions[policyName]

	for _, v := range versions {
		if v.VersionID == versionID {
			cp := *v

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s/%s", ErrPolicyVersionNotFound, policyName, versionID)
}

// ListPolicyVersions returns all versions of a policy.
func (b *InMemoryBackend) ListPolicyVersions(policyName string) ([]*PolicyVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.policies[policyName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrPolicyNotFound, policyName)
	}

	versions := b.policyVersions[policyName]
	out := make([]*PolicyVersion, len(versions))

	for i, v := range versions {
		cp := *v
		out[i] = &cp
	}

	return out, nil
}

// DeletePolicyVersion deletes a specific version of a policy.
// The default version cannot be deleted.
func (b *InMemoryBackend) DeletePolicyVersion(policyName, versionID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	versions := b.policyVersions[policyName]
	filtered := make([]*PolicyVersion, 0, len(versions))
	found := false

	for _, v := range versions {
		if v.VersionID == versionID {
			found = true
			if v.IsDefaultVersion {
				return fmt.Errorf("%w: cannot delete default policy version %s", ErrDeleteConflict, versionID)
			}
		} else {
			filtered = append(filtered, v)
		}
	}

	if !found {
		return fmt.Errorf("%w: %s/%s", ErrPolicyVersionNotFound, policyName, versionID)
	}

	b.policyVersions[policyName] = filtered

	return nil
}

// SetDefaultPolicyVersion sets the default version of a policy.
func (b *InMemoryBackend) SetDefaultPolicyVersion(policyName, versionID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	versions := b.policyVersions[policyName]
	found := false

	for _, v := range versions {
		if v.VersionID == versionID {
			v.IsDefaultVersion = true
			found = true
		} else {
			v.IsDefaultVersion = false
		}
	}

	if !found {
		return fmt.Errorf("%w: %s/%s", ErrPolicyVersionNotFound, policyName, versionID)
	}

	return nil
}

// -----------------------------------------------------------
// TopicRuleDestination operations
// -----------------------------------------------------------

// CreateTopicRuleDestination creates a new topic rule destination.
func (b *InMemoryBackend) CreateTopicRuleDestination(
	input *CreateTopicRuleDestinationInput,
) (*TopicRuleDestination, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:ruledestination/http/%s",
		b.region, b.accountID, uuid.NewString())

	dest := &TopicRuleDestination{
		ARN:    arn,
		Status: "ENABLED",
	}

	if input.DestinationConfiguration != nil && input.DestinationConfiguration.HTTPURLConfiguration != nil {
		dest.HTTPURLProperties = &HTTPURLDestinationProperties{
			ConfirmationURL: input.DestinationConfiguration.HTTPURLConfiguration.ConfirmationURL,
		}
	}

	b.topicRuleDestinations[arn] = dest

	return dest, nil
}

// GetTopicRuleDestination returns a topic rule destination by ARN.
func (b *InMemoryBackend) GetTopicRuleDestination(arn string) (*TopicRuleDestination, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	dest, ok := b.topicRuleDestinations[arn]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTopicRuleDestinationNotFound, arn)
	}

	cp := *dest

	return &cp, nil
}

// ListTopicRuleDestinations returns all topic rule destinations.
func (b *InMemoryBackend) ListTopicRuleDestinations() []*TopicRuleDestination {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.topicRuleDestinations)
	out := make([]*TopicRuleDestination, 0, len(keys))

	for _, k := range keys {
		cp := *b.topicRuleDestinations[k]
		out = append(out, &cp)
	}

	return out
}

// UpdateTopicRuleDestination updates the status of a topic rule destination.
func (b *InMemoryBackend) UpdateTopicRuleDestination(input *UpdateTopicRuleDestinationInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	dest, ok := b.topicRuleDestinations[input.ARN]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTopicRuleDestinationNotFound, input.ARN)
	}

	dest.Status = input.Status

	return nil
}

// DeleteTopicRuleDestination deletes a topic rule destination by ARN.
func (b *InMemoryBackend) DeleteTopicRuleDestination(arn string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topicRuleDestinations[arn]; !ok {
		return fmt.Errorf("%w: %s", ErrTopicRuleDestinationNotFound, arn)
	}

	delete(b.topicRuleDestinations, arn)

	return nil
}

// -----------------------------------------------------------
// CertificateProvider operations
// -----------------------------------------------------------

// CreateCertificateProvider creates a new certificate provider.
func (b *InMemoryBackend) CreateCertificateProvider(
	input *CreateCertificateProviderInput,
) (*CertificateProvider, error) {
	if input.CertificateProviderName == "" {
		return nil, fmt.Errorf("%w: CertificateProviderName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.certificateProviders[input.CertificateProviderName]; exists {
		return nil, fmt.Errorf("%w: certificate provider %q already exists",
			ErrAlreadyExists, input.CertificateProviderName)
	}

	arn := fmt.Sprintf("arn:aws:iot:%s:%s:certificateprovider/%s",
		b.region, b.accountID, input.CertificateProviderName)

	ops := make([]string, len(input.AccountDefaultForOperations))
	copy(ops, input.AccountDefaultForOperations)

	cp := &CertificateProvider{
		CertificateProviderName:     input.CertificateProviderName,
		ARN:                         arn,
		LambdaFunctionARN:           input.LambdaFunctionARN,
		AccountDefaultForOperations: ops,
		CreatedAt:                   time.Now(),
		LastModifiedAt:              time.Now(),
	}

	b.certificateProviders[input.CertificateProviderName] = cp

	return cp, nil
}

// DescribeCertificateProvider returns a certificate provider by name.
func (b *InMemoryBackend) DescribeCertificateProvider(name string) (*CertificateProvider, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cp, ok := b.certificateProviders[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCertificateProviderNotFound, name)
	}

	result := *cp
	result.AccountDefaultForOperations = make([]string, len(cp.AccountDefaultForOperations))
	copy(result.AccountDefaultForOperations, cp.AccountDefaultForOperations)

	return &result, nil
}

// ListCertificateProviders returns all certificate providers sorted by name.
func (b *InMemoryBackend) ListCertificateProviders() []*CertificateProvider {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.certificateProviders)
	out := make([]*CertificateProvider, 0, len(keys))

	for _, k := range keys {
		cp := *b.certificateProviders[k]
		cp.AccountDefaultForOperations = make([]string, len(b.certificateProviders[k].AccountDefaultForOperations))
		copy(cp.AccountDefaultForOperations, b.certificateProviders[k].AccountDefaultForOperations)
		out = append(out, &cp)
	}

	return out
}

// UpdateCertificateProvider updates an existing certificate provider.
func (b *InMemoryBackend) UpdateCertificateProvider(input *UpdateCertificateProviderInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp, ok := b.certificateProviders[input.CertificateProviderName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrCertificateProviderNotFound, input.CertificateProviderName)
	}

	if input.LambdaFunctionARN != "" {
		cp.LambdaFunctionARN = input.LambdaFunctionARN
	}

	if input.AccountDefaultForOperations != nil {
		ops := make([]string, len(input.AccountDefaultForOperations))
		copy(ops, input.AccountDefaultForOperations)
		cp.AccountDefaultForOperations = ops
	}

	cp.LastModifiedAt = time.Now()

	return nil
}

// DeleteCertificateProvider deletes a certificate provider by name.
func (b *InMemoryBackend) DeleteCertificateProvider(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.certificateProviders[name]; !ok {
		return fmt.Errorf("%w: %s", ErrCertificateProviderNotFound, name)
	}

	delete(b.certificateProviders, name)

	return nil
}

// addThingToGroupByName adds thingName to groupName in thingGroupMembers (dedup).
// Must be called with b.mu held.
func (b *InMemoryBackend) addThingToGroupByName(thingName, groupName string) {
	members := b.thingGroupMembers[groupName]

	if slices.Contains(members, thingName) {
		return
	}

	b.thingGroupMembers[groupName] = append(members, thingName)
}

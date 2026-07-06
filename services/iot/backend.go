package iot

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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

	// ErrIndexNotFound is returned when a fleet index does not exist.
	ErrIndexNotFound = errors.New("index not found")

	// ErrVersionsLimitExceeded is returned when a policy already has the maximum allowed versions.
	ErrVersionsLimitExceeded = errors.New("versions limit exceeded")

	// ErrShadowNotFound is returned when a Device Shadow does not exist.
	ErrShadowNotFound = errors.New("shadow not found")
)

// RuleDispatcher is implemented by the CLI wiring layer and dispatches rule actions.
type RuleDispatcher interface {
	SendToSQS(queueURL, body string) error
	InvokeLambda(ctx context.Context, functionARN string, payload []byte) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	dispatcher             RuleDispatcher
	shadows                map[shadowKey]*ThingShadow // thing+name → shadow
	resourceTags           map[string]map[string]string
	certificateTransfers   map[string]string
	thingBillingGroups     map[string]string
	thingThingGroups       map[string][]string
	packageVersionSboms    map[string]*SbomDocument
	jobTargets             map[string][]string
	securityProfileTargets map[string][]string
	thingPrincipals        map[string][]string
	auditMitigationTasks   map[string]string
	auditTasks             map[string]string
	thingGroupMembers      map[string][]string
	policyVersions         map[string][]*PolicyVersion
	provTemplateVersions   map[string][]*ProvisioningTemplateVersion
	policyTargets          map[string][]string

	// registry lets Reset collapse every converted resource table's
	// lifecycle to one call (registry.ResetAll()) instead of hand-rolled
	// re-initialization. See store_setup.go's registerAllTables for the
	// full list of tables and the (documented) fields left as raw maps
	// above/below instead.
	registry *store.Registry

	customMetrics              *store.Table[CustomMetric]
	rules                      *store.Table[TopicRule]
	fleetMetrics               *store.Table[FleetMetric]
	thingTypes                 *store.Table[ThingType]
	thingGroups                *store.Table[ThingGroup]
	certificates               *store.Table[Certificate]
	topicRuleDestinations      *store.Table[TopicRuleDestination]
	certificateProviders       *store.Table[CertificateProvider]
	jobs                       *store.Table[Job]
	jobExecutions              *store.Table[JobExecution]
	jobTemplates               *store.Table[JobTemplate]
	roleAliases                *store.Table[RoleAlias]
	domainConfigs              *store.Table[DomainConfiguration]
	dimensions                 *store.Table[Dimension]
	authorizers                *store.Table[Authorizer]
	billingGroups              *store.Table[BillingGroup]
	scheduledAudits            *store.Table[ScheduledAudit]
	mitigationActions          *store.Table[MitigationAction]
	securityProfiles           *store.Table[SecurityProfile]
	caCertificates             *store.Table[CACertificate]
	streams                    *store.Table[IoTStream]
	policies                   *store.Table[Policy]
	provTemplates              *store.Table[ProvisioningTemplate]
	things                     *store.Table[Thing]
	auditTaskObjects           *store.Table[AuditTask]
	otaUpdates                 *store.Table[OTAUpdate]
	iotPackages                *store.Table[IoTPackage]
	auditSuppressions          *store.Table[AuditSuppression]
	auditFindings              *store.Table[AuditFinding]
	v2LoggingLevels            *store.Table[V2LoggingLevel]
	commands                   *store.Table[IoTCommand]
	registrationTasks          *store.Table[ThingRegistrationTask]
	auditMitigationTaskObjects *store.Table[AuditMitigationTask]
	detectMitigationTasks      *store.Table[DetectMitigationTask]
	activeViolations           *store.Table[ActiveViolation]

	auditConfiguration         *AccountAuditConfiguration
	packageVersions2           map[string]map[string]*IoTPackageVersion
	packageConfig              *PackageConfiguration
	v2LoggingOptions           *V2LoggingOptions
	loggingOptions             *LoggingOptions
	eventConfigurations        *EventConfigurations
	commandExecutions          map[string]*IoTCommandExecution
	thingIndexingConfig        *ThingIndexingConfiguration
	thingGroupIndexingConfig   *ThingGroupIndexingConfiguration
	auditMitigationExecutions  map[string][]*AuditMitigationActionExecution
	detectMitigationExecutions map[string][]*DetectMitigationActionExecution
	accountEncryptionConfig    *AccountEncryptionConfiguration
	sbomValidationResults      map[string][]*SbomValidationResult
	metricValues               map[string][]*MetricDatapoint
	thingConnectivity          map[string]*ThingConnectivityData
	behaviorTrainingSummaries  map[string][]*BehaviorModelTrainingSummary
	registrationCode           string
	defaultAuthorizer          string
	accountID                  string
	region                     string
	violationEvents            []*ViolationEvent
	mqttPort                   int
	mu                         sync.RWMutex
}

// Compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// mqttDefaultPort is the default TCP port for the embedded MQTT broker.
const mqttDefaultPort = 1883

// NewInMemoryBackend creates a new InMemoryBackend with default values.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry: store.NewRegistry(),

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
		thingGroupMembers:      make(map[string][]string),
		policyVersions:         make(map[string][]*PolicyVersion),
		provTemplateVersions:   make(map[string][]*ProvisioningTemplateVersion),
		resourceTags:           make(map[string]map[string]string),
		packageVersions2:       make(map[string]map[string]*IoTPackageVersion),
		shadows:                make(map[shadowKey]*ThingShadow),
		commandExecutions:      make(map[string]*IoTCommandExecution),

		auditMitigationExecutions:  make(map[string][]*AuditMitigationActionExecution),
		detectMitigationExecutions: make(map[string][]*DetectMitigationActionExecution),

		sbomValidationResults:     make(map[string][]*SbomValidationResult),
		metricValues:              make(map[string][]*MetricDatapoint),
		thingConnectivity:         make(map[string]*ThingConnectivityData),
		behaviorTrainingSummaries: make(map[string][]*BehaviorModelTrainingSummary),

		accountID: "000000000000",
		region:    "us-east-1",
		mqttPort:  mqttDefaultPort,
	}

	registerAllTables(b)

	return b
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	b := NewInMemoryBackend()
	b.accountID = accountID
	b.region = region

	return b
}

// resetBatch3 clears all batch-3 backend state (called from Reset, lock held).
func (b *InMemoryBackend) resetBatch3() {
	b.packageVersions2 = make(map[string]map[string]*IoTPackageVersion)
	b.packageConfig = nil
	b.v2LoggingOptions = nil
	b.loggingOptions = nil
	b.eventConfigurations = nil
	b.commandExecutions = make(map[string]*IoTCommandExecution)
}

// Reset clears all backend state. Useful for test isolation.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Clears every table registered in store_setup.go's registerAllTables
	// (things, thingTypes, thingGroups, certificates, policies, rules, jobs,
	// jobExecutions, jobTemplates, billingGroups, topicRuleDestinations,
	// certificateProviders, roleAliases, domainConfigs, dimensions,
	// authorizers, scheduledAudits, mitigationActions, securityProfiles,
	// caCertificates, streams, provTemplates, auditTaskObjects, otaUpdates,
	// iotPackages, auditSuppressions, auditFindings, v2LoggingLevels,
	// commands, registrationTasks, auditMitigationTaskObjects,
	// detectMitigationTasks, activeViolations, fleetMetrics, customMetrics)
	// in one call instead of one hand-rolled make() per map.
	//
	// b.shadows is deliberately NOT part of the registry and NOT cleared
	// here -- see store_setup.go's registerAllTables comment for why this
	// preserves a pre-existing quirk byte-for-byte.
	b.registry.ResetAll()

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
	b.thingGroupMembers = make(map[string][]string)
	b.policyVersions = make(map[string][]*PolicyVersion)
	b.provTemplateVersions = make(map[string][]*ProvisioningTemplateVersion)
	b.resourceTags = make(map[string]map[string]string)
	b.resetBatch3()
	b.registrationCode = ""
	b.defaultAuthorizer = ""
	b.auditConfiguration = nil
	b.thingIndexingConfig = nil
	b.thingGroupIndexingConfig = nil
	b.resetDeviceDefender()
	b.resetFinalOps()
}

// resetFinalOps clears final-stub-batch backend state (called from Reset,
// lock held).
func (b *InMemoryBackend) resetFinalOps() {
	b.accountEncryptionConfig = nil
	b.sbomValidationResults = make(map[string][]*SbomValidationResult)
	b.metricValues = make(map[string][]*MetricDatapoint)
	b.thingConnectivity = make(map[string]*ThingConnectivityData)
	b.behaviorTrainingSummaries = make(map[string][]*BehaviorModelTrainingSummary)
}

// resetDeviceDefender clears all Device Defender backend state (audit + detect
// mitigation-action tasks and violations). Called from Reset, lock held.
func (b *InMemoryBackend) resetDeviceDefender() {
	b.auditMitigationExecutions = make(map[string][]*AuditMitigationActionExecution)
	b.detectMitigationExecutions = make(map[string][]*DetectMitigationActionExecution)
	b.violationEvents = nil
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

	out := make([]*TopicRule, 0, b.rules.Len())

	for _, r := range b.rules.All() {
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
	keys := collections.SortedKeys(m)

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

	if b.things.Has(input.ThingName) {
		return nil, fmt.Errorf("%w: thing %q already exists", ErrAlreadyExists, input.ThingName)
	}

	attrs := make(map[string]string)

	if input.AttributePayload != nil && input.AttributePayload.Attributes != nil {
		maps.Copy(attrs, input.AttributePayload.Attributes)
	}

	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("thing/%s", input.ThingName))
	id := uuid.NewString()

	b.things.Put(&Thing{
		ThingName:     input.ThingName,
		ThingTypeName: input.ThingTypeName,
		ThingType:     input.ThingTypeName,
		ThingID:       id,
		Attributes:    attrs,
		ARN:           arn,
		Version:       1,
		CreatedAt:     time.Now(),
	})

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

	t, ok := b.things.Get(thingName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	return cloneThing(t), nil
}

// ListThings returns all Things sorted by name.
func (b *InMemoryBackend) ListThings() []*Thing {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.things.Snapshot()
	out := make([]*Thing, 0, len(items))

	for _, v := range items {
		out = append(out, cloneThing(v))
	}

	return out
}

// DeleteThing deletes a Thing by name.
func (b *InMemoryBackend) DeleteThing(thingName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.things.Has(thingName) {
		return fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	if principals := b.thingPrincipals[thingName]; len(principals) > 0 {
		return fmt.Errorf("%w: thing %q has attached principals", ErrDeleteConflict, thingName)
	}

	b.things.Delete(thingName)

	return nil
}

// CreateTopicRule creates a new IoT Topic Rule.
func (b *InMemoryBackend) CreateTopicRule(input *CreateTopicRuleInput) error {
	if input.RuleName == "" {
		return fmt.Errorf("%w: RuleName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.rules.Has(input.RuleName) {
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

	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("rule/%s", input.RuleName))

	sqlVersion := payload.AWSIoTSQLVersion
	if sqlVersion == "" {
		sqlVersion = "2015-10-08"
	}

	b.rules.Put(&TopicRule{
		RuleName:         input.RuleName,
		ARN:              arn,
		SQL:              payload.SQL,
		AWSIoTSQLVersion: sqlVersion,
		Description:      payload.Description,
		Actions:          actions,
		Enabled:          !payload.RuleDisabled,
		CreatedAt:        time.Now(),
	})

	return nil
}

// GetTopicRule returns a deep copy of an existing Topic Rule.
func (b *InMemoryBackend) GetTopicRule(ruleName string) (*TopicRule, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	r, ok := b.rules.Get(ruleName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	return cloneTopicRule(r), nil
}

// ListTopicRules returns all Topic Rules sorted by name.
func (b *InMemoryBackend) ListTopicRules() []*TopicRule {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.rules.Snapshot()
	out := make([]*TopicRule, 0, len(items))

	for _, v := range items {
		out = append(out, cloneTopicRule(v))
	}

	return out
}

// DeleteTopicRule deletes a Topic Rule by name.
func (b *InMemoryBackend) DeleteTopicRule(ruleName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.rules.Has(ruleName) {
		return fmt.Errorf("%w: %s", ErrRuleNotFound, ruleName)
	}

	b.rules.Delete(ruleName)

	return nil
}

// CreatePolicy creates a new IoT Policy.
func (b *InMemoryBackend) CreatePolicy(input *CreatePolicyInput) (*CreatePolicyOutput, error) {
	if input.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.policies.Has(input.PolicyName) {
		return nil, fmt.Errorf("%w: policy %q already exists", ErrAlreadyExists, input.PolicyName)
	}

	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("policy/%s", input.PolicyName))
	now := time.Now()

	b.policies.Put(&Policy{
		PolicyName:     input.PolicyName,
		PolicyDocument: input.PolicyDocument,
		ARN:            arn,
		CreatedAt:      now,
		LastModifiedAt: now,
	})

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

// AttachPrincipalPolicy attaches a policy to a principal. The attachment is
// recorded in the same policyTargets index used by AttachPolicy/DetachPolicy
// so that ListPrincipalPolicies, ListPolicyPrincipals, and
// DetachPrincipalPolicy observe it.
func (b *InMemoryBackend) AttachPrincipalPolicy(input *AttachPrincipalPolicyInput) error {
	if input.PolicyName == "" || input.Principal == "" {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.policyTargets[input.PolicyName] = append(b.policyTargets[input.PolicyName], input.Principal)

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

	result := computeSbomValidationResult(input.Sbom)
	b.sbomValidationResults[key] = []*SbomValidationResult{result}

	return &AssociateSbomWithPackageVersionOutput{
		PackageName:          input.PackageName,
		VersionName:          input.VersionName,
		Sbom:                 input.Sbom,
		SbomValidationStatus: result.ValidationResult,
	}, nil
}

// AssociateTargetsWithJob associates targets with a continuous job.
func (b *InMemoryBackend) AssociateTargetsWithJob(
	input *AssociateTargetsWithJobInput,
) (*AssociateTargetsWithJobOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.jobTargets[input.JobID] = append(b.jobTargets[input.JobID], input.Targets...)

	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("job/%s", input.JobID))

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
// Unknown task IDs still succeed (matching the legacy behavior of this
// operation) but, when the task is known, its rich AuditMitigationTask record
// (as returned by DescribeAuditMitigationActionsTask) is transitioned to
// CANCELED with an end time, keeping the two representations consistent.
func (b *InMemoryBackend) CancelAuditMitigationActionsTask(input *CancelAuditMitigationActionsTaskInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.auditMitigationTasks[input.TaskID] = string(JobStatusCanceled)
	if t, ok := b.auditMitigationTaskObjects.Get(input.TaskID); ok {
		t.TaskStatus = string(JobStatusCanceled)
		t.EndTime = float64(time.Now().Unix())
	}

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

	p, ok := b.policies.Get(policyName)
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

	if !b.policies.Has(policyName) {
		return fmt.Errorf("%w: %s", ErrPolicyNotFound, policyName)
	}

	if targets := b.policyTargets[policyName]; len(targets) > 0 {
		return fmt.Errorf("%w: policy %q has attached targets", ErrDeleteConflict, policyName)
	}

	b.policies.Delete(policyName)

	return nil
}

// ListPolicies returns all policies sorted by name.
func (b *InMemoryBackend) ListPolicies() []*Policy {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.policies.Snapshot()
	out := make([]*Policy, 0, len(items))

	for _, v := range items {
		cp := *v
		out = append(out, &cp)
	}

	return out
}

// DisableTopicRule disables an existing topic rule.
func (b *InMemoryBackend) DisableTopicRule(ruleName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	r, ok := b.rules.Get(ruleName)
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

	r, ok := b.rules.Get(ruleName)
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

	r, ok := b.rules.Get(input.RuleName)
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

	t, ok := b.things.Get(input.ThingName)
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

	if input.AttributePayload != nil {
		if input.AttributePayload.Merge != nil && !*input.AttributePayload.Merge {
			t.Attributes = make(map[string]string)
		} else if t.Attributes == nil {
			t.Attributes = make(map[string]string)
		}

		if input.AttributePayload.Attributes != nil {
			maps.Copy(t.Attributes, input.AttributePayload.Attributes)
		}
	}

	t.Version++

	return nil
}

// ListThingPrincipals returns principals attached to the given thing.
func (b *InMemoryBackend) ListThingPrincipals(thingName string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.things.Has(thingName) {
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
		t.ARN = arn.Build("iot", b.region, b.accountID, fmt.Sprintf("thing/%s", t.ThingName))
	}

	if t.Attributes == nil {
		t.Attributes = make(map[string]string)
	}

	if t.Version == 0 {
		t.Version = 1
	}

	b.things.Put(&t)
}

// AddPolicyInternal seeds a Policy directly into the backend for testing.
func (b *InMemoryBackend) AddPolicyInternal(p Policy) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if p.ARN == "" {
		p.ARN = arn.Build("iot", b.region, b.accountID, fmt.Sprintf("policy/%s", p.PolicyName))
	}

	b.policies.Put(&p)
}

// AddRuleInternal seeds a TopicRule directly into the backend for testing.
func (b *InMemoryBackend) AddRuleInternal(r TopicRule) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if r.ARN == "" {
		r.ARN = arn.Build("iot", b.region, b.accountID, fmt.Sprintf("rule/%s", r.RuleName))
	}

	if r.Actions == nil {
		r.Actions = []RuleAction{}
	}

	b.rules.Put(&r)
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

// ErrRegistrationTaskNotFound is returned when a bulk thing registration task does not exist.
var ErrRegistrationTaskNotFound = errors.New("thing registration task not found")

// ErrManagedJobTemplateNotFound is returned when a managed job template does not exist.
var ErrManagedJobTemplateNotFound = errors.New("managed job template not found")

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

// certStatusPendingTransfer is the AWS IoT certificate PENDING_TRANSFER status value.
const certStatusPendingTransfer = "PENDING_TRANSFER"

// certStatusRevoked is the AWS IoT certificate REVOKED status value.
const certStatusRevoked = "REVOKED"

// certStatusPendingActivation is the AWS IoT certificate PENDING_ACTIVATION status value.
const certStatusPendingActivation = "PENDING_ACTIVATION"

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

	if b.thingTypes.Has(input.ThingTypeName) {
		return nil, fmt.Errorf("%w: thing type %q already exists", ErrAlreadyExists, input.ThingTypeName)
	}

	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("thingtype/%s", input.ThingTypeName))
	tt := &ThingType{
		ThingTypeName:        input.ThingTypeName,
		ThingTypeID:          uuid.NewString(),
		ThingTypeARN:         arn,
		Description:          input.Description,
		SearchableAttributes: append([]string(nil), input.SearchableAttributes...),
		CreatedAt:            time.Now(),
	}

	b.thingTypes.Put(tt)

	return tt, nil
}

// DescribeThingType returns a ThingType by name.
func (b *InMemoryBackend) DescribeThingType(thingTypeName string) (*ThingType, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tt, ok := b.thingTypes.Get(thingTypeName)
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

	items := b.thingTypes.Snapshot()
	out := make([]*ThingType, 0, len(items))

	for _, v := range items {
		cp := *v
		out = append(out, &cp)
	}

	return out
}

// DeprecateThingType marks a thing type as deprecated (or un-deprecates it).
func (b *InMemoryBackend) DeprecateThingType(input *DeprecateThingTypeInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tt, ok := b.thingTypes.Get(input.ThingTypeName)
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

	tt, ok := b.thingTypes.Get(thingTypeName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrThingTypeNotFound, thingTypeName)
	}

	if !tt.Deprecated {
		return fmt.Errorf("%w: thing type %q must be deprecated before deletion", ErrValidation, thingTypeName)
	}

	b.thingTypes.Delete(thingTypeName)

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

	if b.thingGroups.Has(input.ThingGroupName) {
		return nil, fmt.Errorf("%w: thing group %q already exists", ErrAlreadyExists, input.ThingGroupName)
	}

	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("thinggroup/%s", input.ThingGroupName))
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

	b.thingGroups.Put(tg)
	b.thingGroupMembers[input.ThingGroupName] = []string{}

	return tg, nil
}

// DescribeThingGroup returns a ThingGroup by name.
func (b *InMemoryBackend) DescribeThingGroup(thingGroupName string) (*ThingGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tg, ok := b.thingGroups.Get(thingGroupName)
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

	items := b.thingGroups.Snapshot()
	out := make([]*ThingGroup, 0, len(items))

	for _, v := range items {
		tg := v
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

	tg, ok := b.thingGroups.Get(input.ThingGroupName)
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

	if !b.thingGroups.Has(thingGroupName) {
		return fmt.Errorf("%w: %s", ErrThingGroupNotFound, thingGroupName)
	}

	b.thingGroups.Delete(thingGroupName)
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

	if !b.thingGroups.Has(input.ThingGroupName) {
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
	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("cert/%s", certID))
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
	b.certificates.Put(cert)

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
	b.certificates.Put(cert)

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

	cert, ok := b.certificates.Get(certificateID)
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

	items := b.certificates.Snapshot()
	out := make([]*Certificate, 0, len(items))

	for _, v := range items {
		cp := *v
		out = append(out, &cp)
	}

	return out
}

// isValidCertStatus reports whether s is a legal AWS IoT certificate status.
func isValidCertStatus(s string) bool {
	switch s {
	case certStatusActive, certStatusInactive, certStatusRevoked,
		certStatusPendingTransfer, certStatusPendingActivation:
		return true
	}

	return false
}

// UpdateCertificate updates the status of a certificate.
func (b *InMemoryBackend) UpdateCertificate(input *UpdateCertificateInput) error {
	switch input.NewStatus {
	case certStatusPendingTransfer, certStatusPendingActivation:
		return fmt.Errorf("%w: status %q cannot be set via UpdateCertificate", ErrValidation, input.NewStatus)
	}

	if input.NewStatus != "" && !isValidCertStatus(input.NewStatus) {
		return fmt.Errorf("%w: invalid certificate status %q", ErrValidation, input.NewStatus)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	cert, ok := b.certificates.Get(input.CertificateID)
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

	cert, ok := b.certificates.Get(certificateID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrCertificateNotFound, certificateID)
	}

	if cert.Status == certStatusActive {
		return fmt.Errorf("%w: certificate %q must be deactivated before deletion", ErrDeleteConflict, certificateID)
	}

	b.certificates.Delete(certificateID)

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
			if p, ok := b.policies.Get(policyName); ok {
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

// maxPolicyVersions is the maximum number of versions allowed per policy (AWS limit).
const maxPolicyVersions = 5

// CreatePolicyVersion creates a new version of an existing policy.
func (b *InMemoryBackend) CreatePolicyVersion(input *CreatePolicyVersionInput) (*PolicyVersion, error) {
	if input.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.policies.Get(input.PolicyName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPolicyNotFound, input.PolicyName)
	}

	versions := b.policyVersions[input.PolicyName]

	if len(versions) >= maxPolicyVersions {
		return nil, fmt.Errorf(
			"%w: policy %q already has %d versions",
			ErrVersionsLimitExceeded,
			input.PolicyName,
			maxPolicyVersions,
		)
	}

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

	if !b.policies.Has(policyName) {
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

	arn := arn.Build("iot", b.region, b.accountID,
		fmt.Sprintf("ruledestination/http/%s", uuid.NewString()))

	dest := &TopicRuleDestination{
		ARN: arn,
	}

	if input.DestinationConfiguration != nil && input.DestinationConfiguration.HTTPURLConfiguration != nil {
		dest.HTTPURLProperties = &HTTPURLDestinationProperties{
			ConfirmationURL: input.DestinationConfiguration.HTTPURLConfiguration.ConfirmationURL,
		}
		// HTTP destinations require confirmation before they can be used,
		// matching AWS's real IN_PROGRESS -> ENABLED lifecycle.
		dest.Status = statusInProgress
		dest.ConfirmationToken = randomHex(certIDHexLen)
	} else {
		dest.Status = statusEnabled
	}

	b.topicRuleDestinations.Put(dest)

	return dest, nil
}

// GetTopicRuleDestination returns a topic rule destination by ARN.
func (b *InMemoryBackend) GetTopicRuleDestination(arn string) (*TopicRuleDestination, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	dest, ok := b.topicRuleDestinations.Get(arn)
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

	items := b.topicRuleDestinations.Snapshot()
	out := make([]*TopicRuleDestination, 0, len(items))

	for _, v := range items {
		cp := *v
		out = append(out, &cp)
	}

	return out
}

// UpdateTopicRuleDestination updates the status of a topic rule destination.
func (b *InMemoryBackend) UpdateTopicRuleDestination(input *UpdateTopicRuleDestinationInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	dest, ok := b.topicRuleDestinations.Get(input.ARN)
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

	if !b.topicRuleDestinations.Has(arn) {
		return fmt.Errorf("%w: %s", ErrTopicRuleDestinationNotFound, arn)
	}

	b.topicRuleDestinations.Delete(arn)

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

	if b.certificateProviders.Has(input.CertificateProviderName) {
		return nil, fmt.Errorf("%w: certificate provider %q already exists",
			ErrAlreadyExists, input.CertificateProviderName)
	}

	arn := arn.Build("iot", b.region, b.accountID,
		fmt.Sprintf("certificateprovider/%s", input.CertificateProviderName))

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

	b.certificateProviders.Put(cp)

	return cp, nil
}

// DescribeCertificateProvider returns a certificate provider by name.
func (b *InMemoryBackend) DescribeCertificateProvider(name string) (*CertificateProvider, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cp, ok := b.certificateProviders.Get(name)
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

	items := b.certificateProviders.Snapshot()
	out := make([]*CertificateProvider, 0, len(items))

	for _, v := range items {
		cp := *v
		cp.AccountDefaultForOperations = make([]string, len(v.AccountDefaultForOperations))
		copy(cp.AccountDefaultForOperations, v.AccountDefaultForOperations)
		out = append(out, &cp)
	}

	return out
}

// UpdateCertificateProvider updates an existing certificate provider.
func (b *InMemoryBackend) UpdateCertificateProvider(input *UpdateCertificateProviderInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp, ok := b.certificateProviders.Get(input.CertificateProviderName)
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

	if !b.certificateProviders.Has(name) {
		return fmt.Errorf("%w: %s", ErrCertificateProviderNotFound, name)
	}

	b.certificateProviders.Delete(name)

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

// -----------------------------------------------------------
// Device Shadow operations
// -----------------------------------------------------------

// GetThingShadow returns the shadow for a thing (classic or named).
func (b *InMemoryBackend) GetThingShadow(thingName, shadowName string) (*ThingShadow, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.things.Has(thingName) {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	key := shadowKey{thingName: thingName, shadowName: shadowName}
	s, ok := b.shadows[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	cp := *s

	return &cp, nil
}

// UpdateThingShadow creates or updates the shadow for a thing.
func (b *InMemoryBackend) UpdateThingShadow(thingName, shadowName string, state map[string]any) (*ThingShadow, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.things.Has(thingName) {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	key := shadowKey{thingName: thingName, shadowName: shadowName}
	existing := b.shadows[key]

	version := int64(1)
	if existing != nil {
		version = existing.Version + 1
	}

	s := &ThingShadow{
		State:   state,
		Version: version,
	}
	b.shadows[key] = s

	cp := *s

	return &cp, nil
}

// DeleteThingShadow deletes the shadow for a thing.
func (b *InMemoryBackend) DeleteThingShadow(thingName, shadowName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.things.Has(thingName) {
		return fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	key := shadowKey{thingName: thingName, shadowName: shadowName}
	if _, ok := b.shadows[key]; !ok {
		return fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	delete(b.shadows, key)

	return nil
}

// ListNamedShadowsForThing returns all named shadow names for a thing.
func (b *InMemoryBackend) ListNamedShadowsForThing(thingName string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.things.Has(thingName) {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	var names []string

	for k := range b.shadows {
		if k.thingName == thingName && k.shadowName != "" {
			names = append(names, k.shadowName)
		}
	}

	slices.Sort(names)

	return names, nil
}

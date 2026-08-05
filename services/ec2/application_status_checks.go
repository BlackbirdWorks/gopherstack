package ec2

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// Implements the Application Status Check family (aws-sdk-go-v2 ec2 v1.319):
// health-check definitions, their instance/tag associations, suppression,
// and the aggregated per-instance status read back via
// DescribeApplicationStatus. That op never fabricates "ok"/"impaired"/
// "initializing" since this backend runs no real HTTP health checks — see
// computeApplicationStatusLocked.

var (
	// ErrApplicationStatusCheckNotFound is returned when a check ID doesn't
	// exist or is deleted.
	ErrApplicationStatusCheckNotFound = errors.New("InvalidApplicationStatusCheckId.NotFound")
	// ErrInvalidParameterCombination is returned when Associate/
	// DisassociateApplicationStatusCheck gets both or neither of
	// InstanceIds/TargetTagAssociations.
	ErrInvalidParameterCombination = errors.New("InvalidParameterCombination")
	// ErrTooManyApplicationStatusChecks is returned when
	// CreateApplicationStatusCheck would exceed the real 50-per-account limit.
	ErrTooManyApplicationStatusChecks = errors.New("ApplicationStatusCheckLimitExceeded")
)

const (
	appStatusCheckProtocolHTTP  = "http"
	appStatusCheckProtocolHTTPS = "https"

	appStatusCheckAggregationIncluded = "included"
	appStatusCheckAggregationExcluded = "excluded"

	// Defaults per api_op_CreateApplicationStatusCheck.go doc comment.
	appStatusCheckDefaultPath                   = "/"
	appStatusCheckDefaultInterval               = 60
	appStatusCheckDefaultTimeout                = 6
	appStatusCheckDefaultFailureThreshold       = 2
	appStatusCheckDefaultSuccessThreshold       = 5
	appStatusCheckDefaultStatusCodeMatcher      = "200"
	appStatusCheckDefaultInitGracePeriodSeconds = 300

	// maxApplicationStatusChecksPerAccount: real AWS quota, 50/account.
	maxApplicationStatusChecksPerAccount = 50

	// appStatusAssocType{Instance,Tag}: AssociationTypeEnum wire values used
	// by ApplicationStatusCheckAssociationObject.
	appStatusAssocTypeInstance = "instance-id"
	appStatusAssocTypeTag      = "tag"

	// appStatusAssocType{Instance,Tag}Wire: SuccessfulAssociationResponseObject/
	// UnsuccessfulAssociationResponseObject.AssociationType use INSTANCE_ID/EC2TAG
	// — a distinct vocabulary from appStatusAssocType{Instance,Tag} above.
	appStatusAssocTypeInstanceWire = "INSTANCE_ID"
	appStatusAssocTypeTagWire      = "EC2TAG"

	// The three ApplicationStatusEnum values this backend can honestly
	// compute from tracked state — see computeApplicationStatusLocked.
	appStatusNotApplicable    = "not-applicable"
	appStatusInsufficientData = "insufficient-data"
	appStatusSuppressed       = "suppressed"
)

// ApplicationStatusCheck is a health-check definition that, once associated
// with instances or tags, monitors their application health. Mirrors the
// real ApplicationStatusCheckResponseObject.
//
// Deleted checks are kept indefinitely rather than purged after real AWS's
// undocumented grace period — see PARITY.md gaps.
type ApplicationStatusCheck struct {
	CreationTime                     time.Time `json:"creationTime"`
	LastUpdatedAt                    time.Time `json:"lastUpdatedAt"`
	ModifyTime                       time.Time `json:"modifyTime"`
	DeletionTime                     time.Time `json:"deletionTime"`
	ApplicationStatusCheckID         string    `json:"applicationStatusCheckID,omitempty"`
	Protocol                         string    `json:"protocol,omitempty"`
	Aggregation                      string    `json:"aggregation,omitempty"`
	IPScope                          string    `json:"ipScope,omitempty"`
	IPVersion                        string    `json:"ipVersion,omitempty"`
	Path                             string    `json:"path,omitempty"`
	StatusCodeMatcher                string    `json:"statusCodeMatcher,omitempty"`
	Port                             int       `json:"port,omitempty"`
	DeviceIndex                      int       `json:"deviceIndex,omitempty"`
	FailureThreshold                 int       `json:"failureThreshold,omitempty"`
	InitializationGracePeriodSeconds int       `json:"initializationGracePeriodSeconds,omitempty"`
	Interval                         int       `json:"interval,omitempty"`
	SuccessThreshold                 int       `json:"successThreshold,omitempty"`
	Timeout                          int       `json:"timeout,omitempty"`
	Deleted                          bool      `json:"deleted,omitempty"`
}

// ApplicationStatusCheckParams carries the optional fields shared by
// CreateApplicationStatusCheck and ModifyApplicationStatusCheck. A nil field
// means "not specified": Create applies the real default, Modify leaves the
// current value alone.
type ApplicationStatusCheckParams struct {
	Protocol                         *string
	Aggregation                      *string
	IPScope                          *string
	IPVersion                        *string
	Path                             *string
	StatusCodeMatcher                *string
	Port                             *int
	DeviceIndex                      *int
	FailureThreshold                 *int
	InitializationGracePeriodSeconds *int
	Interval                         *int
	SuccessThreshold                 *int
	Timeout                          *int
}

// CustomTagKeyValue is a tag key/value pair, used for
// Associate/DisassociateApplicationStatusCheck's TargetTagAssociations.
type CustomTagKeyValue struct {
	Key   string
	Value string
}

// ApplicationStatusCheckAssociation associates an application status check
// with either a specific instance or a tag key/value pair (instances with a
// matching tag are automatically monitored). Exactly one of InstanceID or
// TagKey/TagValue is set, per AssociationType.
type ApplicationStatusCheckAssociation struct {
	ApplicationStatusCheckID string `json:"applicationStatusCheckID,omitempty"`
	AssociationType          string `json:"associationType,omitempty"`
	InstanceID               string `json:"instanceID,omitempty"`
	TagKey                   string `json:"tagKey,omitempty"`
	TagValue                 string `json:"tagValue,omitempty"`
}

// ApplicationStatusAssociationResult is one outcome of Associate/
// DisassociateApplicationStatusCheck. Reason is only set when unsuccessful.
type ApplicationStatusAssociationResult struct {
	ApplicationStatusCheckID string
	AssociationType          string
	AssociationValue         string
	Reason                   string
}

// ApplicationStatusSuppression records that application status check
// results are (or, once disabled, were) suppressed for an instance. A zero
// ResumeAt means suppression is indefinite until explicitly disabled.
type ApplicationStatusSuppression struct {
	SuppressAt time.Time `json:"suppressAt"`
	ResumeAt   time.Time `json:"resumeAt"`
	InstanceID string    `json:"instanceID,omitempty"`
}

// ApplicationStatusSuppressionFailure is one Enable/
// DisableApplicationStatusCheckSuppression failure.
type ApplicationStatusSuppressionFailure struct {
	InstanceID string
	Reason     string
}

// InstanceApplicationStatus is one instance's aggregated application status,
// as returned by DescribeApplicationStatus. See computeApplicationStatusLocked
// for why Status is only ever one of "not-applicable"/"insufficient-data"/"suppressed".
type InstanceApplicationStatus struct {
	StatusTimeStamp  time.Time
	ResumeAt         time.Time
	InstanceID       string
	AvailabilityZone string
	// AvailabilityZoneID is never populated: this backend does not track a
	// separate AZ-ID from AZ-name for instances. Documented gap, not fabricated.
	AvailabilityZoneID string
	Status             string
}

// applyApplicationStatusCheckParams validates and applies each non-nil field
// of p onto check, leaving fields left nil in p unchanged. Shared by Create
// (against a check pre-populated with defaults) and Modify (against the
// stored check).
func applyApplicationStatusCheckParams(check *ApplicationStatusCheck, p ApplicationStatusCheckParams) error {
	if err := applyAppStatusCheckProtocolAndPort(check, p); err != nil {
		return err
	}

	if err := applyAppStatusCheckAggregationAndPath(check, p); err != nil {
		return err
	}

	if err := applyAppStatusCheckThresholds(check, p); err != nil {
		return err
	}

	applyAppStatusCheckMiscFields(check, p)

	if check.Timeout >= check.Interval {
		return fmt.Errorf("%w: Timeout must be less than Interval", ErrInvalidParameter)
	}

	return nil
}

func applyAppStatusCheckProtocolAndPort(check *ApplicationStatusCheck, p ApplicationStatusCheckParams) error {
	if p.Protocol != nil {
		if *p.Protocol != appStatusCheckProtocolHTTP && *p.Protocol != appStatusCheckProtocolHTTPS {
			return fmt.Errorf("%w: Protocol must be http or https", ErrInvalidParameter)
		}

		check.Protocol = *p.Protocol
	}

	if p.Port != nil {
		if *p.Port < 1 || *p.Port > 65535 {
			return fmt.Errorf("%w: Port must be between 1 and 65535", ErrInvalidParameter)
		}

		check.Port = *p.Port
	}

	return nil
}

func applyAppStatusCheckAggregationAndPath(check *ApplicationStatusCheck, p ApplicationStatusCheckParams) error {
	if p.Aggregation != nil {
		if *p.Aggregation != appStatusCheckAggregationIncluded && *p.Aggregation != appStatusCheckAggregationExcluded {
			return fmt.Errorf("%w: Aggregation must be included or excluded", ErrInvalidParameter)
		}

		check.Aggregation = *p.Aggregation
	}

	if p.Path != nil {
		if !strings.HasPrefix(*p.Path, "/") {
			return fmt.Errorf("%w: Path must start with /", ErrInvalidParameter)
		}

		check.Path = *p.Path
	}

	return nil
}

func applyAppStatusCheckThresholds(check *ApplicationStatusCheck, p ApplicationStatusCheckParams) error {
	if p.FailureThreshold != nil {
		if *p.FailureThreshold <= 0 {
			return fmt.Errorf("%w: FailureThreshold must be greater than 0", ErrInvalidParameter)
		}

		check.FailureThreshold = *p.FailureThreshold
	}

	if p.SuccessThreshold != nil {
		if *p.SuccessThreshold <= 0 {
			return fmt.Errorf("%w: SuccessThreshold must be greater than 0", ErrInvalidParameter)
		}

		check.SuccessThreshold = *p.SuccessThreshold
	}

	if p.Timeout != nil {
		check.Timeout = *p.Timeout
	}

	if p.Interval != nil {
		check.Interval = *p.Interval
	}

	return nil
}

// applyAppStatusCheckMiscFields applies the fields with no validation rules
// of their own (device index, IP scope/version, status code matcher, and
// grace period).
func applyAppStatusCheckMiscFields(check *ApplicationStatusCheck, p ApplicationStatusCheckParams) {
	if p.DeviceIndex != nil {
		check.DeviceIndex = *p.DeviceIndex
	}

	if p.InitializationGracePeriodSeconds != nil {
		check.InitializationGracePeriodSeconds = *p.InitializationGracePeriodSeconds
	}

	if p.IPScope != nil {
		check.IPScope = *p.IPScope
	}

	if p.IPVersion != nil {
		check.IPVersion = *p.IPVersion
	}

	if p.StatusCodeMatcher != nil {
		check.StatusCodeMatcher = *p.StatusCodeMatcher
	}
}

// CreateApplicationStatusCheck creates a new application status check.
// Protocol and Port are required; every other field falls back to its real,
// documented AWS default when not specified in p.
func (b *InMemoryBackend) CreateApplicationStatusCheck(
	p ApplicationStatusCheckParams,
) (*ApplicationStatusCheck, error) {
	if p.Protocol == nil || *p.Protocol == "" {
		return nil, fmt.Errorf("%w: Protocol is required", ErrInvalidParameter)
	}

	if p.Port == nil {
		return nil, fmt.Errorf("%w: Port is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateApplicationStatusCheck")
	defer b.mu.Unlock()

	activeCount := 0

	for _, c := range b.applicationStatusChecks.All() {
		if !c.Deleted {
			activeCount++
		}
	}

	if activeCount >= maxApplicationStatusChecksPerAccount {
		return nil, fmt.Errorf(
			"%w: maximum of %d application status checks per account",
			ErrTooManyApplicationStatusChecks,
			maxApplicationStatusChecksPerAccount,
		)
	}

	now := time.Now().UTC()
	check := &ApplicationStatusCheck{
		ApplicationStatusCheckID:         newApplicationStatusCheckID(),
		Aggregation:                      appStatusCheckAggregationIncluded,
		Path:                             appStatusCheckDefaultPath,
		Interval:                         appStatusCheckDefaultInterval,
		Timeout:                          appStatusCheckDefaultTimeout,
		FailureThreshold:                 appStatusCheckDefaultFailureThreshold,
		SuccessThreshold:                 appStatusCheckDefaultSuccessThreshold,
		StatusCodeMatcher:                appStatusCheckDefaultStatusCodeMatcher,
		InitializationGracePeriodSeconds: appStatusCheckDefaultInitGracePeriodSeconds,
		CreationTime:                     now,
		LastUpdatedAt:                    now,
		ModifyTime:                       now,
	}

	if err := applyApplicationStatusCheckParams(check, p); err != nil {
		return nil, err
	}

	b.applicationStatusChecks.Put(check)

	cp := *check

	return &cp, nil
}

// ModifyApplicationStatusCheck updates an existing application status check.
// Fields left unset in p retain their current stored value.
func (b *InMemoryBackend) ModifyApplicationStatusCheck(
	id string,
	p ApplicationStatusCheckParams,
) (*ApplicationStatusCheck, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ApplicationStatusCheckId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyApplicationStatusCheck")
	defer b.mu.Unlock()

	check, ok := b.applicationStatusChecks.Get(id)
	if !ok || check.Deleted {
		return nil, fmt.Errorf("%w: %s", ErrApplicationStatusCheckNotFound, id)
	}

	updated := *check
	if err := applyApplicationStatusCheckParams(&updated, p); err != nil {
		return nil, err
	}

	updated.LastUpdatedAt = time.Now().UTC()
	updated.ModifyTime = updated.LastUpdatedAt

	b.applicationStatusChecks.Put(&updated)

	cp := updated

	return &cp, nil
}

// DescribeApplicationStatusChecks returns application status checks,
// optionally filtered by ID and by the "aggregation" filter. Deleted checks
// are excluded unless includeAll is true.
func (b *InMemoryBackend) DescribeApplicationStatusChecks(
	ids []string,
	filters map[string][]string,
	includeAll bool,
) []*ApplicationStatusCheck {
	b.mu.RLock("DescribeApplicationStatusChecks")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*ApplicationStatusCheck, 0, b.applicationStatusChecks.Len())

	for _, c := range b.applicationStatusChecks.All() {
		if len(idSet) > 0 && !idSet[c.ApplicationStatusCheckID] {
			continue
		}

		if c.Deleted && !includeAll {
			continue
		}

		if !matchesAppStatusCheckFilters(c, filters) {
			continue
		}

		cp := *c
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ApplicationStatusCheckID < out[j].ApplicationStatusCheckID
	})

	return out
}

func matchesAppStatusCheckFilters(c *ApplicationStatusCheck, filters map[string][]string) bool {
	for name, values := range filters {
		if name != "aggregation" {
			continue
		}

		if !slices.Contains(values, c.Aggregation) {
			return false
		}
	}

	return true
}

// DeleteApplicationStatusCheck soft-deletes a check (see the
// ApplicationStatusCheck doc comment) and cascades to its associations.
func (b *InMemoryBackend) DeleteApplicationStatusCheck(id string) (*ApplicationStatusCheck, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ApplicationStatusCheckId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteApplicationStatusCheck")
	defer b.mu.Unlock()

	check, ok := b.applicationStatusChecks.Get(id)
	if !ok || check.Deleted {
		return nil, fmt.Errorf("%w: %s", ErrApplicationStatusCheckNotFound, id)
	}

	check.Deleted = true
	check.DeletionTime = time.Now().UTC()

	for _, a := range b.applicationStatusCheckAssociations.All() {
		if a.ApplicationStatusCheckID == id {
			b.applicationStatusCheckAssociations.Delete(appStatusCheckAssociationKeyFn(a))
		}
	}

	cp := *check

	return &cp, nil
}

func appStatusCheckAssociationKeyFn(a *ApplicationStatusCheckAssociation) string {
	if a.AssociationType == appStatusAssocTypeInstance {
		return a.ApplicationStatusCheckID + ":instance:" + a.InstanceID
	}

	return a.ApplicationStatusCheckID + ":tag:" + a.TagKey + "=" + a.TagValue
}

// AssociateApplicationStatusCheck associates an application status check
// with either instances or tags (exactly one of instanceIDs/tagAssociations
// must be non-empty, matching the real InvalidParameterCombination rule).
func (b *InMemoryBackend) AssociateApplicationStatusCheck(
	checkID string,
	instanceIDs []string,
	tagAssociations []CustomTagKeyValue,
) ([]ApplicationStatusAssociationResult, []ApplicationStatusAssociationResult, error) {
	if checkID == "" {
		return nil, nil, fmt.Errorf("%w: ApplicationStatusCheckId is required", ErrInvalidParameter)
	}

	hasInstances := len(instanceIDs) > 0
	hasTags := len(tagAssociations) > 0

	if hasInstances == hasTags {
		return nil, nil, fmt.Errorf(
			"%w: specify either InstanceIds or TargetTagAssociations, but not both",
			ErrInvalidParameterCombination,
		)
	}

	b.mu.Lock("AssociateApplicationStatusCheck")
	defer b.mu.Unlock()

	if check, ok := b.applicationStatusChecks.Get(checkID); !ok || check.Deleted {
		return nil, nil, fmt.Errorf("%w: %s", ErrApplicationStatusCheckNotFound, checkID)
	}

	var successful, unsuccessful []ApplicationStatusAssociationResult
	if hasInstances {
		successful, unsuccessful = b.associateInstancesLocked(checkID, instanceIDs)
	} else {
		successful, unsuccessful = b.associateTagsLocked(checkID, tagAssociations)
	}

	return successful, unsuccessful, nil
}

func (b *InMemoryBackend) associateInstancesLocked(
	checkID string,
	instanceIDs []string,
) ([]ApplicationStatusAssociationResult, []ApplicationStatusAssociationResult) {
	var successful, unsuccessful []ApplicationStatusAssociationResult

	for _, instID := range instanceIDs {
		if _, ok := b.instances.Get(instID); !ok {
			unsuccessful = append(unsuccessful, ApplicationStatusAssociationResult{
				ApplicationStatusCheckID: checkID,
				AssociationType:          appStatusAssocTypeInstanceWire,
				AssociationValue:         instID,
				Reason:                   ErrInstanceNotFound.Error(),
			})

			continue
		}

		assoc := &ApplicationStatusCheckAssociation{
			ApplicationStatusCheckID: checkID,
			AssociationType:          appStatusAssocTypeInstance,
			InstanceID:               instID,
		}
		b.applicationStatusCheckAssociations.Put(assoc)

		successful = append(successful, ApplicationStatusAssociationResult{
			ApplicationStatusCheckID: checkID,
			AssociationType:          appStatusAssocTypeInstanceWire,
			AssociationValue:         instID,
		})
	}

	return successful, unsuccessful
}

func (b *InMemoryBackend) associateTagsLocked(
	checkID string,
	tagAssociations []CustomTagKeyValue,
) ([]ApplicationStatusAssociationResult, []ApplicationStatusAssociationResult) {
	var successful, unsuccessful []ApplicationStatusAssociationResult

	for _, kv := range tagAssociations {
		value := kv.Key + "=" + kv.Value

		if kv.Key == "" {
			unsuccessful = append(unsuccessful, ApplicationStatusAssociationResult{
				ApplicationStatusCheckID: checkID,
				AssociationType:          appStatusAssocTypeTagWire,
				AssociationValue:         value,
				Reason:                   "tag key must not be blank",
			})

			continue
		}

		assoc := &ApplicationStatusCheckAssociation{
			ApplicationStatusCheckID: checkID,
			AssociationType:          appStatusAssocTypeTag,
			TagKey:                   kv.Key,
			TagValue:                 kv.Value,
		}
		b.applicationStatusCheckAssociations.Put(assoc)

		successful = append(successful, ApplicationStatusAssociationResult{
			ApplicationStatusCheckID: checkID,
			AssociationType:          appStatusAssocTypeTagWire,
			AssociationValue:         value,
		})
	}

	return successful, unsuccessful
}

// DisassociateApplicationStatusCheck removes an existing association.
// Exactly one of instanceIDs/tagAssociations must be non-empty.
func (b *InMemoryBackend) DisassociateApplicationStatusCheck(
	checkID string,
	instanceIDs []string,
	tagAssociations []CustomTagKeyValue,
) ([]ApplicationStatusAssociationResult, []ApplicationStatusAssociationResult, error) {
	if checkID == "" {
		return nil, nil, fmt.Errorf("%w: ApplicationStatusCheckId is required", ErrInvalidParameter)
	}

	hasInstances := len(instanceIDs) > 0
	hasTags := len(tagAssociations) > 0

	if hasInstances == hasTags {
		return nil, nil, fmt.Errorf(
			"%w: specify either InstanceIds or TargetTagAssociations, but not both",
			ErrInvalidParameterCombination,
		)
	}

	b.mu.Lock("DisassociateApplicationStatusCheck")
	defer b.mu.Unlock()

	if _, ok := b.applicationStatusChecks.Get(checkID); !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrApplicationStatusCheckNotFound, checkID)
	}

	var successful, unsuccessful []ApplicationStatusAssociationResult
	if hasInstances {
		successful, unsuccessful = b.disassociateInstancesLocked(checkID, instanceIDs)
	} else {
		successful, unsuccessful = b.disassociateTagsLocked(checkID, tagAssociations)
	}

	return successful, unsuccessful, nil
}

func (b *InMemoryBackend) disassociateInstancesLocked(
	checkID string,
	instanceIDs []string,
) ([]ApplicationStatusAssociationResult, []ApplicationStatusAssociationResult) {
	var successful, unsuccessful []ApplicationStatusAssociationResult

	for _, instID := range instanceIDs {
		key := checkID + ":instance:" + instID
		if _, ok := b.applicationStatusCheckAssociations.Get(key); !ok {
			unsuccessful = append(unsuccessful, ApplicationStatusAssociationResult{
				ApplicationStatusCheckID: checkID,
				AssociationType:          appStatusAssocTypeInstanceWire,
				AssociationValue:         instID,
				Reason:                   "association not found",
			})

			continue
		}

		b.applicationStatusCheckAssociations.Delete(key)

		successful = append(successful, ApplicationStatusAssociationResult{
			ApplicationStatusCheckID: checkID,
			AssociationType:          appStatusAssocTypeInstanceWire,
			AssociationValue:         instID,
		})
	}

	return successful, unsuccessful
}

func (b *InMemoryBackend) disassociateTagsLocked(
	checkID string,
	tagAssociations []CustomTagKeyValue,
) ([]ApplicationStatusAssociationResult, []ApplicationStatusAssociationResult) {
	var successful, unsuccessful []ApplicationStatusAssociationResult

	for _, kv := range tagAssociations {
		value := kv.Key + "=" + kv.Value
		key := checkID + ":tag:" + value

		if _, ok := b.applicationStatusCheckAssociations.Get(key); !ok {
			unsuccessful = append(unsuccessful, ApplicationStatusAssociationResult{
				ApplicationStatusCheckID: checkID,
				AssociationType:          appStatusAssocTypeTagWire,
				AssociationValue:         value,
				Reason:                   "association not found",
			})

			continue
		}

		b.applicationStatusCheckAssociations.Delete(key)

		successful = append(successful, ApplicationStatusAssociationResult{
			ApplicationStatusCheckID: checkID,
			AssociationType:          appStatusAssocTypeTagWire,
			AssociationValue:         value,
		})
	}

	return successful, unsuccessful
}

// DescribeApplicationStatusCheckAssociations returns associations,
// optionally filtered by check ID and by the "association-type" filter.
// Unrecognised check IDs are simply unmatched rather than erroring.
func (b *InMemoryBackend) DescribeApplicationStatusCheckAssociations(
	checkIDs []string,
	filters map[string][]string,
) []*ApplicationStatusCheckAssociation {
	b.mu.RLock("DescribeApplicationStatusCheckAssociations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(checkIDs))
	for _, id := range checkIDs {
		idSet[id] = true
	}

	out := make([]*ApplicationStatusCheckAssociation, 0)

	for _, a := range b.applicationStatusCheckAssociations.All() {
		if len(idSet) > 0 && !idSet[a.ApplicationStatusCheckID] {
			continue
		}

		if !matchesAppStatusAssociationFilters(a, filters) {
			continue
		}

		cp := *a
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return appStatusCheckAssociationKeyFn(out[i]) < appStatusCheckAssociationKeyFn(out[j])
	})

	return out
}

func matchesAppStatusAssociationFilters(
	a *ApplicationStatusCheckAssociation,
	filters map[string][]string,
) bool {
	for name, values := range filters {
		if name != "association-type" {
			continue
		}

		if !slices.Contains(values, a.AssociationType) {
			return false
		}
	}

	return true
}

// EnableApplicationStatusCheckSuppression suppresses application status
// checks for the given instances. durationSeconds <= 0 suppresses
// indefinitely (matches real AWS: "If you do not specify DurationSeconds,
// suppression continues indefinitely").
func (b *InMemoryBackend) EnableApplicationStatusCheckSuppression(
	instanceIDs []string,
	durationSeconds int,
) ([]*ApplicationStatusSuppression, []ApplicationStatusSuppressionFailure) {
	b.mu.Lock("EnableApplicationStatusCheckSuppression")
	defer b.mu.Unlock()

	var successful []*ApplicationStatusSuppression

	var unsuccessful []ApplicationStatusSuppressionFailure

	now := time.Now().UTC()

	for _, instID := range instanceIDs {
		if _, ok := b.instances.Get(instID); !ok {
			unsuccessful = append(unsuccessful, ApplicationStatusSuppressionFailure{
				InstanceID: instID,
				Reason:     ErrInstanceNotFound.Error(),
			})

			continue
		}

		sup := &ApplicationStatusSuppression{InstanceID: instID, SuppressAt: now}
		if durationSeconds > 0 {
			sup.ResumeAt = now.Add(time.Duration(durationSeconds) * time.Second)
		}

		b.applicationStatusSuppressions.Put(sup)

		cp := *sup
		successful = append(successful, &cp)
	}

	return successful, unsuccessful
}

// DisableApplicationStatusCheckSuppression resumes normal health check
// reporting for the given instances.
func (b *InMemoryBackend) DisableApplicationStatusCheckSuppression(
	instanceIDs []string,
) ([]*ApplicationStatusSuppression, []ApplicationStatusSuppressionFailure) {
	b.mu.Lock("DisableApplicationStatusCheckSuppression")
	defer b.mu.Unlock()

	var successful []*ApplicationStatusSuppression

	var unsuccessful []ApplicationStatusSuppressionFailure

	now := time.Now().UTC()

	for _, instID := range instanceIDs {
		if _, ok := b.instances.Get(instID); !ok {
			unsuccessful = append(unsuccessful, ApplicationStatusSuppressionFailure{
				InstanceID: instID,
				Reason:     ErrInstanceNotFound.Error(),
			})

			continue
		}

		result := &ApplicationStatusSuppression{InstanceID: instID, SuppressAt: now}

		if existing, hadSuppression := b.applicationStatusSuppressions.Get(instID); hadSuppression {
			result.SuppressAt = existing.SuppressAt
			result.ResumeAt = existing.ResumeAt
			b.applicationStatusSuppressions.Delete(instID)
		}

		successful = append(successful, result)
	}

	return successful, unsuccessful
}

// applicationStatusSuppressionActiveLocked reports whether sup is still in
// effect (indefinite, or its ResumeAt has not yet passed).
func applicationStatusSuppressionActiveLocked(sup *ApplicationStatusSuppression) bool {
	return sup.ResumeAt.IsZero() || sup.ResumeAt.After(time.Now().UTC())
}

// DescribeApplicationStatus derives the aggregated instance-level status for
// the requested (or, if empty, every) instance.
//
// This backend runs no real HTTP health checks, so it can never honestly
// report "ok"/"impaired"/"initializing". It only ever returns the three
// ApplicationStatusEnum values derivable from tracked state: "suppressed"
// (active suppression), "not-applicable" (no included-aggregation check
// associated), "insufficient-data" (a check is associated but never run —
// AWS's own documented meaning, not fabricated).
//
// Details is always empty and StatusSince always zero — documented gaps, see
// PARITY.md.
func (b *InMemoryBackend) DescribeApplicationStatus(
	instanceIDs []string,
	filters map[string][]string,
) []*InstanceApplicationStatus {
	b.mu.RLock("DescribeApplicationStatus")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		idSet[id] = true
	}

	includedChecks := b.includedAggregationChecksLocked()

	out := make([]*InstanceApplicationStatus, 0)

	for _, inst := range b.instances.All() {
		if len(idSet) > 0 && !idSet[inst.ID] {
			continue
		}

		status := b.computeApplicationStatusLocked(inst, includedChecks)
		if !matchesAppStatusFilters(status, filters) {
			continue
		}

		out = append(out, status)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })

	return out
}

// includedAggregationChecksLocked returns every non-deleted check with
// Aggregation "included" — the only checks that affect
// DescribeApplicationStatus (excluded checks don't, per real AWS docs).
func (b *InMemoryBackend) includedAggregationChecksLocked() []*ApplicationStatusCheck {
	out := make([]*ApplicationStatusCheck, 0)

	for _, c := range b.applicationStatusChecks.All() {
		if !c.Deleted && c.Aggregation == appStatusCheckAggregationIncluded {
			out = append(out, c)
		}
	}

	return out
}

func (b *InMemoryBackend) computeApplicationStatusLocked(
	inst *Instance,
	includedChecks []*ApplicationStatusCheck,
) *InstanceApplicationStatus {
	result := &InstanceApplicationStatus{
		InstanceID:       inst.ID,
		AvailabilityZone: inst.Placement.AvailabilityZone,
		StatusTimeStamp:  time.Now().UTC(),
	}

	if sup, ok := b.applicationStatusSuppressions.Get(inst.ID); ok && applicationStatusSuppressionActiveLocked(sup) {
		result.Status = appStatusSuppressed
		result.ResumeAt = sup.ResumeAt

		return result
	}

	if b.instanceHasIncludedCheckLocked(inst.ID, includedChecks) {
		result.Status = appStatusInsufficientData
	} else {
		result.Status = appStatusNotApplicable
	}

	return result
}

// instanceHasIncludedCheckLocked reports whether any included-aggregation
// check applies to instanceID, either via a direct instance-id association
// or via a tag-based association matching one of the instance's real tags.
func (b *InMemoryBackend) instanceHasIncludedCheckLocked(
	instanceID string,
	includedChecks []*ApplicationStatusCheck,
) bool {
	if len(includedChecks) == 0 {
		return false
	}

	instTags := b.tags[instanceID]

	for _, check := range includedChecks {
		key := check.ApplicationStatusCheckID + ":instance:" + instanceID
		if _, ok := b.applicationStatusCheckAssociations.Get(key); ok {
			return true
		}

		if b.instanceTagMatchesCheckLocked(check.ApplicationStatusCheckID, instTags) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) instanceTagMatchesCheckLocked(checkID string, instTags map[string]string) bool {
	if len(instTags) == 0 {
		return false
	}

	for _, a := range b.applicationStatusCheckAssociations.All() {
		if a.ApplicationStatusCheckID != checkID || a.AssociationType != appStatusAssocTypeTag {
			continue
		}

		if v, ok := instTags[a.TagKey]; ok && v == a.TagValue {
			return true
		}
	}

	return false
}

func matchesAppStatusFilters(s *InstanceApplicationStatus, filters map[string][]string) bool {
	for name, values := range filters {
		var actual string

		switch name {
		case "status":
			actual = s.Status
		case "availability-zone-id":
			actual = s.AvailabilityZoneID
		default:
			continue
		}

		if !slices.Contains(values, actual) {
			return false
		}
	}

	return true
}

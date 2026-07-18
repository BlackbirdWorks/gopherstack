package iot

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// AttachSecurityProfile attaches a security profile to a target.
func (b *InMemoryBackend) AttachSecurityProfile(input *AttachSecurityProfileInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.securityProfileTargets[input.SecurityProfileName] = appendUnique(
		b.securityProfileTargets[input.SecurityProfileName],
		input.SecurityProfileTargetArn,
	)

	return nil
}

// DetachSecurityProfile removes a target ARN from a security profile.
func (b *InMemoryBackend) DetachSecurityProfile(profileName, targetARN string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	targets := b.securityProfileTargets[profileName]
	filtered := make([]string, 0, len(targets))
	for _, t := range targets {
		if t != targetARN {
			filtered = append(filtered, t)
		}
	}
	b.securityProfileTargets[profileName] = filtered

	return nil
}

// ListTargetsForSecurityProfile returns the target ARNs attached to a security profile.
func (b *InMemoryBackend) ListTargetsForSecurityProfile(profileName string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	targets := b.securityProfileTargets[profileName]
	out := make([]string, len(targets))
	copy(out, targets)

	return out
}

// ListSecurityProfilesForTarget returns profile names attached to a target ARN.
func (b *InMemoryBackend) ListSecurityProfilesForTarget(targetARN string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []string
	for profileName, targets := range b.securityProfileTargets {
		if slices.Contains(targets, targetARN) {
			out = append(out, profileName)
		}
	}
	sort.Strings(out)

	return out
}

// SecurityProfile represents an IoT security profile.
type SecurityProfile struct {
	Tags                       map[string]string `json:"tags,omitempty"`
	SecurityProfileName        string            `json:"securityProfileName"`
	SecurityProfileARN         string            `json:"securityProfileArn"`
	SecurityProfileDescription string            `json:"securityProfileDescription,omitempty"`
	Version                    int64             `json:"version"`
	CreationDate               float64           `json:"creationDate,omitempty"`
	LastModifiedDate           float64           `json:"lastModifiedDate,omitempty"`
}

func cloneSecurityProfile(sp *SecurityProfile) *SecurityProfile {
	cp := *sp

	return &cp
}

func (b *InMemoryBackend) securityProfileARN(name string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("securityprofile/%s", name))
}

// CreateSecurityProfileInput holds input for CreateSecurityProfile.
type CreateSecurityProfileInput struct {
	Tags                       map[string]string `json:"tags,omitempty"`
	SecurityProfileName        string            `json:"securityProfileName"`
	SecurityProfileDescription string            `json:"securityProfileDescription,omitempty"`
}

func (b *InMemoryBackend) CreateSecurityProfile(
	input *CreateSecurityProfileInput,
) (*SecurityProfile, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.securityProfiles.Has(input.SecurityProfileName) {
		return nil, fmt.Errorf(
			"security profile %q already exists: %w",
			input.SecurityProfileName,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().Unix())
	sp := &SecurityProfile{
		SecurityProfileName:        input.SecurityProfileName,
		SecurityProfileARN:         b.securityProfileARN(input.SecurityProfileName),
		SecurityProfileDescription: input.SecurityProfileDescription,
		Tags:                       input.Tags,
		Version:                    1,
		CreationDate:               now,
		LastModifiedDate:           now,
	}
	b.securityProfiles.Put(sp)

	return cloneSecurityProfile(sp), nil
}

func (b *InMemoryBackend) DescribeSecurityProfile(name string) (*SecurityProfile, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sp, ok := b.securityProfiles.Get(name)
	if !ok {
		return nil, fmt.Errorf("security profile %q not found: %w", name, ErrResourceNotFound)
	}

	return cloneSecurityProfile(sp), nil
}

func (b *InMemoryBackend) ListSecurityProfiles() []*SecurityProfile {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*SecurityProfile, 0, b.securityProfiles.Len())
	for _, v := range b.securityProfiles.Snapshot() {
		out = append(out, cloneSecurityProfile(v))
	}

	return out
}

func (b *InMemoryBackend) UpdateSecurityProfile(
	name, description string,
) (*SecurityProfile, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sp, ok := b.securityProfiles.Get(name)
	if !ok {
		return nil, fmt.Errorf("security profile %q not found: %w", name, ErrResourceNotFound)
	}
	if description != "" {
		sp.SecurityProfileDescription = description
	}
	sp.Version++
	sp.LastModifiedDate = float64(time.Now().Unix())

	return cloneSecurityProfile(sp), nil
}

func (b *InMemoryBackend) DeleteSecurityProfile(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.securityProfiles.Has(name) {
		return fmt.Errorf("security profile %q not found: %w", name, ErrResourceNotFound)
	}
	b.securityProfiles.Delete(name)

	return nil
}

// SecurityProfileBehavior describes one behavior to validate.
type SecurityProfileBehavior struct {
	Criteria *SecurityProfileBehaviorCriteria `json:"criteria,omitempty"`
	Name     string                           `json:"name"`
	Metric   string                           `json:"metric,omitempty"`
}

// SecurityProfileBehaviorCriteria is the criteria portion of a behavior.
type SecurityProfileBehaviorCriteria struct {
	ComparisonOperator           string `json:"comparisonOperator,omitempty"`
	DurationSeconds              int32  `json:"durationSeconds,omitempty"`
	ConsecutiveDatapointsToAlarm int32  `json:"consecutiveDatapointsToAlarm,omitempty"`
	ConsecutiveDatapointsToClear int32  `json:"consecutiveDatapointsToClear,omitempty"`
}

// isValidComparisonOperator reports whether op is one of the AWS IoT Device
// Defender comparison operators (types.ComparisonOperator).
func isValidComparisonOperator(op string) bool {
	switch op {
	case "less-than", "less-than-equals", "greater-than", "greater-than-equals",
		"in-cidr-set", "not-in-cidr-set", "in-port-set", "not-in-port-set",
		"in-set", "not-in-set":
		return true
	default:
		return false
	}
}

// ValidateSecurityProfileBehaviors validates the supplied behaviors against
// AWS's structural rules (name required, criteria required, comparison
// operator must be a known value, non-negative durations/thresholds).
func (b *InMemoryBackend) ValidateSecurityProfileBehaviors(
	behaviors []SecurityProfileBehavior,
) (bool, []string) {
	errs := make([]string, 0, len(behaviors))

	for _, beh := range behaviors {
		errs = append(errs, validateOneBehavior(beh)...)
	}

	return len(errs) == 0, errs
}

func validateOneBehavior(beh SecurityProfileBehavior) []string {
	var errs []string

	if beh.Name == "" {
		errs = append(errs, "behavior name is required")
	}

	if beh.Criteria == nil {
		errs = append(errs, fmt.Sprintf("behavior %q: criteria is required", beh.Name))

		return errs
	}

	if beh.Criteria.ComparisonOperator != "" && !isValidComparisonOperator(beh.Criteria.ComparisonOperator) {
		errs = append(errs, fmt.Sprintf(
			"behavior %q: invalid comparisonOperator %q", beh.Name, beh.Criteria.ComparisonOperator,
		))
	}

	if beh.Criteria.DurationSeconds < 0 {
		errs = append(errs, fmt.Sprintf("behavior %q: durationSeconds must not be negative", beh.Name))
	}

	if beh.Criteria.ConsecutiveDatapointsToAlarm < 0 {
		errs = append(errs, fmt.Sprintf("behavior %q: consecutiveDatapointsToAlarm must not be negative", beh.Name))
	}

	if beh.Criteria.ConsecutiveDatapointsToClear < 0 {
		errs = append(errs, fmt.Sprintf("behavior %q: consecutiveDatapointsToClear must not be negative", beh.Name))
	}

	return errs
}

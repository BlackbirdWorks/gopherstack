package awsconfig

import (
	"fmt"
	"slices"
	"strings"
)

// PutConfigurationRecorder creates or updates a configuration recorder.
// When updating an existing recorder, the Status is preserved; RoleARN and RecordingGroup are updated.
// A new recorder starts in PENDING state.
func (b *InMemoryBackend) PutConfigurationRecorder(name, roleARN string, recordingGroup *RecordingGroup) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationRecorder name is required", ErrValidation)
	}

	if roleARN == "" {
		return fmt.Errorf("%w: ConfigurationRecorder roleARN is required", ErrValidation)
	}

	b.mu.Lock("PutConfigurationRecorder")
	defer b.mu.Unlock()

	if existing, ok := b.recorders.Get(name); ok {
		existing.RoleARN = roleARN
		existing.RecordingGroup = recordingGroup

		return nil
	}

	b.recorders.Put(&ConfigurationRecorder{
		Name:           name,
		RoleARN:        roleARN,
		Status:         recorderStatusPending,
		RecordingGroup: recordingGroup,
	})

	return nil
}

// recorderArn builds the ARN for a configuration recorder owned by this backend,
// matching the "arn" field the real service serializes on ConfigurationRecorder
// (aws-sdk-go-v2/service/configservice/types.ConfigurationRecorder.Arn).
func (b *InMemoryBackend) recorderArn(name string) string {
	return fmt.Sprintf("arn:aws:config:%s:%s:config-recorder/%s", b.region, b.accountID, name)
}

// DescribeConfigurationRecorders returns configuration recorders filtered by the
// provided name list.  An empty/nil names list returns all recorders sorted by name.
func (b *InMemoryBackend) DescribeConfigurationRecorders(names []string) []ConfigurationRecorder {
	b.mu.RLock("DescribeConfigurationRecorders")
	defer b.mu.RUnlock()

	out := make([]ConfigurationRecorder, 0, b.recorders.Len())

	if len(names) == 0 {
		for _, r := range b.recorders.All() {
			cp := *r
			cp.Arn = b.recorderArn(r.Name)
			out = append(out, cp)
		}
	} else {
		for _, n := range names {
			if r, ok := b.recorders.Get(n); ok {
				cp := *r
				cp.Arn = b.recorderArn(r.Name)
				out = append(out, cp)
			}
		}
	}

	slices.SortFunc(out, func(a, b ConfigurationRecorder) int {
		if a.Name < b.Name {
			return -1
		}

		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return out
}

// StartConfigurationRecorder starts a configuration recorder.
func (b *InMemoryBackend) StartConfigurationRecorder(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationRecorderName is required", ErrValidation)
	}

	b.mu.Lock("StartConfigurationRecorder")
	defer b.mu.Unlock()

	r, ok := b.recorders.Get(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	if b.channels.Len() == 0 {
		return fmt.Errorf("%w: no delivery channel configured", ErrNoDeliveryChannel)
	}

	r.Status = recorderStatusActive

	return nil
}

// StopConfigurationRecorder stops an active configuration recorder.
func (b *InMemoryBackend) StopConfigurationRecorder(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationRecorderName is required", ErrValidation)
	}

	b.mu.Lock("StopConfigurationRecorder")
	defer b.mu.Unlock()

	r, ok := b.recorders.Get(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	r.Status = recorderStatusPending

	return nil
}

// DeleteConfigurationRecorder removes a configuration recorder by name.
func (b *InMemoryBackend) DeleteConfigurationRecorder(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigurationRecorderName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConfigurationRecorder")
	defer b.mu.Unlock()

	if !b.recorders.Has(name) {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	b.recorders.Delete(name)

	return nil
}

// recorderStatus builds a ConfigurationRecorderStatus from a recorder.
func recorderStatus(r *ConfigurationRecorder) ConfigurationRecorderStatus {
	recording := r.Status == recorderStatusActive
	lastStatus := recorderStatusPending
	if recording {
		lastStatus = recorderStatusSuccess
	}

	return ConfigurationRecorderStatus{
		Name:       r.Name,
		Recording:  recording,
		LastStatus: lastStatus,
	}
}

// DescribeConfigurationRecorderStatus returns recording status for recorders filtered
// by the provided name list.  An empty/nil list returns status for all recorders,
// sorted by name.
func (b *InMemoryBackend) DescribeConfigurationRecorderStatus(names []string) []ConfigurationRecorderStatus {
	b.mu.RLock("DescribeConfigurationRecorderStatus")
	defer b.mu.RUnlock()

	out := make([]ConfigurationRecorderStatus, 0, b.recorders.Len())

	if len(names) == 0 {
		for _, r := range b.recorders.All() {
			out = append(out, recorderStatus(r))
		}
	} else {
		for _, n := range names {
			if r, ok := b.recorders.Get(n); ok {
				out = append(out, recorderStatus(r))
			}
		}
	}

	slices.SortFunc(out, func(a, b ConfigurationRecorderStatus) int {
		if a.Name < b.Name {
			return -1
		}

		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return out
}

// recorderNameFromArn extracts a recorder's name from either a bare name or a
// full "arn:aws:config:<region>:<account>:config-recorder/<name>" ARN, so
// AssociateResourceTypes/DisassociateResourceTypes accept both forms (real
// SDK callers always send the full ARN; unit tests exercise the bare name).
func recorderNameFromArn(recorderARN string) string {
	if idx := strings.LastIndex(recorderARN, "/"); idx >= 0 {
		return recorderARN[idx+1:]
	}

	return recorderARN
}

// mergeResourceTypes returns existing with every type in added that is not
// already present appended, preserving existing order and de-duplicating.
func mergeResourceTypes(existing, added []string) []string {
	seen := make(map[string]struct{}, len(existing))
	for _, t := range existing {
		seen[t] = struct{}{}
	}

	out := existing

	for _, t := range added {
		if _, ok := seen[t]; ok {
			continue
		}

		seen[t] = struct{}{}
		out = append(out, t)
	}

	return out
}

// removeResourceTypes returns existing with every type in removed dropped.
func removeResourceTypes(existing, removed []string) []string {
	if len(existing) == 0 || len(removed) == 0 {
		return existing
	}

	drop := make(map[string]struct{}, len(removed))
	for _, t := range removed {
		drop[t] = struct{}{}
	}

	out := existing[:0]

	for _, t := range existing {
		if _, ok := drop[t]; !ok {
			out = append(out, t)
		}
	}

	return out
}

// AssociateResourceTypes adds resourceTypes to a configuration recorder's
// RecordingGroup, matching AssociateResourceTypesInput/Output
// (aws-sdk-go-v2/service/configservice). recorderARN may be the recorder's
// bare name or its full ARN. Errors with ErrNotFound (wire type
// NoSuchConfigurationRecorderException) when no matching recorder exists,
// matching the real API's declared error model instead of fabricating a
// synthetic recorder for unknown input.
func (b *InMemoryBackend) AssociateResourceTypes(
	recorderARN string,
	resourceTypes []string,
) (*ConfigurationRecorder, error) {
	if recorderARN == "" {
		return nil, fmt.Errorf("%w: ConfigurationRecorderArn is required", ErrValidation)
	}

	b.mu.Lock("AssociateResourceTypes")
	defer b.mu.Unlock()

	name := recorderNameFromArn(recorderARN)

	r, ok := b.recorders.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, recorderARN)
	}

	if r.RecordingGroup == nil {
		r.RecordingGroup = &RecordingGroup{}
	}

	r.RecordingGroup.ResourceTypes = mergeResourceTypes(r.RecordingGroup.ResourceTypes, resourceTypes)

	cp := *r
	cp.Arn = b.recorderArn(r.Name)
	rgCopy := *r.RecordingGroup
	cp.RecordingGroup = &rgCopy

	return &cp, nil
}

// DisassociateResourceTypes removes resourceTypes from a configuration
// recorder's RecordingGroup, the inverse of AssociateResourceTypes.
// recorderARN may be the recorder's bare name or its full ARN. Errors with
// ErrNotFound (wire type NoSuchConfigurationRecorderException) when no
// matching recorder exists, matching the real API's declared error model
// (verified against aws-sdk-go-v2/service/configservice's
// DisassociateResourceTypes deserializer).
func (b *InMemoryBackend) DisassociateResourceTypes(recorderARN string, resourceTypes []string) error {
	if recorderARN == "" {
		return fmt.Errorf("%w: ConfigurationRecorderArn is required", ErrValidation)
	}

	b.mu.Lock("DisassociateResourceTypes")
	defer b.mu.Unlock()

	name := recorderNameFromArn(recorderARN)

	r, ok := b.recorders.Get(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, recorderARN)
	}

	if r.RecordingGroup != nil {
		r.RecordingGroup.ResourceTypes = removeResourceTypes(r.RecordingGroup.ResourceTypes, resourceTypes)
	}

	return nil
}

// DeleteServiceLinkedConfigurationRecorder is a no-op stub.
func (b *InMemoryBackend) DeleteServiceLinkedConfigurationRecorder(_ string) error { return nil }

// PutServiceLinkedConfigurationRecorder is a no-op stub.
func (b *InMemoryBackend) PutServiceLinkedConfigurationRecorder() error { return nil }

// ListConfigurationRecorders returns summaries of all configuration recorders.
func (b *InMemoryBackend) ListConfigurationRecorders() []ConfigurationRecorderSummary {
	b.mu.RLock("ListConfigurationRecorders")
	defer b.mu.RUnlock()

	all := b.recorders.All()
	out := make([]ConfigurationRecorderSummary, 0, len(all))

	for _, r := range all {
		arn := fmt.Sprintf(
			"arn:aws:config:%s:%s:config-recorder/%s",
			b.region, b.accountID, r.Name,
		)
		out = append(out, ConfigurationRecorderSummary{
			Arn:            arn,
			Name:           r.Name,
			RecordingScope: "INTERNAL",
		})
	}

	return out
}

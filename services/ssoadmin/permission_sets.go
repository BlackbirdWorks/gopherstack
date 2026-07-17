package ssoadmin

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// CreatePermissionSet creates a new permission set within an SSO instance.
func (b *InMemoryBackend) CreatePermissionSet(
	instanceArn, name, description, sessionDuration, relayState string,
	tags map[string]string,
) (*PermissionSet, error) {
	b.mu.Lock("CreatePermissionSet")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceArn) {
		return nil, ErrInstanceNotFound
	}

	if err := validatePermissionSetName(name); err != nil {
		return nil, err
	}

	if err := validateSessionDuration(sessionDuration); err != nil {
		return nil, err
	}

	if len(tags) > 0 {
		if err := validateTags(tags); err != nil {
			return nil, err
		}
	}

	for _, ps := range b.permissionSetsByInstance.Get(instanceArn) {
		if ps.Name == name {
			return nil, ErrPermissionSetAlreadyExists
		}
	}

	instanceID := instanceARNToID(instanceArn)
	id := uuid.NewString()[:uuidShortLen]
	psArn := arn.Build("sso", "", "", fmt.Sprintf("permissionSet/%s/%s", instanceID, id))

	if sessionDuration == "" {
		sessionDuration = defaultSessionDuration
	}

	ps := &PermissionSet{
		PermissionSetArn: psArn,
		InstanceArn:      instanceArn,
		Name:             name,
		Description:      description,
		SessionDuration:  sessionDuration,
		RelayState:       relayState,
		CreatedDate:      time.Now().UTC(),
		Tags:             make(map[string]string),
	}
	maps.Copy(ps.Tags, tags)
	b.permissionSets.Put(ps)

	cp := b.copyPermissionSet(ps)

	return cp, nil
}

// DescribePermissionSet returns a specific permission set.
func (b *InMemoryBackend) DescribePermissionSet(instanceArn, permissionSetArn string) (*PermissionSet, error) {
	b.mu.RLock("DescribePermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return nil, ErrPermissionSetNotFound
	}

	return b.copyPermissionSet(ps), nil
}

// ListPermissionSets returns all permission sets for an SSO instance.
func (b *InMemoryBackend) ListPermissionSets(instanceArn string) []*PermissionSet {
	b.mu.RLock("ListPermissionSets")
	defer b.mu.RUnlock()

	grouped := b.permissionSetsByInstance.Get(instanceArn)
	list := make([]*PermissionSet, 0, len(grouped))

	for _, ps := range grouped {
		list = append(list, b.copyPermissionSet(ps))
	}

	return list
}

// DeletePermissionSet removes a permission set and cascades to its assignments.
// Returns ConflictException if the permission set is still assigned to any accounts.
func (b *InMemoryBackend) DeletePermissionSet(instanceArn, permissionSetArn string) error {
	b.mu.Lock("DeletePermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}

	// AWS returns ConflictException if there are active account assignments.
	key := assignmentKey(instanceArn, permissionSetArn)
	if len(b.assignments[key]) > 0 {
		return ErrPermissionSetHasAssignments
	}

	b.permissionSets.Delete(permissionSetArn)
	b.permissionBoundaries.Delete(permissionSetArn)
	delete(b.customerManagedPolicies, permissionSetArn)
	delete(b.assignments, key)

	return nil
}

// UpdatePermissionSet updates a permission set's mutable fields.
func (b *InMemoryBackend) UpdatePermissionSet(
	instanceArn, permissionSetArn, description, sessionDuration, relayState string,
) error {
	b.mu.Lock("UpdatePermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	if sessionDuration != "" {
		if err := validateSessionDuration(sessionDuration); err != nil {
			return err
		}
	}
	if description != "" {
		ps.Description = description
	}
	if sessionDuration != "" {
		ps.SessionDuration = sessionDuration
	}
	if relayState != "" {
		ps.RelayState = relayState
	}

	return nil
}

// pruneOldestStatus removes the oldest entry from t when it has reached maxStatusEntries.
// Must be called with b.mu held for writing.
func pruneOldestStatus(t *store.Table[ProvisioningStatus]) {
	if t.Len() < maxStatusEntries {
		return
	}

	var oldest *ProvisioningStatus

	for _, v := range t.All() {
		if oldest == nil || v.CreatedDate.Before(oldest.CreatedDate) {
			oldest = v
		}
	}

	if oldest != nil {
		t.Delete(oldest.RequestID)
	}
}

// ProvisionPermissionSet initiates provisioning of a permission set.
// targetType is AWS_ACCOUNT or ALL_PROVISIONED_ACCOUNTS; targetID is required for AWS_ACCOUNT.
func (b *InMemoryBackend) ProvisionPermissionSet(
	instanceArn, permissionSetArn, targetType, targetID string,
) (string, error) {
	b.mu.Lock("ProvisionPermissionSet")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceArn) {
		return "", ErrInstanceNotFound
	}
	if _, ok := b.permissionSets.Get(permissionSetArn); !ok {
		return "", ErrPermissionSetNotFound
	}

	requestID := uuid.NewString()
	pruneOldestStatus(b.provisioningStatuses)
	b.provisioningStatuses.Put(&ProvisioningStatus{
		RequestID:        requestID,
		Status:           statusInProgress,
		CreatedDate:      time.Now().UTC(),
		PermissionSetArn: permissionSetArn,
		TargetType:       targetType,
		AccountID:        targetID,
	})

	return requestID, nil
}

// DescribePermissionSetProvisioningStatus returns the status of a provisioning request.
// Lazily transitions IN_PROGRESS → SUCCEEDED on first poll.
func (b *InMemoryBackend) DescribePermissionSetProvisioningStatus(
	_ string,
	provisioningRequestID string,
) (*ProvisioningStatus, error) {
	b.mu.Lock("DescribePermissionSetProvisioningStatus")
	defer b.mu.Unlock()

	status, ok := b.provisioningStatuses.Get(provisioningRequestID)
	if !ok {
		return nil, ErrRequestNotFound
	}
	if status.Status == statusInProgress {
		status.Status = statusSucceeded
	}

	cp := *status

	return &cp, nil
}

// ListPermissionSetProvisioningStatus returns permission-set provisioning statuses sorted by date descending.
// filterStatus filters by status when non-empty.
func (b *InMemoryBackend) ListPermissionSetProvisioningStatus(_, filterStatus string) []*ProvisioningStatus {
	b.mu.RLock("ListPermissionSetProvisioningStatus")
	defer b.mu.RUnlock()

	result := make([]*ProvisioningStatus, 0, b.provisioningStatuses.Len())
	for _, status := range b.provisioningStatuses.All() {
		if filterStatus != "" && status.Status != filterStatus {
			continue
		}
		cp := *status
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedDate.After(result[j].CreatedDate)
	})

	return result
}

// validatePermissionSetName checks the name regex [\w+=,.@-]+ and length limit.
func validatePermissionSetName(name string) error {
	if len(name) > maxPermissionSetNameLen {
		return fmt.Errorf("%w: permission set name must not exceed %d characters",
			awserr.ErrInvalidParameter, maxPermissionSetNameLen)
	}
	if name != "" && !permissionSetNameRe.MatchString(name) {
		return fmt.Errorf("%w: permission set name contains invalid characters (allowed: [\\w+=,.@-])",
			awserr.ErrInvalidParameter)
	}

	return nil
}

// validateSessionDuration validates ISO8601 duration in range PT1H..PT12H.
// Valid examples: PT1H, PT2H, PT12H, PT1H30M. Invalid: PT13H, PT30M, P1D.
func validateSessionDuration(dur string) error {
	if dur == "" {
		return nil
	}
	if !strings.HasPrefix(dur, "PT") {
		return fmt.Errorf("%w: SessionDuration must be an ISO8601 duration starting with PT (e.g. PT1H)",
			awserr.ErrInvalidParameter)
	}
	// Parse hours and minutes from PTxHyM / PTxH / PTyM
	rest := dur[2:]
	var hours, minutes int
	if h := strings.Index(rest, "H"); h >= 0 {
		if _, err := fmt.Sscanf(rest[:h], "%d", &hours); err != nil || hours < 0 {
			return fmt.Errorf("%w: SessionDuration has invalid hours component", awserr.ErrInvalidParameter)
		}
		rest = rest[h+1:]
	}
	if m := strings.Index(rest, "M"); m >= 0 {
		if _, err := fmt.Sscanf(rest[:m], "%d", &minutes); err != nil || minutes < 0 {
			return fmt.Errorf("%w: SessionDuration has invalid minutes component", awserr.ErrInvalidParameter)
		}
		rest = rest[m+1:]
	}
	if rest != "" {
		return fmt.Errorf("%w: SessionDuration has unsupported components", awserr.ErrInvalidParameter)
	}
	totalMinutes := hours*minutesPerHour + minutes
	if totalMinutes < minSessionMinutes || totalMinutes > maxSessionMinutes {
		return fmt.Errorf("%w: SessionDuration must be between PT1H and PT12H", awserr.ErrInvalidParameter)
	}

	return nil
}

// copyPermissionSet returns a deep copy of a PermissionSet. Must be called with mu held.
func (b *InMemoryBackend) copyPermissionSet(ps *PermissionSet) *PermissionSet {
	cp := *ps
	cp.Tags = make(map[string]string, len(ps.Tags))
	maps.Copy(cp.Tags, ps.Tags)
	cp.ManagedPolicies = make([]ManagedPolicy, len(ps.ManagedPolicies))
	copy(cp.ManagedPolicies, ps.ManagedPolicies)

	return &cp
}

// AddPermissionSetInternal adds a pre-built PermissionSet directly to the backend for test seeding.
func (b *InMemoryBackend) AddPermissionSetInternal(instanceArn, name string) *PermissionSet {
	b.mu.Lock("AddPermissionSetInternal")
	defer b.mu.Unlock()

	instanceID := instanceARNToID(instanceArn)
	id := uuid.NewString()[:uuidShortLen]
	psArn := arn.Build("sso", "", "", fmt.Sprintf("permissionSet/%s/%s", instanceID, id))
	ps := &PermissionSet{
		PermissionSetArn: psArn,
		InstanceArn:      instanceArn,
		Name:             name,
		SessionDuration:  defaultSessionDuration,
		CreatedDate:      time.Now().UTC(),
		Tags:             make(map[string]string),
	}
	b.permissionSets.Put(ps)

	return b.copyPermissionSet(ps)
}

// ListPermissionSetsProvisionedToAccount returns the permission set ARNs provisioned to a specific account.
func (b *InMemoryBackend) ListPermissionSetsProvisionedToAccount(instanceArn, accountID string) []string {
	b.mu.RLock("ListPermissionSetsProvisionedToAccount")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}
	for key, assignments := range b.assignments {
		// key format: instanceArn|permissionSetArn
		if !strings.HasPrefix(key, instanceArn+"|") {
			continue
		}
		for _, a := range assignments {
			if a.AccountID == accountID {
				psArn := strings.TrimPrefix(key, instanceArn+"|")
				seen[psArn] = struct{}{}
			}
		}
	}
	result := collections.SortedKeys(seen)

	return result
}

// ListAccountsForProvisionedPermissionSet returns account IDs where a permission set has assignments.
func (b *InMemoryBackend) ListAccountsForProvisionedPermissionSet(
	instanceArn, permissionSetArn string,
) ([]string, error) {
	b.mu.RLock("ListAccountsForProvisionedPermissionSet")
	defer b.mu.RUnlock()

	if !b.instances.Has(instanceArn) {
		return nil, ErrInstanceNotFound
	}
	if _, ok := b.permissionSets.Get(permissionSetArn); !ok {
		return nil, ErrPermissionSetNotFound
	}

	key := assignmentKey(instanceArn, permissionSetArn)
	seen := map[string]struct{}{}
	for _, a := range b.assignments[key] {
		seen[a.AccountID] = struct{}{}
	}

	result := collections.SortedKeys(seen)

	return result, nil
}

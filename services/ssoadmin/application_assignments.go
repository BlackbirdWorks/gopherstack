package ssoadmin

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// CreateApplicationAssignment assigns a principal to an application.
func (b *InMemoryBackend) CreateApplicationAssignment(applicationArn, principalID, principalType string) error {
	b.mu.Lock("CreateApplicationAssignment")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	for _, a := range b.applicationAssignments[applicationArn] {
		if a.PrincipalID == principalID && a.PrincipalType == principalType {
			return nil
		}
	}
	b.applicationAssignments[applicationArn] = append(
		b.applicationAssignments[applicationArn],
		&ApplicationAssignment{
			ApplicationArn: applicationArn,
			PrincipalID:    principalID,
			PrincipalType:  principalType,
		},
	)

	return nil
}

// DescribeApplicationAssignment returns a specific application assignment.
func (b *InMemoryBackend) DescribeApplicationAssignment(
	applicationArn,
	principalID,
	principalType string,
) (*ApplicationAssignment, error) {
	b.mu.RLock("DescribeApplicationAssignment")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return nil, ErrApplicationNotFound
	}
	for _, assignment := range b.applicationAssignments[applicationArn] {
		if assignment.PrincipalID == principalID && assignment.PrincipalType == principalType {
			cp := *assignment

			return &cp, nil
		}
	}

	return nil, ErrAssignmentNotFound
}

// ListApplicationAssignments returns assignments for an application.
func (b *InMemoryBackend) ListApplicationAssignments(applicationArn string) ([]*ApplicationAssignment, error) {
	b.mu.RLock("ListApplicationAssignments")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return nil, ErrApplicationNotFound
	}

	assignments := b.applicationAssignments[applicationArn]
	result := make([]*ApplicationAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		cp := *assignment
		result = append(result, &cp)
	}

	return result, nil
}

// DeleteApplicationAssignment removes a principal assignment from an application.
func (b *InMemoryBackend) DeleteApplicationAssignment(applicationArn, principalID, principalType string) error {
	b.mu.Lock("DeleteApplicationAssignment")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	all := b.applicationAssignments[applicationArn]
	found := false
	var remaining []*ApplicationAssignment
	for _, a := range all {
		if a.PrincipalID == principalID && a.PrincipalType == principalType {
			found = true
		} else {
			remaining = append(remaining, a)
		}
	}
	if !found {
		return ErrAssignmentNotFound
	}
	b.applicationAssignments[applicationArn] = remaining

	return nil
}

// PutApplicationAssignmentConfiguration sets assignment configuration on an application.
func (b *InMemoryBackend) PutApplicationAssignmentConfiguration(applicationArn string, assignmentRequired bool) error {
	b.mu.Lock("PutApplicationAssignmentConfiguration")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	b.applicationAssignConfig[applicationArn] = assignmentRequired

	return nil
}

// PutApplicationSessionConfiguration sets the UserBackgroundSessionApplicationStatus
// (ENABLED or DISABLED) on an application. Matches the real
// PutApplicationSessionConfigurationInput wire shape -- this is NOT a session
// duration (there is no such member on the real API; see
// GetApplicationSessionConfigurationOutput in the real SDK's
// api_op_GetApplicationSessionConfiguration.go).
func (b *InMemoryBackend) PutApplicationSessionConfiguration(applicationArn, userBackgroundSessionStatus string) error {
	b.mu.Lock("PutApplicationSessionConfiguration")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	if userBackgroundSessionStatus != "" &&
		userBackgroundSessionStatus != userBackgroundSessionStatusEnabled &&
		userBackgroundSessionStatus != userBackgroundSessionStatusDisabled {
		return fmt.Errorf("%w: UserBackgroundSessionApplicationStatus must be ENABLED or DISABLED",
			awserr.ErrInvalidParameter)
	}
	b.applicationSessions[applicationArn] = userBackgroundSessionStatus

	return nil
}

// GetApplicationAssignmentConfiguration returns the assignment configuration for an application.
func (b *InMemoryBackend) GetApplicationAssignmentConfiguration(applicationArn string) (bool, error) {
	b.mu.RLock("GetApplicationAssignmentConfiguration")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return false, ErrApplicationNotFound
	}
	required := b.applicationAssignConfig[applicationArn]

	return required, nil
}

// GetApplicationSessionConfiguration returns the UserBackgroundSessionApplicationStatus
// (ENABLED or DISABLED) for an application.
func (b *InMemoryBackend) GetApplicationSessionConfiguration(applicationArn string) (string, error) {
	b.mu.RLock("GetApplicationSessionConfiguration")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return "", ErrApplicationNotFound
	}
	status := b.applicationSessions[applicationArn]

	return status, nil
}

// ListApplicationAssignmentsForPrincipal returns all application assignments for a specific principal.
func (b *InMemoryBackend) ListApplicationAssignmentsForPrincipal(
	instanceArn, principalID, principalType string,
) []*ApplicationAssignment {
	b.mu.RLock("ListApplicationAssignmentsForPrincipal")
	defer b.mu.RUnlock()

	var result []*ApplicationAssignment
	for _, app := range b.applicationsByInstance.Get(instanceArn) {
		for _, a := range b.applicationAssignments[app.ApplicationArn] {
			if a.PrincipalID == principalID && a.PrincipalType == principalType {
				cp := *a
				result = append(result, &cp)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ApplicationArn < result[j].ApplicationArn
	})

	return result
}

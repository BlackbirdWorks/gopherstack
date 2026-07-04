package quicksight

import (
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"
)

const (
	iamAssignmentStatusEnabled  = "ENABLED"
	iamAssignmentStatusDisabled = "DISABLED"
	iamAssignmentStatusDraft    = "DRAFT"
)

func isValidIAMAssignmentStatus(status string) bool {
	switch status {
	case iamAssignmentStatusEnabled, iamAssignmentStatusDisabled, iamAssignmentStatusDraft:
		return true
	}

	return false
}

// storedIAMPolicyAssignment is the persisted representation of a QuickSight IAM
// policy assignment, scoped by account and namespace.
type storedIAMPolicyAssignment struct {
	Identities       map[string][]string `json:"identities,omitempty"`
	AssignmentID     string              `json:"assignmentId"`
	AssignmentName   string              `json:"assignmentName"`
	AssignmentStatus string              `json:"assignmentStatus"`
	PolicyArn        string              `json:"policyArn"`
	Namespace        string              `json:"namespace"`
}

func (a *storedIAMPolicyAssignment) toIAMPolicyAssignment() *IAMPolicyAssignment {
	return &IAMPolicyAssignment{
		AssignmentID:     a.AssignmentID,
		AssignmentName:   a.AssignmentName,
		AssignmentStatus: a.AssignmentStatus,
		PolicyArn:        a.PolicyArn,
		Namespace:        a.Namespace,
		Identities:       cloneIdentities(a.Identities),
	}
}

func cloneIdentities(identities map[string][]string) map[string][]string {
	if len(identities) == 0 {
		return nil
	}

	out := make(map[string][]string, len(identities))
	for k, v := range identities {
		out[k] = append([]string(nil), v...)
	}

	return out
}

func iamPolicyAssignmentKey(accountID, namespace, assignmentName string) string {
	return accountID + "/" + namespace + "/" + assignmentName
}

// ---- IAM Policy Assignments ----

func (b *InMemoryBackend) CreateIAMPolicyAssignment(
	accountID, namespace, assignmentName, assignmentStatus, policyArn string,
	identities map[string][]string,
) (*IAMPolicyAssignment, error) {
	if assignmentName == "" {
		return nil, ErrValidation
	}

	if assignmentStatus == "" {
		assignmentStatus = iamAssignmentStatusEnabled
	}
	if !isValidIAMAssignmentStatus(assignmentStatus) {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateIAMPolicyAssignment")
	defer b.mu.Unlock()

	if _, ok := b.namespaces[nsKey(accountID, namespace)]; !ok {
		return nil, ErrNamespaceNotFound
	}

	key := iamPolicyAssignmentKey(accountID, namespace, assignmentName)
	if _, exists := b.iamPolicyAssignments[key]; exists {
		return nil, ErrIAMPolicyAssignmentAlreadyExists
	}

	a := &storedIAMPolicyAssignment{
		AssignmentID:     uuid.New().String(),
		AssignmentName:   assignmentName,
		AssignmentStatus: assignmentStatus,
		PolicyArn:        policyArn,
		Namespace:        namespace,
		Identities:       maps.Clone(identities),
	}
	b.iamPolicyAssignments[key] = a

	return a.toIAMPolicyAssignment(), nil
}

func (b *InMemoryBackend) DescribeIAMPolicyAssignment(
	accountID, namespace, assignmentName string,
) (*IAMPolicyAssignment, error) {
	b.mu.RLock("DescribeIAMPolicyAssignment")
	defer b.mu.RUnlock()

	a, ok := b.iamPolicyAssignments[iamPolicyAssignmentKey(accountID, namespace, assignmentName)]
	if !ok {
		return nil, ErrIAMPolicyAssignmentNotFound
	}

	return a.toIAMPolicyAssignment(), nil
}

func (b *InMemoryBackend) UpdateIAMPolicyAssignment(
	accountID, namespace, assignmentName, assignmentStatus, policyArn string,
	identities map[string][]string,
) (*IAMPolicyAssignment, error) {
	if assignmentStatus != "" && !isValidIAMAssignmentStatus(assignmentStatus) {
		return nil, ErrValidation
	}

	b.mu.Lock("UpdateIAMPolicyAssignment")
	defer b.mu.Unlock()

	key := iamPolicyAssignmentKey(accountID, namespace, assignmentName)
	a, ok := b.iamPolicyAssignments[key]
	if !ok {
		return nil, ErrIAMPolicyAssignmentNotFound
	}

	if assignmentStatus != "" {
		a.AssignmentStatus = assignmentStatus
	}
	if policyArn != "" {
		a.PolicyArn = policyArn
	}
	if identities != nil {
		a.Identities = maps.Clone(identities)
	}

	return a.toIAMPolicyAssignment(), nil
}

func (b *InMemoryBackend) DeleteIAMPolicyAssignment(accountID, namespace, assignmentName string) error {
	b.mu.Lock("DeleteIAMPolicyAssignment")
	defer b.mu.Unlock()

	key := iamPolicyAssignmentKey(accountID, namespace, assignmentName)
	if _, ok := b.iamPolicyAssignments[key]; !ok {
		return ErrIAMPolicyAssignmentNotFound
	}
	delete(b.iamPolicyAssignments, key)

	return nil
}

func (b *InMemoryBackend) allIAMPolicyAssignmentsLocked(accountID, namespace string) []*storedIAMPolicyAssignment {
	prefix := accountID + "/" + namespace + "/"
	all := make([]*storedIAMPolicyAssignment, 0, len(b.iamPolicyAssignments))
	for k, a := range b.iamPolicyAssignments {
		if strings.HasPrefix(k, prefix) {
			all = append(all, a)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].AssignmentName < all[j].AssignmentName })

	return all
}

func (b *InMemoryBackend) ListIAMPolicyAssignments(
	accountID, namespace, statusFilter string,
	maxResults int32,
	nextToken string,
) ([]*IAMPolicyAssignment, string, error) {
	b.mu.RLock("ListIAMPolicyAssignments")
	defer b.mu.RUnlock()

	all := b.allIAMPolicyAssignmentsLocked(accountID, namespace)

	var filtered []*storedIAMPolicyAssignment
	for _, a := range all {
		if statusFilter == "" || a.AssignmentStatus == statusFilter {
			filtered = append(filtered, a)
		}
	}

	result, next := paginateIAMPolicyAssignments(filtered, maxResults, nextToken)

	return result, next, nil
}

func paginateIAMPolicyAssignments(
	all []*storedIAMPolicyAssignment,
	maxResults int32,
	nextToken string,
) ([]*IAMPolicyAssignment, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, a := range all {
			if a.AssignmentName == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].AssignmentName
	} else {
		end = len(all)
	}

	result := make([]*IAMPolicyAssignment, 0, end-start)
	for _, a := range all[start:end] {
		result = append(result, a.toIAMPolicyAssignment())
	}

	return result, next
}

// ListIAMPolicyAssignmentsForUser returns the ENABLED assignments in namespace
// whose Identities reference userName (directly, by user name).
func (b *InMemoryBackend) ListIAMPolicyAssignmentsForUser(
	accountID, namespace, userName string,
	maxResults int32,
	nextToken string,
) ([]*IAMPolicyAssignment, string, error) {
	b.mu.RLock("ListIAMPolicyAssignmentsForUser")
	defer b.mu.RUnlock()

	all := b.allIAMPolicyAssignmentsLocked(accountID, namespace)

	var filtered []*storedIAMPolicyAssignment
	for _, a := range all {
		if a.AssignmentStatus != iamAssignmentStatusEnabled {
			continue
		}
		if assignmentHasIdentity(a, userName) {
			filtered = append(filtered, a)
		}
	}

	result, next := paginateIAMPolicyAssignments(filtered, maxResults, nextToken)

	return result, next, nil
}

func assignmentHasIdentity(a *storedIAMPolicyAssignment, name string) bool {
	for _, members := range a.Identities {
		if slices.Contains(members, name) {
			return true
		}
	}

	return false
}

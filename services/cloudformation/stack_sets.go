package cloudformation

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/google/uuid"
)

const (
	statusComplete               = "COMPLETE"
	statusEnabled                = "ENABLED"
	resourceScanCompletePercent  = 100
	typeKindResource             = "RESOURCE"
	typeVisibilityPublic         = "PUBLIC"
	typeVisibilityPrivate        = "PRIVATE"
	typeStatusDeprecated         = "DEPRECATED"
	provisioningTypeFullyMutable = "FULLY_MUTABLE"
	driftStatusDrifted           = "DRIFTED"
	driftStatusModified          = "MODIFIED"
	driftStatusDeleted           = "DELETED"
)

func (b *InMemoryBackend) CreateStackSet(
	name, description, templateBody string,
) (*StackSet, error) {
	b.mu.Lock("CreateStackSet")
	defer b.mu.Unlock()
	if b.stackSets.Has(name) {
		return nil, ErrStackSetAlreadyExists
	}
	ss := &StackSet{
		StackSetID:   uuid.New().String(),
		StackSetName: name,
		Description:  description,
		TemplateBody: templateBody,
		Status:       "ACTIVE",
	}
	b.stackSets.Put(ss)

	return ss, nil
}

func (b *InMemoryBackend) UpdateStackSet(
	name, description, templateBody string,
) (*StackSet, error) {
	b.mu.Lock("UpdateStackSet")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(name)
	if !ok {
		return nil, ErrStackSetNotFound
	}
	if description != "" {
		ss.Description = description
	}
	if templateBody != "" {
		ss.TemplateBody = templateBody
	}
	b.recordStackSetOperation(name, "UPDATE")

	return ss, nil
}

func (b *InMemoryBackend) DeleteStackSet(name string) error {
	b.mu.Lock("DeleteStackSet")
	defer b.mu.Unlock()
	if !b.stackSets.Has(name) {
		// DeleteStackSet's modeled error set (OperationInProgressException,
		// StackSetNotEmptyException) has no "not found" case — like DeleteStack,
		// deleting a StackSet that doesn't exist (or was already deleted) is a
		// silent no-op in real AWS, not an error.
		return nil
	}
	if len(b.stackInstances[name]) > 0 {
		return ErrStackSetNotEmpty
	}
	b.stackSets.Delete(name)
	delete(b.stackInstances, name)

	return nil
}

func (b *InMemoryBackend) DescribeStackSet(name string) (*StackSet, error) {
	b.mu.RLock("DescribeStackSet")
	defer b.mu.RUnlock()
	ss, ok := b.stackSets.Get(name)
	if !ok {
		return nil, ErrStackSetNotFound
	}

	return ss, nil
}

func (b *InMemoryBackend) ListStackSets(nextToken string) (page.Page[StackSetSummary], error) {
	b.mu.RLock("ListStackSets")
	defer b.mu.RUnlock()
	result := make([]StackSetSummary, 0, b.stackSets.Len())
	for _, ss := range b.stackSets.All() {
		result = append(result, StackSetSummary{
			StackSetID:   ss.StackSetID,
			StackSetName: ss.StackSetName,
			Status:       ss.Status,
			Description:  ss.Description,
		})
	}
	sort.Slice(
		result,
		func(i, j int) bool { return result[i].StackSetName < result[j].StackSetName },
	)

	return page.New(result, nextToken, 0, cfnDefaultPageSize), nil
}

func (b *InMemoryBackend) DetectStackSetDrift(stackSetName string) (string, error) {
	b.mu.Lock("DetectStackSetDrift")
	defer b.mu.Unlock()
	if !b.stackSets.Has(stackSetName) {
		return "", ErrStackSetNotFound
	}
	opID := b.recordStackSetOperation(stackSetName, "DETECT_DRIFT")

	return opID, nil
}

// recordStackSetOperation creates a StackSetOperation record and returns its ID.
// Caller must hold b.mu.Lock.
func (b *InMemoryBackend) recordStackSetOperation(stackSetName, action string) string {
	opID := uuid.New().String()
	if b.stackSetOperations[stackSetName] == nil {
		b.stackSetOperations[stackSetName] = make(map[string]*StackSetOperation)
	}
	b.stackSetOperations[stackSetName][opID] = &StackSetOperation{
		OperationID:  opID,
		StackSetName: stackSetName,
		Action:       action,
		Status:       "SUCCEEDED",
		CreatedAt:    time.Now(),
	}
	if b.stackSetOpResults[stackSetName] == nil {
		b.stackSetOpResults[stackSetName] = make(map[string][]StackSetOperationResult)
	}
	b.trimStackSetOperations(stackSetName)

	return opID
}

// recordOpResults records per-account/region operation results. Caller must hold b.mu.Lock.
func (b *InMemoryBackend) recordOpResults(
	stackSetName, opID string,
	accounts, regions []string,
	status string,
) {
	if b.stackSetOpResults[stackSetName] == nil {
		b.stackSetOpResults[stackSetName] = make(map[string][]StackSetOperationResult)
	}
	for _, acct := range accounts {
		for _, region := range regions {
			b.stackSetOpResults[stackSetName][opID] = append(
				b.stackSetOpResults[stackSetName][opID],
				StackSetOperationResult{
					Account: acct,
					Region:  region,
					Status:  status,
				},
			)
		}
	}
}

const maxOpsPerStackSet = 1000

func (b *InMemoryBackend) ListStackSetOperations(
	stackSetName, nextToken string,
) (page.Page[StackSetOperationSummary], error) {
	b.mu.RLock("ListStackSetOperations")
	defer b.mu.RUnlock()
	ops := b.stackSetOperations[stackSetName]
	sorted := make([]*StackSetOperation, 0, len(ops))
	for _, op := range ops {
		sorted = append(sorted, op)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})
	summaries := make([]StackSetOperationSummary, 0, len(sorted))
	for _, op := range sorted {
		summaries = append(summaries, StackSetOperationSummary{
			OperationID:  op.OperationID,
			Action:       op.Action,
			Status:       op.Status,
			CreationTime: op.CreatedAt,
		})
	}

	return page.New(summaries, nextToken, 0, cfnDefaultPageSize), nil
}

// trimStackSetOperations evicts the oldest entries when a stack set exceeds maxOpsPerStackSet.
// Caller must hold b.mu.Lock.
func (b *InMemoryBackend) trimStackSetOperations(stackSetName string) {
	ops := b.stackSetOperations[stackSetName]
	if len(ops) <= maxOpsPerStackSet {
		return
	}
	sorted := make([]*StackSetOperation, 0, len(ops))
	for _, op := range ops {
		sorted = append(sorted, op)
	}
	sort.Slice(
		sorted,
		func(i, j int) bool { return sorted[i].CreatedAt.Before(sorted[j].CreatedAt) },
	)
	evict := len(sorted) - maxOpsPerStackSet
	for _, op := range sorted[:evict] {
		delete(ops, op.OperationID)
		delete(b.stackSetOpResults[stackSetName], op.OperationID)
	}
}

func (b *InMemoryBackend) DescribeStackSetOperation(
	stackSetName, operationID string,
) (*StackSetOperation, error) {
	b.mu.RLock("DescribeStackSetOperation")
	defer b.mu.RUnlock()
	ops := b.stackSetOperations[stackSetName]
	if ops == nil {
		return nil, fmt.Errorf("%w: %s in %s", ErrOperationNotFound, operationID, stackSetName)
	}
	op, ok := ops[operationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s in %s", ErrOperationNotFound, operationID, stackSetName)
	}

	return op, nil
}

func (b *InMemoryBackend) StopStackSetOperation(stackSetName, operationID string) error {
	b.mu.Lock("StopStackSetOperation")
	defer b.mu.Unlock()
	ops := b.stackSetOperations[stackSetName]
	if ops == nil {
		return fmt.Errorf("%w: %s in %s", ErrOperationNotFound, operationID, stackSetName)
	}
	op, ok := ops[operationID]
	if !ok {
		return fmt.Errorf("%w: %s in %s", ErrOperationNotFound, operationID, stackSetName)
	}
	if op.Status != "RUNNING" {
		return fmt.Errorf("%w: %s (current: %s)", ErrOperationNotRunning, operationID, op.Status)
	}
	op.Status = "STOPPED"

	return nil
}

func (b *InMemoryBackend) ListStackSetOperationResults(
	stackSetName, operationID, _ string,
) ([]StackSetOperationResult, error) {
	b.mu.RLock("ListStackSetOperationResults")
	defer b.mu.RUnlock()
	opResults, ok := b.stackSetOpResults[stackSetName]
	if !ok {
		return []StackSetOperationResult{}, nil
	}
	results, ok := opResults[operationID]
	if !ok {
		return []StackSetOperationResult{}, nil
	}
	out := make([]StackSetOperationResult, len(results))
	copy(out, results)

	return out, nil
}

func (b *InMemoryBackend) ListStackSetAutoDeploymentTargets(
	stackSetName string,
) ([]AutoDeploymentTarget, error) {
	b.mu.RLock("ListStackSetAutoDeploymentTargets")
	defer b.mu.RUnlock()
	if !b.stackSets.Has(stackSetName) {
		return nil, ErrStackSetNotFound
	}
	// SERVICE_MANAGED stack sets target OUs; for SELF_MANAGED emulation we have no OU hierarchy,
	// so synthesise one target per unique account using the account ID as the OU ID.
	seen := make(map[string]bool)
	targets := make([]AutoDeploymentTarget, 0)
	for _, inst := range b.stackInstances[stackSetName] {
		if !seen[inst.Account] {
			seen[inst.Account] = true
			targets = append(targets, AutoDeploymentTarget{
				OrganizationalUnitID: inst.Account,
				Regions:              []string{inst.Region},
			})
		} else {
			for i, t := range targets {
				if t.OrganizationalUnitID == inst.Account {
					targets[i].Regions = append(targets[i].Regions, inst.Region)

					break
				}
			}
		}
	}

	return targets, nil
}

func (b *InMemoryBackend) ImportStacksToStackSet(stackSetName string, stackIDs []string) error {
	b.mu.Lock("ImportStacksToStackSet")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return ErrStackSetNotFound
	}
	opID := b.recordStackSetOperation(stackSetName, "IMPORT")
	for _, stackID := range stackIDs {
		// Skip duplicates.
		already := false
		for _, inst := range b.stackInstances[stackSetName] {
			if inst.StackID == stackID {
				already = true

				break
			}
		}
		if already {
			continue
		}
		account, region := parseStackARN(stackID)
		b.stackInstances[stackSetName] = append(b.stackInstances[stackSetName], StackInstance{
			StackSetID:      ss.StackSetID,
			StackSetName:    stackSetName,
			StackID:         stackID,
			Account:         account,
			Region:          region,
			Status:          "CURRENT",
			DriftStatus:     "NOT_CHECKED",
			LastOperationID: opID,
		})
	}

	return nil
}

func (b *InMemoryBackend) ActivateOrganizationsAccess() error {
	b.mu.Lock("ActivateOrganizationsAccess")
	defer b.mu.Unlock()
	b.orgAccessEnabled = true

	return nil
}

func (b *InMemoryBackend) DeactivateOrganizationsAccess() error {
	b.mu.Lock("DeactivateOrganizationsAccess")
	defer b.mu.Unlock()
	b.orgAccessEnabled = false

	return nil
}

func (b *InMemoryBackend) DescribeOrganizationsAccess() (string, error) {
	b.mu.RLock("DescribeOrganizationsAccess")
	defer b.mu.RUnlock()
	if b.orgAccessEnabled {
		return statusEnabled, nil
	}

	return "DISABLED", nil
}

package cloudformation

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

const (
	statusComplete              = "COMPLETE"
	statusEnabled               = "ENABLED"
	resourceScanCompletePercent = 100
)

// ---- Stack Sets ----

func (b *InMemoryBackend) CreateStackSet(name, description, templateBody string) (*StackSet, error) {
	b.mu.Lock("CreateStackSet")
	defer b.mu.Unlock()
	if _, ok := b.stackSets[name]; ok {
		return nil, ErrStackSetAlreadyExists
	}
	ss := &StackSet{
		StackSetID:   uuid.New().String(),
		StackSetName: name,
		Description:  description,
		TemplateBody: templateBody,
		Status:       "ACTIVE",
	}
	b.stackSets[name] = ss

	return ss, nil
}

func (b *InMemoryBackend) UpdateStackSet(name, templateBody string) (*StackSet, error) {
	b.mu.Lock("UpdateStackSet")
	defer b.mu.Unlock()
	ss, ok := b.stackSets[name]
	if !ok {
		return nil, ErrStackSetNotFound
	}
	if templateBody != "" {
		ss.TemplateBody = templateBody
	}

	return ss, nil
}

func (b *InMemoryBackend) DeleteStackSet(name string) error {
	b.mu.Lock("DeleteStackSet")
	defer b.mu.Unlock()
	if _, ok := b.stackSets[name]; !ok {
		return ErrStackSetNotFound
	}
	delete(b.stackSets, name)
	delete(b.stackInstances, name)

	return nil
}

func (b *InMemoryBackend) DescribeStackSet(name string) (*StackSet, error) {
	b.mu.RLock("DescribeStackSet")
	defer b.mu.RUnlock()
	ss, ok := b.stackSets[name]
	if !ok {
		return nil, ErrStackSetNotFound
	}

	return ss, nil
}

func (b *InMemoryBackend) ListStackSets(_ string) ([]StackSetSummary, error) {
	b.mu.RLock("ListStackSets")
	defer b.mu.RUnlock()
	result := make([]StackSetSummary, 0, len(b.stackSets))
	for _, ss := range b.stackSets {
		result = append(result, StackSetSummary{
			StackSetID:   ss.StackSetID,
			StackSetName: ss.StackSetName,
			Status:       ss.Status,
			Description:  ss.Description,
		})
	}

	return result, nil
}

func (b *InMemoryBackend) CreateStackInstances(stackSetName string, accounts, regions []string) error {
	b.mu.Lock("CreateStackInstances")
	defer b.mu.Unlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
		return ErrStackSetNotFound
	}
	for _, acct := range accounts {
		for _, region := range regions {
			b.stackInstances[stackSetName] = append(b.stackInstances[stackSetName], StackInstance{
				StackSetName: stackSetName,
				Account:      acct,
				Region:       region,
				Status:       "CURRENT",
			})
		}
	}

	return nil
}

func (b *InMemoryBackend) DeleteStackInstances(stackSetName string, accounts, regions []string) error {
	b.mu.Lock("DeleteStackInstances")
	defer b.mu.Unlock()
	instances := b.stackInstances[stackSetName]
	filtered := instances[:0]
	for _, inst := range instances {
		keep := true
		for _, acct := range accounts {
			for _, region := range regions {
				if inst.Account == acct && inst.Region == region {
					keep = false
				}
			}
		}
		if keep {
			filtered = append(filtered, inst)
		}
	}
	b.stackInstances[stackSetName] = filtered

	return nil
}

func (b *InMemoryBackend) UpdateStackInstances(stackSetName string, _, _ []string) error {
	b.mu.RLock("UpdateStackInstances")
	defer b.mu.RUnlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
		return ErrStackSetNotFound
	}

	return nil
}

func (b *InMemoryBackend) ListStackInstances(stackSetName, _ string) ([]StackInstance, error) {
	b.mu.RLock("ListStackInstances")
	defer b.mu.RUnlock()

	return append([]StackInstance(nil), b.stackInstances[stackSetName]...), nil
}

func (b *InMemoryBackend) DescribeStackInstance(stackSetName, account, region string) (*StackInstance, error) {
	b.mu.RLock("DescribeStackInstance")
	defer b.mu.RUnlock()
	for _, inst := range b.stackInstances[stackSetName] {
		if inst.Account == account && inst.Region == region {
			i := inst

			return &i, nil
		}
	}

	return nil, ErrStackInstanceNotFound
}

func (b *InMemoryBackend) DetectStackSetDrift(stackSetName string) (string, error) {
	b.mu.RLock("DetectStackSetDrift")
	defer b.mu.RUnlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
		return "", ErrStackSetNotFound
	}

	return uuid.New().String(), nil
}

func (b *InMemoryBackend) ListStackSetOperations(_, _ string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) DescribeStackSetOperation(_, _ string) (string, error) {
	return "SUCCEEDED", nil
}

func (b *InMemoryBackend) StopStackSetOperation(_, _ string) error {
	return nil
}

func (b *InMemoryBackend) ListStackSetOperationResults(_, _, _ string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) ListStackSetAutoDeploymentTargets(_ string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) ImportStacksToStackSet(stackSetName string, _ []string) error {
	b.mu.RLock("ImportStacksToStackSet")
	defer b.mu.RUnlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
		return ErrStackSetNotFound
	}

	return nil
}

func (b *InMemoryBackend) ListStackInstanceResourceDrifts(_, _, _, _ string) ([]string, error) {
	return []string{}, nil
}

// ---- Generated Templates ----

func (b *InMemoryBackend) CreateGeneratedTemplate(name string, _ []string) (*GeneratedTemplate, error) {
	b.mu.Lock("CreateGeneratedTemplate")
	defer b.mu.Unlock()
	gt := &GeneratedTemplate{
		GeneratedTemplateID:   uuid.New().String(),
		GeneratedTemplateName: name,
		Status:                statusComplete,
		TemplateBody:          `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`,
	}
	b.generatedTemplates[gt.GeneratedTemplateID] = gt

	return gt, nil
}

func (b *InMemoryBackend) UpdateGeneratedTemplate(id, name string) error {
	b.mu.Lock("UpdateGeneratedTemplate")
	defer b.mu.Unlock()
	gt, ok := b.generatedTemplates[id]
	if !ok {
		return ErrGeneratedTemplateNotFound
	}
	if name != "" {
		gt.GeneratedTemplateName = name
	}

	return nil
}

func (b *InMemoryBackend) DeleteGeneratedTemplate(id string) error {
	b.mu.Lock("DeleteGeneratedTemplate")
	defer b.mu.Unlock()
	delete(b.generatedTemplates, id)

	return nil
}

func (b *InMemoryBackend) DescribeGeneratedTemplate(id string) (*GeneratedTemplate, error) {
	b.mu.RLock("DescribeGeneratedTemplate")
	defer b.mu.RUnlock()
	gt, ok := b.generatedTemplates[id]
	if !ok {
		return nil, ErrGeneratedTemplateNotFound
	}

	return gt, nil
}

func (b *InMemoryBackend) GetGeneratedTemplate(id string) (string, error) {
	b.mu.RLock("GetGeneratedTemplate")
	defer b.mu.RUnlock()
	gt, ok := b.generatedTemplates[id]
	if !ok {
		return "", ErrGeneratedTemplateNotFound
	}

	return gt.TemplateBody, nil
}

func (b *InMemoryBackend) ListGeneratedTemplates(_ string) ([]GeneratedTemplate, error) {
	b.mu.RLock("ListGeneratedTemplates")
	defer b.mu.RUnlock()
	result := make([]GeneratedTemplate, 0, len(b.generatedTemplates))
	for _, gt := range b.generatedTemplates {
		result = append(result, *gt)
	}

	return result, nil
}

// ---- Resource Scans ----

func (b *InMemoryBackend) StartResourceScan() (string, error) {
	b.mu.Lock("StartResourceScan")
	defer b.mu.Unlock()
	scanID := uuid.New().String()
	b.resourceScans[scanID] = &ResourceScan{
		ResourceScanID:      scanID,
		Status:              statusComplete,
		PercentageCompleted: resourceScanCompletePercent,
	}

	return scanID, nil
}

func (b *InMemoryBackend) DescribeResourceScan(scanID string) (*ResourceScan, error) {
	b.mu.RLock("DescribeResourceScan")
	defer b.mu.RUnlock()
	rs, ok := b.resourceScans[scanID]
	if !ok {
		return nil, ErrResourceScanNotFound
	}

	return rs, nil
}

func (b *InMemoryBackend) ListResourceScans(_ string) ([]ResourceScan, error) {
	b.mu.RLock("ListResourceScans")
	defer b.mu.RUnlock()
	result := make([]ResourceScan, 0, len(b.resourceScans))
	for _, rs := range b.resourceScans {
		result = append(result, *rs)
	}

	return result, nil
}

func (b *InMemoryBackend) ListResourceScanResources(_, _ string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) ListResourceScanRelatedResources(_ string, _ []string) ([]string, error) {
	return []string{}, nil
}

// ---- Type Management ----

func (b *InMemoryBackend) ActivateType(_, _ string) error   { return nil }
func (b *InMemoryBackend) DeactivateType(_, _ string) error { return nil }

func (b *InMemoryBackend) RegisterType(_, _ string) (string, error) {
	return uuid.New().String(), nil
}

func (b *InMemoryBackend) DeregisterType(_ string) error { return nil }
func (b *InMemoryBackend) PublishType(_ string) error    { return nil }

func (b *InMemoryBackend) SetTypeDefaultVersion(_, _ string) error { return nil }
func (b *InMemoryBackend) SetTypeConfiguration(_, _ string) error  { return nil }
func (b *InMemoryBackend) BatchDescribeTypeConfigurations(_ []string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) ListTypes(_ string) ([]TypeSummary, error) {
	return []TypeSummary{}, nil
}

func (b *InMemoryBackend) ListTypeVersions(_, _ string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) ListTypeRegistrations(_, _ string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) DescribeTypeRegistration(_ string) (string, error) {
	return statusComplete, nil
}

func (b *InMemoryBackend) TestType(_, _ string) (string, error) {
	return uuid.New().String(), nil
}

func (b *InMemoryBackend) RegisterPublisher(_ string) (string, error) {
	return uuid.New().String(), nil
}

func (b *InMemoryBackend) DescribePublisher(_ string) (string, error) {
	return "VERIFIED", nil
}

// ---- Stack Refactor ----

func (b *InMemoryBackend) CreateStackRefactor(_ string, _ []string) (string, error) {
	return uuid.New().String(), nil
}

func (b *InMemoryBackend) DescribeStackRefactor(_ string) (string, error) {
	return "CREATE_COMPLETE", nil
}

func (b *InMemoryBackend) ExecuteStackRefactor(_ string) error {
	return nil
}

func (b *InMemoryBackend) ListStackRefactors(_ string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) ListStackRefactorActions(_ string) ([]string, error) {
	return []string{}, nil
}

// ---- Org Access ----

func (b *InMemoryBackend) ActivateOrganizationsAccess() error   { return nil }
func (b *InMemoryBackend) DeactivateOrganizationsAccess() error { return nil }
func (b *InMemoryBackend) DescribeOrganizationsAccess() (string, error) {
	return statusEnabled, nil
}

// ---- Misc ----

func (b *InMemoryBackend) SignalResource(_, _, _, _ string) error { return nil }

func (b *InMemoryBackend) RollbackStack(_ context.Context, nameOrID string) error {
	b.mu.Lock("RollbackStack")
	defer b.mu.Unlock()
	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrStackNotFound, nameOrID)
	}
	stack.StackStatus = statusRollbackComplete

	return nil
}

func (b *InMemoryBackend) RecordHandlerProgress(_, _ string) error { return nil }

func (b *InMemoryBackend) GetHookResult(_ string) (string, error) {
	return "IN_PROGRESS", nil
}

func (b *InMemoryBackend) ListHookResults(_, _ string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) DescribeChangeSetHooks(_, _ string) ([]string, error) {
	return []string{}, nil
}

func (b *InMemoryBackend) DescribeEvents(_ string) ([]StackEvent, error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()
	all := make([]StackEvent, 0, len(b.events))
	for _, evts := range b.events {
		all = append(all, evts...)
	}

	return all, nil
}

func (b *InMemoryBackend) UpdateTerminationProtection(nameOrID string, _ bool) error {
	b.mu.RLock("UpdateTerminationProtection")
	defer b.mu.RUnlock()
	_, ok := b.resolveStack(nameOrID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrStackNotFound, nameOrID)
	}

	return nil
}

func (b *InMemoryBackend) ValidateTemplate(templateBody string) (*TemplateSummary, error) {
	return b.GetTemplateSummary(templateBody, "")
}

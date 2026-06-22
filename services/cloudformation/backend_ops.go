package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	statusComplete              = "COMPLETE"
	statusEnabled               = "ENABLED"
	resourceScanCompletePercent = 100
	typeKindResource            = "RESOURCE"
	typeVisibilityPublic        = "PUBLIC"
	typeVisibilityPrivate       = "PRIVATE"
	typeStatusDeprecated        = "DEPRECATED"
	driftStatusDrifted          = "DRIFTED"
	driftStatusModified         = "MODIFIED"
	driftStatusDeleted          = "DELETED"
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

func (b *InMemoryBackend) UpdateStackSet(name, description, templateBody string) (*StackSet, error) {
	b.mu.Lock("UpdateStackSet")
	defer b.mu.Unlock()
	ss, ok := b.stackSets[name]
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
	if _, ok := b.stackSets[name]; !ok {
		return ErrStackSetNotFound
	}
	if len(b.stackInstances[name]) > 0 {
		return ErrStackSetNotEmpty
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

func (b *InMemoryBackend) CreateStackInstances(stackSetName string, accounts, regions []string) (string, error) {
	b.mu.Lock("CreateStackInstances")
	defer b.mu.Unlock()
	ss, ok := b.stackSets[stackSetName]
	if !ok {
		return "", ErrStackSetNotFound
	}
	opID := b.recordStackSetOperation(stackSetName, "CREATE_INSTANCES")
	for _, acct := range accounts {
		for _, region := range regions {
			// Deduplicate: skip if instance already exists.
			alreadyExists := false
			for _, existing := range b.stackInstances[stackSetName] {
				if existing.Account == acct && existing.Region == region {
					alreadyExists = true

					break
				}
			}
			if alreadyExists {
				continue
			}
			stackResource := fmt.Sprintf("stack/%s/%s", stackSetName, uuid.New().String())
			instanceStackID := arn.Build("cloudformation", region, acct, stackResource)
			b.stackInstances[stackSetName] = append(b.stackInstances[stackSetName], StackInstance{
				StackSetID:      ss.StackSetID,
				StackSetName:    stackSetName,
				StackID:         instanceStackID,
				Account:         acct,
				Region:          region,
				Status:          "CURRENT",
				DriftStatus:     "NOT_CHECKED",
				LastOperationID: opID,
			})
		}
	}
	b.recordOpResults(stackSetName, opID, accounts, regions, "SUCCEEDED")

	return opID, nil
}

func (b *InMemoryBackend) DeleteStackInstances(stackSetName string, accounts, regions []string) (string, error) {
	b.mu.Lock("DeleteStackInstances")
	defer b.mu.Unlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
		return "", ErrStackSetNotFound
	}
	instances := b.stackInstances[stackSetName]
	filtered := make([]StackInstance, 0, len(instances))
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
	opID := b.recordStackSetOperation(stackSetName, "DELETE_INSTANCES")
	b.recordOpResults(stackSetName, opID, accounts, regions, "SUCCEEDED")

	return opID, nil
}

func (b *InMemoryBackend) UpdateStackInstances(stackSetName string, accounts, regions []string) (string, error) {
	b.mu.Lock("UpdateStackInstances")
	defer b.mu.Unlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
		return "", ErrStackSetNotFound
	}
	opID := b.recordStackSetOperation(stackSetName, "UPDATE_INSTANCES")
	if len(accounts) > 0 && len(regions) > 0 {
		b.recordOpResults(stackSetName, opID, accounts, regions, "SUCCEEDED")
	}

	return opID, nil
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
	b.mu.Lock("DetectStackSetDrift")
	defer b.mu.Unlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
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

	return opID
}

// recordOpResults records per-account/region operation results. Caller must hold b.mu.Lock.
func (b *InMemoryBackend) recordOpResults(stackSetName, opID string, accounts, regions []string, status string) {
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

func (b *InMemoryBackend) ListStackSetOperations(stackSetName, _ string) ([]string, error) {
	b.mu.RLock("ListStackSetOperations")
	defer b.mu.RUnlock()
	ops := b.stackSetOperations[stackSetName]
	result := make([]*StackSetOperation, 0, len(ops))
	for _, op := range ops {
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	ids := make([]string, 0, len(result))
	for _, op := range result {
		ids = append(ids, op.OperationID)
	}

	return ids, nil
}

func (b *InMemoryBackend) DescribeStackSetOperation(stackSetName, operationID string) (*StackSetOperation, error) {
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

func (b *InMemoryBackend) ListStackSetAutoDeploymentTargets(stackSetName string) ([]string, error) {
	b.mu.RLock("ListStackSetAutoDeploymentTargets")
	defer b.mu.RUnlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
		return nil, ErrStackSetNotFound
	}
	seen := make(map[string]bool)
	accounts := make([]string, 0)
	for _, inst := range b.stackInstances[stackSetName] {
		if !seen[inst.Account] {
			seen[inst.Account] = true
			accounts = append(accounts, inst.Account)
		}
	}

	return accounts, nil
}

func (b *InMemoryBackend) ImportStacksToStackSet(stackSetName string, _ []string) error {
	b.mu.Lock("ImportStacksToStackSet")
	defer b.mu.Unlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
		return ErrStackSetNotFound
	}
	b.recordStackSetOperation(stackSetName, "IMPORT")

	return nil
}

func (b *InMemoryBackend) ListStackInstanceResourceDrifts(stackSetName, _, _, _ string) ([]string, error) {
	b.mu.RLock("ListStackInstanceResourceDrifts")
	defer b.mu.RUnlock()
	if _, ok := b.stackSets[stackSetName]; !ok {
		return nil, ErrStackSetNotFound
	}

	return []string{}, nil
}

// ---- Generated Templates ----

func (b *InMemoryBackend) CreateGeneratedTemplate(name string, resourceIDs []string) (*GeneratedTemplate, error) {
	b.mu.Lock("CreateGeneratedTemplate")
	defer b.mu.Unlock()
	// Build a template body from the given resource IDs. Each resource ID is
	// treated as "Type/LogicalID"; if it doesn't have that form we synthesize a
	// generic resource.
	templateBody := b.buildGeneratedTemplateBody(resourceIDs)
	gt := &GeneratedTemplate{
		GeneratedTemplateID:   uuid.New().String(),
		GeneratedTemplateName: name,
		Status:                statusComplete,
		TemplateBody:          templateBody,
	}
	b.generatedTemplates[gt.GeneratedTemplateID] = gt

	return gt, nil
}

type cfnResource struct {
	Properties map[string]any `json:"Properties"`
	Type       string         `json:"Type"`
}

type cfnTemplate struct {
	Resources                map[string]cfnResource `json:"Resources"`
	AWSTemplateFormatVersion string                 `json:"AWSTemplateFormatVersion"`
	Description              string                 `json:"Description"`
}

// parseResourceIDs converts a slice of "Type/LogicalID" strings into a cfnResource map.
func parseResourceIDs(resourceIDs []string) map[string]cfnResource {
	resources := make(map[string]cfnResource)
	for i, rid := range resourceIDs {
		resType := cfnTypeWaitConditionHandle
		logicalID := fmt.Sprintf("GeneratedResource%d", i+1)
		if idx := strings.Index(rid, "/"); idx > 0 {
			resType = rid[:idx]
			rawLogical := rid[idx+1:]
			sanitised := strings.Map(func(r rune) rune {
				if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
					return r
				}

				return -1
			}, rawLogical)
			if sanitised != "" {
				logicalID = sanitised
			}
		}
		resources[logicalID] = cfnResource{
			Type:       resType,
			Properties: map[string]any{},
		}
	}

	return resources
}

// marshalGeneratedTemplate marshals a cfnResource map into a CloudFormation template JSON string.
func marshalGeneratedTemplate(resources map[string]cfnResource) string {
	body, err := json.Marshal(cfnTemplate{
		AWSTemplateFormatVersion: "2010-09-09",
		Description:              "Generated template",
		Resources:                resources,
	})
	if err != nil {
		return `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`
	}

	return string(body)
}

// buildGeneratedTemplateBody synthesises a minimal CloudFormation template
// from a list of resource identifiers (Type/LogicalID pairs).
// Caller must hold the lock.
func (b *InMemoryBackend) buildGeneratedTemplateBody(resourceIDs []string) string {
	resources := parseResourceIDs(resourceIDs)
	if len(resources) == 0 {
		// Build from existing stack resources as a convenience.
		for _, stack := range b.stacks {
			if stack.StackStatus == statusDeleteComplete {
				continue
			}
			for _, res := range b.resources[stack.StackID] {
				resources[res.LogicalID] = cfnResource{
					Type:       res.Type,
					Properties: map[string]any{},
				}
			}
		}
	}
	if len(resources) == 0 {
		return `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`
	}

	return marshalGeneratedTemplate(resources)
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
	// Populate scan items from existing active stacks.
	items := make([]ScannedResource, 0)
	for _, stack := range b.stacks {
		if stack.StackStatus == statusDeleteComplete {
			continue
		}
		for _, res := range b.resources[stack.StackID] {
			items = append(items, ScannedResource{
				ResourceType:       res.Type,
				ResourceIdentifier: res.PhysicalID,
				ManagedByStack:     true,
				StackID:            stack.StackID,
			})
		}
	}
	// If no managed resources were found, include a synthetic placeholder so that
	// callers always see at least one resource (matches legacy mock behaviour).
	if len(items) == 0 {
		items = []ScannedResource{
			{
				ResourceType:       "AWS::S3::Bucket",
				ResourceIdentifier: "example-bucket",
				ManagedByStack:     false,
			},
		}
	}
	b.resourceScanItems[scanID] = items

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

func (b *InMemoryBackend) ListResourceScanResources(scanID, _ string) ([]ScannedResource, error) {
	b.mu.RLock("ListResourceScanResources")
	defer b.mu.RUnlock()
	if _, ok := b.resourceScans[scanID]; !ok {
		return nil, ErrResourceScanNotFound
	}
	items := b.resourceScanItems[scanID]
	out := make([]ScannedResource, len(items))
	copy(out, items)

	return out, nil
}

func (b *InMemoryBackend) ListResourceScanRelatedResources(scanID string, _ []string) ([]string, error) {
	b.mu.RLock("ListResourceScanRelatedResources")
	defer b.mu.RUnlock()
	if _, ok := b.resourceScans[scanID]; !ok {
		return nil, ErrResourceScanNotFound
	}

	return []string{}, nil
}

// ---- Type Management ----

func (b *InMemoryBackend) ActivateType(typeName, typeArn string) error {
	b.mu.Lock("ActivateType")
	defer b.mu.Unlock()
	key := typeArn
	if key == "" {
		key = "arn:aws:cloudformation:::type/resource/" + typeName
	}
	if t, ok := b.typeRegistry[key]; ok {
		t.IsActivated = true
	} else {
		b.typeRegistry[key] = &RegisteredType{
			TypeArn:     key,
			TypeName:    typeName,
			Type:        typeKindResource,
			VersionID:   "00000001",
			Status:      statusComplete,
			IsActivated: true,
		}
	}

	return nil
}

func (b *InMemoryBackend) DeactivateType(typeName, typeArn string) error {
	b.mu.Lock("DeactivateType")
	defer b.mu.Unlock()
	key := typeArn
	if key == "" {
		key = "arn:aws:cloudformation:::type/resource/" + typeName
	}
	if t, ok := b.typeRegistry[key]; ok {
		t.IsActivated = false
	}

	return nil
}

func (b *InMemoryBackend) RegisterType(typeName, _ string) (string, error) {
	b.mu.Lock("RegisterType")
	defer b.mu.Unlock()
	token := uuid.New().String()
	typeArn := "arn:aws:cloudformation:::type/resource/" + typeName
	// Each call to RegisterType creates a new version.
	existingVersions := b.typeVersions[typeArn]
	versionNum := len(existingVersions) + 1
	versionID := fmt.Sprintf("%08d", versionNum)
	b.typeVersions[typeArn] = append(b.typeVersions[typeArn], &RegisteredTypeVersion{
		TypeArn:   typeArn,
		VersionID: versionID,
		IsDefault: true,
		Status:    statusComplete,
	})
	// Mark prior versions as non-default.
	for i := range b.typeVersions[typeArn][:len(b.typeVersions[typeArn])-1] {
		b.typeVersions[typeArn][i].IsDefault = false
	}
	if t, ok := b.typeRegistry[typeArn]; ok {
		t.VersionID = versionID
		t.DefaultVersion = versionID
	} else {
		b.typeRegistry[typeArn] = &RegisteredType{
			TypeArn:        typeArn,
			TypeName:       typeName,
			Type:           "RESOURCE",
			VersionID:      versionID,
			DefaultVersion: versionID,
			Status:         statusComplete,
		}
	}
	b.typeRegistrations[token] = &TypeRegistrationRecord{
		Token:    token,
		TypeName: typeName,
		TypeArn:  typeArn,
		Status:   statusComplete,
	}

	return token, nil
}

func (b *InMemoryBackend) DeregisterType(typeArn string) error {
	b.mu.Lock("DeregisterType")
	defer b.mu.Unlock()
	t, ok := b.typeRegistry[typeArn]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, typeArn)
	}
	t.Status = typeStatusDeprecated

	return nil
}

func (b *InMemoryBackend) PublishType(typeName string) error {
	b.mu.Lock("PublishType")
	defer b.mu.Unlock()
	typeArn := "arn:aws:cloudformation:::type/resource/" + typeName
	if t, ok := b.typeRegistry[typeArn]; ok {
		t.IsPublished = true
	}

	return nil
}

func (b *InMemoryBackend) SetTypeDefaultVersion(typeArn, version string) error {
	b.mu.Lock("SetTypeDefaultVersion")
	defer b.mu.Unlock()
	if t, ok := b.typeRegistry[typeArn]; ok {
		t.DefaultVersion = version
		t.VersionID = version
	}
	// Update typeVersions IsDefault flags.
	for _, v := range b.typeVersions[typeArn] {
		v.IsDefault = v.VersionID == version
	}

	return nil
}

func (b *InMemoryBackend) SetTypeConfiguration(typeName, configuration string) error {
	b.mu.Lock("SetTypeConfiguration")
	defer b.mu.Unlock()
	b.typeConfigs[typeName] = configuration

	return nil
}

func (b *InMemoryBackend) BatchDescribeTypeConfigurations(typeConfigIdentifiers []string) ([]string, error) {
	b.mu.RLock("BatchDescribeTypeConfigurations")
	defer b.mu.RUnlock()
	configs := make([]string, 0, len(typeConfigIdentifiers))
	for _, id := range typeConfigIdentifiers {
		if cfg, ok := b.typeConfigs[id]; ok {
			configs = append(configs, cfg)
		}
	}

	return configs, nil
}

func (b *InMemoryBackend) ListTypes(_ string) ([]TypeSummary, error) {
	b.mu.RLock("ListTypes")
	defer b.mu.RUnlock()
	result := make([]TypeSummary, 0, len(b.typeRegistry))
	for _, t := range b.typeRegistry {
		if t.Status == typeStatusDeprecated {
			continue
		}
		if t.Status == statusComplete || t.IsActivated {
			visibility := "PRIVATE"
			if t.IsPublished {
				visibility = typeVisibilityPublic
			}
			result = append(result, TypeSummary{
				TypeName:    t.TypeName,
				TypeArn:     t.TypeArn,
				Type:        t.Type,
				Visibility:  visibility,
				Description: t.Configuration,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TypeName < result[j].TypeName })

	return result, nil
}

func (b *InMemoryBackend) ListTypeVersions(typeName, _ string) ([]string, error) {
	b.mu.RLock("ListTypeVersions")
	defer b.mu.RUnlock()
	typeArn := "arn:aws:cloudformation:::type/resource/" + typeName
	if versions, ok := b.typeVersions[typeArn]; ok && len(versions) > 0 {
		ids := make([]string, 0, len(versions))
		for _, v := range versions {
			ids = append(ids, v.VersionID)
		}

		return ids, nil
	}
	// Fallback: if no version records but type exists, return its current version.
	if t, ok := b.typeRegistry[typeArn]; ok {
		return []string{t.VersionID}, nil
	}

	return []string{}, nil
}

func (b *InMemoryBackend) ListTypeRegistrations(typeName, _ string) ([]string, error) {
	b.mu.RLock("ListTypeRegistrations")
	defer b.mu.RUnlock()
	var tokens []string
	for token, rec := range b.typeRegistrations {
		if typeName == "" || rec.TypeName == typeName {
			tokens = append(tokens, token)
		}
	}

	return tokens, nil
}

func (b *InMemoryBackend) DescribeTypeRegistration(registrationToken string) (string, error) {
	b.mu.RLock("DescribeTypeRegistration")
	defer b.mu.RUnlock()
	rec, ok := b.typeRegistrations[registrationToken]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrRegistrationTokenNotFound, registrationToken)
	}

	return rec.Status, nil
}

func (b *InMemoryBackend) TestType(typeName, typeArn string) (string, error) {
	b.mu.Lock("TestType")
	defer b.mu.Unlock()
	token := uuid.New().String()
	key := typeArn
	if key == "" {
		key = "arn:aws:cloudformation:::type/resource/" + typeName
	}
	b.typeRegistrations[token] = &TypeRegistrationRecord{
		Token:    token,
		TypeName: typeName,
		TypeArn:  key,
		Status:   statusComplete,
	}

	return token, nil
}

func (b *InMemoryBackend) RegisterPublisher(connectionArn string) (string, error) {
	b.mu.Lock("RegisterPublisher")
	defer b.mu.Unlock()
	publisherID := uuid.New().String()
	b.publishers[publisherID] = &Publisher{
		PublisherID:   publisherID,
		ConnectionArn: connectionArn,
		Status:        "VERIFIED",
	}

	return publisherID, nil
}

func (b *InMemoryBackend) DescribePublisher(publisherID string) (string, error) {
	b.mu.RLock("DescribePublisher")
	defer b.mu.RUnlock()
	p, ok := b.publishers[publisherID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrPublisherNotFound, publisherID)
	}

	return p.Status, nil
}

// ---- Stack Refactor ----

func (b *InMemoryBackend) CreateStackRefactor(description string, stackDefinitions []string) (string, error) {
	b.mu.Lock("CreateStackRefactor")
	defer b.mu.Unlock()
	refactorID := uuid.New().String()
	b.stackRefactors[refactorID] = &StackRefactor{
		RefactorID:       refactorID,
		Description:      description,
		Status:           "CREATE_COMPLETE",
		StackDefinitions: stackDefinitions,
	}

	return refactorID, nil
}

func (b *InMemoryBackend) DescribeStackRefactor(stackRefactorID string) (string, error) {
	b.mu.RLock("DescribeStackRefactor")
	defer b.mu.RUnlock()
	r, ok := b.stackRefactors[stackRefactorID]
	if !ok {
		return "", nil
	}

	return r.Status, nil
}

func (b *InMemoryBackend) ExecuteStackRefactor(stackRefactorID string) error {
	b.mu.Lock("ExecuteStackRefactor")
	defer b.mu.Unlock()
	r, ok := b.stackRefactors[stackRefactorID]
	if !ok {
		return nil
	}
	r.Status = "EXECUTE_COMPLETE"

	return nil
}

func (b *InMemoryBackend) ListStackRefactors(_ string) ([]string, error) {
	b.mu.RLock("ListStackRefactors")
	defer b.mu.RUnlock()
	ids := make([]string, 0, len(b.stackRefactors))
	for id := range b.stackRefactors {
		ids = append(ids, id)
	}

	return ids, nil
}

func (b *InMemoryBackend) ListStackRefactorActions(_ string) ([]string, error) {
	return []string{}, nil
}

// ---- Org Access ----

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

// ---- Misc ----

func (b *InMemoryBackend) SignalResource(stackName, logicalID, uniqueID, status string) error {
	b.mu.Lock("SignalResource")
	defer b.mu.Unlock()
	if _, ok := b.resolveStack(stackName); !ok {
		return fmt.Errorf("%w: %s", ErrStackNotFound, stackName)
	}
	key := stackName + "/" + logicalID
	b.signals[key] = append(b.signals[key], SignalRecord{
		UniqueID: uniqueID,
		Status:   status,
	})

	return nil
}

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

func (b *InMemoryBackend) RecordHandlerProgress(bearerToken, operationStatus string) error {
	b.mu.Lock("RecordHandlerProgress")
	defer b.mu.Unlock()
	b.handlerProgress[bearerToken] = operationStatus

	return nil
}

func (b *InMemoryBackend) GetHookResult(hookResultToken string) (string, error) {
	b.mu.RLock("GetHookResult")
	defer b.mu.RUnlock()
	r, ok := b.hookResults[hookResultToken]
	if !ok {
		return "SUCCEEDED", nil
	}

	return r.HookStatus, nil
}

func (b *InMemoryBackend) ListHookResults(hookResultToken, _ string) ([]string, error) {
	b.mu.RLock("ListHookResults")
	defer b.mu.RUnlock()
	var results []string
	if hookResultToken != "" {
		if r, ok := b.hookResults[hookResultToken]; ok {
			results = append(results, r.HookStatus)
		}
	} else {
		for _, r := range b.hookResults {
			results = append(results, r.HookStatus)
		}
	}

	return results, nil
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

func (b *InMemoryBackend) UpdateTerminationProtection(nameOrID string, enable bool) error {
	b.mu.Lock("UpdateTerminationProtection")
	defer b.mu.Unlock()
	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrStackNotFound, nameOrID)
	}
	stack.EnableTerminationProtection = enable

	return nil
}

func (b *InMemoryBackend) ValidateTemplate(templateBody string) (*TemplateSummary, error) {
	return b.GetTemplateSummary(templateBody, "")
}

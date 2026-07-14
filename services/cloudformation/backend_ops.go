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
	"github.com/blackbirdworks/gopherstack/pkgs/page"
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

// ---- Stack Sets ----

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

func (b *InMemoryBackend) CreateStackInstances(
	ctx context.Context,
	stackSetName string,
	accounts, regions []string,
) (string, error) {
	b.mu.Lock("CreateStackInstances")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return "", ErrStackSetNotFound
	}
	opID := b.recordStackSetOperation(stackSetName, "CREATE_INSTANCES")
	for _, acct := range accounts {
		for _, region := range regions {
			// Deduplicate: skip if instance already exists.
			if b.stackInstanceExists(stackSetName, acct, region) {
				continue
			}
			inst := b.provisionStackInstance(ctx, ss, acct, region, opID)
			b.stackInstances[stackSetName] = append(b.stackInstances[stackSetName], inst)
		}
	}
	b.recordOpResults(stackSetName, opID, accounts, regions, "SUCCEEDED")

	return opID, nil
}

// stackInstanceExists reports whether a stack instance for the account/region
// already exists in the stack set. Must be called with b.mu held.
func (b *InMemoryBackend) stackInstanceExists(stackSetName, acct, region string) bool {
	for _, existing := range b.stackInstances[stackSetName] {
		if existing.Account == acct && existing.Region == region {
			return true
		}
	}

	return false
}

// provisionStackInstance provisions a real child stack for a stack-set instance
// using the stack set's template, so the instance's resources are actually
// created (matching AWS, which deploys a managed child stack per account/region
// rather than merely recording a row). The child stack is named
// StackSet-<setName>-<uuid> to mirror AWS naming. Must be called with b.mu held.
func (b *InMemoryBackend) provisionStackInstance(
	ctx context.Context,
	ss *StackSet,
	acct, region, opID string,
) StackInstance {
	status := "CURRENT"
	statusReason := ""
	childName := fmt.Sprintf("StackSet-%s-%s", ss.StackSetName, uuid.New().String())

	var instanceStackID string
	if ss.TemplateBody != "" {
		child, err := b.createStackLocked(ctx, childName, ss.TemplateBody, nil, StackOptions{}, "")
		if err != nil {
			status = "INOPERABLE"
			statusReason = err.Error()
		} else {
			instanceStackID = child.StackID
		}
	}
	if instanceStackID == "" {
		stackResource := fmt.Sprintf("stack/%s/%s", ss.StackSetName, uuid.New().String())
		instanceStackID = arn.Build("cloudformation", region, acct, stackResource)
	}

	return StackInstance{
		StackSetID:      ss.StackSetID,
		StackSetName:    ss.StackSetName,
		StackID:         instanceStackID,
		Account:         acct,
		Region:          region,
		Status:          status,
		StatusReason:    statusReason,
		DriftStatus:     "NOT_CHECKED",
		LastOperationID: opID,
	}
}

func (b *InMemoryBackend) DeleteStackInstances(
	ctx context.Context,
	stackSetName string,
	accounts, regions []string,
) (string, error) {
	b.mu.Lock("DeleteStackInstances")
	defer b.mu.Unlock()
	if !b.stackSets.Has(stackSetName) {
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

			continue
		}
		// Tear down the provisioned child stack, if any.
		if childName, ok := b.stackIDIndex[inst.StackID]; ok {
			_ = b.deleteStackLocked(ctx, childName)
		}
	}
	b.stackInstances[stackSetName] = filtered
	opID := b.recordStackSetOperation(stackSetName, "DELETE_INSTANCES")
	b.recordOpResults(stackSetName, opID, accounts, regions, "SUCCEEDED")

	return opID, nil
}

func (b *InMemoryBackend) UpdateStackInstances(
	stackSetName string,
	accounts, regions []string,
) (string, error) {
	b.mu.Lock("UpdateStackInstances")
	defer b.mu.Unlock()
	if !b.stackSets.Has(stackSetName) {
		return "", ErrStackSetNotFound
	}
	opID := b.recordStackSetOperation(stackSetName, "UPDATE_INSTANCES")
	if len(accounts) > 0 && len(regions) > 0 {
		b.recordOpResults(stackSetName, opID, accounts, regions, "SUCCEEDED")
	}

	return opID, nil
}

func (b *InMemoryBackend) ListStackInstances(
	stackSetName, nextToken string,
) (page.Page[StackInstance], error) {
	b.mu.RLock("ListStackInstances")
	defer b.mu.RUnlock()
	instances := append([]StackInstance(nil), b.stackInstances[stackSetName]...)

	return page.New(instances, nextToken, 0, cfnDefaultPageSize), nil
}

func (b *InMemoryBackend) DescribeStackInstance(
	stackSetName, account, region string,
) (*StackInstance, error) {
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

// parseStackARN extracts account and region from a CloudFormation stack ARN.
// Format: arn:aws:cloudformation:REGION:ACCOUNT:stack/NAME/ID.
func parseStackARN(stackARN string) (string, string) {
	const stackARNMinParts = 6
	parts := strings.Split(stackARN, ":")
	// parts: [arn, aws, cloudformation, REGION, ACCOUNT, stack/NAME/ID]
	if len(parts) >= stackARNMinParts {
		return parts[4], parts[3]
	}

	return "", ""
}

func (b *InMemoryBackend) ListStackInstanceResourceDrifts(
	stackSetName, _ /* operationID */, account, region string,
) ([]StackResourceDrift, error) {
	b.mu.RLock("ListStackInstanceResourceDrifts")
	defer b.mu.RUnlock()
	if !b.stackSets.Has(stackSetName) {
		return nil, ErrStackSetNotFound
	}
	// Find the matching stack instance's stack ID.
	var instanceStackID string
	for _, inst := range b.stackInstances[stackSetName] {
		if (account == "" || inst.Account == account) && (region == "" || inst.Region == region) {
			instanceStackID = inst.StackID

			break
		}
	}
	if instanceStackID == "" {
		return []StackResourceDrift{}, nil
	}
	driftMap := b.resourceDriftStatus[instanceStackID]
	drifts := make([]StackResourceDrift, 0, len(driftMap))
	for logicalID, status := range driftMap {
		if status == driftStatusInSync {
			continue
		}
		drifts = append(drifts, StackResourceDrift{
			StackID:                  instanceStackID,
			LogicalResourceID:        logicalID,
			StackResourceDriftStatus: status,
		})
	}

	return drifts, nil
}

// ---- Generated Templates ----

func (b *InMemoryBackend) CreateGeneratedTemplate(
	name string,
	resourceIDs []string,
) (*GeneratedTemplate, error) {
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
	b.generatedTemplates.Put(gt)

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
		for _, stack := range b.stacks.All() {
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
	gt, ok := b.generatedTemplates.Get(id)
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
	if !b.generatedTemplates.Has(id) {
		return ErrGeneratedTemplateNotFound
	}
	b.generatedTemplates.Delete(id)

	return nil
}

func (b *InMemoryBackend) DescribeGeneratedTemplate(id string) (*GeneratedTemplate, error) {
	b.mu.RLock("DescribeGeneratedTemplate")
	defer b.mu.RUnlock()
	gt, ok := b.generatedTemplates.Get(id)
	if !ok {
		return nil, ErrGeneratedTemplateNotFound
	}

	return gt, nil
}

func (b *InMemoryBackend) GetGeneratedTemplate(id string) (string, error) {
	b.mu.RLock("GetGeneratedTemplate")
	defer b.mu.RUnlock()
	gt, ok := b.generatedTemplates.Get(id)
	if !ok {
		return "", ErrGeneratedTemplateNotFound
	}

	return gt.TemplateBody, nil
}

func (b *InMemoryBackend) ListGeneratedTemplates(
	nextToken string,
) (page.Page[GeneratedTemplate], error) {
	b.mu.RLock("ListGeneratedTemplates")
	defer b.mu.RUnlock()
	result := make([]GeneratedTemplate, 0, b.generatedTemplates.Len())
	for _, gt := range b.generatedTemplates.All() {
		result = append(result, *gt)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GeneratedTemplateName < result[j].GeneratedTemplateName
	})

	return page.New(result, nextToken, 0, cfnDefaultPageSize), nil
}

// ---- Resource Scans ----

func (b *InMemoryBackend) StartResourceScan() (string, error) {
	b.mu.Lock("StartResourceScan")
	defer b.mu.Unlock()
	scanID := uuid.New().String()
	b.resourceScans.Put(&ResourceScan{
		ResourceScanID:      scanID,
		Status:              statusComplete,
		PercentageCompleted: resourceScanCompletePercent,
	})
	// Populate scan items from existing active stacks.
	items := make([]ScannedResource, 0)
	for _, stack := range b.stacks.All() {
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
				ResourceType:       resTypeS3Bucket,
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
	rs, ok := b.resourceScans.Get(scanID)
	if !ok {
		return nil, ErrResourceScanNotFound
	}

	return rs, nil
}

func (b *InMemoryBackend) ListResourceScans(nextToken string) (page.Page[ResourceScan], error) {
	b.mu.RLock("ListResourceScans")
	defer b.mu.RUnlock()
	result := make([]ResourceScan, 0, b.resourceScans.Len())
	for _, rs := range b.resourceScans.All() {
		result = append(result, *rs)
	}

	return page.New(result, nextToken, 0, cfnDefaultPageSize), nil
}

func (b *InMemoryBackend) ListResourceScanResources(scanID, _ string) ([]ScannedResource, error) {
	b.mu.RLock("ListResourceScanResources")
	defer b.mu.RUnlock()
	if !b.resourceScans.Has(scanID) {
		return nil, ErrResourceScanNotFound
	}
	items := b.resourceScanItems[scanID]
	out := make([]ScannedResource, len(items))
	copy(out, items)

	return out, nil
}

func (b *InMemoryBackend) ListResourceScanRelatedResources(
	scanID string,
	_ []string,
) ([]string, error) {
	b.mu.RLock("ListResourceScanRelatedResources")
	defer b.mu.RUnlock()
	if !b.resourceScans.Has(scanID) {
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
	if t, ok := b.typeRegistry.Get(key); ok {
		t.IsActivated = true
	} else {
		b.typeRegistry.Put(&RegisteredType{
			TypeArn:     key,
			TypeName:    typeName,
			Type:        typeKindResource,
			VersionID:   "00000001",
			Status:      statusComplete,
			IsActivated: true,
		})
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
	t, ok := b.typeRegistry.Get(key)
	if !ok || !t.IsActivated {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, key)
	}
	t.IsActivated = false

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
	if t, ok := b.typeRegistry.Get(typeArn); ok {
		t.VersionID = versionID
		t.DefaultVersion = versionID
	} else {
		b.typeRegistry.Put(&RegisteredType{
			TypeArn:        typeArn,
			TypeName:       typeName,
			Type:           "RESOURCE",
			VersionID:      versionID,
			DefaultVersion: versionID,
			Status:         statusComplete,
		})
	}
	b.typeRegistrations.Put(&TypeRegistrationRecord{
		Token:    token,
		TypeName: typeName,
		TypeArn:  typeArn,
		Status:   statusComplete,
	})

	return token, nil
}

func (b *InMemoryBackend) DeregisterType(typeArn string) error {
	b.mu.Lock("DeregisterType")
	defer b.mu.Unlock()
	t, ok := b.typeRegistry.Get(typeArn)
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
	t, ok := b.typeRegistry.Get(typeArn)
	if !ok {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, typeArn)
	}
	t.IsPublished = true

	return nil
}

func (b *InMemoryBackend) SetTypeDefaultVersion(typeArn, version string) error {
	b.mu.Lock("SetTypeDefaultVersion")
	defer b.mu.Unlock()
	if t, ok := b.typeRegistry.Get(typeArn); ok {
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

func (b *InMemoryBackend) BatchDescribeTypeConfigurations(
	typeConfigIdentifiers []string,
) ([]TypeConfigurationDetail, error) {
	b.mu.RLock("BatchDescribeTypeConfigurations")
	defer b.mu.RUnlock()
	details := make([]TypeConfigurationDetail, 0, len(typeConfigIdentifiers))
	for _, id := range typeConfigIdentifiers {
		cfg := b.typeConfigs[id]
		if cfg == "" {
			// Also check by looking up the registry entry (typeName may be a key in typeConfigs).
			typeArn := "arn:aws:cloudformation:::type/resource/" + id
			if t, ok := b.typeRegistry.Get(typeArn); ok {
				cfg = t.Configuration
			}
		}
		details = append(details, TypeConfigurationDetail{
			TypeName:               id,
			Configuration:          cfg,
			IsDefaultConfiguration: true,
		})
	}

	return details, nil
}

func (b *InMemoryBackend) ListTypes(_ string) ([]TypeSummary, error) {
	b.mu.RLock("ListTypes")
	defer b.mu.RUnlock()
	result := make([]TypeSummary, 0, b.typeRegistry.Len())
	for _, t := range b.typeRegistry.All() {
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
	if t, ok := b.typeRegistry.Get(typeArn); ok {
		return []string{t.VersionID}, nil
	}

	return []string{}, nil
}

func (b *InMemoryBackend) ListTypeRegistrations(typeName, _ string) ([]string, error) {
	b.mu.RLock("ListTypeRegistrations")
	defer b.mu.RUnlock()
	var tokens []string
	for _, rec := range b.typeRegistrations.All() {
		if typeName == "" || rec.TypeName == typeName {
			tokens = append(tokens, rec.Token)
		}
	}

	return tokens, nil
}

func (b *InMemoryBackend) DescribeTypeRegistration(registrationToken string) (string, error) {
	b.mu.RLock("DescribeTypeRegistration")
	defer b.mu.RUnlock()
	rec, ok := b.typeRegistrations.Get(registrationToken)
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
	b.typeRegistrations.Put(&TypeRegistrationRecord{
		Token:    token,
		TypeName: typeName,
		TypeArn:  key,
		Status:   statusComplete,
	})

	return token, nil
}

func (b *InMemoryBackend) RegisterPublisher(connectionArn string) (string, error) {
	b.mu.Lock("RegisterPublisher")
	defer b.mu.Unlock()
	publisherID := uuid.New().String()
	b.publishers.Put(&Publisher{
		PublisherID:   publisherID,
		ConnectionArn: connectionArn,
		Status:        "VERIFIED",
	})

	return publisherID, nil
}

func (b *InMemoryBackend) DescribePublisher(publisherID string) (string, error) {
	b.mu.RLock("DescribePublisher")
	defer b.mu.RUnlock()
	p, ok := b.publishers.Get(publisherID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrPublisherNotFound, publisherID)
	}

	return p.Status, nil
}

// ---- Stack Refactor ----

func (b *InMemoryBackend) CreateStackRefactor(
	description string,
	stackDefinitions []string,
) (string, error) {
	b.mu.Lock("CreateStackRefactor")
	defer b.mu.Unlock()
	refactorID := uuid.New().String()
	b.stackRefactors.Put(&StackRefactor{
		RefactorID:       refactorID,
		Description:      description,
		Status:           "CREATE_COMPLETE",
		StackDefinitions: stackDefinitions,
	})

	return refactorID, nil
}

func (b *InMemoryBackend) DescribeStackRefactor(stackRefactorID string) (string, error) {
	b.mu.RLock("DescribeStackRefactor")
	defer b.mu.RUnlock()
	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		// Unlike CreateStackRefactor/ExecuteStackRefactor/List*, DescribeStackRefactor's
		// SDK-modeled error set includes StackRefactorNotFoundException — it is not
		// fire-and-forget, so an unknown ID must be a real error, not an empty 200.
		return "", fmt.Errorf("%w: %s", ErrStackRefactorNotFound, stackRefactorID)
	}

	return r.Status, nil
}

func (b *InMemoryBackend) ExecuteStackRefactor(stackRefactorID string) error {
	b.mu.Lock("ExecuteStackRefactor")
	defer b.mu.Unlock()
	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		return nil
	}
	r.Status = "EXECUTE_COMPLETE"

	return nil
}

func (b *InMemoryBackend) ListStackRefactors(_ string) ([]StackRefactorSummary, error) {
	b.mu.RLock("ListStackRefactors")
	defer b.mu.RUnlock()
	summaries := make([]StackRefactorSummary, 0, b.stackRefactors.Len())
	for _, r := range b.stackRefactors.All() {
		summaries = append(summaries, StackRefactorSummary{
			StackRefactorID: r.RefactorID,
			Status:          r.Status,
			Description:     r.Description,
		})
	}

	return summaries, nil
}

func (b *InMemoryBackend) ListStackRefactorActions(
	stackRefactorID string,
) ([]StackRefactorAction, error) {
	b.mu.RLock("ListStackRefactorActions")
	defer b.mu.RUnlock()
	r, ok := b.stackRefactors.Get(stackRefactorID)
	if !ok {
		return []StackRefactorAction{}, nil
	}
	actions := make([]StackRefactorAction, 0, len(r.StackDefinitions))
	for _, def := range r.StackDefinitions {
		actions = append(actions, StackRefactorAction{
			Action:      "MOVE",
			Description: def,
		})
	}

	return actions, nil
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
	r, ok := b.hookResults.Get(hookResultToken)
	if !ok {
		return "SUCCEEDED", nil
	}

	return r.HookStatus, nil
}

func (b *InMemoryBackend) ListHookResults(hookResultToken, _ string) ([]HookResult, error) {
	b.mu.RLock("ListHookResults")
	defer b.mu.RUnlock()
	var results []HookResult
	if hookResultToken != "" {
		if r, ok := b.hookResults.Get(hookResultToken); ok {
			results = append(results, *r)
		}
	} else {
		for _, r := range b.hookResults.All() {
			results = append(results, *r)
		}
	}

	return results, nil
}

func (b *InMemoryBackend) DescribeChangeSetHooks(_, _ string) ([]ChangeSetHook, error) {
	// Hook configurations are not emulated; return empty list (valid AWS response
	// when no hook configurations are active for the change set).
	return []ChangeSetHook{}, nil
}

func (b *InMemoryBackend) DescribeEvents(
	stackName, nextToken string,
) (page.Page[StackEvent], error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()
	if stackName != "" {
		stack, ok := b.resolveStack(stackName)
		if !ok {
			return page.Page[StackEvent]{}, fmt.Errorf("%w: %s", ErrStackNotFound, stackName)
		}
		evts := b.events[stack.StackID]
		all := make([]StackEvent, len(evts))
		copy(all, evts)
		// AWS returns events newest-first.
		sort.Slice(all, func(i, j int) bool {
			return all[i].Timestamp.After(all[j].Timestamp)
		})

		return page.New(all, nextToken, 0, cfnDefaultPageSize), nil
	}
	// No filter: collect events across all stacks.
	var total int
	for _, evts := range b.events {
		total += len(evts)
	}
	all := make([]StackEvent, 0, total)
	for _, evts := range b.events {
		all = append(all, evts...)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp.After(all[j].Timestamp)
	})

	return page.New(all, nextToken, 0, cfnDefaultPageSize), nil
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

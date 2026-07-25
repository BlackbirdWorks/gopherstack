package cloudformation

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/google/uuid"
)

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
		DriftStatus:     driftStatusNotChecked,
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

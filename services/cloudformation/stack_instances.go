package cloudformation

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// instanceTarget is a resolved (account, source-OU) pair. ouID is empty for
// targets given as explicit Accounts rather than via DeploymentTargets.
// OrganizationalUnitIds.
type instanceTarget struct {
	account string
	ouID    string
}

// resolveInstanceTargets merges explicit accounts with OU-expanded accounts.
// ouIDs requires the StackSet's PermissionModel to be SERVICE_MANAGED,
// matching real AWS, which rejects OU-based deployment targets on
// self-managed StackSets. Must be called with b.mu held.
func (b *InMemoryBackend) resolveInstanceTargets(
	ss *StackSet, accounts, ouIDs []string,
) ([]instanceTarget, error) {
	targets := make([]instanceTarget, 0, len(accounts)+len(ouIDs))
	for _, a := range accounts {
		targets = append(targets, instanceTarget{account: a})
	}
	if len(ouIDs) == 0 {
		return targets, nil
	}
	if ss.PermissionModel != stackSetPermissionServiceManaged {
		return nil, ErrServiceManagedRequired
	}
	if !b.orgAccessEnabled {
		return nil, ErrOrganizationsAccessNotActive
	}
	if b.orgDirectory == nil {
		return nil, ErrOrganizationsNotWired
	}

	seen := make(map[string]bool)
	for _, ou := range ouIDs {
		accts, err := b.orgDirectory.ResolveAccountIDsUnderParent(ou)
		if err != nil {
			return nil, fmt.Errorf("resolve accounts for organizational unit %s: %w", ou, err)
		}
		for _, a := range accts {
			if seen[a] {
				continue
			}
			seen[a] = true
			targets = append(targets, instanceTarget{account: a, ouID: ou})
		}
	}

	return targets, nil
}

func (b *InMemoryBackend) CreateStackInstances(
	ctx context.Context,
	stackSetName string,
	accounts, ouIDs, regions []string,
) (string, error) {
	b.mu.Lock("CreateStackInstances")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return "", ErrStackSetNotFound
	}

	targets, err := b.resolveInstanceTargets(ss, accounts, ouIDs)
	if err != nil {
		return "", err
	}

	opID := b.recordStackSetOperation(stackSetName, "CREATE_INSTANCES")
	touchedAccounts := make([]string, 0, len(targets))
	for _, t := range targets {
		touchedAccounts = append(touchedAccounts, t.account)
		for _, region := range regions {
			// Deduplicate: skip if instance already exists.
			if b.stackInstanceExists(stackSetName, t.account, region) {
				continue
			}
			inst := b.provisionStackInstance(ctx, ss, t.account, region, opID)
			inst.OrganizationalUnitID = t.ouID
			b.stackInstances[stackSetName] = append(b.stackInstances[stackSetName], inst)
		}
	}
	b.recordOpResults(stackSetName, opID, touchedAccounts, regions, "SUCCEEDED")

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

// stackInstanceTeardownFailure records that an instance targeted for
// removal could not actually have its child stack torn down, so the caller
// can report it instead of the instance silently disappearing.
type stackInstanceTeardownFailure struct {
	account string
	region  string
	reason  string
}

// deleteMatchingStackInstances filters stackSetName's instances down to
// those NOT matching any (account, region) pair, tearing down each removed
// instance's provisioned child stack. An instance whose child-stack teardown
// fails is NOT dropped: real CloudFormation leaves it in the StackSet as
// INOPERABLE rather than discarding it (cloudformation@v1.76.1
// types/types.go:1894, StackInstance.Status doc: "INOPERABLE: A
// DeleteStackInstances operation has failed and left the stack in an
// unstable state"). Must be called with b.mu held.
func (b *InMemoryBackend) deleteMatchingStackInstances(
	ctx context.Context, stackSetName string, accounts, regions []string,
) []stackInstanceTeardownFailure {
	instances := b.stackInstances[stackSetName]
	filtered := make([]StackInstance, 0, len(instances))
	var failed []stackInstanceTeardownFailure
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
		if childName, teardownOK := b.stackIDIndex[inst.StackID]; teardownOK {
			if err := b.deleteStackLocked(ctx, childName); err != nil {
				inst.Status = "INOPERABLE"
				inst.StatusReason = err.Error()
				filtered = append(filtered, inst)
				failed = append(failed, stackInstanceTeardownFailure{
					account: inst.Account,
					region:  inst.Region,
					reason:  err.Error(),
				})
			}
		}
	}
	b.stackInstances[stackSetName] = filtered

	return failed
}

// recordStackInstanceDeleteResults records DeleteStackInstances' per-
// account/region operation results: FAILED (with StatusReason) for pairs
// whose child-stack teardown failed, SUCCEEDED for the rest. Also flips the
// operation's own Status to FAILED when any pair failed, matching
// StackSetOperationStatus's FAILED value (cloudformation@v1.76.1
// types/enums.go:1742). Caller must hold b.mu.Lock.
func (b *InMemoryBackend) recordStackInstanceDeleteResults(
	stackSetName, opID string, accounts, regions []string, failed []stackInstanceTeardownFailure,
) {
	type pair struct{ account, region string }
	reasonByPair := make(map[pair]string, len(failed))
	for _, f := range failed {
		reasonByPair[pair{f.account, f.region}] = f.reason
	}
	if b.stackSetOpResults[stackSetName] == nil {
		b.stackSetOpResults[stackSetName] = make(map[string][]StackSetOperationResult)
	}
	for _, acct := range accounts {
		for _, region := range regions {
			result := StackSetOperationResult{Account: acct, Region: region, Status: "SUCCEEDED"}
			if reason, failedPair := reasonByPair[pair{acct, region}]; failedPair {
				result.Status = cfnStatusFailed
				result.StatusReason = reason
			}
			b.stackSetOpResults[stackSetName][opID] = append(b.stackSetOpResults[stackSetName][opID], result)
		}
	}
	if len(failed) > 0 {
		if op, ok := b.stackSetOperations[stackSetName][opID]; ok {
			op.Status = cfnStatusFailed
		}
	}
}

func (b *InMemoryBackend) DeleteStackInstances(
	ctx context.Context,
	stackSetName string,
	accounts, ouIDs, regions []string,
) (string, error) {
	b.mu.Lock("DeleteStackInstances")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return "", ErrStackSetNotFound
	}
	if len(ouIDs) > 0 {
		targets, err := b.resolveInstanceTargets(ss, nil, ouIDs)
		if err != nil {
			return "", err
		}
		for _, t := range targets {
			accounts = append(accounts, t.account)
		}
	}
	failed := b.deleteMatchingStackInstances(ctx, stackSetName, accounts, regions)
	opID := b.recordStackSetOperation(stackSetName, "DELETE_INSTANCES")
	b.recordStackInstanceDeleteResults(stackSetName, opID, accounts, regions, failed)

	return opID, nil
}

func (b *InMemoryBackend) UpdateStackInstances(
	stackSetName string,
	accounts, ouIDs, regions []string,
) (string, error) {
	b.mu.Lock("UpdateStackInstances")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return "", ErrStackSetNotFound
	}
	if len(ouIDs) > 0 {
		targets, err := b.resolveInstanceTargets(ss, nil, ouIDs)
		if err != nil {
			return "", err
		}
		for _, t := range targets {
			accounts = append(accounts, t.account)
		}
	}
	opID := b.recordStackSetOperation(stackSetName, "UPDATE_INSTANCES")
	if len(accounts) > 0 && len(regions) > 0 {
		b.recordOpResults(stackSetName, opID, accounts, regions, "SUCCEEDED")
	}

	return opID, nil
}

// ListStackInstancesFilter holds ListStackInstancesInput's optional
// narrowing members (cloudformation@v1.76.1 api_op_ListStackInstances.go):
// StackInstanceAccount/StackInstanceRegion match exactly, and Filters
// entries with Name DRIFT_STATUS/LAST_OPERATION_ID match against the
// instance's own DriftStatus/LastOperationID. DETAILED_STATUS is accepted on
// the wire but not enforced here -- this backend has no separate detailed
// status distinct from Status (see StackInstance in models.go), and
// DetailedStatus's real values (PENDING/RUNNING/SUCCEEDED/FAILED/...) don't
// correspond to StackInstanceStatus's (CURRENT/OUTDATED/INOPERABLE), so
// mapping one onto the other would fabricate data rather than filter it.
type ListStackInstancesFilter struct {
	StackInstanceAccount string
	StackInstanceRegion  string
	DriftStatus          string
	LastOperationID      string
}

func matchesStackInstanceFilter(inst *StackInstance, filter ListStackInstancesFilter) bool {
	if filter.StackInstanceAccount != "" && inst.Account != filter.StackInstanceAccount {
		return false
	}
	if filter.StackInstanceRegion != "" && inst.Region != filter.StackInstanceRegion {
		return false
	}
	if filter.DriftStatus != "" && inst.DriftStatus != filter.DriftStatus {
		return false
	}
	if filter.LastOperationID != "" && inst.LastOperationID != filter.LastOperationID {
		return false
	}

	return true
}

func (b *InMemoryBackend) ListStackInstances(
	stackSetName, nextToken string,
	filter ListStackInstancesFilter,
) (page.Page[StackInstance], error) {
	b.mu.RLock("ListStackInstances")
	defer b.mu.RUnlock()

	all := b.stackInstances[stackSetName]
	instances := make([]StackInstance, 0, len(all))
	for _, inst := range all {
		if matchesStackInstanceFilter(&inst, filter) {
			instances = append(instances, inst)
		}
	}

	return page.New(instances, nextToken, 0, cfnDefaultPageSize), nil
}

func (b *InMemoryBackend) DescribeStackInstance(
	stackSetName, account, region string,
) (*StackInstance, error) {
	b.mu.RLock("DescribeStackInstance")
	defer b.mu.RUnlock()
	if !b.stackSets.Has(stackSetName) {
		return nil, fmt.Errorf("%w: %s", ErrStackSetNotFound, stackSetName)
	}
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
	// Prefer the full drift detail captured by DetectStackResourceDrift (same
	// resourceDriftDetail map DescribeStackResourceDrifts already prefers),
	// which carries ResourceType/PhysicalResourceID/Timestamp that
	// resourceDriftStatus alone (bare status per logical ID) doesn't have.
	detailMap := b.resourceDriftDetail[instanceStackID]
	drifts := make([]StackResourceDrift, 0, len(driftMap))
	for logicalID, status := range driftMap {
		if status == driftStatusInSync {
			continue
		}
		if detailMap != nil {
			if d, ok := detailMap[logicalID]; ok {
				drifts = append(drifts, d)

				continue
			}
		}
		drifts = append(drifts, StackResourceDrift{
			StackID:                  instanceStackID,
			LogicalResourceID:        logicalID,
			StackResourceDriftStatus: status,
		})
	}

	return drifts, nil
}

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

// Account filter type values (DeploymentTargets.AccountFilterType,
// cloudformation@v1.76.1 types/enums.go AccountFilterType). "" is the wire
// default, equivalent to accountFilterUnion (docs.aws.amazon.com/
// AWSCloudFormation/latest/APIReference/API_DeploymentTargets.html: "This is
// the default value if AccountFilterType is not provided").
const (
	accountFilterIntersection = "INTERSECTION"
	accountFilterDifference   = "DIFFERENCE"
	accountFilterUnion        = "UNION"
)

// resolveInstanceTargets merges explicit accounts with OU-expanded accounts
// according to filterType (DeploymentTargets.AccountFilterType), matching
// API_DeploymentTargets.html:
//   - "" / UNION: OU accounts plus the explicit accounts.
//   - NONE: the OU accounts only (explicit accounts are ignored).
//   - INTERSECTION: only explicit accounts that also belong to the OUs.
//   - DIFFERENCE: OU accounts minus the explicit accounts.
//
// ouIDs requires the StackSet's PermissionModel to be SERVICE_MANAGED,
// matching real AWS, which rejects OU-based deployment targets on
// self-managed StackSets. Must be called with b.mu held.
func (b *InMemoryBackend) resolveInstanceTargets(
	ss *StackSet, accounts, ouIDs []string, filterType string,
) ([]instanceTarget, error) {
	if len(ouIDs) == 0 {
		return combineAccountFilter(filterType, accounts, nil), nil
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
	ouAccounts := make([]instanceTarget, 0, len(ouIDs))
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
			ouAccounts = append(ouAccounts, instanceTarget{account: a, ouID: ou})
		}
	}

	return combineAccountFilter(filterType, accounts, ouAccounts), nil
}

// combineAccountFilter applies filterType's documented set operation between
// the explicit account list and the OU-resolved accounts. ouAccounts' order
// is preserved for NONE/INTERSECTION/DIFFERENCE; explicit-then-OU order is
// preserved for UNION, matching the pre-existing union behavior.
func combineAccountFilter(filterType string, explicit []string, ouAccounts []instanceTarget) []instanceTarget {
	ouByAccount := make(map[string]string, len(ouAccounts))
	ouOrder := make([]string, 0, len(ouAccounts))

	for _, t := range ouAccounts {
		if _, ok := ouByAccount[t.account]; !ok {
			ouOrder = append(ouOrder, t.account)
		}

		ouByAccount[t.account] = t.ouID
	}

	explicitSet := make(map[string]bool, len(explicit))
	for _, a := range explicit {
		explicitSet[a] = true
	}

	switch filterType {
	case valueNone:
		return filterOUAccounts(ouOrder, ouByAccount, func(string) bool { return true })
	case accountFilterIntersection:
		return filterOUAccounts(ouOrder, ouByAccount, func(a string) bool { return explicitSet[a] })
	case accountFilterDifference:
		return filterOUAccounts(ouOrder, ouByAccount, func(a string) bool { return !explicitSet[a] })
	default: // "" or UNION
		out := make([]instanceTarget, 0, len(explicit)+len(ouOrder))
		seen := make(map[string]bool, len(explicit)+len(ouOrder))

		for _, a := range explicit {
			if seen[a] {
				continue
			}

			seen[a] = true
			out = append(out, instanceTarget{account: a, ouID: ouByAccount[a]})
		}

		for _, a := range ouOrder {
			if seen[a] {
				continue
			}

			seen[a] = true
			out = append(out, instanceTarget{account: a, ouID: ouByAccount[a]})
		}

		return out
	}
}

// filterOUAccounts returns the ouOrder accounts (in order) for which keep
// reports true, each carrying its resolved OU ID.
func filterOUAccounts(ouOrder []string, ouByAccount map[string]string, keep func(string) bool) []instanceTarget {
	out := make([]instanceTarget, 0, len(ouOrder))

	for _, a := range ouOrder {
		if keep(a) {
			out = append(out, instanceTarget{account: a, ouID: ouByAccount[a]})
		}
	}

	return out
}

func (b *InMemoryBackend) CreateStackInstances(
	ctx context.Context,
	stackSetName string,
	accounts, ouIDs, regions []string,
	filterType string,
	opOpts ...StackSetOpOption,
) (string, error) {
	prefs, err := resolveOpPreferences(opOpts)
	if err != nil {
		return "", err
	}

	b.mu.Lock("CreateStackInstances")
	defer b.mu.Unlock()
	if !b.stackSets.Has(stackSetName) {
		return "", ErrStackSetNotFound
	}

	ss, _ := b.stackSets.Get(stackSetName)

	targets, err := b.resolveInstanceTargets(ss, accounts, ouIDs, filterType)
	if err != nil {
		return "", err
	}

	apply := func(ctx context.Context, opID string, u stackSetUnit) string {
		cur, ok := b.stackSets.Get(stackSetName)
		if !ok {
			return "stack set no longer exists"
		}

		if b.stackInstanceExists(stackSetName, u.account, u.region) {
			return ""
		}

		inst := b.provisionStackInstance(ctx, cur, u.account, u.region, opID)
		inst.OrganizationalUnitID = u.ouID
		b.stackInstances[stackSetName] = append(b.stackInstances[stackSetName], inst)

		if inst.Status == statusOutdated {
			return inst.StatusReason
		}

		return ""
	}

	return b.admitStackSetOp(ctx, stackSetName, "CREATE", prefs, false, targetUnits(targets, regions), apply), nil
}

// targetUnits expands resolved targets across regions, account-major.
func targetUnits(targets []instanceTarget, regions []string) []stackSetUnit {
	units := make([]stackSetUnit, 0, len(targets)*len(regions))

	for _, t := range targets {
		for _, region := range regions {
			units = append(units, stackSetUnit{account: t.account, region: region, ouID: t.ouID})
		}
	}

	return units
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
		switch {
		case err != nil:
			status = statusOutdated
			statusReason = err.Error()
		case isFailedCreateStatus(child.StackStatus):
			status = statusOutdated
			statusReason = child.StackStatusReason
			instanceStackID = child.StackID
		default:
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
	ctx context.Context, stackSetName string, accounts, regions []string, retainStacks bool,
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
		// RetainStacks: drop the stack-instance association only -- the
		// child stack itself is left in place, un-managed by the set.
		if retainStacks {
			continue
		}
		if childName, teardownOK := b.stackIDIndex[inst.StackID]; teardownOK {
			if err := b.deleteStackLocked(ctx, childName); err != nil {
				inst.Status = statusInoperable
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

func (b *InMemoryBackend) DeleteStackInstances(
	ctx context.Context,
	stackSetName string,
	accounts, ouIDs, regions []string,
	retainStacks bool,
	filterType string,
	opOpts ...StackSetOpOption,
) (string, error) {
	prefs, err := resolveOpPreferences(opOpts)
	if err != nil {
		return "", err
	}

	b.mu.Lock("DeleteStackInstances")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return "", ErrStackSetNotFound
	}

	targets, err := b.resolveInstanceTargets(ss, accounts, ouIDs, filterType)
	if err != nil {
		return "", err
	}

	apply := func(ctx context.Context, _ string, u stackSetUnit) string {
		failed := b.deleteMatchingStackInstances(
			ctx, stackSetName, []string{u.account}, []string{u.region}, retainStacks,
		)
		if len(failed) > 0 {
			return failed[0].reason
		}

		return ""
	}

	return b.admitStackSetOp(
		ctx,
		stackSetName,
		"DELETE",
		prefs,
		retainStacks,
		targetUnits(targets, regions),
		apply,
	), nil
}

func (b *InMemoryBackend) UpdateStackInstances(
	stackSetName string,
	accounts, ouIDs, regions []string,
	filterType string,
	opOpts ...StackSetOpOption,
) (string, error) {
	prefs, err := resolveOpPreferences(opOpts)
	if err != nil {
		return "", err
	}

	b.mu.Lock("UpdateStackInstances")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return "", ErrStackSetNotFound
	}

	targets, err := b.resolveInstanceTargets(ss, accounts, ouIDs, filterType)
	if err != nil {
		return "", err
	}

	apply := func(context.Context, string, stackSetUnit) string { return "" }

	return b.admitStackSetOp(
		context.Background(), stackSetName, "UPDATE", prefs, false, targetUnits(targets, regions), apply,
	), nil
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
	stackSetName string, maxResults int, nextToken string,
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

	limit := min(maxResults, cfnListMaxPageSize)

	return page.New(instances, nextToken, limit, cfnDefaultPageSize), nil
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

package cloudformation

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	statusComplete                   = "COMPLETE"
	statusEnabled                    = "ENABLED"
	resourceScanCompletePercent      = 100
	typeKindResource                 = "RESOURCE"
	typeVisibilityPublic             = "PUBLIC"
	typeVisibilityPrivate            = "PRIVATE"
	typeStatusDeprecated             = "DEPRECATED"
	provisioningTypeFullyMutable     = "FULLY_MUTABLE"
	driftStatusDrifted               = "DRIFTED"
	driftStatusModified              = "MODIFIED"
	driftStatusDeleted               = "DELETED"
	driftStatusNotChecked            = "NOT_CHECKED"
	stackSetPermissionServiceManaged = "SERVICE_MANAGED"
)

// StackSetOptions holds the optional fields accepted by CreateStackSet and
// UpdateStackSet beyond name/description/templateBody, mirroring the shape of
// StackOptions for regular stacks.
type StackSetOptions struct {
	AutoDeployment        *AutoDeployment
	ManagedExecution      *ManagedExecution
	AdministrationRoleARN string
	ExecutionRoleName     string
	PermissionModel       string
	Capabilities          []string
	Parameters            []Parameter
	Tags                  []Tag
	OrganizationalUnitIDs []string
}

func (b *InMemoryBackend) CreateStackSet(
	name, description, templateBody string,
	opts StackSetOptions,
) (*StackSet, error) {
	b.mu.Lock("CreateStackSet")
	defer b.mu.Unlock()
	if b.stackSets.Has(name) {
		return nil, ErrStackSetAlreadyExists
	}
	stackSetID := uuid.New().String()
	ss := &StackSet{
		StackSetID:   stackSetID,
		StackSetName: name,
		Description:  description,
		TemplateBody: templateBody,
		Status:       statusActive,
		StackSetARN: arn.Build(
			"cloudformation", b.region, b.accountID, "stackset/"+name+":"+stackSetID,
		),
		AdministrationRoleARN: opts.AdministrationRoleARN,
		ExecutionRoleName:     opts.ExecutionRoleName,
		PermissionModel:       opts.PermissionModel,
		Capabilities:          opts.Capabilities,
		Parameters:            opts.Parameters,
		Tags:                  opts.Tags,
		OrganizationalUnitIDs: opts.OrganizationalUnitIDs,
		AutoDeployment:        opts.AutoDeployment,
		ManagedExecution:      opts.ManagedExecution,
	}
	b.stackSets.Put(ss)

	return ss, nil
}

func (b *InMemoryBackend) UpdateStackSet(
	name, description, templateBody string,
	opts StackSetOptions,
	opOpts ...StackSetOpOption,
) (*StackSet, string, error) {
	prefs, err := resolveOpPreferences(opOpts)
	if err != nil {
		return nil, "", err
	}

	b.mu.Lock("UpdateStackSet")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(name)
	if !ok {
		return nil, "", ErrStackSetNotFound
	}
	if description != "" {
		ss.Description = description
	}
	if templateBody != "" {
		ss.TemplateBody = templateBody
	}
	if opts.AdministrationRoleARN != "" {
		ss.AdministrationRoleARN = opts.AdministrationRoleARN
	}
	if opts.ExecutionRoleName != "" {
		ss.ExecutionRoleName = opts.ExecutionRoleName
	}
	if opts.PermissionModel != "" {
		ss.PermissionModel = opts.PermissionModel
	}
	if opts.Capabilities != nil {
		ss.Capabilities = opts.Capabilities
	}
	if opts.Parameters != nil {
		ss.Parameters = opts.Parameters
	}
	if opts.Tags != nil {
		ss.Tags = opts.Tags
	}
	if opts.OrganizationalUnitIDs != nil {
		ss.OrganizationalUnitIDs = opts.OrganizationalUnitIDs
	}
	if opts.AutoDeployment != nil {
		ss.AutoDeployment = opts.AutoDeployment
	}
	if opts.ManagedExecution != nil {
		ss.ManagedExecution = opts.ManagedExecution
	}
	noop := func(context.Context, string, stackSetUnit) string { return "" }
	opID := b.admitStackSetOp(context.Background(), name, "UPDATE", prefs, false, nil, noop)

	return ss, opID, nil
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
	if b.stackSetHasActiveOps(name) {
		return ErrOperationInProgress
	}
	if len(b.stackInstances[name]) > 0 {
		return ErrStackSetNotEmpty
	}
	b.stackSets.Delete(name)
	delete(b.stackInstances, name)
	delete(b.stackSetOperations, name)
	delete(b.stackSetOpResults, name)

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

// StackSetRegions returns the deduplicated, sorted list of Amazon Web
// Services Regions the given StackSet currently has stack instances deployed
// in, matching real DescribeStackSetResult.StackSet.Regions. This is derived
// live from b.stackInstances rather than stored on the StackSet record itself
// -- storing it directly would create a second source of truth that could
// drift out of sync with the actual instances (same rationale as the
// driftByStackID reverse index rebuilt in Restore).
func (b *InMemoryBackend) StackSetRegions(name string) []string {
	b.mu.RLock("StackSetRegions")
	defer b.mu.RUnlock()

	seen := make(map[string]bool)
	var regions []string
	for _, inst := range b.stackInstances[name] {
		if !seen[inst.Region] {
			seen[inst.Region] = true
			regions = append(regions, inst.Region)
		}
	}
	sort.Strings(regions)

	return regions
}

// cfnListMaxPageSize caps StackSet list operations at 100, matching the
// documented maximum of ListGeneratedTemplates/ListResourceScans. The pinned
// SDK's ListStackSets/ListStackSetOperations/ListStackInstances doc comments
// (cloudformation@v1.76.1 api_op_ListStackSets.go:66-69 etc.) state a
// MaxResults field exists but give no numeric default or maximum, so this
// repo's existing 100-item convention (cfnDefaultPageSize) is reused for
// both rather than inventing an unverified number.
const cfnListMaxPageSize = cfnDefaultPageSize

func (b *InMemoryBackend) ListStackSets(
	maxResults int, nextToken, status string,
) (page.Page[StackSetSummary], error) {
	b.mu.RLock("ListStackSets")
	defer b.mu.RUnlock()
	result := make([]StackSetSummary, 0, b.stackSets.Len())
	for _, ss := range b.stackSets.All() {
		if status != "" && ss.Status != status {
			continue
		}

		result = append(result, StackSetSummary{
			StackSetID:       ss.StackSetID,
			StackSetName:     ss.StackSetName,
			Status:           ss.Status,
			Description:      ss.Description,
			AutoDeployment:   ss.AutoDeployment,
			ManagedExecution: ss.ManagedExecution,
			PermissionModel:  ss.PermissionModel,
		})
	}
	sort.Slice(
		result,
		func(i, j int) bool { return result[i].StackSetName < result[j].StackSetName },
	)

	limit := min(maxResults, cfnListMaxPageSize)

	return page.New(result, nextToken, limit, cfnDefaultPageSize), nil
}

func (b *InMemoryBackend) DetectStackSetDrift(stackSetName string, opOpts ...StackSetOpOption) (string, error) {
	prefs, err := resolveOpPreferences(opOpts)
	if err != nil {
		return "", err
	}

	b.mu.Lock("DetectStackSetDrift")
	defer b.mu.Unlock()
	if !b.stackSets.Has(stackSetName) {
		return "", ErrStackSetNotFound
	}

	units := make([]stackSetUnit, 0, len(b.stackInstances[stackSetName]))
	for _, inst := range b.stackInstances[stackSetName] {
		units = append(units, stackSetUnit{account: inst.Account, region: inst.Region})
	}

	apply := func(_ context.Context, _ string, u stackSetUnit) string {
		instances := b.stackInstances[stackSetName]
		for i := range instances {
			if instances[i].Account == u.account && instances[i].Region == u.region {
				b.detectInstanceDrift(&instances[i], time.Now())
			}
		}

		return ""
	}

	return b.admitStackSetOp(context.Background(), stackSetName, "DETECT_DRIFT", prefs, false, units, apply), nil
}

// detectInstanceDrift runs the same per-resource comparison DetectStackDrift uses against the
// instance's provisioned child stack. Caller must hold b.mu.Lock.
func (b *InMemoryBackend) detectInstanceDrift(inst *StackInstance, now time.Time) {
	stackName, ok := b.stackIDIndex[inst.StackID]
	if !ok {
		inst.DriftStatus = driftStatusNotChecked

		return
	}

	stack, ok := b.stacks.Get(stackName)
	if !ok {
		inst.DriftStatus = driftStatusNotChecked

		return
	}

	inst.DriftStatus = driftStatusInSync
	for _, status := range b.compareStackResources(stack) {
		if status != driftStatusInSync {
			inst.DriftStatus = driftStatusDrifted

			break
		}
	}

	inst.LastDriftCheckTimestamp = &now
}

const maxOpsPerStackSet = 1000

func (b *InMemoryBackend) ListStackSetOperations(
	stackSetName string, maxResults int, nextToken string,
) (page.Page[StackSetOperationSummary], error) {
	b.mu.RLock("ListStackSetOperations")
	defer b.mu.RUnlock()
	ops := b.stackSetOperations[stackSetName]
	sorted := make([]*StackSetOperation, 0, len(ops))
	for _, op := range ops {
		sorted = append(sorted, op)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].CreatedAt.Equal(sorted[j].CreatedAt) {
			return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
		}

		return sorted[i].OperationID < sorted[j].OperationID
	})
	summaries := make([]StackSetOperationSummary, 0, len(sorted))
	for _, op := range sorted {
		summaries = append(summaries, StackSetOperationSummary{
			OperationID:  op.OperationID,
			Action:       op.Action,
			Status:       op.Status,
			CreationTime: op.CreatedAt,
			EndTime:      op.EndedAt,
			Preferences:  op.Preferences,
			StatusReason: op.StatusReason,
			FailedCount:  op.FailedCount,
		})
	}

	limit := min(maxResults, cfnListMaxPageSize)

	return page.New(summaries, nextToken, limit, cfnDefaultPageSize), nil
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

	// The SDK's DescribeStackSetOperation error model has a distinct
	// StackSetNotFoundException case (unlike this op's siblings), so an
	// unknown StackSetName must surface that instead of the generic
	// OperationNotFoundException used when the StackSet exists but the
	// operation ID doesn't.
	if !b.stackSets.Has(stackSetName) {
		return nil, fmt.Errorf("%w: %s", ErrStackSetNotFound, stackSetName)
	}

	ops := b.stackSetOperations[stackSetName]
	if ops == nil {
		return nil, fmt.Errorf("%w: %s in %s", ErrOperationNotFound, operationID, stackSetName)
	}
	op, ok := ops[operationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s in %s", ErrOperationNotFound, operationID, stackSetName)
	}

	cp := *op

	return &cp, nil
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
	if op.Status != opStatusRunning {
		return fmt.Errorf("%w: %s (current: %s)", ErrOperationNotRunning, operationID, op.Status)
	}
	run := b.stackSetRuns[operationID]
	if run == nil {
		now := time.Now()
		op.Status = opStatusStopped
		op.EndedAt = &now

		return nil
	}

	run.stop = true
	op.Status = opStatusStopping

	return nil
}

// ListStackSetOperationResults returns per-account/region operation
// results, paginated by MaxResults/NextToken (real query-protocol form
// fields, api_op_ListStackSetOperationResults.go).
func (b *InMemoryBackend) ListStackSetOperationResults(
	stackSetName, operationID string, maxResults int, nextToken string, statuses []string,
) (page.Page[StackSetOperationResult], error) {
	b.mu.RLock("ListStackSetOperationResults")
	defer b.mu.RUnlock()
	if !b.stackSets.Has(stackSetName) {
		return page.Page[StackSetOperationResult]{}, fmt.Errorf("%w: %s", ErrStackSetNotFound, stackSetName)
	}
	if _, ok := b.stackSetOperations[stackSetName][operationID]; !ok {
		return page.Page[StackSetOperationResult]{}, fmt.Errorf(
			"%w: %s in %s", ErrOperationNotFound, operationID, stackSetName,
		)
	}
	results := b.stackSetOpResults[stackSetName][operationID]
	out := make([]StackSetOperationResult, 0, len(results))

	for _, r := range results {
		if len(statuses) == 0 || slices.Contains(statuses, r.Status) {
			out = append(out, r)
		}
	}

	return page.New(out, nextToken, maxResults, cfnDefaultPageSize), nil
}

// ListStackSetAutoDeploymentTargets returns a StackSet's automatic
// deployment targets, paginated by MaxResults/NextToken (real
// query-protocol form fields, api_op_ListStackSetAutoDeploymentTargets.go).
func (b *InMemoryBackend) ListStackSetAutoDeploymentTargets(
	stackSetName string, maxResults int, nextToken string,
) (page.Page[AutoDeploymentTarget], error) {
	b.mu.RLock("ListStackSetAutoDeploymentTargets")
	defer b.mu.RUnlock()
	if !b.stackSets.Has(stackSetName) {
		return page.Page[AutoDeploymentTarget]{}, ErrStackSetNotFound
	}

	byOU := make(map[string]int) // OU ID -> index in targets
	targets := make([]AutoDeploymentTarget, 0)
	for _, inst := range b.stackInstances[stackSetName] {
		// SERVICE_MANAGED instances carry the OU they were deployed through
		// (see resolveInstanceTargets); self-managed instances have none, so
		// fall back to one synthetic target per account.
		ouID := inst.OrganizationalUnitID
		if ouID == "" {
			ouID = inst.Account
		}
		if idx, ok := byOU[ouID]; ok {
			if !slices.Contains(targets[idx].Regions, inst.Region) {
				targets[idx].Regions = append(targets[idx].Regions, inst.Region)
			}

			continue
		}
		byOU[ouID] = len(targets)
		targets = append(targets, AutoDeploymentTarget{
			OrganizationalUnitID: ouID,
			Regions:              []string{inst.Region},
		})
	}

	return page.New(targets, nextToken, maxResults, cfnDefaultPageSize), nil
}

// importOUFor returns the entry of ouIDs enclosing account. Must be called with b.mu held.
func (b *InMemoryBackend) importOUFor(account string, ouIDs []string) (string, error) {
	chain, err := b.orgDirectory.OrganizationalUnitIDsForAccount(account)
	if err != nil {
		return "", fmt.Errorf("resolve organizational unit for account %s: %w", account, err)
	}

	for _, id := range chain {
		if slices.Contains(ouIDs, id) {
			return id, nil
		}
	}

	return "", fmt.Errorf("%w: account %s", ErrAccountNotInOrganizationalUnits, account)
}

// stampImportOUs maps each imported stack to the listed OU enclosing its account. Must be called with b.mu held.
func (b *InMemoryBackend) stampImportOUs(ss *StackSet, stackIDs, ouIDs []string) (map[string]string, error) {
	if len(ouIDs) == 0 {
		return map[string]string{}, nil
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
	stamped := make(map[string]string, len(stackIDs))
	for _, stackID := range stackIDs {
		account, _ := parseStackARN(stackID)
		ou, err := b.importOUFor(account, ouIDs)
		if err != nil {
			return nil, err
		}
		stamped[stackID] = ou
	}

	return stamped, nil
}

// ImportStacksToStackSet adopts stacks into a stack set; with ouIDs each instance is stamped with the
// listed OU that encloses its account.
func (b *InMemoryBackend) ImportStacksToStackSet(
	stackSetName string, stackIDs, ouIDs []string, opOpts ...StackSetOpOption,
) (string, error) {
	prefs, err := resolveOpPreferences(opOpts)
	if err != nil {
		return "", err
	}

	b.mu.Lock("ImportStacksToStackSet")
	defer b.mu.Unlock()
	ss, ok := b.stackSets.Get(stackSetName)
	if !ok {
		return "", ErrStackSetNotFound
	}
	stamped, err := b.stampImportOUs(ss, stackIDs, ouIDs)
	if err != nil {
		return "", err
	}

	units := make([]stackSetUnit, 0, len(stackIDs))
	unitStack := make(map[stackSetUnit]string, len(stackIDs))
	for _, stackID := range stackIDs {
		account, region := parseStackARN(stackID)
		u := stackSetUnit{account: account, region: region, ouID: stamped[stackID]}
		units = append(units, u)
		unitStack[u] = stackID
	}

	apply := func(_ context.Context, opID string, u stackSetUnit) string {
		stackID := unitStack[u]
		for _, inst := range b.stackInstances[stackSetName] {
			if inst.StackID == stackID {
				return ""
			}
		}
		b.stackInstances[stackSetName] = append(b.stackInstances[stackSetName], StackInstance{
			StackSetID:           ss.StackSetID,
			StackSetName:         stackSetName,
			StackID:              stackID,
			Account:              u.account,
			Region:               u.region,
			OrganizationalUnitID: u.ouID,
			Status:               "CURRENT",
			DriftStatus:          driftStatusNotChecked,
			LastOperationID:      opID,
		})

		return ""
	}

	return b.admitStackSetOp(context.Background(), stackSetName, "IMPORT", prefs, false, units, apply), nil
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

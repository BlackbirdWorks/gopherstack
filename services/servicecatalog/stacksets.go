package servicecatalog

import (
	"context"
	"slices"
)

const (
	stackSetSucceeded = "SUCCEEDED"
	dashboardType     = "AWS::CloudWatch::Dashboard"
)

// StackSetPreferences are the StackSet operation preferences taken from ProvisioningPreferences.
type StackSetPreferences struct {
	FailureToleranceCount      *int32
	FailureTolerancePercentage *int32
	MaxConcurrencyCount        *int32
	MaxConcurrencyPercentage   *int32
}

// StackSetRequest describes the StackSet and instances behind a STACKSET product launch.
type StackSetRequest struct {
	Preferences   StackSetPreferences
	Parameters    map[string]string
	Tags          map[string]string
	Name          string
	Template      string
	AdminRole     string
	ExecutionRole string
	Accounts      []string
	Regions       []string
}

// StackSetInstance is the engine's view of a stack instance.
type StackSetInstance struct {
	Account string
	Region  string
	Status  string
	Reason  string
}

// StackSetResult is the outcome of creating a StackSet and its instances.
type StackSetResult struct {
	ID        string
	Status    string
	Reason    string
	Instances []StackSetInstance
}

// StackSetEngine provisions CloudFormation StackSets for STACKSET products.
type StackSetEngine interface {
	CreateStackSet(ctx context.Context, req StackSetRequest) (StackSetResult, error)
	DeleteStackSet(ctx context.Context, name string) error
	StackSetInstances(ctx context.Context, name string) ([]StackSetInstance, error)
}

// ProvisioningPreferences is the ProvisioningPreferences shape; the StackSet fields apply to STACKSET products.
type ProvisioningPreferences struct {
	StackSetFailureToleranceCount      *int32   `json:"StackSetFailureToleranceCount,omitempty"`
	StackSetFailureTolerancePercentage *int32   `json:"StackSetFailureTolerancePercentage,omitempty"`
	StackSetMaxConcurrencyCount        *int32   `json:"StackSetMaxConcurrencyCount,omitempty"`
	StackSetMaxConcurrencyPercentage   *int32   `json:"StackSetMaxConcurrencyPercentage,omitempty"`
	StackSetAccounts                   []string `json:"StackSetAccounts,omitempty"`
	StackSetRegions                    []string `json:"StackSetRegions,omitempty"`
}

const maxPercent = 100

func validCount(v *int32) bool { return v == nil || *v >= 0 }

func validPercent(v *int32) bool { return v == nil || (*v >= 0 && *v <= maxPercent) }

func (p *ProvisioningPreferences) validate() error {
	if p == nil {
		return nil
	}

	if p.StackSetFailureToleranceCount != nil && p.StackSetFailureTolerancePercentage != nil {
		return invalid("specify either StackSetFailureToleranceCount or StackSetFailureTolerancePercentage, not both")
	}

	if p.StackSetMaxConcurrencyCount != nil && p.StackSetMaxConcurrencyPercentage != nil {
		return invalid("specify either StackSetMaxConcurrencyCount or StackSetMaxConcurrencyPercentage, not both")
	}

	if !validCount(p.StackSetFailureToleranceCount) || !validCount(p.StackSetMaxConcurrencyCount) ||
		!validPercent(p.StackSetFailureTolerancePercentage) || !validPercent(p.StackSetMaxConcurrencyPercentage) {
		return invalid("StackSet counts must not be negative and percentages must be between 0 and 100")
	}

	return nil
}

func (p *ProvisioningPreferences) operation() StackSetPreferences {
	if p == nil {
		return StackSetPreferences{}
	}

	return StackSetPreferences{
		FailureToleranceCount:      p.StackSetFailureToleranceCount,
		FailureTolerancePercentage: p.StackSetFailureTolerancePercentage,
		MaxConcurrencyCount:        p.StackSetMaxConcurrencyCount,
		MaxConcurrencyPercentage:   p.StackSetMaxConcurrencyPercentage,
	}
}

// stackSetConfig is the Properties of a STACKSET launch constraint.
type stackSetConfig struct {
	AdminRole     string
	ExecutionRole string
	Accounts      []string
	Regions       []string
}

func parseStackSetConfig(doc map[string]any) *stackSetConfig {
	props, _ := doc["Properties"].(map[string]any)

	return &stackSetConfig{
		AdminRole:     stringField(props, "AdminRole"),
		ExecutionRole: stringField(props, "ExecutionRole"),
		Accounts:      stringList(props["AccountList"]),
		Regions:       stringList(props["RegionList"]),
	}
}

func stringList(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))

	for _, e := range list {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

// targets narrows the constraint's accounts and Regions to the ones named in the preferences.
func (c *stackSetConfig) targets(p *ProvisioningPreferences) ([]string, []string, error) {
	if p == nil {
		return c.Accounts, c.Regions, nil
	}

	accounts, err := narrow("StackSetAccounts", p.StackSetAccounts, c.Accounts)
	if err != nil {
		return nil, nil, err
	}

	regions, err := narrow("StackSetRegions", p.StackSetRegions, c.Regions)
	if err != nil {
		return nil, nil, err
	}

	return accounts, regions, nil
}

func narrow(field string, given, allowed []string) ([]string, error) {
	if len(given) == 0 {
		return allowed, nil
	}

	return subsetOf(field, given, allowed)
}

func subsetOf(field string, given, allowed []string) ([]string, error) {
	for _, g := range given {
		if !slices.Contains(allowed, g) {
			return nil, invalid("%s value %q is not in the STACKSET constraint", field, g)
		}
	}

	return slices.Clone(given), nil
}

func (r StackSetResult) stackInfo() StackInfo {
	st := "CREATE_COMPLETE"
	if r.Status != stackSetSucceeded {
		st = "CREATE_FAILED"
	}

	return StackInfo{ID: r.ID, Status: st, Reason: r.Reason}
}

func (b *InMemoryBackend) stackSetEngine() StackSetEngine {
	b.mu.RLock("stackSetEngine")
	defer b.mu.RUnlock()

	return b.sets
}

// createStackSet runs the CloudFormation StackSet behind a freshly started STACKSET provisioning.
func (b *InMemoryBackend) createStackSet(
	ctx context.Context, lp *launchPlan, pp *ProvisionedProduct, rec *Record,
	in *ProvisionProductInput, template string, params map[string]string,
) *RecordOutputDetail {
	accounts, regions, _ := lp.stackSet.targets(in.ProvisioningPreferences)

	res, err := b.stackSetEngine().CreateStackSet(ctx, StackSetRequest{
		Name: pp.StackName, Template: template, Parameters: params, Tags: rec.Tags,
		AdminRole: lp.stackSet.AdminRole, ExecutionRole: lp.stackSet.ExecutionRole,
		Accounts: accounts, Regions: regions, Preferences: in.ProvisioningPreferences.operation(),
	})

	return b.finish(pp.ID, rec.ID, res.stackInfo(), err, false)
}

func (b *InMemoryBackend) deleteBacking(ctx context.Context, typ, name string) error {
	if typ == ppTypeCFNStackSet {
		eng := b.stackSetEngine()
		if eng == nil {
			return errStackEngineUnavailable
		}

		return eng.DeleteStackSet(ctx, name)
	}

	return b.engineOrNil().DeleteStack(ctx, name)
}

// ListStackInstancesForProvisionedProductInput is the ListStackInstancesForProvisionedProduct request.
type ListStackInstancesForProvisionedProductInput struct {
	ProvisionedProductID string `json:"ProvisionedProductId"`
	Paging
}

// StackInstanceWire is the StackInstance shape.
type StackInstanceWire struct {
	Account             string `json:"Account"`
	Region              string `json:"Region"`
	StackInstanceStatus string `json:"StackInstanceStatus"`
}

// ListStackInstancesForProvisionedProductOutput is the ListStackInstancesForProvisionedProduct response.
type ListStackInstancesForProvisionedProductOutput struct {
	NextPageToken  string              `json:"NextPageToken,omitempty"`
	StackInstances []StackInstanceWire `json:"StackInstances"`
}

// ListStackInstancesForProvisionedProduct lists the stack instances of a CFN_STACKSET provisioned product.
func (b *InMemoryBackend) ListStackInstancesForProvisionedProduct(
	ctx context.Context,
	in *ListStackInstancesForProvisionedProductInput,
) (*ListStackInstancesForProvisionedProductOutput, error) {
	b.mu.RLock("ListStackInstancesForProvisionedProduct")
	pp, err := b.provisionedLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ProvisionedProductID, "")
	eng := b.sets

	var name, typ string
	if err == nil {
		name, typ = pp.StackName, pp.Type
	}

	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	if typ != ppTypeCFNStackSet {
		return nil, invalid("provisioned product %s is not a CFN_STACKSET product", in.ProvisionedProductID)
	}

	if eng == nil {
		return nil, fail(ErrOperationNotSupported, "%v", errStackEngineUnavailable)
	}

	insts, err := eng.StackSetInstances(ctx, name)
	if err != nil {
		return nil, fail(ErrResourceNotFound, "stack set %s: %v", name, err)
	}

	wire := make([]StackInstanceWire, 0, len(insts))
	for _, i := range insts {
		wire = append(wire, StackInstanceWire{Account: i.Account, Region: i.Region, StackInstanceStatus: i.Status})
	}

	pg, err := pageOf(wire, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListStackInstancesForProvisionedProductOutput{StackInstances: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}

// CloudWatchDashboard is the CloudWatchDashboard shape.
type CloudWatchDashboard struct {
	Name string `json:"Name"`
}

// dashboardsOf reports the CloudWatch dashboards the stack of a provisioned product created.
func (b *InMemoryBackend) dashboardsOf(ctx context.Context, stack string) []CloudWatchDashboard {
	out := []CloudWatchDashboard{}

	eng := b.engineOrNil()
	if eng == nil || stack == "" {
		return out
	}

	res, err := eng.StackResources(ctx, stack)
	if err != nil {
		return out
	}

	for _, r := range res {
		if r.Type == dashboardType {
			out = append(out, CloudWatchDashboard{Name: r.PhysicalID})
		}
	}

	return out
}

func (b *InMemoryBackend) checkLaunchRole(ctx context.Context, roleARN string) error {
	b.mu.RLock("checkLaunchRole")
	rc := b.roles
	b.mu.RUnlock()

	if rc == nil || roleARN == "" {
		return nil
	}

	if err := rc.CheckLaunchRole(ctx, roleARN); err != nil {
		return invalid("Service Catalog cannot assume the launch role %s: %v", roleARN, err)
	}

	return nil
}

func (b *InMemoryBackend) checkRules(
	ctx context.Context, lp *launchPlan, info TemplateInfo, params map[string]string,
) error {
	if len(lp.templateRule) == 0 {
		return nil
	}

	b.mu.RLock("checkRules")
	lookup := b.lookup
	b.mu.RUnlock()

	types := make(map[string]string, len(info.Parameters))
	for _, p := range info.Parameters {
		types[p.Key] = p.Type
	}

	return checkTemplateRules(newRuleEnv(ctx, lookup, ruleValues(info, params), types), lp.templateRule)
}

// tagOptionTags are the tags of the active single-valued TagOptions of a launch path.
func (b *InMemoryBackend) tagOptionTags(account string, lp *launchPlan) map[string]string {
	out := map[string]string{}

	for _, s := range b.tagOptionSummariesLocked(account, lp.path, lp.prod.ID) {
		if len(s.Values) == 1 {
			out[s.Key] = s.Values[0]
		}
	}

	return out
}

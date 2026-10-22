package servicecatalog

import (
	"context"
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName      = "servicecatalog"
	signingName      = "servicecatalog"
	defaultRegion    = "us-east-1"
	idSuffixLen      = 13
	maxPageSize      = 20
	maxTags          = 50
	servicePrincipal = "servicecatalog.amazonaws.com"
)

// ErrStackNotFound is returned by a StackEngine when the named stack does not exist.
var ErrStackNotFound = errors.New("stack not found")

// TemplateParameter is a parameter declared by a CloudFormation template.
type TemplateParameter struct {
	Key                   string
	Type                  string
	Default               string
	Description           string
	ConstraintDescription string
	AllowedPattern        string
	AllowedValues         []string
	NoEcho                bool
}

// TemplateOutput is an output declared by a CloudFormation template.
type TemplateOutput struct {
	Key         string
	Description string
}

// TemplateInfo is the validated shape of a CloudFormation template.
type TemplateInfo struct {
	Parameters []TemplateParameter
	Outputs    []TemplateOutput
}

// StackRequest describes a stack to create, update or preview.
type StackRequest struct {
	Parameters       map[string]string
	Tags             map[string]string
	Name             string
	Template         string
	RoleARN          string
	ChangeSetName    string
	NotificationARNs []string
	Update           bool
}

// StackInfo is the engine's view of a stack.
type StackInfo struct {
	ID      string
	Name    string
	Status  string
	Reason  string
	Outputs []Output
}

// StackResource identifies a resource of a stack.
type StackResource struct {
	LogicalID  string
	PhysicalID string
	Type       string
}

// ChangeSetInfo is the engine's view of a change set.
type ChangeSetInfo struct {
	Status  string
	Reason  string
	Changes []PlanChange
}

// StackEngine provisions CloudFormation stacks for provisioned products.
type StackEngine interface {
	FetchTemplate(ctx context.Context, url string) (string, error)
	StackTemplate(ctx context.Context, nameOrID string) (string, error)
	InspectTemplate(ctx context.Context, body string) (TemplateInfo, error)
	CreateStack(ctx context.Context, req StackRequest) (StackInfo, error)
	UpdateStack(ctx context.Context, req StackRequest) (StackInfo, error)
	DeleteStack(ctx context.Context, nameOrID string) error
	DescribeStack(ctx context.Context, nameOrID string) (StackInfo, error)
	StackResources(ctx context.Context, nameOrID string) ([]StackResource, error)
	CreateChangeSet(ctx context.Context, req StackRequest) (ChangeSetInfo, error)
	ExecuteChangeSet(ctx context.Context, stackName, changeSetName string) error
	DeleteChangeSet(ctx context.Context, stackName, changeSetName string) error
}

// OrgDirectory resolves organization nodes and the Organizations service-access toggle.
type OrgDirectory interface {
	AccountsUnder(nodeType, value string) ([]string, error)
	EnableServiceAccess(principal string) error
	DisableServiceAccess(principal string) error
	ServiceAccessEnabled(principal string) (bool, error)
}

// AutomationRunner starts SSM automations for service actions.
type AutomationRunner interface {
	DocumentExists(ctx context.Context, name string) bool
	StartAutomation(ctx context.Context, document, version string, params map[string][]string) (string, error)
}

// BudgetChecker reports whether a budget exists in the caller's account.
type BudgetChecker interface {
	BudgetExists(ctx context.Context, name string) bool
}

// RoleChecker verifies Service Catalog can assume a launch constraint role.
type RoleChecker interface {
	CheckLaunchRole(ctx context.Context, roleARN string) error
}

// InMemoryBackend is the Service Catalog state; one coarse lock guards every table.
type InMemoryBackend struct {
	portfolios    *store.Table[Portfolio]
	products      *store.Table[Product]
	artifacts     *store.Table[Artifact]
	associations  *store.Table[Association]
	principals    *store.Table[Principal]
	constraints   *store.Table[Constraint]
	tagOptions    *store.Table[TagOption]
	tagOptionLink *store.Table[TagOptionLink]
	budgetLinks   *store.Table[BudgetLink]
	actions       *store.Table[ServiceAction]
	actionLinks   *store.Table[ServiceActionLink]
	shares        *store.Table[Share]
	provisioned   *store.Table[ProvisionedProduct]
	records       *store.Table[Record]
	copies        *store.Table[CopyOperation]
	plans         *store.Table[Plan]
	accounts      *store.Table[AccountState]
	registry      *store.Registry
	mu            *lockmetrics.RWMutex
	idem          *idempotency.Memo
	now           func() time.Time
	engine        StackEngine
	org           OrgDirectory
	runner        AutomationRunner
	budgets       BudgetChecker
	sets          StackSetEngine
	roles         RoleChecker
	lookup        ResourceLookup
	accountID     string
	region        string
	dwell         time.Duration
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		idem:      idempotency.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// ServicePrincipal is the service principal Service Catalog assumes launch roles as.
func ServicePrincipal() string { return servicePrincipal }

// Region returns the default Region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the default account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// SetStackEngine wires the CloudFormation engine used to provision products.
func (b *InMemoryBackend) SetStackEngine(e StackEngine) {
	b.mu.Lock("SetStackEngine")
	defer b.mu.Unlock()

	b.engine = e
}

// SetOrgDirectory wires Organizations.
func (b *InMemoryBackend) SetOrgDirectory(o OrgDirectory) {
	b.mu.Lock("SetOrgDirectory")
	defer b.mu.Unlock()

	b.org = o
}

// SetAutomationRunner wires SSM Automation for service actions.
func (b *InMemoryBackend) SetAutomationRunner(r AutomationRunner) {
	b.mu.Lock("SetAutomationRunner")
	defer b.mu.Unlock()

	b.runner = r
}

// SetBudgetChecker wires the Budgets lookup.
func (b *InMemoryBackend) SetBudgetChecker(c BudgetChecker) {
	b.mu.Lock("SetBudgetChecker")
	defer b.mu.Unlock()

	b.budgets = c
}

// SetStackSetEngine wires the CloudFormation StackSet engine used by STACKSET products.
func (b *InMemoryBackend) SetStackSetEngine(e StackSetEngine) {
	b.mu.Lock("SetStackSetEngine")
	defer b.mu.Unlock()

	b.sets = e
}

// SetRoleChecker wires the launch role trust check.
func (b *InMemoryBackend) SetRoleChecker(r RoleChecker) {
	b.mu.Lock("SetRoleChecker")
	defer b.mu.Unlock()

	b.roles = r
}

// SetResourceLookup wires the account resource lookup used by TEMPLATE rule functions.
func (b *InMemoryBackend) SetResourceLookup(l ResourceLookup) {
	b.mu.Lock("SetResourceLookup")
	defer b.mu.Unlock()

	b.lookup = l
}

// SetLifecycleDelay keeps records IN_PROGRESS and copy operations pending for d; zero settles immediately.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.dwell = d
}

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.idem.Clear()
}

func (b *InMemoryBackend) callerAccount(ctx context.Context) string {
	if a := awsmeta.Account(ctx); a != "" {
		return a
	}

	return b.accountID
}

func (b *InMemoryBackend) callerRegion(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return b.region
}

func newID(prefix string) string { return awsid.NewN(prefix, idSuffixLen) }

func (b *InMemoryBackend) portfolioARN(account, region, id string) string {
	return arn.Build("catalog", region, account, "portfolio/"+id)
}

func (b *InMemoryBackend) productARN(account, region, id string) string {
	return arn.Build("catalog", region, account, "product/"+id)
}

func (b *InMemoryBackend) provisionedARN(account, region, name, id string) string {
	return arn.Build(serviceName, region, account, "stack/"+name+"/"+id)
}

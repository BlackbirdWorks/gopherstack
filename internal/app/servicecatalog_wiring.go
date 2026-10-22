package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	budgetsbackend "github.com/blackbirdworks/gopherstack/services/budgets"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	servicecatalogbackend "github.com/blackbirdworks/gopherstack/services/servicecatalog"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

var errNoCFNRegion = errors.New("no CloudFormation backend for the region")

// serviceCatalogStacks provisions Service Catalog products through the CloudFormation backend of the request Region.
type serviceCatalogStacks struct {
	cfn *cfnbackend.Handler
}

type templateFetcher interface {
	FetchS3URL(ctx context.Context, rawURL string) (string, error)
}

type templateValidator interface {
	ValidateTemplate(templateBody string) (*cfnbackend.TemplateSummary, error)
}

func (s *serviceCatalogStacks) backend(ctx context.Context) (cfnbackend.StorageBackend, error) {
	bk := s.cfn.BackendFor(awsmeta.Region(ctx))
	if bk == nil {
		return nil, errNoCFNRegion
	}

	return bk, nil
}

func (s *serviceCatalogStacks) FetchTemplate(ctx context.Context, url string) (string, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return "", err
	}

	f, ok := bk.(templateFetcher)
	if !ok {
		return "", fmt.Errorf("%w: template URLs are not supported", errNoCFNRegion)
	}

	return f.FetchS3URL(ctx, url)
}

func (s *serviceCatalogStacks) StackTemplate(ctx context.Context, nameOrID string) (string, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return "", err
	}

	return bk.GetTemplate(nameOrID)
}

func (s *serviceCatalogStacks) InspectTemplate(
	ctx context.Context,
	body string,
) (servicecatalogbackend.TemplateInfo, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return servicecatalogbackend.TemplateInfo{}, err
	}

	var sum *cfnbackend.TemplateSummary

	if v, ok := bk.(templateValidator); ok {
		sum, err = v.ValidateTemplate(body)
	} else {
		sum, err = bk.GetTemplateSummary(body, "")
	}

	if err != nil {
		return servicecatalogbackend.TemplateInfo{}, err
	}

	tpl, err := cfnbackend.ParseTemplate(body)
	if err != nil {
		return servicecatalogbackend.TemplateInfo{}, err
	}

	info := servicecatalogbackend.TemplateInfo{}

	for _, p := range sum.Parameters {
		info.Parameters = append(info.Parameters, servicecatalogbackend.TemplateParameter{
			Key: p.ParameterKey, Type: p.ParameterType, Default: p.DefaultValue, Description: p.Description,
			ConstraintDescription: p.ConstraintDescription, AllowedPattern: p.AllowedPattern,
			AllowedValues: p.AllowedValues, NoEcho: p.NoEcho,
		})
	}

	for name, o := range tpl.Outputs {
		info.Outputs = append(info.Outputs, servicecatalogbackend.TemplateOutput{Key: name, Description: o.Description})
	}

	return info, nil
}

func scCapabilities() []string {
	return []string{"CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"}
}

func cfnParams(m map[string]string) []cfnbackend.Parameter {
	out := make([]cfnbackend.Parameter, 0, len(m))
	for k, v := range m {
		out = append(out, cfnbackend.Parameter{ParameterKey: k, ParameterValue: v})
	}

	return out
}

func cfnTags(m map[string]string) []cfnbackend.Tag {
	out := make([]cfnbackend.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, cfnbackend.Tag{Key: k, Value: v})
	}

	return out
}

func stackInfoOf(st *cfnbackend.Stack) servicecatalogbackend.StackInfo {
	info := servicecatalogbackend.StackInfo{
		ID:     st.StackID,
		Name:   st.StackName,
		Status: st.StackStatus,
		Reason: st.StackStatusReason,
	}

	for _, o := range st.Outputs {
		info.Outputs = append(
			info.Outputs,
			servicecatalogbackend.Output{Key: o.OutputKey, Value: o.OutputValue, Description: o.Description},
		)
	}

	return info
}

func stackOptions(req servicecatalogbackend.StackRequest) cfnbackend.StackOptions {
	return cfnbackend.StackOptions{
		Capabilities: scCapabilities(), NotificationARNs: req.NotificationARNs,
		Tags: cfnTags(req.Tags), RoleARN: req.RoleARN,
	}
}

func (s *serviceCatalogStacks) CreateStack(
	ctx context.Context,
	req servicecatalogbackend.StackRequest,
) (servicecatalogbackend.StackInfo, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return servicecatalogbackend.StackInfo{}, err
	}

	st, err := bk.CreateStack(ctx, req.Name, req.Template, cfnParams(req.Parameters), stackOptions(req))
	if err != nil {
		return servicecatalogbackend.StackInfo{}, err
	}

	return stackInfoOf(st), nil
}

func (s *serviceCatalogStacks) UpdateStack(
	ctx context.Context,
	req servicecatalogbackend.StackRequest,
) (servicecatalogbackend.StackInfo, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return servicecatalogbackend.StackInfo{}, err
	}

	st, err := bk.UpdateStack(ctx, req.Name, req.Template, cfnParams(req.Parameters), stackOptions(req))
	if err != nil {
		return servicecatalogbackend.StackInfo{}, err
	}

	return stackInfoOf(st), nil
}

func (s *serviceCatalogStacks) DeleteStack(ctx context.Context, nameOrID string) error {
	bk, err := s.backend(ctx)
	if err != nil {
		return err
	}

	return bk.DeleteStack(ctx, nameOrID)
}

func (s *serviceCatalogStacks) DescribeStack(
	ctx context.Context,
	nameOrID string,
) (servicecatalogbackend.StackInfo, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return servicecatalogbackend.StackInfo{}, err
	}

	st, err := bk.DescribeStack(nameOrID)
	if err != nil {
		return servicecatalogbackend.StackInfo{}, fmt.Errorf("%w: %w", servicecatalogbackend.ErrStackNotFound, err)
	}

	return stackInfoOf(st), nil
}

func (s *serviceCatalogStacks) StackResources(
	ctx context.Context,
	nameOrID string,
) ([]servicecatalogbackend.StackResource, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return nil, err
	}

	res, err := bk.DescribeStackResources(nameOrID)
	if err != nil {
		return nil, err
	}

	out := make([]servicecatalogbackend.StackResource, 0, len(res))
	for _, r := range res {
		out = append(
			out,
			servicecatalogbackend.StackResource{LogicalID: r.LogicalID, PhysicalID: r.PhysicalID, Type: r.Type},
		)
	}

	return out, nil
}

func (s *serviceCatalogStacks) CreateChangeSet(
	ctx context.Context,
	req servicecatalogbackend.StackRequest,
) (servicecatalogbackend.ChangeSetInfo, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return servicecatalogbackend.ChangeSetInfo{}, err
	}

	csType := "CREATE"
	if req.Update {
		csType = "UPDATE"
	}

	cs, err := bk.CreateChangeSet(ctx, req.Name, req.ChangeSetName, req.Template, "", cfnParams(req.Parameters),
		scCapabilities(), cfnTags(req.Tags), cfnbackend.CreateChangeSetOptions{
			ChangeSetType: csType, RoleARN: req.RoleARN, NotificationARNs: req.NotificationARNs,
		})
	if err != nil {
		return servicecatalogbackend.ChangeSetInfo{}, err
	}

	info := servicecatalogbackend.ChangeSetInfo{Status: cs.Status, Reason: cs.StatusReason}

	for _, c := range cs.Changes {
		info.Changes = append(info.Changes, servicecatalogbackend.PlanChange{
			Action: c.ResourceChange.Action, LogicalResourceID: c.ResourceChange.LogicalID,
			PhysicalID: c.ResourceChange.PhysicalID, ResourceType: c.ResourceChange.ResourceType,
			Replacement: c.ResourceChange.Replacement,
		})
	}

	return info, nil
}

func (s *serviceCatalogStacks) ExecuteChangeSet(ctx context.Context, stackName, changeSetName string) error {
	bk, err := s.backend(ctx)
	if err != nil {
		return err
	}

	return bk.ExecuteChangeSet(ctx, stackName, changeSetName, false, false)
}

func (s *serviceCatalogStacks) DeleteChangeSet(ctx context.Context, stackName, changeSetName string) error {
	bk, err := s.backend(ctx)
	if err != nil {
		return err
	}

	return bk.DeleteChangeSet(stackName, changeSetName)
}

type orgAPI interface {
	ListAccounts() ([]*organizationsbackend.Account, error)
	ResolveAccountIDsUnderParent(parentID string) ([]string, error)
	EnableAWSServiceAccess(servicePrincipal string) error
	DisableAWSServiceAccess(servicePrincipal string) error
	ListAWSServiceAccessForOrganization() ([]organizationsbackend.EnabledServicePrincipal, error)
}

// serviceCatalogOrg resolves organization nodes and the trusted-access toggle through the Organizations backend.
type serviceCatalogOrg struct{ org orgAPI }

func (o *serviceCatalogOrg) AccountsUnder(nodeType, value string) ([]string, error) {
	switch nodeType {
	case "ACCOUNT":
		return []string{value}, nil
	case "ORGANIZATIONAL_UNIT":
		return o.org.ResolveAccountIDsUnderParent(value)
	default:
		accounts, err := o.org.ListAccounts()
		if err != nil {
			return nil, err
		}

		ids := make([]string, 0, len(accounts))
		for _, a := range accounts {
			ids = append(ids, a.ID)
		}

		return ids, nil
	}
}

func (o *serviceCatalogOrg) EnableServiceAccess(principal string) error {
	return o.org.EnableAWSServiceAccess(principal)
}

func (o *serviceCatalogOrg) DisableServiceAccess(principal string) error {
	return o.org.DisableAWSServiceAccess(principal)
}

func (o *serviceCatalogOrg) ServiceAccessEnabled(principal string) (bool, error) {
	enabled, err := o.org.ListAWSServiceAccessForOrganization()
	if err != nil {
		return false, err
	}

	for _, e := range enabled {
		if e.ServicePrincipal == principal {
			return true, nil
		}
	}

	return false, nil
}

// serviceCatalogAutomation starts SSM automations for service actions.
type serviceCatalogAutomation struct{ ssm ssmbackend.StorageBackend }

func (a *serviceCatalogAutomation) DocumentExists(ctx context.Context, name string) bool {
	_, err := a.ssm.GetDocument(ctx, &ssmbackend.GetDocumentInput{Name: name})

	return err == nil
}

func (a *serviceCatalogAutomation) StartAutomation(
	ctx context.Context,
	document, version string,
	params map[string][]string,
) (string, error) {
	out, err := a.ssm.StartAutomationExecution(ctx, &ssmbackend.StartAutomationExecutionInput{
		DocumentName: document, DocumentVersion: version, Parameters: params,
	})
	if err != nil {
		return "", err
	}

	return out.AutomationExecutionID, nil
}

// serviceCatalogBudgets answers whether a budget exists in the caller's account.
type serviceCatalogBudgets struct {
	budgets *budgetsbackend.InMemoryBackend
}

func (b *serviceCatalogBudgets) BudgetExists(ctx context.Context, name string) bool {
	_, err := b.budgets.GetBudget(ctx, awsmeta.Account(ctx), name, false)

	return err == nil
}

// wireServiceCatalogEngine connects Service Catalog to CloudFormation, which is created after the other services.
func wireServiceCatalogEngine(scReg, cfnReg service.Registerable) {
	scH, ok := scReg.(*servicecatalogbackend.Handler)
	if !ok {
		return
	}

	if cfnH, isCFN := cfnReg.(*cfnbackend.Handler); isCFN {
		stacks := &serviceCatalogStacks{cfn: cfnH}
		scH.Backend.SetStackEngine(stacks)
		scH.Backend.SetStackSetEngine(stacks)
	}
}

// wireServiceCatalog connects Service Catalog to Organizations, SSM Automation and Budgets.
func wireServiceCatalog(byName map[string]service.Registerable) {
	scH, ok := byName["ServiceCatalog"].(*servicecatalogbackend.Handler)
	if !ok {
		return
	}

	if orgH, isOrg := byName["Organizations"].(*organizationsbackend.Handler); isOrg {
		if api, canAPI := orgH.Backend.(orgAPI); canAPI {
			scH.Backend.SetOrgDirectory(&serviceCatalogOrg{org: api})
		}
	}

	if ssmH, isSSM := byName["SSM"].(*ssmbackend.Handler); isSSM {
		scH.Backend.SetAutomationRunner(&serviceCatalogAutomation{ssm: ssmH.Backend})
	}

	if ec2H, isEC2 := byName["EC2"].(*ec2backend.Handler); isEC2 {
		scH.Backend.SetResourceLookup(serviceCatalogResources{ec2: ec2H.Backend})
	}

	if bH, isBudgets := byName["Budgets"].(*budgetsbackend.Handler); isBudgets {
		scH.Backend.SetBudgetChecker(&serviceCatalogBudgets{budgets: bH.Backend})
	}
}

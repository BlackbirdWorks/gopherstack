package ecs

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ----- Handler: ListAccountSettings -----

type listAccountSettingsInput struct {
	Name         string `json:"name,omitempty"`
	PrincipalArn string `json:"principalArn,omitempty"`
	NextToken    string `json:"nextToken,omitempty"`
	MaxResults   int    `json:"maxResults,omitempty"`
}

type listAccountSettingsOutput struct {
	NextToken string               `json:"nextToken,omitempty"`
	Settings  []accountSettingView `json:"settings"`
}

func (h *Handler) handleListAccountSettings(
	_ context.Context,
	in *listAccountSettingsInput,
) (*listAccountSettingsOutput, error) {
	settings, err := h.Backend.ListAccountSettings(in.Name, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	views := make([]accountSettingView, 0, len(settings))
	for _, s := range settings {
		views = append(views, accountSettingView(s))
	}

	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })

	p := page.New(views, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listAccountSettingsOutput{Settings: p.Data, NextToken: p.Next}, nil
}

// ----- Handler: PutAccountSetting -----

type putAccountSettingInput struct {
	Name         string `json:"name"`
	Value        string `json:"value"`
	PrincipalArn string `json:"principalArn,omitempty"`
}

type putAccountSettingOutput struct {
	Setting accountSettingView `json:"setting"`
}

func (h *Handler) handlePutAccountSetting(
	_ context.Context,
	in *putAccountSettingInput,
) (*putAccountSettingOutput, error) {
	setting, err := h.Backend.PutAccountSetting(in.Name, in.Value, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	return &putAccountSettingOutput{Setting: accountSettingView{
		Name:         setting.Name,
		Value:        setting.Value,
		PrincipalArn: setting.PrincipalArn,
	}}, nil
}

// ----- Handler: PutAccountSettingDefault -----

type putAccountSettingDefaultInput struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type putAccountSettingDefaultOutput struct {
	Setting accountSettingView `json:"setting"`
}

func (h *Handler) handlePutAccountSettingDefault(
	_ context.Context,
	in *putAccountSettingDefaultInput,
) (*putAccountSettingDefaultOutput, error) {
	setting, err := h.Backend.PutAccountSettingDefault(in.Name, in.Value)
	if err != nil {
		return nil, err
	}

	return &putAccountSettingDefaultOutput{Setting: accountSettingView{
		Name:  setting.Name,
		Value: setting.Value,
	}}, nil
}

// ----- Handler: ListAttributes -----

type listAttributesInput struct {
	Cluster       string `json:"cluster,omitempty"`
	TargetType    string `json:"targetType,omitempty"`
	AttributeName string `json:"attributeName,omitempty"`
	TargetID      string `json:"targetId,omitempty"`
	NextToken     string `json:"nextToken,omitempty"`
	MaxResults    int    `json:"maxResults,omitempty"`
}

type listAttributesOutput struct {
	NextToken  string           `json:"nextToken,omitempty"`
	Attributes []attributeInput `json:"attributes"`
}

func (h *Handler) handleListAttributes(
	_ context.Context,
	in *listAttributesInput,
) (*listAttributesOutput, error) {
	attrs, err := h.Backend.ListAttributes(in.Cluster, in.TargetType, in.AttributeName, in.TargetID)
	if err != nil {
		return nil, err
	}

	views := make([]attributeInput, 0, len(attrs))
	for _, a := range attrs {
		views = append(views, attributeInput(a))
	}

	p := page.New(views, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listAttributesOutput{Attributes: p.Data, NextToken: p.Next}, nil
}

// ----- Handler: PutAttributes -----

type putAttributesInput struct {
	Cluster    string           `json:"cluster,omitempty"`
	Attributes []attributeInput `json:"attributes"`
}

type putAttributesOutput struct {
	Attributes []attributeInput `json:"attributes"`
}

func (h *Handler) handlePutAttributes(
	_ context.Context,
	in *putAttributesInput,
) (*putAttributesOutput, error) {
	attrs := make([]Attribute, 0, len(in.Attributes))
	for _, a := range in.Attributes {
		attrs = append(attrs, Attribute(a))
	}

	created, err := h.Backend.PutAttributes(in.Cluster, attrs)
	if err != nil {
		return nil, err
	}

	views := make([]attributeInput, 0, len(created))
	for _, a := range created {
		views = append(views, attributeInput(a))
	}

	return &putAttributesOutput{Attributes: views}, nil
}

// ----- Handler: PutClusterCapacityProviders -----

// cpStrategyItemInput is a short alias for capacity provider strategy item (for struct layout).
type cpStrategyItemInput struct {
	CapacityProvider string `json:"capacityProvider"`
	Base             int    `json:"base,omitempty"`
	Weight           int    `json:"weight,omitempty"`
}

// capacityProviderStrategyItemInput is kept for backward compat with existing handler code.
type capacityProviderStrategyItemInput = cpStrategyItemInput

type clusterSettingView struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type putClusterCapacityProvidersInput struct {
	Cluster                         string                              `json:"cluster,omitempty"`
	CapacityProviders               []string                            `json:"capacityProviders"`
	DefaultCapacityProviderStrategy []capacityProviderStrategyItemInput `json:"defaultCapacityProviderStrategy,omitempty"`
}

type putClusterCapacityProvidersOutput struct {
	Cluster clusterView `json:"cluster"`
}

func (h *Handler) handlePutClusterCapacityProviders(
	_ context.Context,
	in *putClusterCapacityProvidersInput,
) (*putClusterCapacityProvidersOutput, error) {
	strategy := make([]CapacityProviderStrategyItem, 0, len(in.DefaultCapacityProviderStrategy))
	for _, item := range in.DefaultCapacityProviderStrategy {
		strategy = append(strategy, CapacityProviderStrategyItem(item))
	}

	cluster, err := h.Backend.PutClusterCapacityProviders(
		in.Cluster,
		in.CapacityProviders,
		strategy,
	)
	if err != nil {
		return nil, err
	}

	return &putClusterCapacityProvidersOutput{Cluster: toClusterView(*cluster)}, nil
}

// ----- Handler: UpdateClusterSettings -----

type updateClusterSettingsInput struct {
	Cluster  string               `json:"cluster,omitempty"`
	Settings []clusterSettingView `json:"settings"`
}

type updateClusterSettingsOutput struct {
	Cluster clusterView `json:"cluster"`
}

func (h *Handler) handleUpdateClusterSettings(
	_ context.Context,
	in *updateClusterSettingsInput,
) (*updateClusterSettingsOutput, error) {
	settings := make([]ClusterSetting, 0, len(in.Settings))
	for _, s := range in.Settings {
		settings = append(settings, ClusterSetting(s))
	}

	cluster, err := h.Backend.UpdateClusterSettings(in.Cluster, settings)
	if err != nil {
		return nil, err
	}

	return &updateClusterSettingsOutput{Cluster: toClusterView(*cluster)}, nil
}

// ----- Handler: UpdateContainerAgent -----

type updateContainerAgentInput struct {
	Cluster           string `json:"cluster,omitempty"`
	ContainerInstance string `json:"containerInstance"`
}

type updateContainerAgentOutput struct {
	ContainerInstance containerInstanceView `json:"containerInstance"`
}

func (h *Handler) handleUpdateContainerAgent(
	_ context.Context,
	in *updateContainerAgentInput,
) (*updateContainerAgentOutput, error) {
	ci, err := h.Backend.UpdateContainerAgent(in.Cluster, in.ContainerInstance)
	if err != nil {
		return nil, err
	}

	return &updateContainerAgentOutput{ContainerInstance: toContainerInstanceView(*ci)}, nil
}

// ----- Handler: UpdateExpressGatewayService -----

type updateExpressGatewayServiceInput struct {
	ServiceArn            string `json:"serviceArn"`
	ExecutionRoleArn      string `json:"executionRoleArn,omitempty"`
	InfrastructureRoleArn string `json:"infrastructureRoleArn,omitempty"`
}

type updateExpressGatewayServiceOutput struct {
	Service expressGatewayServiceView `json:"service"`
}

func (h *Handler) handleUpdateExpressGatewayService(
	_ context.Context,
	in *updateExpressGatewayServiceInput,
) (*updateExpressGatewayServiceOutput, error) {
	svc, err := h.Backend.UpdateExpressGatewayService(UpdateExpressGatewayServiceInput{
		ServiceArn:            in.ServiceArn,
		ExecutionRoleArn:      in.ExecutionRoleArn,
		InfrastructureRoleArn: in.InfrastructureRoleArn,
	})
	if err != nil {
		return nil, err
	}

	return &updateExpressGatewayServiceOutput{Service: toExpressGatewayServiceView(*svc)}, nil
}

// ----- Handler: GetTaskProtection -----

type taskProtectionView struct {
	ExpirationDate    *float64 `json:"expirationDate,omitempty"`
	TaskArn           string   `json:"taskArn"`
	ProtectionEnabled bool     `json:"protectionEnabled"`
}

func toTaskProtectionView(tp TaskProtection) taskProtectionView {
	v := taskProtectionView{
		TaskArn:           tp.TaskArn,
		ProtectionEnabled: tp.ProtectionEnabled,
	}

	if tp.ExpirationDate != nil {
		ts := float64(tp.ExpirationDate.Unix())
		v.ExpirationDate = &ts
	}

	return v
}

type getTaskProtectionInput struct {
	Cluster string   `json:"cluster,omitempty"`
	Tasks   []string `json:"tasks,omitempty"`
}

type getTaskProtectionOutput struct {
	ProtectedTasks []taskProtectionView `json:"protectedTasks"`
	Failures       []failureView        `json:"failures"`
}

func (h *Handler) handleGetTaskProtection(
	_ context.Context,
	in *getTaskProtectionInput,
) (*getTaskProtectionOutput, error) {
	protections, failures, err := h.Backend.GetTaskProtection(in.Cluster, in.Tasks)
	if err != nil {
		return nil, err
	}

	views := make([]taskProtectionView, 0, len(protections))
	for _, tp := range protections {
		views = append(views, toTaskProtectionView(tp))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &getTaskProtectionOutput{ProtectedTasks: views, Failures: failViews}, nil
}

// ----- Handler: UpdateTaskProtection -----

type updateTaskProtectionInput struct {
	ExpiresInMinutes  *int     `json:"expiresInMinutes,omitempty"`
	Cluster           string   `json:"cluster,omitempty"`
	Tasks             []string `json:"tasks"`
	ProtectionEnabled bool     `json:"protectionEnabled"`
}

type updateTaskProtectionOutput struct {
	ProtectedTasks []taskProtectionView `json:"protectedTasks"`
	Failures       []failureView        `json:"failures"`
}

func (h *Handler) handleUpdateTaskProtection(
	_ context.Context,
	in *updateTaskProtectionInput,
) (*updateTaskProtectionOutput, error) {
	protections, failures, err := h.Backend.UpdateTaskProtection(
		in.Cluster, in.Tasks, in.ProtectionEnabled, in.ExpiresInMinutes,
	)
	if err != nil {
		return nil, err
	}

	views := make([]taskProtectionView, 0, len(protections))
	for _, tp := range protections {
		views = append(views, toTaskProtectionView(tp))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &updateTaskProtectionOutput{ProtectedTasks: views, Failures: failViews}, nil
}

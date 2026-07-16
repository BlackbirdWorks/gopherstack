package ecs

import (
	"context"
	"sort"
	"strings"
)

// ----- Cluster handlers -----

type createClusterInput struct {
	ClusterName                     string                `json:"clusterName"`
	Settings                        []clusterSettingView  `json:"settings,omitempty"`
	CapacityProviders               []string              `json:"capacityProviders,omitempty"`
	DefaultCapacityProviderStrategy []cpStrategyItemInput `json:"defaultCapacityProviderStrategy,omitempty"`
	Tags                            []Tag                 `json:"tags,omitempty"`
}

type createClusterOutput struct {
	Cluster clusterView `json:"cluster"`
}

func (h *Handler) handleCreateCluster(
	_ context.Context,
	in *createClusterInput,
) (*createClusterOutput, error) {
	settings := make([]ClusterSetting, 0, len(in.Settings))
	for _, s := range in.Settings {
		settings = append(settings, ClusterSetting(s))
	}

	cluster, err := h.Backend.CreateCluster(CreateClusterInput{
		ClusterName:                     in.ClusterName,
		Settings:                        settings,
		CapacityProviders:               in.CapacityProviders,
		DefaultCapacityProviderStrategy: toCPStrategyItems(in.DefaultCapacityProviderStrategy),
		Tags:                            in.Tags,
	})
	if err != nil {
		return nil, err
	}

	view := toClusterView(*cluster)
	// CreateCluster has no `include` gating (unlike DescribeClusters): the
	// tags just supplied are echoed back on the created resource.
	if len(in.Tags) > 0 {
		view.Tags = in.Tags
	}

	return &createClusterOutput{Cluster: view}, nil
}

type listClustersInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listClustersOutput struct {
	NextToken   string   `json:"nextToken,omitempty"`
	ClusterArns []string `json:"clusterArns"`
}

func (h *Handler) handleListClusters(
	_ context.Context,
	in *listClustersInput,
) (*listClustersOutput, error) {
	clusters, err := h.Backend.ListClusters()
	if err != nil {
		return nil, err
	}

	arns := make([]string, 0, len(clusters))
	for _, c := range clusters {
		arns = append(arns, c.ClusterArn)
	}

	sort.Strings(arns)

	arns, nextToken := applyNextTokenSlice(arns, in.NextToken, in.MaxResults)

	return &listClustersOutput{ClusterArns: arns, NextToken: nextToken}, nil
}

type describeClustersInput struct {
	Clusters []string `json:"clusters"`
	Include  []string `json:"include,omitempty"`
}

type describeClustersOutput struct {
	Clusters []clusterView `json:"clusters"`
	Failures []failureView `json:"failures"`
}

// describeClusterIncludeTags is the AWS-defined `include` value that requests
// resource tags be returned alongside each Cluster (see also
// describeTaskIncludeTags for the equivalent DescribeTaskDefinition option).
const describeClusterIncludeTags = "TAGS"

func (h *Handler) handleDescribeClusters(
	_ context.Context,
	in *describeClustersInput,
) (*describeClustersOutput, error) {
	clusters, failures, err := h.Backend.DescribeClusters(in.Clusters)
	if err != nil {
		return nil, err
	}

	wantTags := false

	for _, opt := range in.Include {
		if strings.EqualFold(opt, describeClusterIncludeTags) {
			wantTags = true

			break
		}
	}

	views := make([]clusterView, 0, len(clusters))
	for _, c := range clusters {
		v := toClusterView(c)

		if wantTags {
			tags, terr := h.Backend.ListTagsForResource(c.ClusterArn)
			if terr != nil {
				return nil, terr
			}

			v.Tags = tags
		}

		views = append(views, v)
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeClustersOutput{Clusters: views, Failures: failViews}, nil
}

type deleteClusterInput struct {
	Cluster string `json:"cluster"`
}

type deleteClusterOutput struct {
	Cluster clusterView `json:"cluster"`
}

func (h *Handler) handleDeleteCluster(
	_ context.Context,
	in *deleteClusterInput,
) (*deleteClusterOutput, error) {
	cluster, err := h.Backend.DeleteCluster(in.Cluster)
	if err != nil {
		return nil, err
	}

	return &deleteClusterOutput{Cluster: toClusterView(*cluster)}, nil
}

// ----- Shared view/failure types (primary caller: DescribeClusters) -----

// failureView is the shared AWS "failures" wire shape returned alongside
// batch describe/list/delete operations across nearly every ECS resource
// family (clusters, services, tasks, container instances, capacity
// providers, task definitions, service deployments, task protection, and
// daemon operations). DescribeClusters is its first caller in the original
// handler.go, so it lives here; every other family references it cross-file.
type failureView struct {
	Arn    string `json:"arn,omitempty"`
	Detail string `json:"detail,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type clusterView struct {
	ClusterArn                        string                `json:"clusterArn"`
	ClusterName                       string                `json:"clusterName"`
	Status                            string                `json:"status"`
	DefaultCapacityProviderStrategy   []cpStrategyItemInput `json:"defaultCapacityProviderStrategy"`
	Settings                          []clusterSettingView  `json:"settings,omitempty"`
	CapacityProviders                 []string              `json:"capacityProviders"`
	Tags                              []Tag                 `json:"tags,omitempty"`
	CreatedAt                         float64               `json:"createdAt"`
	ActiveServicesCount               int                   `json:"activeServicesCount"`
	PendingTasksCount                 int                   `json:"pendingTasksCount"`
	RegisteredContainerInstancesCount int                   `json:"registeredContainerInstancesCount"`
	RunningTasksCount                 int                   `json:"runningTasksCount"`
}

func toClusterView(c Cluster) clusterView {
	v := clusterView{
		ClusterArn:                        c.ClusterArn,
		ClusterName:                       c.ClusterName,
		Status:                            c.Status,
		CreatedAt:                         float64(c.CreatedAt.Unix()),
		ActiveServicesCount:               c.ActiveServicesCount,
		PendingTasksCount:                 c.PendingTasksCount,
		RegisteredContainerInstancesCount: c.RegisteredContainerInstancesCount,
		RunningTasksCount:                 c.RunningTasksCount,
		DefaultCapacityProviderStrategy:   []cpStrategyItemInput{},
	}

	if c.CapacityProviders != nil {
		v.CapacityProviders = c.CapacityProviders
	} else {
		v.CapacityProviders = []string{}
	}

	for _, item := range c.DefaultCapacityProviderStrategy {
		v.DefaultCapacityProviderStrategy = append(v.DefaultCapacityProviderStrategy,
			cpStrategyItemInput(item))
	}

	for _, s := range c.Settings {
		v.Settings = append(v.Settings, clusterSettingView(s))
	}

	return v
}

// toCPStrategyItems converts handler cpStrategyItemInput to backend CapacityProviderStrategyItem.
func toCPStrategyItems(in []cpStrategyItemInput) []CapacityProviderStrategyItem {
	if len(in) == 0 {
		return nil
	}

	out := make([]CapacityProviderStrategyItem, len(in))
	for i, item := range in {
		out[i] = CapacityProviderStrategyItem(item)
	}

	return out
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

// ----- UpdateCluster -----

type updateClusterInput struct {
	Cluster                         string                `json:"cluster"`
	CapacityProviders               []string              `json:"capacityProviders,omitempty"`
	DefaultCapacityProviderStrategy []cpStrategyItemInput `json:"defaultCapacityProviderStrategy,omitempty"`
	Settings                        []clusterSettingView  `json:"settings,omitempty"`
}

type updateClusterOutput struct {
	Cluster clusterView `json:"cluster"`
}

func (h *Handler) handleUpdateCluster(
	_ context.Context,
	in *updateClusterInput,
) (*updateClusterOutput, error) {
	input := UpdateClusterInput{
		Cluster: in.Cluster,
	}

	if in.CapacityProviders != nil {
		input.CapacityProviders = in.CapacityProviders
	}

	for _, item := range in.DefaultCapacityProviderStrategy {
		input.DefaultCapacityProviderStrategy = append(
			input.DefaultCapacityProviderStrategy,
			CapacityProviderStrategyItem(item),
		)
	}

	for _, s := range in.Settings {
		input.Settings = append(input.Settings, ClusterSetting(s))
	}

	c, err := h.Backend.UpdateCluster(input)
	if err != nil {
		return nil, err
	}

	return &updateClusterOutput{Cluster: toClusterView(*c)}, nil
}

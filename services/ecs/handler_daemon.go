package ecs

// handler_daemon.go implements the real ECS Managed Daemon operations:
// CreateDaemon, DeleteDaemon, DescribeDaemon, UpdateDaemon, ListDaemons,
// DescribeDaemonDeployments, ListDaemonDeployments, DescribeDaemonRevisions,
// RegisterDaemonTaskDefinition, DescribeDaemonTaskDefinition,
// DeleteDaemonTaskDefinition, and ListDaemonTaskDefinitions.
//
// These back onto real, typed backend state (see backend_daemon.go) rather
// than returning fixed placeholder values.

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// float64Unix converts a time.Time to Unix-epoch-seconds, matching the AWS
// JSON-protocol timestamp wire format used throughout this package.
func float64Unix(t time.Time) float64 {
	return float64(t.Unix())
}

// ----- Shared view types -----

type daemonAlarmConfigurationInput struct {
	AlarmNames []string `json:"alarmNames,omitempty"`
	Enable     bool     `json:"enable,omitempty"`
}

type daemonDeploymentConfigurationInput struct {
	Alarms            *daemonAlarmConfigurationInput `json:"alarms,omitempty"`
	BakeTimeInMinutes int                            `json:"bakeTimeInMinutes,omitempty"`
	DrainPercent      float64                        `json:"drainPercent,omitempty"`
}

func toDaemonDeploymentConfiguration(in *daemonDeploymentConfigurationInput) *DaemonDeploymentConfiguration {
	if in == nil {
		return nil
	}

	cfg := &DaemonDeploymentConfiguration{
		BakeTimeInMinutes: in.BakeTimeInMinutes,
		DrainPercent:      in.DrainPercent,
	}

	if in.Alarms != nil {
		cfg.Alarms = &DaemonAlarmConfiguration{
			AlarmNames: in.Alarms.AlarmNames,
			Enable:     in.Alarms.Enable,
		}
	}

	return cfg
}

type daemonAlarmConfigurationView struct {
	AlarmNames []string `json:"alarmNames,omitempty"`
	Enable     bool     `json:"enable,omitempty"`
}

type daemonDeploymentConfigurationView struct {
	Alarms            *daemonAlarmConfigurationView `json:"alarms,omitempty"`
	BakeTimeInMinutes int                           `json:"bakeTimeInMinutes,omitempty"`
	DrainPercent      float64                       `json:"drainPercent,omitempty"`
}

func toDaemonDeploymentConfigurationView(cfg *DaemonDeploymentConfiguration) *daemonDeploymentConfigurationView {
	if cfg == nil {
		return nil
	}

	v := &daemonDeploymentConfigurationView{
		BakeTimeInMinutes: cfg.BakeTimeInMinutes,
		DrainPercent:      cfg.DrainPercent,
	}

	if cfg.Alarms != nil {
		v.Alarms = &daemonAlarmConfigurationView{
			AlarmNames: cfg.Alarms.AlarmNames,
			Enable:     cfg.Alarms.Enable,
		}
	}

	return v
}

// ----- Handler: CreateDaemon -----

type createDaemonInput struct {
	DeploymentConfiguration *daemonDeploymentConfigurationInput `json:"deploymentConfiguration,omitempty"`
	CapacityProviderArns    []string                            `json:"capacityProviderArns"`
	DaemonName              string                              `json:"daemonName"`
	DaemonTaskDefinitionArn string                              `json:"daemonTaskDefinitionArn"`
	ClientToken             string                              `json:"clientToken,omitempty"`
	ClusterArn              string                              `json:"clusterArn,omitempty"`
	PropagateTags           string                              `json:"propagateTags,omitempty"`
	Tags                    []tagInput                          `json:"tags,omitempty"`
	EnableECSManagedTags    bool                                `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand    bool                                `json:"enableExecuteCommand,omitempty"`
}

type createDaemonOutput struct {
	DaemonArn     string  `json:"daemonArn,omitempty"`
	DeploymentArn string  `json:"deploymentArn,omitempty"`
	Status        string  `json:"status,omitempty"`
	CreatedAt     float64 `json:"createdAt,omitempty"`
}

func tagsFromInput(in []tagInput) []Tag {
	tags := make([]Tag, 0, len(in))
	for _, t := range in {
		tags = append(tags, Tag(t))
	}

	return tags
}

func (h *Handler) handleCreateDaemon(_ context.Context, in *createDaemonInput) (*createDaemonOutput, error) {
	d, err := h.Backend.CreateDaemon(CreateDaemonInput{
		DaemonName:              in.DaemonName,
		ClusterArn:              in.ClusterArn,
		DaemonTaskDefinitionArn: in.DaemonTaskDefinitionArn,
		CapacityProviderArns:    in.CapacityProviderArns,
		DeploymentConfiguration: toDaemonDeploymentConfiguration(in.DeploymentConfiguration),
		PropagateTags:           in.PropagateTags,
		EnableECSManagedTags:    in.EnableECSManagedTags,
		EnableExecuteCommand:    in.EnableExecuteCommand,
		Tags:                    tagsFromInput(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return &createDaemonOutput{
		CreatedAt:     float64Unix(d.CreatedAt),
		DaemonArn:     d.DaemonArn,
		DeploymentArn: d.DeploymentArn,
		Status:        d.Status,
	}, nil
}

// ----- Handler: DeleteDaemon -----

type deleteDaemonInput struct {
	DaemonArn string `json:"daemonArn"`
}

type deleteDaemonOutput struct {
	DaemonArn     string  `json:"daemonArn,omitempty"`
	DeploymentArn string  `json:"deploymentArn,omitempty"`
	Status        string  `json:"status,omitempty"`
	CreatedAt     float64 `json:"createdAt,omitempty"`
	UpdatedAt     float64 `json:"updatedAt,omitempty"`
}

func (h *Handler) handleDeleteDaemon(_ context.Context, in *deleteDaemonInput) (*deleteDaemonOutput, error) {
	d, err := h.Backend.DeleteDaemon(in.DaemonArn)
	if err != nil {
		return nil, err
	}

	return &deleteDaemonOutput{
		CreatedAt:     float64Unix(d.CreatedAt),
		UpdatedAt:     float64Unix(d.UpdatedAt),
		DaemonArn:     d.DaemonArn,
		DeploymentArn: d.DeploymentArn,
		Status:        d.Status,
	}, nil
}

// ----- Handler: DescribeDaemon -----

type describeDaemonInput struct {
	DaemonArn string `json:"daemonArn"`
}

type daemonRevisionDetailView struct {
	Arn               string                       `json:"arn,omitempty"`
	CapacityProviders []daemonCapacityProviderView `json:"capacityProviders,omitempty"`
	TotalRunningCount int                          `json:"totalRunningCount,omitempty"`
}

type daemonCapacityProviderView struct {
	Arn          string `json:"arn,omitempty"`
	RunningCount int    `json:"runningCount,omitempty"`
}

type daemonDetailView struct {
	ClusterArn       string                     `json:"clusterArn,omitempty"`
	DaemonArn        string                     `json:"daemonArn,omitempty"`
	DeploymentArn    string                     `json:"deploymentArn,omitempty"`
	Status           string                     `json:"status,omitempty"`
	CurrentRevisions []daemonRevisionDetailView `json:"currentRevisions,omitempty"`
	CreatedAt        float64                    `json:"createdAt,omitempty"`
	UpdatedAt        float64                    `json:"updatedAt,omitempty"`
}

type describeDaemonOutput struct {
	Daemon *daemonDetailView `json:"daemon,omitempty"`
}

// daemonCurrentRevisions builds the CurrentRevisions view for a daemon's active
// revision, reporting zero running counts per capacity provider since this
// emulator does not simulate container-instance-level daemon task placement.
func daemonCurrentRevisions(d *Daemon, revArn string) []daemonRevisionDetailView {
	cps := make([]daemonCapacityProviderView, 0, len(d.CapacityProviderArns))
	for _, cp := range d.CapacityProviderArns {
		cps = append(cps, daemonCapacityProviderView{Arn: cp})
	}

	return []daemonRevisionDetailView{{
		Arn:               revArn,
		CapacityProviders: cps,
	}}
}

func (h *Handler) handleDescribeDaemon(_ context.Context, in *describeDaemonInput) (*describeDaemonOutput, error) {
	d, err := h.Backend.DescribeDaemon(in.DaemonArn)
	if err != nil {
		return nil, err
	}

	view := &daemonDetailView{
		CreatedAt:     float64Unix(d.CreatedAt),
		UpdatedAt:     float64Unix(d.UpdatedAt),
		ClusterArn:    d.ClusterArn,
		DaemonArn:     d.DaemonArn,
		DeploymentArn: d.DeploymentArn,
		Status:        d.Status,
	}

	if d.CurrentDaemonRevisionArn != "" {
		view.CurrentRevisions = daemonCurrentRevisions(d, d.CurrentDaemonRevisionArn)
	}

	return &describeDaemonOutput{Daemon: view}, nil
}

// ----- Handler: UpdateDaemon -----

type updateDaemonInput struct {
	DeploymentConfiguration *daemonDeploymentConfigurationInput `json:"deploymentConfiguration,omitempty"`
	DaemonArn               string                              `json:"daemonArn"`
	DaemonTaskDefinitionArn string                              `json:"daemonTaskDefinitionArn"`
	PropagateTags           string                              `json:"propagateTags,omitempty"`
	CapacityProviderArns    []string                            `json:"capacityProviderArns"`
	EnableECSManagedTags    bool                                `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand    bool                                `json:"enableExecuteCommand,omitempty"`
}

type updateDaemonOutput struct {
	DaemonArn     string  `json:"daemonArn,omitempty"`
	DeploymentArn string  `json:"deploymentArn,omitempty"`
	Status        string  `json:"status,omitempty"`
	CreatedAt     float64 `json:"createdAt,omitempty"`
	UpdatedAt     float64 `json:"updatedAt,omitempty"`
}

func (h *Handler) handleUpdateDaemon(_ context.Context, in *updateDaemonInput) (*updateDaemonOutput, error) {
	d, err := h.Backend.UpdateDaemon(UpdateDaemonInput{
		DaemonArn:               in.DaemonArn,
		DaemonTaskDefinitionArn: in.DaemonTaskDefinitionArn,
		CapacityProviderArns:    in.CapacityProviderArns,
		DeploymentConfiguration: toDaemonDeploymentConfiguration(in.DeploymentConfiguration),
		PropagateTags:           in.PropagateTags,
		EnableECSManagedTags:    in.EnableECSManagedTags,
		EnableExecuteCommand:    in.EnableExecuteCommand,
	})
	if err != nil {
		return nil, err
	}

	return &updateDaemonOutput{
		CreatedAt:     float64Unix(d.CreatedAt),
		UpdatedAt:     float64Unix(d.UpdatedAt),
		DaemonArn:     d.DaemonArn,
		DeploymentArn: d.DeploymentArn,
		Status:        d.Status,
	}, nil
}

// ----- Handler: ListDaemons -----

type listDaemonsInput struct {
	ClusterArn           string   `json:"clusterArn,omitempty"`
	NextToken            string   `json:"nextToken,omitempty"`
	CapacityProviderArns []string `json:"capacityProviderArns,omitempty"`
	MaxResults           int      `json:"maxResults,omitempty"`
}

type daemonSummaryView struct {
	DaemonArn string  `json:"daemonArn,omitempty"`
	Status    string  `json:"status,omitempty"`
	CreatedAt float64 `json:"createdAt,omitempty"`
	UpdatedAt float64 `json:"updatedAt,omitempty"`
}

type listDaemonsOutput struct {
	NextToken           string              `json:"nextToken,omitempty"`
	DaemonSummariesList []daemonSummaryView `json:"daemonSummariesList"`
}

func (h *Handler) handleListDaemons(_ context.Context, in *listDaemonsInput) (*listDaemonsOutput, error) {
	daemons, err := h.Backend.ListDaemons(ListDaemonsInput{
		ClusterArn:           in.ClusterArn,
		CapacityProviderArns: in.CapacityProviderArns,
	})
	if err != nil {
		return nil, err
	}

	views := make([]daemonSummaryView, 0, len(daemons))
	for _, d := range daemons {
		views = append(views, daemonSummaryView{
			CreatedAt: float64Unix(d.CreatedAt),
			UpdatedAt: float64Unix(d.UpdatedAt),
			DaemonArn: d.DaemonArn,
			Status:    d.Status,
		})
	}

	sort.Slice(views, func(i, j int) bool { return views[i].DaemonArn < views[j].DaemonArn })

	p := page.New(views, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listDaemonsOutput{DaemonSummariesList: p.Data, NextToken: p.Next}, nil
}

// ----- Handler: DescribeDaemonDeployments -----

type describeDaemonDeploymentsInput struct {
	DaemonDeploymentArns []string `json:"daemonDeploymentArns"`
}

type daemonDeploymentRevisionDetailView struct {
	Arn                        string                   `json:"arn,omitempty"`
	CapacityProviders          []daemonDeploymentCPView `json:"capacityProviders,omitempty"`
	TotalDrainingInstanceCount int                      `json:"totalDrainingInstanceCount,omitempty"`
	TotalRunningInstanceCount  int                      `json:"totalRunningInstanceCount,omitempty"`
}

type daemonDeploymentCPView struct {
	Arn                   string `json:"arn,omitempty"`
	DrainingInstanceCount int    `json:"drainingInstanceCount,omitempty"`
	RunningInstanceCount  int    `json:"runningInstanceCount,omitempty"`
}

type daemonDeploymentView struct {
	DeploymentConfiguration *daemonDeploymentConfigurationView  `json:"deploymentConfiguration,omitempty"`
	TargetDaemonRevision    *daemonDeploymentRevisionDetailView `json:"targetDaemonRevision,omitempty"`
	ClusterArn              string                              `json:"clusterArn,omitempty"`
	DaemonDeploymentArn     string                              `json:"daemonDeploymentArn,omitempty"`
	Status                  string                              `json:"status,omitempty"`
	StatusReason            string                              `json:"statusReason,omitempty"`
	CreatedAt               float64                             `json:"createdAt,omitempty"`
	StartedAt               float64                             `json:"startedAt,omitempty"`
	FinishedAt              float64                             `json:"finishedAt,omitempty"`
	StoppedAt               float64                             `json:"stoppedAt,omitempty"`
}

func toDaemonDeploymentView(dep *DaemonDeployment) daemonDeploymentView {
	v := daemonDeploymentView{
		DeploymentConfiguration: toDaemonDeploymentConfigurationView(dep.DeploymentConfiguration),
		CreatedAt:               float64Unix(dep.CreatedAt),
		ClusterArn:              dep.ClusterArn,
		DaemonDeploymentArn:     dep.DaemonDeploymentArn,
		Status:                  dep.Status,
		StatusReason:            dep.StatusReason,
	}

	if dep.StartedAt != nil {
		v.StartedAt = float64Unix(*dep.StartedAt)
	}

	if dep.FinishedAt != nil {
		v.FinishedAt = float64Unix(*dep.FinishedAt)
	}

	if dep.TargetDaemonRevisionArn != "" {
		v.TargetDaemonRevision = &daemonDeploymentRevisionDetailView{Arn: dep.TargetDaemonRevisionArn}
	}

	return v
}

type describeDaemonDeploymentsOutput struct {
	DaemonDeployments []daemonDeploymentView `json:"daemonDeployments"`
	Failures          []failureView          `json:"failures"`
}

func (h *Handler) handleDescribeDaemonDeployments(
	_ context.Context,
	in *describeDaemonDeploymentsInput,
) (*describeDaemonDeploymentsOutput, error) {
	deps, failures, err := h.Backend.DescribeDaemonDeployments(in.DaemonDeploymentArns)
	if err != nil {
		return nil, err
	}

	views := make([]daemonDeploymentView, 0, len(deps))
	for _, dep := range deps {
		views = append(views, toDaemonDeploymentView(&dep))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeDaemonDeploymentsOutput{DaemonDeployments: views, Failures: failViews}, nil
}

// ----- Handler: ListDaemonDeployments -----

type listDaemonDeploymentsInput struct {
	DaemonArn  string   `json:"daemonArn"`
	NextToken  string   `json:"nextToken,omitempty"`
	Status     []string `json:"status,omitempty"`
	MaxResults int      `json:"maxResults,omitempty"`
}

type daemonDeploymentSummaryView struct {
	ClusterArn              string  `json:"clusterArn,omitempty"`
	DaemonArn               string  `json:"daemonArn,omitempty"`
	DaemonDeploymentArn     string  `json:"daemonDeploymentArn,omitempty"`
	Status                  string  `json:"status,omitempty"`
	StatusReason            string  `json:"statusReason,omitempty"`
	TargetDaemonRevisionArn string  `json:"targetDaemonRevisionArn,omitempty"`
	CreatedAt               float64 `json:"createdAt,omitempty"`
	StartedAt               float64 `json:"startedAt,omitempty"`
	FinishedAt              float64 `json:"finishedAt,omitempty"`
	StoppedAt               float64 `json:"stoppedAt,omitempty"`
}

type listDaemonDeploymentsOutput struct {
	NextToken         string                        `json:"nextToken,omitempty"`
	DaemonDeployments []daemonDeploymentSummaryView `json:"daemonDeployments"`
}

func (h *Handler) handleListDaemonDeployments(
	_ context.Context,
	in *listDaemonDeploymentsInput,
) (*listDaemonDeploymentsOutput, error) {
	deps, err := h.Backend.ListDaemonDeployments(ListDaemonDeploymentsInput{
		DaemonArn: in.DaemonArn,
		Status:    in.Status,
	})
	if err != nil {
		return nil, err
	}

	views := make([]daemonDeploymentSummaryView, 0, len(deps))
	for _, dep := range deps {
		v := daemonDeploymentSummaryView{
			CreatedAt:               float64Unix(dep.CreatedAt),
			ClusterArn:              dep.ClusterArn,
			DaemonArn:               dep.DaemonArn,
			DaemonDeploymentArn:     dep.DaemonDeploymentArn,
			Status:                  dep.Status,
			StatusReason:            dep.StatusReason,
			TargetDaemonRevisionArn: dep.TargetDaemonRevisionArn,
		}

		if dep.StartedAt != nil {
			v.StartedAt = float64Unix(*dep.StartedAt)
		}

		if dep.FinishedAt != nil {
			v.FinishedAt = float64Unix(*dep.FinishedAt)
		}

		views = append(views, v)
	}

	sort.Slice(views, func(i, j int) bool {
		return views[i].DaemonDeploymentArn < views[j].DaemonDeploymentArn
	})

	p := page.New(views, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listDaemonDeploymentsOutput{DaemonDeployments: p.Data, NextToken: p.Next}, nil
}

// ----- Handler: DescribeDaemonRevisions -----

type describeDaemonRevisionsInput struct {
	DaemonRevisionArns []string `json:"daemonRevisionArns"`
}

type daemonContainerImageView struct {
	ContainerName string `json:"containerName,omitempty"`
	Image         string `json:"image,omitempty"`
}

type daemonRevisionView struct {
	ClusterArn              string                     `json:"clusterArn,omitempty"`
	DaemonArn               string                     `json:"daemonArn,omitempty"`
	DaemonRevisionArn       string                     `json:"daemonRevisionArn,omitempty"`
	DaemonTaskDefinitionArn string                     `json:"daemonTaskDefinitionArn,omitempty"`
	PropagateTags           string                     `json:"propagateTags,omitempty"`
	ContainerImages         []daemonContainerImageView `json:"containerImages,omitempty"`
	CreatedAt               float64                    `json:"createdAt,omitempty"`
	EnableECSManagedTags    bool                       `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand    bool                       `json:"enableExecuteCommand,omitempty"`
}

// daemonContainerImages resolves the container images referenced by the
// revision's daemon task definition, if it can still be found.
func (h *Handler) daemonContainerImages(daemonTaskDefinitionArn string) []daemonContainerImageView {
	td, err := h.Backend.DescribeDaemonTaskDefinition(daemonTaskDefinitionArn)
	if err != nil {
		return nil
	}

	images := make([]daemonContainerImageView, 0, len(td.ContainerDefinitions))
	for _, cd := range td.ContainerDefinitions {
		images = append(images, daemonContainerImageView{ContainerName: cd.Name, Image: cd.Image})
	}

	return images
}

type describeDaemonRevisionsOutput struct {
	DaemonRevisions []daemonRevisionView `json:"daemonRevisions"`
	Failures        []failureView        `json:"failures"`
}

func (h *Handler) handleDescribeDaemonRevisions(
	_ context.Context,
	in *describeDaemonRevisionsInput,
) (*describeDaemonRevisionsOutput, error) {
	revs, failures, err := h.Backend.DescribeDaemonRevisions(in.DaemonRevisionArns)
	if err != nil {
		return nil, err
	}

	views := make([]daemonRevisionView, 0, len(revs))
	for _, rev := range revs {
		views = append(views, daemonRevisionView{
			CreatedAt:               float64Unix(rev.CreatedAt),
			ContainerImages:         h.daemonContainerImages(rev.DaemonTaskDefinitionArn),
			ClusterArn:              rev.ClusterArn,
			DaemonArn:               rev.DaemonArn,
			DaemonRevisionArn:       rev.DaemonRevisionArn,
			DaemonTaskDefinitionArn: rev.DaemonTaskDefinitionArn,
			PropagateTags:           rev.PropagateTags,
			EnableECSManagedTags:    rev.EnableECSManagedTags,
			EnableExecuteCommand:    rev.EnableExecuteCommand,
		})
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeDaemonRevisionsOutput{DaemonRevisions: views, Failures: failViews}, nil
}

// ----- Daemon container definition / volume conversions -----

type daemonContainerDefinitionInput struct {
	HealthCheck            *HealthCheck           `json:"healthCheck,omitempty"`
	LogConfiguration       *LogConfiguration      `json:"logConfiguration,omitempty"`
	RepositoryCredentials  *RepositoryCredentials `json:"repositoryCredentials,omitempty"`
	FirelensConfiguration  *FirelensConfiguration `json:"firelensConfiguration,omitempty"`
	Name                   string                 `json:"name"`
	Image                  string                 `json:"image"`
	WorkingDirectory       string                 `json:"workingDirectory,omitempty"`
	User                   string                 `json:"user,omitempty"`
	Command                []string               `json:"command,omitempty"`
	EntryPoint             []string               `json:"entryPoint,omitempty"`
	Environment            []KeyValuePair         `json:"environment,omitempty"`
	EnvironmentFiles       []EnvironmentFile      `json:"environmentFiles,omitempty"`
	Secrets                []SecretReference      `json:"secrets,omitempty"`
	MountPoints            []MountPoint           `json:"mountPoints,omitempty"`
	DependsOn              []ContainerDependency  `json:"dependsOn,omitempty"`
	CPU                    int                    `json:"cpu,omitempty"`
	Memory                 int                    `json:"memory,omitempty"`
	MemoryReservation      int                    `json:"memoryReservation,omitempty"`
	StartTimeout           int                    `json:"startTimeout,omitempty"`
	StopTimeout            int                    `json:"stopTimeout,omitempty"`
	Essential              bool                   `json:"essential"`
	Interactive            bool                   `json:"interactive,omitempty"`
	PseudoTerminal         bool                   `json:"pseudoTerminal,omitempty"`
	Privileged             bool                   `json:"privileged,omitempty"`
	ReadonlyRootFilesystem bool                   `json:"readonlyRootFilesystem,omitempty"`
}

func toDaemonContainerDefinitions(in []daemonContainerDefinitionInput) []DaemonContainerDefinition {
	out := make([]DaemonContainerDefinition, 0, len(in))
	for _, c := range in {
		out = append(out, DaemonContainerDefinition(c))
	}

	return out
}

type daemonVolumeInput struct {
	Host *HostVolumeProperties `json:"host,omitempty"`
	Name string                `json:"name"`
}

func toDaemonVolumes(in []daemonVolumeInput) []DaemonVolume {
	out := make([]DaemonVolume, 0, len(in))
	for _, v := range in {
		out = append(out, DaemonVolume(v))
	}

	return out
}

// ----- Handler: RegisterDaemonTaskDefinition -----

type registerDaemonTaskDefinitionInput struct {
	Family               string                           `json:"family"`
	CPU                  string                           `json:"cpu,omitempty"`
	Memory               string                           `json:"memory,omitempty"`
	ExecutionRoleArn     string                           `json:"executionRoleArn,omitempty"`
	TaskRoleArn          string                           `json:"taskRoleArn,omitempty"`
	ContainerDefinitions []daemonContainerDefinitionInput `json:"containerDefinitions"`
	Volumes              []daemonVolumeInput              `json:"volumes,omitempty"`
	Tags                 []tagInput                       `json:"tags,omitempty"`
}

type registerDaemonTaskDefinitionOutput struct {
	DaemonTaskDefinitionArn string `json:"daemonTaskDefinitionArn,omitempty"`
}

func (h *Handler) handleRegisterDaemonTaskDefinition(
	_ context.Context,
	in *registerDaemonTaskDefinitionInput,
) (*registerDaemonTaskDefinitionOutput, error) {
	td, err := h.Backend.RegisterDaemonTaskDefinition(RegisterDaemonTaskDefinitionInput{
		Family:               in.Family,
		CPU:                  in.CPU,
		Memory:               in.Memory,
		ExecutionRoleArn:     in.ExecutionRoleArn,
		TaskRoleArn:          in.TaskRoleArn,
		ContainerDefinitions: toDaemonContainerDefinitions(in.ContainerDefinitions),
		Volumes:              toDaemonVolumes(in.Volumes),
		Tags:                 tagsFromInput(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return &registerDaemonTaskDefinitionOutput{DaemonTaskDefinitionArn: td.DaemonTaskDefinitionArn}, nil
}

// ----- Handler: DescribeDaemonTaskDefinition -----

type describeDaemonTaskDefinitionInput struct {
	DaemonTaskDefinition string `json:"daemonTaskDefinition"`
}

type daemonTaskDefinitionView struct {
	Family                  string                           `json:"family,omitempty"`
	CPU                     string                           `json:"cpu,omitempty"`
	DaemonTaskDefinitionArn string                           `json:"daemonTaskDefinitionArn,omitempty"`
	ExecutionRoleArn        string                           `json:"executionRoleArn,omitempty"`
	Memory                  string                           `json:"memory,omitempty"`
	RegisteredBy            string                           `json:"registeredBy,omitempty"`
	Status                  string                           `json:"status,omitempty"`
	TaskRoleArn             string                           `json:"taskRoleArn,omitempty"`
	ContainerDefinitions    []daemonContainerDefinitionInput `json:"containerDefinitions"`
	Volumes                 []daemonVolumeInput              `json:"volumes,omitempty"`
	DeleteRequestedAt       float64                          `json:"deleteRequestedAt,omitempty"`
	RegisteredAt            float64                          `json:"registeredAt,omitempty"`
	Revision                int                              `json:"revision,omitempty"`
}

func toDaemonContainerDefinitionViews(in []DaemonContainerDefinition) []daemonContainerDefinitionInput {
	out := make([]daemonContainerDefinitionInput, 0, len(in))
	for _, c := range in {
		out = append(out, daemonContainerDefinitionInput(c))
	}

	return out
}

func toDaemonVolumeViews(in []DaemonVolume) []daemonVolumeInput {
	out := make([]daemonVolumeInput, 0, len(in))
	for _, v := range in {
		out = append(out, daemonVolumeInput(v))
	}

	return out
}

func toDaemonTaskDefinitionView(td *DaemonTaskDefinition) *daemonTaskDefinitionView {
	v := &daemonTaskDefinitionView{
		ContainerDefinitions:    toDaemonContainerDefinitionViews(td.ContainerDefinitions),
		Volumes:                 toDaemonVolumeViews(td.Volumes),
		CPU:                     td.CPU,
		DaemonTaskDefinitionArn: td.DaemonTaskDefinitionArn,
		ExecutionRoleArn:        td.ExecutionRoleArn,
		Family:                  td.Family,
		Memory:                  td.Memory,
		RegisteredAt:            float64Unix(td.RegisteredAt),
		RegisteredBy:            td.RegisteredBy,
		Status:                  td.Status,
		TaskRoleArn:             td.TaskRoleArn,
		Revision:                td.Revision,
	}

	if td.DeleteRequestedAt != nil {
		v.DeleteRequestedAt = float64Unix(*td.DeleteRequestedAt)
	}

	return v
}

type describeDaemonTaskDefinitionOutput struct {
	DaemonTaskDefinition *daemonTaskDefinitionView `json:"daemonTaskDefinition,omitempty"`
}

func (h *Handler) handleDescribeDaemonTaskDefinition(
	_ context.Context,
	in *describeDaemonTaskDefinitionInput,
) (*describeDaemonTaskDefinitionOutput, error) {
	td, err := h.Backend.DescribeDaemonTaskDefinition(in.DaemonTaskDefinition)
	if err != nil {
		return nil, err
	}

	return &describeDaemonTaskDefinitionOutput{DaemonTaskDefinition: toDaemonTaskDefinitionView(td)}, nil
}

// ----- Handler: DeleteDaemonTaskDefinition -----

type deleteDaemonTaskDefinitionInput struct {
	DaemonTaskDefinition string `json:"daemonTaskDefinition"`
}

type deleteDaemonTaskDefinitionOutput struct {
	DaemonTaskDefinitionArn string `json:"daemonTaskDefinitionArn,omitempty"`
}

func (h *Handler) handleDeleteDaemonTaskDefinition(
	_ context.Context,
	in *deleteDaemonTaskDefinitionInput,
) (*deleteDaemonTaskDefinitionOutput, error) {
	td, err := h.Backend.DeleteDaemonTaskDefinition(in.DaemonTaskDefinition)
	if err != nil {
		return nil, err
	}

	return &deleteDaemonTaskDefinitionOutput{DaemonTaskDefinitionArn: td.DaemonTaskDefinitionArn}, nil
}

// ----- Handler: ListDaemonTaskDefinitions -----

type listDaemonTaskDefinitionsInput struct {
	Family       string `json:"family,omitempty"`
	FamilyPrefix string `json:"familyPrefix,omitempty"`
	NextToken    string `json:"nextToken,omitempty"`
	Revision     string `json:"revision,omitempty"`
	Sort         string `json:"sort,omitempty"`
	Status       string `json:"status,omitempty"`
	MaxResults   int    `json:"maxResults,omitempty"`
}

type daemonTaskDefinitionSummaryView struct {
	Arn               string  `json:"arn,omitempty"`
	RegisteredBy      string  `json:"registeredBy,omitempty"`
	Status            string  `json:"status,omitempty"`
	DeleteRequestedAt float64 `json:"deleteRequestedAt,omitempty"`
	RegisteredAt      float64 `json:"registeredAt,omitempty"`
}

type listDaemonTaskDefinitionsOutput struct {
	NextToken             string                            `json:"nextToken,omitempty"`
	DaemonTaskDefinitions []daemonTaskDefinitionSummaryView `json:"daemonTaskDefinitions"`
}

func (h *Handler) handleListDaemonTaskDefinitions(
	_ context.Context,
	in *listDaemonTaskDefinitionsInput,
) (*listDaemonTaskDefinitionsOutput, error) {
	tds, err := h.Backend.ListDaemonTaskDefinitions(ListDaemonTaskDefinitionsInput{
		Family:       in.Family,
		FamilyPrefix: in.FamilyPrefix,
		Status:       in.Status,
	})
	if err != nil {
		return nil, err
	}

	views := make([]daemonTaskDefinitionSummaryView, 0, len(tds))
	for _, td := range tds {
		v := daemonTaskDefinitionSummaryView{
			RegisteredAt: float64Unix(td.RegisteredAt),
			Arn:          td.DaemonTaskDefinitionArn,
			RegisteredBy: td.RegisteredBy,
			Status:       td.Status,
		}

		if td.DeleteRequestedAt != nil {
			v.DeleteRequestedAt = float64Unix(*td.DeleteRequestedAt)
		}

		views = append(views, v)
	}

	sort.Slice(views, func(i, j int) bool { return views[i].Arn < views[j].Arn })

	if in.Sort == "DESC" {
		for i, j := 0, len(views)-1; i < j; i, j = i+1, j-1 {
			views[i], views[j] = views[j], views[i]
		}
	}

	p := page.New(views, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listDaemonTaskDefinitionsOutput{DaemonTaskDefinitions: p.Data, NextToken: p.Next}, nil
}

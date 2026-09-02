package ecs

import "context"

// ----- Task handlers -----

type containerOverrideInput struct {
	CPU         *int              `json:"cpu,omitempty"`
	Memory      *int              `json:"memory,omitempty"`
	Name        string            `json:"name"`
	Command     []string          `json:"command,omitempty"`
	Environment []KeyValuePair    `json:"environment,omitempty"`
	Secrets     []SecretReference `json:"secrets,omitempty"`
}

type taskOverrideInput struct {
	TaskRoleArn        string                   `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn   string                   `json:"executionRoleArn,omitempty"`
	CPU                string                   `json:"cpu,omitempty"`
	Memory             string                   `json:"memory,omitempty"`
	ContainerOverrides []containerOverrideInput `json:"containerOverrides,omitempty"`
}

type runTaskInput struct {
	Overrides                *taskOverrideInput         `json:"overrides,omitempty"`
	NetworkConfiguration     *networkConfigurationInput `json:"networkConfiguration,omitempty"`
	Cluster                  string                     `json:"cluster,omitempty"`
	TaskDefinition           string                     `json:"taskDefinition"`
	LaunchType               string                     `json:"launchType,omitempty"`
	Group                    string                     `json:"group,omitempty"`
	StartedBy                string                     `json:"startedBy,omitempty"`
	PlatformVersion          string                     `json:"platformVersion,omitempty"`
	PropagateTags            string                     `json:"propagateTags,omitempty"`
	Tags                     []Tag                      `json:"tags,omitempty"`
	CapacityProviderStrategy []cpStrategyItemInput      `json:"capacityProviderStrategy,omitempty"`
	Count                    int                        `json:"count,omitempty"`
	EnableECSManagedTags     bool                       `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand     bool                       `json:"enableExecuteCommand,omitempty"`
}

type runTaskOutput struct {
	Tasks []taskView `json:"tasks"`
}

func (h *Handler) handleRunTask(_ context.Context, in *runTaskInput) (*runTaskOutput, error) {
	tasks, err := h.Backend.RunTask(RunTaskInput{
		Cluster:                  in.Cluster,
		TaskDefinition:           in.TaskDefinition,
		Count:                    in.Count,
		LaunchType:               in.LaunchType,
		Group:                    in.Group,
		StartedBy:                in.StartedBy,
		PlatformVersion:          in.PlatformVersion,
		PropagateTags:            in.PropagateTags,
		EnableECSManagedTags:     in.EnableECSManagedTags,
		Tags:                     in.Tags,
		CapacityProviderStrategy: toCPStrategyItems(in.CapacityProviderStrategy),
		Overrides:                toTaskOverride(in.Overrides),
		NetworkConfiguration:     toNetworkConfiguration(in.NetworkConfiguration),
		EnableExecuteCommand:     in.EnableExecuteCommand,
	})
	if err != nil {
		return nil, err
	}

	views := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		views = append(views, toTaskView(t))
	}

	return &runTaskOutput{Tasks: views}, nil
}

type describeTasksInput struct {
	Cluster string   `json:"cluster,omitempty"`
	Tasks   []string `json:"tasks"`
}

type describeTasksOutput struct {
	Tasks    []taskView    `json:"tasks"`
	Failures []failureView `json:"failures"`
}

func (h *Handler) handleDescribeTasks(
	_ context.Context,
	in *describeTasksInput,
) (*describeTasksOutput, error) {
	tasks, failures, err := h.Backend.DescribeTasks(in.Cluster, in.Tasks)
	if err != nil {
		return nil, err
	}

	views := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		views = append(views, toTaskView(t))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeTasksOutput{Tasks: views, Failures: failViews}, nil
}

type stopTaskInput struct {
	Cluster string `json:"cluster,omitempty"`
	Task    string `json:"task"`
	Reason  string `json:"reason,omitempty"`
}

type stopTaskOutput struct {
	Task taskView `json:"task"`
}

func (h *Handler) handleStopTask(_ context.Context, in *stopTaskInput) (*stopTaskOutput, error) {
	task, err := h.Backend.StopTask(in.Cluster, in.Task, in.Reason)
	if err != nil {
		return nil, err
	}

	return &stopTaskOutput{Task: toTaskView(*task)}, nil
}

type listTasksInput struct {
	Cluster           string `json:"cluster,omitempty"`
	ContainerInstance string `json:"containerInstance,omitempty"`
	Family            string `json:"family,omitempty"`
	ServiceName       string `json:"serviceName,omitempty"`
	DesiredStatus     string `json:"desiredStatus,omitempty"`
	LaunchType        string `json:"launchType,omitempty"`
	StartedBy         string `json:"startedBy,omitempty"`
	NextToken         string `json:"nextToken,omitempty"`
	MaxResults        int    `json:"maxResults,omitempty"`
}

type listTasksOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	TaskArns  []string `json:"taskArns"`
}

func (h *Handler) handleListTasks(_ context.Context, in *listTasksInput) (*listTasksOutput, error) {
	arns, err := h.Backend.ListTasksFiltered(ListTasksInput{
		Cluster:           in.Cluster,
		ContainerInstance: in.ContainerInstance,
		Family:            in.Family,
		ServiceName:       in.ServiceName,
		DesiredStatus:     in.DesiredStatus,
		LaunchType:        in.LaunchType,
		StartedBy:         in.StartedBy,
	})
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	arns, nextToken := applyNextTokenSlice(arns, in.NextToken, in.MaxResults)

	return &listTasksOutput{TaskArns: arns, NextToken: nextToken}, nil
}

// ----- StartTask -----

type startTaskInput struct {
	Cluster            string   `json:"cluster,omitempty"`
	TaskDefinition     string   `json:"taskDefinition"`
	Group              string   `json:"group,omitempty"`
	StartedBy          string   `json:"startedBy,omitempty"`
	ContainerInstances []string `json:"containerInstances"`
}

type startTaskOutput struct {
	Failures []failureView `json:"failures"`
	Tasks    []taskView    `json:"tasks"`
}

func (h *Handler) handleStartTask(
	_ context.Context,
	in *startTaskInput,
) (*startTaskOutput, error) {
	tasks, failures, err := h.Backend.StartTask(StartTaskInput{
		Cluster:            in.Cluster,
		TaskDefinition:     in.TaskDefinition,
		ContainerInstances: in.ContainerInstances,
		Group:              in.Group,
		StartedBy:          in.StartedBy,
	})
	if err != nil {
		return nil, err
	}

	views := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		views = append(views, toTaskView(t))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &startTaskOutput{Tasks: views, Failures: failViews}, nil
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

// ----- ExecuteCommand handler -----

type executeCommandInput struct {
	Cluster     string `json:"cluster,omitempty"`
	Task        string `json:"task"`
	Container   string `json:"container,omitempty"`
	Command     string `json:"command"`
	Interactive bool   `json:"interactive,omitempty"`
}

type executeCommandOutput struct {
	ClusterArn    string  `json:"clusterArn"`
	ContainerArn  string  `json:"containerArn"`
	ContainerName string  `json:"containerName"`
	TaskArn       string  `json:"taskArn"`
	Session       session `json:"session"`
	Interactive   bool    `json:"interactive"`
}

type session struct {
	SessionID  string `json:"sessionId"`
	StreamURL  string `json:"streamUrl"`
	TokenValue string `json:"tokenValue"`
}

func (h *Handler) handleExecuteCommand(
	_ context.Context,
	in *executeCommandInput,
) (*executeCommandOutput, error) {
	out, err := h.Backend.ExecuteCommand(
		in.Cluster,
		in.Task,
		in.Container,
		in.Command,
		in.Interactive,
	)
	if err != nil {
		return nil, err
	}

	return &executeCommandOutput{
		ClusterArn:    out.ClusterArn,
		ContainerArn:  out.ContainerArn,
		ContainerName: out.ContainerName,
		TaskArn:       out.TaskArn,
		Interactive:   out.Interactive,
		Session: session{
			SessionID:  out.Session.SessionID,
			StreamURL:  out.Session.StreamURL,
			TokenValue: out.Session.TokenValue,
		},
	}, nil
}

// ----- View types (JSON serialization) -----

type taskAttachmentDetailView struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type taskAttachmentView struct {
	ID      string                     `json:"id"`
	Type    string                     `json:"type"`
	Status  string                     `json:"status"`
	Details []taskAttachmentDetailView `json:"details,omitempty"`
}

type containerOverrideView struct {
	CPU         *int              `json:"cpu,omitempty"`
	Memory      *int              `json:"memory,omitempty"`
	Name        string            `json:"name"`
	Command     []string          `json:"command,omitempty"`
	Environment []KeyValuePair    `json:"environment,omitempty"`
	Secrets     []SecretReference `json:"secrets,omitempty"`
}

type taskOverrideView struct {
	TaskRoleArn        string                  `json:"taskRoleArn,omitempty"`
	ExecutionRoleArn   string                  `json:"executionRoleArn,omitempty"`
	CPU                string                  `json:"cpu,omitempty"`
	Memory             string                  `json:"memory,omitempty"`
	ContainerOverrides []containerOverrideView `json:"containerOverrides,omitempty"`
}

// containerNetworkInterfaceView is the handler view of a container's network interface.
type containerNetworkInterfaceView struct {
	AttachmentID       string `json:"attachmentId,omitempty"`
	PrivateIpv4Address string `json:"privateIpv4Address,omitempty"`
	Ipv6Address        string `json:"ipv6Address,omitempty"`
}

// containerNetworkBindingView is the handler view of a container's port binding.
type containerNetworkBindingView struct {
	BindIP        string `json:"bindIP,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
	ContainerPort int    `json:"containerPort,omitempty"`
	HostPort      int    `json:"hostPort,omitempty"`
}

// containerView is the handler view of a runtime container within a task.
type containerView struct {
	ContainerArn      string                          `json:"containerArn,omitempty"`
	TaskArn           string                          `json:"taskArn,omitempty"`
	Name              string                          `json:"name"`
	Image             string                          `json:"image,omitempty"`
	ImageDigest       string                          `json:"imageDigest,omitempty"`
	RuntimeID         string                          `json:"runtimeId,omitempty"`
	LastStatus        string                          `json:"lastStatus"`
	HealthStatus      string                          `json:"healthStatus,omitempty"`
	CPU               string                          `json:"cpu,omitempty"`
	Memory            string                          `json:"memory,omitempty"`
	MemoryReservation string                          `json:"memoryReservation,omitempty"`
	Reason            string                          `json:"reason,omitempty"`
	ExitCode          *int                            `json:"exitCode,omitempty"`
	NetworkInterfaces []containerNetworkInterfaceView `json:"networkInterfaces,omitempty"`
	NetworkBindings   []containerNetworkBindingView   `json:"networkBindings,omitempty"`
}

type taskView struct {
	Overrides            *taskOverrideView         `json:"overrides,omitempty"`
	NetworkConfiguration *networkConfigurationView `json:"networkConfiguration,omitempty"`
	TaskArn              string                    `json:"taskArn"`
	ClusterArn           string                    `json:"clusterArn"`
	TaskDefinitionArn    string                    `json:"taskDefinitionArn"`
	LastStatus           string                    `json:"lastStatus"`
	DesiredStatus        string                    `json:"desiredStatus"`
	Connectivity         string                    `json:"connectivity,omitempty"`
	StoppedReason        string                    `json:"stoppedReason,omitempty"`
	Group                string                    `json:"group,omitempty"`
	LaunchType           string                    `json:"launchType,omitempty"`
	ContainerInstanceArn string                    `json:"containerInstanceArn,omitempty"`
	StartedBy            string                    `json:"startedBy,omitempty"`
	PlatformVersion      string                    `json:"platformVersion,omitempty"`
	PlatformFamily       string                    `json:"platformFamily,omitempty"`
	RuntimeID            string                    `json:"runtimeId,omitempty"`
	PropagateTags        string                    `json:"propagateTags,omitempty"`
	CapacityProviderName string                    `json:"capacityProviderName,omitempty"`
	Attachments          []taskAttachmentView      `json:"attachments,omitempty"`
	Containers           []containerView           `json:"containers,omitempty"`
	Tags                 []Tag                     `json:"tags,omitempty"`
	StartedAt            float64                   `json:"startedAt,omitempty"`
	StoppedAt            float64                   `json:"stoppedAt,omitempty"`
	ConnectivityAt       float64                   `json:"connectivityAt,omitempty"`
	EnableExecuteCommand bool                      `json:"enableExecuteCommand,omitempty"`
}

func toTaskView(t Task) taskView {
	v := taskView{
		TaskArn:              t.TaskArn,
		ClusterArn:           t.ClusterArn,
		TaskDefinitionArn:    t.TaskDefinitionArn,
		LastStatus:           t.LastStatus,
		DesiredStatus:        t.DesiredStatus,
		Connectivity:         t.Connectivity,
		StoppedReason:        t.StoppedReason,
		Group:                t.Group,
		LaunchType:           t.LaunchType,
		ContainerInstanceArn: t.ContainerInstanceArn,
		StartedBy:            t.StartedBy,
		PlatformVersion:      t.PlatformVersion,
		PlatformFamily:       t.PlatformFamily,
		RuntimeID:            t.RuntimeID,
		PropagateTags:        t.PropagateTags,
		CapacityProviderName: t.CapacityProviderName,
		Tags:                 t.Tags,
		Overrides:            toTaskOverrideView(t.Overrides),
		NetworkConfiguration: toNetworkConfigurationView(t.NetworkConfiguration),
		EnableExecuteCommand: t.EnableExecuteCommand,
	}

	for _, a := range t.Attachments {
		details := make([]taskAttachmentDetailView, 0, len(a.Details))
		for _, d := range a.Details {
			details = append(details, taskAttachmentDetailView(d))
		}

		v.Attachments = append(v.Attachments, taskAttachmentView{
			ID:      a.ID,
			Type:    a.Type,
			Status:  a.Status,
			Details: details,
		})
	}

	for _, c := range t.Containers {
		cv := containerView{
			ContainerArn:      c.ContainerArn,
			TaskArn:           c.TaskArn,
			Name:              c.Name,
			Image:             c.Image,
			ImageDigest:       c.ImageDigest,
			RuntimeID:         c.RuntimeID,
			LastStatus:        c.LastStatus,
			HealthStatus:      c.HealthStatus,
			CPU:               c.CPU,
			Memory:            c.Memory,
			MemoryReservation: c.MemoryReservation,
			Reason:            c.Reason,
			ExitCode:          c.ExitCode,
		}

		for _, ni := range c.NetworkInterfaces {
			cv.NetworkInterfaces = append(cv.NetworkInterfaces, containerNetworkInterfaceView(ni))
		}

		for _, nb := range c.NetworkBindings {
			cv.NetworkBindings = append(cv.NetworkBindings, containerNetworkBindingView(nb))
		}

		v.Containers = append(v.Containers, cv)
	}

	if t.StartedAt != nil {
		v.StartedAt = float64(t.StartedAt.Unix())
	}

	if t.StoppedAt != nil {
		v.StoppedAt = float64(t.StoppedAt.Unix())
	}

	if t.ConnectivityAt != nil {
		v.ConnectivityAt = float64(t.ConnectivityAt.Unix())
	}

	return v
}

// toTaskOverride converts handler input to backend type.
func toTaskOverride(in *taskOverrideInput) *TaskOverride {
	if in == nil {
		return nil
	}

	out := &TaskOverride{
		TaskRoleArn:      in.TaskRoleArn,
		ExecutionRoleArn: in.ExecutionRoleArn,
		CPU:              in.CPU,
		Memory:           in.Memory,
	}

	for _, co := range in.ContainerOverrides {
		out.ContainerOverrides = append(out.ContainerOverrides, ContainerOverride(co))
	}

	return out
}

// toTaskOverrideView converts backend type to view.
func toTaskOverrideView(in *TaskOverride) *taskOverrideView {
	if in == nil {
		return nil
	}

	out := &taskOverrideView{
		TaskRoleArn:      in.TaskRoleArn,
		ExecutionRoleArn: in.ExecutionRoleArn,
		CPU:              in.CPU,
		Memory:           in.Memory,
	}

	for _, co := range in.ContainerOverrides {
		out.ContainerOverrides = append(out.ContainerOverrides, containerOverrideView(co))
	}

	return out
}

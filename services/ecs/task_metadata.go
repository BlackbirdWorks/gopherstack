package ecs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	cpuUnitsPerVCPUMetadata = 1024.0
	containerTypeNormal     = "NORMAL"
	statsTimeout            = 5 * time.Second
	tagsMetadataPath        = "taskWithTags"
)

// ErrContainerStatsUnavailable is returned when the task runner cannot report container stats.
var ErrContainerStatsUnavailable = errors.New("container stats unavailable")

// containerStatsRunner is implemented by runners that can report a container's resource stats.
type containerStatsRunner interface {
	ContainerStatsJSON(ctx context.Context, containerID string) (json.RawMessage, error)
}

type metadataLimits struct {
	CPU    *float64 `json:"CPU,omitempty"`
	Memory *int     `json:"Memory,omitempty"`
}

type metadataNetwork struct {
	NetworkMode   string   `json:"NetworkMode"`
	IPv4Addresses []string `json:"IPv4Addresses,omitempty"`
}

type metadataPort struct {
	Protocol      string `json:"Protocol"`
	HostIP        string `json:"HostIp,omitempty"`
	ContainerPort int    `json:"ContainerPort"`
	HostPort      int    `json:"HostPort,omitempty"`
}

// ContainerMetadata is the task metadata v4 container document.
type ContainerMetadata struct {
	Limits        metadataLimits    `json:"Limits"`
	Labels        map[string]string `json:"Labels"`
	ExitCode      *int              `json:"ExitCode,omitempty"`
	DockerID      string            `json:"DockerId"`
	Name          string            `json:"Name"`
	DockerName    string            `json:"DockerName"`
	Image         string            `json:"Image"`
	ImageID       string            `json:"ImageID,omitempty"`
	DesiredStatus string            `json:"DesiredStatus"`
	KnownStatus   string            `json:"KnownStatus"`
	CreatedAt     string            `json:"CreatedAt,omitempty"`
	StartedAt     string            `json:"StartedAt,omitempty"`
	Type          string            `json:"Type"`
	ContainerARN  string            `json:"ContainerARN,omitempty"`
	LogDriver     string            `json:"LogDriver,omitempty"`
	LogOptions    map[string]string `json:"LogOptions,omitempty"`
	Networks      []metadataNetwork `json:"Networks,omitempty"`
	Ports         []metadataPort    `json:"Ports,omitempty"`
}

// TaskMetadata is the task metadata v4 task document.
type TaskMetadata struct {
	Limits                metadataLimits      `json:"Limits"`
	ContainerInstanceTags map[string]string   `json:"ContainerInstanceTags,omitempty"`
	TaskTags              map[string]string   `json:"TaskTags,omitempty"`
	Cluster               string              `json:"Cluster"`
	TaskARN               string              `json:"TaskARN"`
	Family                string              `json:"Family"`
	Revision              string              `json:"Revision"`
	DesiredStatus         string              `json:"DesiredStatus"`
	KnownStatus           string              `json:"KnownStatus"`
	AvailabilityZone      string              `json:"AvailabilityZone"`
	LaunchType            string              `json:"LaunchType"`
	ServiceName           string              `json:"ServiceName,omitempty"`
	Containers            []ContainerMetadata `json:"Containers"`
}

type taskCredentialsDocument struct {
	RoleArn         string `json:"RoleArn"`
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string `json:"SecretAccessKey"`
	Token           string `json:"Token"`
	Expiration      string `json:"Expiration"`
}

type credentialsErrorDocument struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func tagMap(tags []Tag) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// findTaskByID returns a copy of the task whose ID is taskID.
func (b *InMemoryBackend) findTaskByID(taskID string) (Task, bool) {
	b.mu.RLock("findTaskByID")
	defer b.mu.RUnlock()

	for _, t := range b.tasks.All() {
		if taskIDFromARN(t.TaskArn) == taskID {
			return b.taskWithLiveTagsLocked(t), true
		}
	}

	return Task{}, false
}

func vcpuLimit(cpuUnits string) *float64 {
	n, err := strconv.Atoi(cpuUnits)
	if err != nil || n <= 0 {
		return nil
	}

	v := float64(n) / cpuUnitsPerVCPUMetadata

	return &v
}

func memLimit(mib string) *int {
	n, err := strconv.Atoi(mib)
	if err != nil || n <= 0 {
		return nil
	}

	return &n
}

func containerLimits(cd ContainerDefinition) metadataLimits {
	l := metadataLimits{}

	if cd.CPU > 0 {
		v := float64(cd.CPU)
		l.CPU = &v
	}

	if cd.Memory > 0 {
		m := cd.Memory
		l.Memory = &m
	}

	return l
}

func timeStr(t *time.Time) string {
	if t == nil {
		return ""
	}

	return t.UTC().Format(time.RFC3339Nano)
}

func containerLabels(task Task, td *TaskDefinition, name string) map[string]string {
	return map[string]string{
		"com.amazonaws.ecs.cluster":                 task.ClusterArn,
		"com.amazonaws.ecs.container-name":          name,
		"com.amazonaws.ecs.task-arn":                task.TaskArn,
		"com.amazonaws.ecs.task-definition-family":  td.Family,
		"com.amazonaws.ecs.task-definition-version": strconv.Itoa(td.Revision),
	}
}

func containerMetadataFor(task Task, td *TaskDefinition, cd ContainerDefinition, c Container) ContainerMetadata {
	id := c.RuntimeID
	if id == "" {
		id = taskIDFromARN(task.TaskArn) + "-" + c.Name
	}

	m := ContainerMetadata{
		DockerID:      id,
		Name:          c.Name,
		DockerName:    "ecs-" + td.Family + "-" + strconv.Itoa(td.Revision) + "-" + c.Name,
		Image:         c.Image,
		ImageID:       c.ImageDigest,
		Labels:        containerLabels(task, td, c.Name),
		DesiredStatus: task.DesiredStatus,
		KnownStatus:   c.LastStatus,
		Limits:        containerLimits(cd),
		Type:          containerTypeNormal,
		ContainerARN:  c.ContainerArn,
		ExitCode:      c.ExitCode,
		CreatedAt:     timeStr(task.CreatedAt),
	}

	if task.StartedAt != nil {
		m.StartedAt = timeStr(task.StartedAt)
	}

	if cd.LogConfiguration != nil {
		m.LogDriver = cd.LogConfiguration.LogDriver
		m.LogOptions = cd.LogConfiguration.Options
	}

	if len(c.NetworkInterfaces) > 0 {
		n := metadataNetwork{NetworkMode: effectiveEC2NetworkMode(td.NetworkMode)}
		for _, ni := range c.NetworkInterfaces {
			if ni.PrivateIpv4Address != "" {
				n.IPv4Addresses = append(n.IPv4Addresses, ni.PrivateIpv4Address)
			}
		}

		m.Networks = []metadataNetwork{n}
	}

	for _, nb := range c.NetworkBindings {
		m.Ports = append(m.Ports, metadataPort{
			Protocol: nb.Protocol, ContainerPort: nb.ContainerPort, HostPort: nb.HostPort, HostIP: nb.BindIP,
		})
	}

	return m
}

func containerDefByName(td *TaskDefinition, name string) ContainerDefinition {
	for _, cd := range td.ContainerDefinitions {
		if cd.Name == name {
			return cd
		}
	}

	return ContainerDefinition{Name: name}
}

func (b *InMemoryBackend) taskAvailabilityZone() string { return b.region + "a" }

// TaskMetadataFor builds the task metadata v4 document for the task with ID taskID.
func (b *InMemoryBackend) TaskMetadataFor(taskID string, withTags bool) (TaskMetadata, bool) {
	task, ok := b.findTaskByID(taskID)
	if !ok {
		return TaskMetadata{}, false
	}

	b.mu.RLock("TaskMetadataFor")
	defer b.mu.RUnlock()

	td, err := b.findTaskDefinitionLocked(task.TaskDefinitionArn)
	if err != nil {
		return TaskMetadata{}, false
	}

	cpu, mem := td.CPU, td.Memory
	if task.Overrides != nil {
		if task.Overrides.CPU != "" {
			cpu = task.Overrides.CPU
		}

		if task.Overrides.Memory != "" {
			mem = task.Overrides.Memory
		}
	}

	doc := TaskMetadata{
		Cluster:          task.ClusterArn,
		TaskARN:          task.TaskArn,
		Family:           td.Family,
		Revision:         strconv.Itoa(td.Revision),
		DesiredStatus:    task.DesiredStatus,
		KnownStatus:      task.LastStatus,
		Limits:           metadataLimits{CPU: vcpuLimit(cpu), Memory: memLimit(mem)},
		AvailabilityZone: b.taskAvailabilityZone(),
		LaunchType:       task.LaunchType,
		Containers:       make([]ContainerMetadata, 0, len(task.Containers)),
	}

	if name, isSvc := serviceNameFromGroup(task.Group); isSvc {
		doc.ServiceName = name
	}

	for _, c := range task.Containers {
		doc.Containers = append(doc.Containers, containerMetadataFor(task, td, containerDefByName(td, c.Name), c))
	}

	if withTags {
		doc.TaskTags = tagMap(task.Tags)

		if task.ContainerInstanceArn != "" {
			doc.ContainerInstanceTags = tagMap(b.resourceTags[resourceTagKey(task.ContainerInstanceArn)])
		}
	}

	return doc, true
}

func (b *InMemoryBackend) containerStats(ctx context.Context, containerID string) (json.RawMessage, error) {
	r, ok := b.runner.(containerStatsRunner)
	if !ok {
		return nil, ErrContainerStatsUnavailable
	}

	return r.ContainerStatsJSON(ctx, containerID)
}

func (h *Handler) homeAndRegionBackends() []*InMemoryBackend {
	out := make([]*InMemoryBackend, 0, 1)

	for _, be := range h.RegionBackends() {
		if bk, ok := be.(*InMemoryBackend); ok {
			out = append(out, bk)
		}
	}

	return out
}

func (h *Handler) backendOfTask(taskID string) (*InMemoryBackend, Task, bool) {
	for _, bk := range h.homeAndRegionBackends() {
		if t, ok := bk.findTaskByID(taskID); ok {
			return bk, t, true
		}
	}

	return nil, Task{}, false
}

func credentialsError(c *echo.Context, status int, code, msg string) error {
	return c.JSON(status, credentialsErrorDocument{Code: code, Message: msg})
}

// ServeTaskCredentials serves the task-role credentials endpoint (container credential provider JSON).
func (h *Handler) ServeTaskCredentials(c *echo.Context) error {
	bk, task, ok := h.backendOfTask(c.Param("task"))
	if !ok || task.LastStatus == statusStopped || task.TaskRoleArn == "" {
		return credentialsError(c, http.StatusNotFound, "NotFound", "no credentials for task")
	}

	creds, err := bk.issueTaskCredentials(task)
	if err != nil {
		return credentialsError(c, http.StatusForbidden, "AccessDenied", err.Error())
	}

	return c.JSON(http.StatusOK, taskCredentialsDocument{
		RoleArn:         task.TaskRoleArn,
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		Token:           creds.SessionToken,
		Expiration:      creds.Expires.UTC().Format(time.RFC3339),
	})
}

// ServeTaskMetadata serves the task metadata endpoint v4: /, /task, /taskWithTags, /stats and /task/stats.
func (h *Handler) ServeTaskMetadata(c *echo.Context) error {
	bk, task, ok := h.backendOfTask(c.Param("task"))
	if !ok {
		return c.String(http.StatusNotFound, "task not found")
	}

	name := c.Param("container")

	var container *Container

	for i := range task.Containers {
		if task.Containers[i].Name == name {
			container = &task.Containers[i]
		}
	}

	if container == nil {
		return c.String(http.StatusNotFound, "container not found")
	}

	doc, ok := bk.TaskMetadataFor(c.Param("task"), false)
	if !ok {
		return c.String(http.StatusNotFound, "task not found")
	}

	switch strings.Trim(c.Param("*"), "/") {
	case "":
		return c.JSON(http.StatusOK, containerDocByName(doc, name))
	case "task":
		return c.JSON(http.StatusOK, doc)
	case tagsMetadataPath:
		tagged, _ := bk.TaskMetadataFor(c.Param("task"), true)

		return c.JSON(http.StatusOK, tagged)
	case "stats":
		return h.serveStats(c, bk, []ContainerMetadata{containerDocByName(doc, name)}, false)
	case "task/stats":
		return h.serveStats(c, bk, doc.Containers, true)
	default:
		return c.String(http.StatusNotFound, "unknown metadata path")
	}
}

func containerDocByName(doc TaskMetadata, name string) ContainerMetadata {
	for _, cm := range doc.Containers {
		if cm.Name == name {
			return cm
		}
	}

	return ContainerMetadata{}
}

func (h *Handler) serveStats(c *echo.Context, bk *InMemoryBackend, containers []ContainerMetadata, byID bool) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), statsTimeout)
	defer cancel()

	all := make(map[string]json.RawMessage, len(containers))

	for _, cm := range containers {
		raw, err := bk.containerStats(ctx, cm.DockerID)
		if err != nil {
			return c.String(http.StatusNotFound, err.Error())
		}

		if !byID {
			return c.JSONBlob(http.StatusOK, raw)
		}

		all[cm.DockerID] = raw
	}

	return c.JSON(http.StatusOK, all)
}

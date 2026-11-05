package ecs

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"slices"
	"strings"

	mobycontainer "github.com/moby/moby/api/types/container"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	dockernetwork "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	defaultPauseImage = "registry.k8s.io/pause:3.10"
	pauseImageEnv     = "GOPHERSTACK_ECS_PAUSE_IMAGE"

	labelInfra = "gopherstack.ecs.infra"

	dockerNetworkContainerPrefix = "container:"
	sysctlNetPrefix              = "net."
)

// taskNet is an awsvpc task's own network: a docker network plus the pause container owning its namespace.
type taskNet struct {
	networkID string
	pauseID   string
	ip        string
}

func pauseImage() string {
	if img := os.Getenv(pauseImageEnv); img != "" {
		return img
	}

	return defaultPauseImage
}

func taskNetworkName(task *Task) string { return "gopherstack-ecs-" + taskIDFromARN(task.TaskArn) }

// netSetup is how one container joins the task's networking.
type netSetup struct {
	mode   string
	shared bool
}

// containerNetwork maps an ECS networkMode to the docker network mode; shared means the container joins the
// task's pause container and carries no network settings of its own.
func containerNetwork(ecsMode string, net *taskNet) netSetup {
	switch ecsMode {
	case networkModeHost:
		return netSetup{mode: "host"}
	case networkModeNone:
		return netSetup{mode: "none"}
	case networkModeAwsvpc:
		if net != nil {
			return netSetup{mode: dockerNetworkContainerPrefix + net.pauseID, shared: true}
		}
	}

	return netSetup{}
}

// setupTaskNetwork creates the awsvpc task's docker network and pause container and records its address.
func (r *realDockerRunner) setupTaskNetwork(ctx context.Context, l *launch, order []ContainerDefinition) error {
	task := l.task
	img := pauseImage()

	if err := r.pullImage(ctx, img, ""); err != nil {
		return &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	name := taskNetworkName(task)

	netID, err := r.cli.NetworkCreate(ctx, name)
	if err != nil {
		return &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	tn := &taskNet{networkID: netID}
	r.mu.Lock()
	r.taskNets[task.TaskArn] = tn
	r.mu.Unlock()

	var mappings []PortMapping

	sysctls := map[string]string{}

	for _, cd := range order {
		mappings = append(mappings, cd.PortMappings...)

		for _, sc := range cd.SystemControls {
			if strings.HasPrefix(sc.Namespace, sysctlNetPrefix) {
				sysctls[sc.Namespace] = sc.Value
			}
		}
	}

	pb, exposed := buildPortMappings(mappings)
	diverted := r.divertPorts(task, mappings, pb)

	labels := container.ManagedLabels("ecs", "")
	labels[labelInfra] = task.TaskArn

	resp, err := r.cli.ContainerCreate(ctx,
		&dockertypes.Config{Image: img, Labels: labels, ExposedPorts: exposed},
		&dockertypes.HostConfig{
			NetworkMode:  mobycontainer.NetworkMode(name),
			PortBindings: pb,
			Sysctls:      sysctls,
		}, nil, nil, "")
	if err != nil {
		releasePending(diverted)

		return &taskLaunchError{Reason: "Task failed to start: create pause container: " + err.Error(), Err: err}
	}

	tn.pauseID = resp.ID
	r.stashFronts(resp.ID, diverted)

	r.mu.Lock()
	r.stopOrder[task.TaskArn] = append(r.stopOrder[task.TaskArn], resp.ID)
	r.mu.Unlock()

	if err = r.attachContainer(ctx, resp.ID); err != nil {
		return &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	if err = r.cli.ContainerStart(ctx, resp.ID, dockertypes.StartOptions{}); err != nil {
		return &taskLaunchError{Reason: "Task failed to start: start pause container: " + err.Error(), Err: err}
	}

	if err = r.startFronts(resp.ID); err != nil {
		return &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	tn.ip = r.networkAddress(ctx, resp.ID, name)
	l.net = tn

	return nil
}

// networkAddress returns containerID's address on the named docker network, empty when unknown.
func (r *realDockerRunner) networkAddress(ctx context.Context, containerID, network string) string {
	resp, err := r.cli.ContainerInspect(ctx, containerID)
	if err != nil || resp.NetworkSettings == nil {
		return ""
	}

	if ep := resp.NetworkSettings.Networks[network]; ep != nil && ep.IPAddress.IsValid() {
		return ep.IPAddress.String()
	}

	return ""
}

// TaskAddress is the address of an awsvpc task's network namespace, the task ENI's private IPv4 address.
func (r *realDockerRunner) TaskAddress(taskArn string) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	if tn := r.taskNets[taskArn]; tn != nil {
		return tn.ip
	}

	return ""
}

// removeTaskNetwork removes the pause container (unless already in removed) and docker network of taskArn.
func (r *realDockerRunner) removeTaskNetwork(ctx context.Context, taskArn string, removed []string) {
	r.mu.Lock()
	tn := r.taskNets[taskArn]
	delete(r.taskNets, taskArn)
	r.mu.Unlock()

	if tn == nil {
		return
	}

	log := logger.Load(ctx)

	if tn.pauseID != "" && !slices.Contains(removed, tn.pauseID) {
		r.closeFronts(tn.pauseID)
		r.cancelLogForwarding(tn.pauseID)

		timeout := 0
		_ = r.cli.ContainerStop(ctx, tn.pauseID, dockertypes.StopOptions{Timeout: &timeout})

		if err := r.cli.ContainerRemove(
			ctx, tn.pauseID, dockertypes.RemoveOptions{Force: true, RemoveVolumes: true},
		); err != nil {
			log.WarnContext(ctx, "failed to remove pause container", "containerID", tn.pauseID, "error", err)
		}
	}

	if err := r.cli.NetworkRemove(ctx, tn.networkID); err != nil {
		log.WarnContext(ctx, "failed to remove task network", "network", tn.networkID, "error", err)
	}
}

// bridgeMappings fills hostPort 0 of bridge-mode mappings from the host port placement allocated for the
// container, so the docker binding matches the NetworkBindings reported on the task.
func bridgeMappings(task *Task, cd ContainerDefinition) []PortMapping {
	out := make([]PortMapping, len(cd.PortMappings))
	copy(out, cd.PortMappings)

	for i := range out {
		if out[i].HostPort != 0 || out[i].ContainerPort == 0 {
			continue
		}

		out[i].HostPort = allocatedHostPort(task, cd.Name, out[i])
	}

	return out
}

func allocatedHostPort(task *Task, containerName string, pm PortMapping) int {
	proto := pm.Protocol
	if proto == "" {
		proto = transportTCP
	}

	for _, c := range task.Containers {
		if c.Name != containerName {
			continue
		}

		for _, nb := range c.NetworkBindings {
			if nb.ContainerPort == pm.ContainerPort && nb.Protocol == proto {
				return nb.HostPort
			}
		}
	}

	return 0
}

// taskNetworkMode is the effective networkMode; the default is bridge.
func taskNetworkMode(td *TaskDefinition) string { return effectiveEC2NetworkMode(td.NetworkMode) }

// addDynamicBindings lets Docker pick the host port for bridge mappings placement allocated none for.
func addDynamicBindings(pb dockernetwork.PortMap, mappings []PortMapping) {
	for _, pm := range mappings {
		if pm.HostPort != 0 || pm.ContainerPort == 0 || pm.ContainerPortRange != "" {
			continue
		}

		proto := pm.Protocol
		if proto == "" {
			proto = transportTCP
		}

		key, err := dockernetwork.ParsePort(fmt.Sprintf("%d/%s", pm.ContainerPort, proto))
		if err != nil {
			continue
		}

		pb[key] = []dockernetwork.PortBinding{{HostIP: netip.MustParseAddr(bindAllInterfaces)}}
	}
}

package ecs

import (
	"fmt"
	"maps"
	"net/netip"
	"strings"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	dockernetwork "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
)

const (
	mebibyte = int64(1) << 20

	nanoCPUsPerVCPU    = int64(1_000_000_000)
	cpuUnitsPerVCPUInt = int64(1024)

	defaultStopTimeoutSeconds = 30

	defaultHealthInterval = 30 * time.Second
	defaultHealthTimeout  = 5 * time.Second
	defaultHealthRetries  = 3

	healthCheckNone = "NONE"
)

// containerInput is everything createContainer needs to map one container definition to Docker.
type containerInput struct {
	task         *Task
	portBindings dockernetwork.PortMap
	exposedPorts dockernetwork.PortSet
	pidMode      string
	ipcMode      string
	env          []string
	mounts       []mount.Mount
	volumesFrom  []string
	network      netSetup
	logSecrets   map[string]string
	cd           ContainerDefinition
}

// buildDockerConfigs maps an ECS container definition onto the Docker create and host configs.
func buildDockerConfigs(in containerInput) (*dockertypes.Config, *dockertypes.HostConfig, error) {
	cd := in.cd

	hostCfg := &dockertypes.HostConfig{
		PortBindings:   in.portBindings,
		Mounts:         in.mounts,
		VolumesFrom:    in.volumesFrom,
		Privileged:     cd.Privileged,
		ReadonlyRootfs: cd.ReadonlyRootFilesystem,
		DNSSearch:      cd.DNSSearchDomains,
		SecurityOpt:    cd.DockerSecurityOptions,
		PidMode:        mobycontainer.PidMode(in.pidMode),
		IpcMode:        mobycontainer.IpcMode(in.ipcMode),
		NetworkMode:    mobycontainer.NetworkMode(in.network.mode),
	}

	if err := applyNetworkSettings(hostCfg, cd, in.network); err != nil {
		return nil, nil, err
	}

	logCfg, err := buildLogConfig(cd.LogConfiguration, in.logSecrets)
	if err != nil {
		return nil, nil, fmt.Errorf("container %q: %w", cd.Name, err)
	}

	hostCfg.LogConfig = logCfg

	taskCPU, taskMem := taskLimits(in.task)
	applyResourceLimits(hostCfg, cd)
	applyTaskLimits(hostCfg, cd, taskCPU, taskMem)
	applyLinuxParameters(hostCfg, cd)

	labels := map[string]string{}
	maps.Copy(labels, cd.DockerLabels)

	labels["gopherstack.ecs.task"] = in.task.TaskArn
	labels["gopherstack.ecs.cluster"] = in.task.ClusterArn

	stopTimeout := defaultStopTimeoutSeconds
	if cd.StopTimeout != nil {
		stopTimeout = *cd.StopTimeout
	}

	cfg := &dockertypes.Config{
		Image:           cd.Image,
		Env:             in.env,
		ExposedPorts:    in.exposedPorts,
		Labels:          labels,
		Cmd:             cd.Command,
		Entrypoint:      cd.EntryPoint,
		User:            cd.User,
		WorkingDir:      cd.WorkingDirectory,
		Hostname:        cd.Hostname,
		NetworkDisabled: cd.DisableNetworking && !in.network.shared,
		OpenStdin:       cd.Interactive,
		Tty:             cd.PseudoTerminal,
		StopTimeout:     &stopTimeout,
		Healthcheck:     buildHealthConfig(cd.HealthCheck),
	}

	if in.network.shared {
		cfg.Hostname = ""
	}

	return cfg, hostCfg, nil
}

// applyNetworkSettings sets DNS, extra hosts and sysctls; a container joined to the task's pause container
// takes none of the namespace-level ones (Docker rejects them) and net.* sysctls live on the pause container.
func applyNetworkSettings(hostCfg *dockertypes.HostConfig, cd ContainerDefinition, nw netSetup) error {
	if !nw.shared {
		dns, err := parseDNSServers(cd.DNSServers)
		if err != nil {
			return err
		}

		hostCfg.DNS = dns

		for _, h := range cd.ExtraHosts {
			hostCfg.ExtraHosts = append(hostCfg.ExtraHosts, h.Hostname+":"+h.IPAddress)
		}
	}

	for _, sc := range cd.SystemControls {
		if nw.shared && strings.HasPrefix(sc.Namespace, sysctlNetPrefix) {
			continue
		}

		if hostCfg.Sysctls == nil {
			hostCfg.Sysctls = make(map[string]string, len(cd.SystemControls))
		}

		hostCfg.Sysctls[sc.Namespace] = sc.Value
	}

	return nil
}

func parseDNSServers(servers []string) ([]netip.Addr, error) {
	out := make([]netip.Addr, 0, len(servers))

	for _, s := range servers {
		addr, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("invalid dnsServers entry %q: %w", s, err)
		}

		out = append(out, addr)
	}

	return out, nil
}

// applyResourceLimits maps cpu (units), memory and memoryReservation (MiB), and ulimits.
func applyResourceLimits(hc *dockertypes.HostConfig, cd ContainerDefinition) {
	hc.CPUShares = int64(cd.CPU)
	hc.Memory = int64(cd.Memory) * mebibyte
	hc.MemoryReservation = int64(cd.MemoryReservation) * mebibyte

	for _, u := range cd.Ulimits {
		hc.Ulimits = append(hc.Ulimits, &mobycontainer.Ulimit{
			Name: u.Name,
			Soft: int64(u.SoftLimit),
			Hard: int64(u.HardLimit),
		})
	}
}

// applyTaskLimits bounds a container by the task's size: a container with no hard memory limit may use up to the
// task's memory, and none may use more than the task's vCPUs. Docker has no per-task cgroup, so the sum across
// containers is not enforced.
func applyTaskLimits(hc *dockertypes.HostConfig, cd ContainerDefinition, taskCPU, taskMem int) {
	if taskMem > 0 && cd.Memory == 0 {
		hc.Memory = int64(taskMem) * mebibyte
	}

	if taskCPU > 0 {
		hc.NanoCPUs = int64(taskCPU) * nanoCPUsPerVCPU / cpuUnitsPerVCPUInt
	}
}

// applyLinuxParameters maps linuxParameters; maxSwap is swap on top of memory, as in ECS.
func applyLinuxParameters(hc *dockertypes.HostConfig, cd ContainerDefinition) {
	lp := cd.LinuxParameters
	if lp == nil {
		return
	}

	if lp.Capabilities != nil {
		hc.CapAdd = lp.Capabilities.Add
		hc.CapDrop = lp.Capabilities.Drop
	}

	if lp.InitProcessEnabled != nil {
		init := *lp.InitProcessEnabled
		hc.Init = &init
	}

	if lp.SharedMemorySize != nil {
		hc.ShmSize = int64(*lp.SharedMemorySize) * mebibyte
	}

	if lp.MaxSwap != nil {
		hc.MemorySwap = hc.Memory + int64(*lp.MaxSwap)*mebibyte
	}

	if lp.Swappiness != nil {
		sw := int64(*lp.Swappiness)
		hc.MemorySwappiness = &sw
	}

	for _, d := range lp.Devices {
		containerPath := d.ContainerPath
		if containerPath == "" {
			containerPath = d.HostPath
		}

		hc.Devices = append(hc.Devices, mobycontainer.DeviceMapping{
			PathOnHost:        d.HostPath,
			PathInContainer:   containerPath,
			CgroupPermissions: devicePermissions(d.Permissions),
		})
	}

	if len(lp.Tmpfs) > 0 {
		hc.Tmpfs = make(map[string]string, len(lp.Tmpfs))
		for _, t := range lp.Tmpfs {
			opts := append([]string{fmt.Sprintf("size=%dm", t.Size)}, t.MountOptions...)
			hc.Tmpfs[t.ContainerPath] = strings.Join(opts, ",")
		}
	}
}

// devicePermissions converts read/write/mknod to Docker's "rwm" letters; empty means all.
func devicePermissions(perms []string) string {
	if len(perms) == 0 {
		return "rwm"
	}

	var b strings.Builder

	for _, p := range perms {
		switch strings.ToLower(p) {
		case "read":
			b.WriteByte('r')
		case "write":
			b.WriteByte('w')
		case "mknod":
			b.WriteByte('m')
		}
	}

	return b.String()
}

// buildHealthConfig maps an ECS healthCheck (seconds) onto Docker's HEALTHCHECK, using ECS's defaults.
func buildHealthConfig(hc *HealthCheck) *mobycontainer.HealthConfig {
	if hc == nil || len(hc.Command) == 0 {
		return nil
	}

	if hc.Command[0] == healthCheckNone {
		return &mobycontainer.HealthConfig{Test: []string{healthCheckNone}}
	}

	cfg := &mobycontainer.HealthConfig{
		Test:        hc.Command,
		Interval:    defaultHealthInterval,
		Timeout:     defaultHealthTimeout,
		Retries:     defaultHealthRetries,
		StartPeriod: time.Duration(hc.StartPeriod) * time.Second,
	}

	if hc.Interval > 0 {
		cfg.Interval = time.Duration(hc.Interval) * time.Second
	}

	if hc.Timeout > 0 {
		cfg.Timeout = time.Duration(hc.Timeout) * time.Second
	}

	if hc.Retries > 0 {
		cfg.Retries = hc.Retries
	}

	return cfg
}

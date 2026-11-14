package ecs

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	k8sMaxNameLen        = 63
	k8sCPUUnitsPerVCPU   = 1024
	k8sMilliPerVCPU      = 1000
	k8sSecretKeySep      = "."
	k8sDefaultCommandSh  = "/bin/sh"
	k8sPodNamePrefix     = "ecs-"
	k8sShmVolume         = "dev-shm"
	k8sTaskMemoryGBUnits = 1024
)

var (
	errK8sUnsupported = errors.New("not supported by the kubernetes task runtime")

	k8sNonLabel = regexp.MustCompile(`[^a-z0-9-]+`)
)

// k8sContainer is one task container as planned onto the Pod.
type k8sContainer struct {
	name string
	def  ContainerDefinition
	init bool
}

// k8sPlan is a task definition mapped onto a Pod.
type k8sPlan struct {
	secret map[string][]byte
	// containers is in task definition order.
	containers []k8sContainer
	spec       corev1.PodSpec
}

// k8sPlanInput carries everything a task definition needs besides the definition itself.
type k8sPlanInput struct {
	plain     map[string][]string
	sensitive map[string][]string
	td        *TaskDefinition
	podName   string
	order     []ContainerDefinition
}

func unsupportedf(format string, args ...any) error {
	return fmt.Errorf("%w: %w: %s", container.ErrSpecUnsupported, errK8sUnsupported, fmt.Sprintf(format, args...))
}

func k8sPodName(taskID string) string {
	name := strings.Trim(k8sNonLabel.ReplaceAllString(strings.ToLower(k8sPodNamePrefix+taskID), "-"), "-")
	if len(name) > k8sMaxNameLen {
		name = strings.TrimRight(name[:k8sMaxNameLen], "-")
	}

	return name
}

func k8sLabelName(s string, used map[string]bool) string {
	base := strings.Trim(k8sNonLabel.ReplaceAllString(strings.ToLower(s), "-"), "-")
	if base == "" {
		base = "c"
	}

	if len(base) > k8sMaxNameLen-4 {
		base = strings.TrimRight(base[:k8sMaxNameLen-4], "-")
	}

	name := base
	for i := 2; used[name]; i++ {
		name = base + "-" + strconv.Itoa(i)
	}

	used[name] = true

	return name
}

// planK8sPod maps a task definition onto a Pod. Anything a Pod cannot honour is refused with
// container.ErrSpecUnsupported rather than approximated.
func planK8sPod(in k8sPlanInput) (*k8sPlan, error) {
	td := in.td
	plan := &k8sPlan{secret: map[string][]byte{}}

	if err := planPodLevel(plan, td); err != nil {
		return nil, err
	}

	vols, err := planVolumes(plan, td)
	if err != nil {
		return nil, err
	}

	inits, err := planInitSet(in.order)
	if err != nil {
		return nil, err
	}

	used := map[string]bool{}
	names := make(map[string]string, len(in.order))

	for _, cd := range in.order {
		names[cd.Name] = k8sLabelName(cd.Name, used)
	}

	mounts := map[string][]corev1.VolumeMount{}

	for _, cd := range in.order {
		m, mErr := containerMountsFor(plan, cd, names[cd.Name], vols)
		if mErr != nil {
			return nil, mErr
		}

		mounts[cd.Name] = m
	}

	grace := int64(defaultStopTimeoutSeconds)
	graceSet := false

	for _, cd := range in.order {
		kc, cErr := buildK8sContainer(plan, in, cd, names[cd.Name], inits[cd.Name], mounts)
		if cErr != nil {
			return nil, cErr
		}

		if cd.StopTimeout != nil {
			if !graceSet || int64(*cd.StopTimeout) > grace {
				grace = int64(*cd.StopTimeout)
			}

			graceSet = true
		}

		if inits[cd.Name] {
			plan.spec.InitContainers = append(plan.spec.InitContainers, kc)
		} else {
			plan.spec.Containers = append(plan.spec.Containers, kc)
		}

		plan.containers = append(plan.containers, k8sContainer{def: cd, name: names[cd.Name], init: inits[cd.Name]})
	}

	plan.spec.TerminationGracePeriodSeconds = &grace
	plan.containers = inDefinitionOrder(plan.containers, td.ContainerDefinitions)

	if len(plan.secret) == 0 {
		plan.secret = nil
	}

	return plan, nil
}

func inDefinitionOrder(planned []k8sContainer, defs []ContainerDefinition) []k8sContainer {
	byName := make(map[string]k8sContainer, len(planned))
	for _, c := range planned {
		byName[c.def.Name] = c
	}

	out := make([]k8sContainer, 0, len(defs))
	for _, d := range defs {
		out = append(out, byName[d.Name])
	}

	return out
}

func planPodLevel(plan *k8sPlan, td *TaskDefinition) error {
	switch td.NetworkMode {
	case networkModeHost:
		return unsupportedf("networkMode host (a Pod has its own network namespace)")
	case "none":
		return unsupportedf("networkMode none")
	}

	switch td.PidMode {
	case networkModeHost:
		return unsupportedf("pidMode host")
	case "task":
		plan.spec.ShareProcessNamespace = new(true)
	}

	if td.IpcMode == "host" {
		return unsupportedf("ipcMode host")
	}

	return nil
}

// planVolumes maps task volumes to emptyDir; every other volume type needs host or network storage.
func planVolumes(plan *k8sPlan, td *TaskDefinition) (map[string]string, error) {
	vols := make(map[string]string, len(td.Volumes))
	used := map[string]bool{}

	for _, v := range td.Volumes {
		switch {
		case v.EFSVolumeConfiguration != nil:
			return nil, unsupportedf("volume %q: efsVolumeConfiguration", v.Name)
		case v.ConfiguredAtLaunch:
			return nil, unsupportedf("volume %q: configuredAtLaunch", v.Name)
		case v.DockerVolumeConfig != nil && v.DockerVolumeConfig.Scope == volumeScopeShared:
			return nil, unsupportedf("volume %q: shared docker volume", v.Name)
		case v.Host != nil && v.Host.SourcePath != "":
			return nil, unsupportedf("volume %q: host path %q", v.Name, v.Host.SourcePath)
		}

		name := "vol-" + k8sLabelName(v.Name, used)
		if len(name) > k8sMaxNameLen {
			name = strings.TrimRight(name[:k8sMaxNameLen], "-")
		}

		vols[v.Name] = name
		plan.spec.Volumes = append(plan.spec.Volumes, corev1.Volume{
			Name: name, EmptyDir: &corev1.EmptyDirVolumeSource{},
		})
	}

	return vols, nil
}

// planInitSet returns the containers some other container waits on with SUCCESS: they run as init containers.
// START is best effort (Pod containers start together, in dependency order); COMPLETE and HEALTHY have no Pod
// equivalent.
func planInitSet(defs []ContainerDefinition) (map[string]bool, error) {
	inits := map[string]bool{}

	for _, cd := range defs {
		for _, d := range cd.DependsOn {
			switch d.Condition {
			case dependsStart:
			case dependsSuccess:
				inits[d.ContainerName] = true
			case dependsComplete:
				return nil, unsupportedf("container %q: dependsOn %q condition COMPLETE", cd.Name, d.ContainerName)
			case dependsHealthy:
				return nil, unsupportedf("container %q: dependsOn %q condition HEALTHY", cd.Name, d.ContainerName)
			default:
				return nil, unsupportedf("container %q: dependsOn condition %q", cd.Name, d.Condition)
			}
		}
	}

	for _, cd := range defs {
		if !inits[cd.Name] {
			continue
		}

		if cd.Essential {
			return nil, unsupportedf("container %q: dependsOn SUCCESS target cannot be essential", cd.Name)
		}

		for _, d := range cd.DependsOn {
			if !inits[d.ContainerName] {
				return nil, unsupportedf(
					"init container %q cannot depend on %q, which starts with the Pod's containers",
					cd.Name,
					d.ContainerName,
				)
			}
		}
	}

	return inits, nil
}

func containerMountsFor(
	plan *k8sPlan, cd ContainerDefinition, k8sName string, vols map[string]string,
) ([]corev1.VolumeMount, error) {
	out := make([]corev1.VolumeMount, 0, len(cd.MountPoints))

	for _, mp := range cd.MountPoints {
		vol, ok := vols[mp.SourceVolume]
		if !ok {
			return nil, fmt.Errorf("container %q %w %q", cd.Name, errUndefinedVolume, mp.SourceVolume)
		}

		out = append(out, corev1.VolumeMount{Name: vol, MountPath: mp.ContainerPath, ReadOnly: mp.ReadOnly})
	}

	lp := cd.LinuxParameters
	if lp == nil {
		return out, nil
	}

	for i, t := range lp.Tmpfs {
		name := fmt.Sprintf("tmpfs-%s-%d", k8sName, i)
		plan.spec.Volumes = append(plan.spec.Volumes, corev1.Volume{
			Name: name, EmptyDir: memoryDir(t.Size),
		})
		out = append(out, corev1.VolumeMount{Name: name, MountPath: t.ContainerPath})
	}

	if lp.SharedMemorySize != nil {
		name := fmt.Sprintf("%s-%s", k8sShmVolume, k8sName)
		plan.spec.Volumes = append(plan.spec.Volumes, corev1.Volume{
			Name: name, EmptyDir: memoryDir(*lp.SharedMemorySize),
		})
		out = append(out, corev1.VolumeMount{Name: name, MountPath: "/dev/shm"})
	}

	return out, nil
}

func memoryDir(mib int) *corev1.EmptyDirVolumeSource {
	src := &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}
	if mib > 0 {
		src.SizeLimit = resource.NewQuantity(int64(mib)*mebibyte, resource.BinarySI)
	}

	return src
}

func buildK8sContainer(
	plan *k8sPlan,
	in k8sPlanInput,
	cd ContainerDefinition,
	name string,
	isInit bool,
	mounts map[string][]corev1.VolumeMount,
) (corev1.Container, error) {
	if err := rejectContainerFields(cd); err != nil {
		return corev1.Container{}, err
	}

	sc, err := containerSecurity(cd)
	if err != nil {
		return corev1.Container{}, err
	}

	vm, err := withVolumesFrom(cd, mounts)
	if err != nil {
		return corev1.Container{}, err
	}

	podNetworkFields(&plan.spec, cd)

	c := corev1.Container{
		Name:            name,
		Image:           cd.Image,
		Command:         cd.EntryPoint,
		Args:            cd.Command,
		WorkingDir:      cd.WorkingDirectory,
		Env:             k8sEnv(plan, in, cd, name),
		Resources:       k8sResources(cd, in.td),
		SecurityContext: sc,
		VolumeMounts:    vm,
		Stdin:           cd.Interactive,
		TTY:             cd.PseudoTerminal,
	}

	if isInit {
		return c, nil
	}

	c.Ports = k8sPorts(cd.PortMappings)

	if probe := k8sProbe(cd.HealthCheck); probe != nil {
		c.ReadinessProbe = probe
	}

	return c, nil
}

func rejectContainerFields(cd ContainerDefinition) error {
	switch {
	case cd.Privileged:
		return unsupportedf("container %q: privileged", cd.Name)
	case cd.DisableNetworking:
		return unsupportedf("container %q: disableNetworking (containers share the Pod network)", cd.Name)
	case cd.RestartPolicy != nil && cd.RestartPolicy.Enabled:
		return unsupportedf("container %q: restartPolicy", cd.Name)
	case cd.RepositoryCredentials != nil:
		return unsupportedf("container %q: repositoryCredentials", cd.Name)
	case cd.LinuxParameters != nil && len(cd.LinuxParameters.Devices) > 0:
		return unsupportedf("container %q: linuxParameters.devices", cd.Name)
	}

	return nil
}

func containerSecurity(cd ContainerDefinition) (*corev1.SecurityContext, error) {
	sc := &corev1.SecurityContext{
		AllowPrivilegeEscalation: new(false),
		ReadOnlyRootFilesystem:   new(cd.ReadonlyRootFilesystem),
	}

	if cd.User != "" {
		uid, gid, err := parseK8sUser(cd.User)
		if err != nil {
			return nil, unsupportedf("container %q: %v", cd.Name, err)
		}

		sc.RunAsUser = &uid
		sc.RunAsGroup = gid
	}

	if lp := cd.LinuxParameters; lp != nil && lp.Capabilities != nil {
		sc.Capabilities = &corev1.Capabilities{}
		for _, c := range lp.Capabilities.Add {
			sc.Capabilities.Add = append(sc.Capabilities.Add, corev1.Capability(c))
		}

		for _, c := range lp.Capabilities.Drop {
			sc.Capabilities.Drop = append(sc.Capabilities.Drop, corev1.Capability(c))
		}
	}

	return sc, nil
}

var errK8sUserForm = errors.New("user must be numeric uid[:gid] or root")

func parseK8sUser(user string) (int64, *int64, error) {
	uidStr, gidStr, hasGID := strings.Cut(user, ":")
	if uidStr == "root" {
		uidStr = "0"
	}

	uid, err := strconv.ParseInt(uidStr, 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: %q", errK8sUserForm, user)
	}

	if !hasGID {
		return uid, nil, nil
	}

	if gidStr == "root" {
		gidStr = "0"
	}

	gid, err := strconv.ParseInt(gidStr, 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: %q", errK8sUserForm, user)
	}

	return uid, &gid, nil
}

// withVolumesFrom adds the mounts of every volumesFrom source, read-only when requested.
func withVolumesFrom(cd ContainerDefinition, mounts map[string][]corev1.VolumeMount) ([]corev1.VolumeMount, error) {
	out := append([]corev1.VolumeMount(nil), mounts[cd.Name]...)
	seen := map[string]bool{}

	for _, m := range out {
		seen[m.MountPath] = true
	}

	for _, vf := range cd.VolumesFrom {
		src, ok := mounts[vf.SourceContainer]
		if !ok {
			return nil, unsupportedf("container %q: volumesFrom %q", cd.Name, vf.SourceContainer)
		}

		for _, m := range src {
			if seen[m.MountPath] {
				continue
			}

			seen[m.MountPath] = true
			m.ReadOnly = m.ReadOnly || vf.ReadOnly
			out = append(out, m)
		}
	}

	return out, nil
}

// podNetworkFields folds the per-container network settings into the Pod, which has one network namespace.
func podNetworkFields(spec *corev1.PodSpec, cd ContainerDefinition) {
	if len(cd.DNSServers) > 0 || len(cd.DNSSearchDomains) > 0 {
		if spec.DNSConfig == nil {
			spec.DNSConfig = &corev1.PodDNSConfig{}
		}

		spec.DNSConfig.Nameservers = appendUnique(spec.DNSConfig.Nameservers, cd.DNSServers)
		spec.DNSConfig.Searches = appendUnique(spec.DNSConfig.Searches, cd.DNSSearchDomains)
		spec.DNSPolicy = corev1.DNSNone
	}

	for _, h := range cd.ExtraHosts {
		addHostAlias(spec, h.IPAddress, h.Hostname)
	}

	if cd.Hostname != "" && spec.Hostname == "" {
		spec.Hostname = k8sLabelName(cd.Hostname, map[string]bool{})
	}

	if len(cd.SystemControls) > 0 {
		if spec.SecurityContext == nil {
			spec.SecurityContext = &corev1.PodSecurityContext{}
		}

		for _, sc := range cd.SystemControls {
			spec.SecurityContext.Sysctls = append(spec.SecurityContext.Sysctls,
				corev1.Sysctl{Name: sc.Namespace, Value: sc.Value})
		}
	}
}

func appendUnique(dst, add []string) []string {
	for _, a := range add {
		found := slices.Contains(dst, a)

		if !found {
			dst = append(dst, a)
		}
	}

	return dst
}

func addHostAlias(spec *corev1.PodSpec, ip, host string) {
	for i := range spec.HostAliases {
		if spec.HostAliases[i].IP == ip {
			spec.HostAliases[i].Hostnames = appendUnique(spec.HostAliases[i].Hostnames, []string{host})

			return
		}
	}

	spec.HostAliases = append(spec.HostAliases, corev1.HostAlias{IP: ip, Hostnames: []string{host}})
}

func k8sSecretKey(container, env string) string { return container + k8sSecretKeySep + env }

// k8sEnv merges plain and sensitive entries (later wins). Sensitive values are stored in the Pod's Secret and
// referenced by key, so they never appear in the Pod spec.
func k8sEnv(plan *k8sPlan, in k8sPlanInput, cd ContainerDefinition, name string) []corev1.EnvVar {
	type entry struct {
		value     string
		sensitive bool
	}

	final := map[string]entry{}

	var order []string

	add := func(kv string, sensitive bool) {
		k, v, _ := strings.Cut(kv, "=")
		if _, seen := final[k]; !seen {
			order = append(order, k)
		}

		final[k] = entry{value: v, sensitive: sensitive}
	}

	for _, kv := range in.plain[cd.Name] {
		add(kv, false)
	}

	for _, kv := range in.sensitive[cd.Name] {
		add(kv, true)
	}

	out := make([]corev1.EnvVar, 0, len(order))

	for _, k := range order {
		e := final[k]
		if !e.sensitive {
			out = append(out, corev1.EnvVar{Name: k, Value: e.value})

			continue
		}

		key := k8sSecretKey(name, k)
		plan.secret[key] = []byte(e.value)
		out = append(out, corev1.EnvVar{Name: k, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				Name: in.podName, Key: key,
			},
		}})
	}

	return out
}

func portNumber(p int) int32 { return int32(min(max(p, 0), math.MaxUint16)) }

// k8sPorts declares containerPort entries; reachability is by Pod IP, so hostPort is not mapped.
func k8sPorts(mappings []PortMapping) []corev1.ContainerPort {
	var out []corev1.ContainerPort

	for _, pm := range mappings {
		if pm.ContainerPort == 0 {
			continue
		}

		proto := corev1.ProtocolTCP
		if strings.EqualFold(pm.Protocol, "udp") {
			proto = corev1.ProtocolUDP
		}

		out = append(out, corev1.ContainerPort{ContainerPort: portNumber(pm.ContainerPort), Protocol: proto})
	}

	return out
}

// k8sProbe maps an ECS health check to an exec readiness probe; NONE or an empty check maps to none.
func k8sProbe(hc *HealthCheck) *corev1.Probe {
	if hc == nil || len(hc.Command) == 0 || hc.Command[0] == healthCheckNone {
		return nil
	}

	var cmd []string

	switch hc.Command[0] {
	case "CMD":
		cmd = hc.Command[1:]
	case "CMD-SHELL":
		if len(hc.Command) <= 1 {
			return nil
		}

		cmd = []string{k8sDefaultCommandSh, "-c", hc.Command[1]}
	default:
		return nil
	}

	if len(cmd) == 0 {
		return nil
	}

	return &corev1.Probe{
		Exec:                &corev1.ExecAction{Command: cmd},
		PeriodSeconds:       int32(durationOrDefault(hc.Interval, defaultHealthInterval)), //nolint:gosec // seconds
		TimeoutSeconds:      int32(durationOrDefault(hc.Timeout, defaultHealthTimeout)),   //nolint:gosec // seconds
		FailureThreshold:    int32(orDefault(hc.Retries, defaultHealthRetries)),           //nolint:gosec // small
		InitialDelaySeconds: int32(hc.StartPeriod),                                        //nolint:gosec // seconds
	}
}

func durationOrDefault(seconds int, def time.Duration) int {
	if seconds > 0 {
		return seconds
	}

	return int(def.Seconds())
}

func orDefault(v, def int) int {
	if v > 0 {
		return v
	}

	return def
}

// k8sResources applies ECS sizing: container memory is a hard limit and memoryReservation the request; container
// cpu is a share (request); task cpu and memory cap every container, as ECS caps the task as a whole.
func k8sResources(cd ContainerDefinition, td *TaskDefinition) corev1.ResourceRequirements {
	req := corev1.ResourceRequirements{Limits: make(corev1.ResourceList), Requests: make(corev1.ResourceList)}

	taskMem, hasTaskMem := parseTaskMemoryMiB(td.Memory)
	taskCPU, hasTaskCPU := parseTaskCPUUnits(td.CPU)

	limitMiB := cd.Memory
	if limitMiB == 0 && hasTaskMem {
		limitMiB = taskMem
	}

	reqMiB := cd.MemoryReservation
	if reqMiB == 0 {
		reqMiB = cd.Memory
	}

	if limitMiB > 0 {
		reqMiB = min(reqMiB, limitMiB)
		req.Limits[corev1.ResourceMemory] = *resource.NewQuantity(int64(limitMiB)*mebibyte, resource.BinarySI)
	}

	if reqMiB > 0 {
		req.Requests[corev1.ResourceMemory] = *resource.NewQuantity(int64(reqMiB)*mebibyte, resource.BinarySI)
	}

	cpuReq := cd.CPU
	if hasTaskCPU {
		req.Limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(unitsToMilli(taskCPU), resource.DecimalSI)
		cpuReq = min(cpuReq, taskCPU)
	}

	if cpuReq > 0 {
		req.Requests[corev1.ResourceCPU] = *resource.NewMilliQuantity(unitsToMilli(cpuReq), resource.DecimalSI)
	}

	if len(req.Limits) == 0 {
		req.Limits = nil
	}

	if len(req.Requests) == 0 {
		req.Requests = nil
	}

	return req
}

func unitsToMilli(units int) int64 {
	return max(int64(units)*k8sMilliPerVCPU/k8sCPUUnitsPerVCPU, 1)
}

// parseTaskCPUUnits reads a task cpu of "256" units or "0.25 vCPU".
func parseTaskCPUUnits(s string) (int, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return 0, false
	}

	if num, ok := strings.CutSuffix(s, "vcpu"); ok {
		f, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
		if err != nil || f <= 0 {
			return 0, false
		}

		return int(f * k8sCPUUnitsPerVCPU), true
	}

	n, err := strconv.Atoi(s)

	return n, err == nil && n > 0
}

// parseTaskMemoryMiB reads a task memory of "512" MiB or "1 GB".
func parseTaskMemoryMiB(s string) (int, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return 0, false
	}

	if num, ok := strings.CutSuffix(s, "gb"); ok {
		f, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
		if err != nil || f <= 0 {
			return 0, false
		}

		return int(f * k8sTaskMemoryGBUnits), true
	}

	n, err := strconv.Atoi(s)

	return n, err == nil && n > 0
}

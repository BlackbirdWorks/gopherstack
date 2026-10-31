package ocicontainerinstances

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/netip"
	"path"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxContainers = 32
	maxVolumes    = 32
	maxPort       = 65535
	defInterval   = 10
	defFailure    = 3
	defSuccess    = 1
	defTimeout    = 1
	maxVnics      = 1
)

func (b *InMemoryBackend) buildInstance(in createIn) (*Instance, *APIError) {
	if aerr := b.checkCompartment(in.Compartment); aerr != nil {
		return nil, aerr
	}

	if in.AD == "" || in.Shape == "" {
		return nil, errMissing("availabilityDomain and shape are required")
	}

	sc, aerr := buildShapeConfig(in)
	if aerr != nil {
		return nil, aerr
	}

	restart := in.Restart
	if restart == "" {
		restart = restartAlways
	}

	if !slices.Contains([]string{restartAlways, restartNever, restartOnFailure}, restart) {
		return nil, errParam("containerRestartPolicy must be ALWAYS, NEVER or ON_FAILURE")
	}

	now := b.nowUTC()
	inst := &Instance{
		ID: newOCID("computecontainerinstance"), DisplayName: in.DisplayName, Compartment: in.Compartment,
		AD: in.AD, FaultDomain: in.FaultDomain, State: stateCreating, Shape: in.Shape, ShapeConfig: sc,
		Restart: restart, Created: now, Etag: newEtag(), Graceful: in.Graceful, Freeform: in.Freeform,
		Defined: in.Defined, Security: in.SecurityCtx, DNS: in.DNS,
	}

	if inst.DisplayName == "" {
		inst.DisplayName = "containerinstance" + now.Format("20060102150405")
	}

	if aerr = b.buildParts(inst, in); aerr != nil {
		return nil, aerr
	}

	return inst, nil
}

func buildShapeConfig(in createIn) (ShapeConfig, *APIError) {
	shape, ok := lookupShape(in.Shape)
	if !ok {
		return ShapeConfig{}, errParam("shape " + in.Shape + " is not a supported container instance shape")
	}

	if in.ShapeConfig == nil || in.ShapeConfig.Ocpus == nil {
		return ShapeConfig{}, errMissing("shapeConfig.ocpus is required")
	}

	ocpus := *in.ShapeConfig.Ocpus
	if ocpus < shape.MinOcpus || ocpus > shape.MaxOcpus {
		return ShapeConfig{}, errParam(fmt.Sprintf("shapeConfig.ocpus must be between %g and %g for %s",
			shape.MinOcpus, shape.MaxOcpus, shape.Name))
	}

	mem := ocpus * shape.DefPerOcpu
	if in.ShapeConfig.MemoryInGBs != nil {
		mem = *in.ShapeConfig.MemoryInGBs
	}

	lo, hi := max(float32(minMemoryGB), ocpus*minPerOcpuGB), min(ocpus*shape.MaxPerOcpu, shape.MaxMemory)
	if in.ShapeConfig.MemoryInGBs == nil {
		mem = min(mem, hi)
	}

	if mem < lo || mem > hi {
		return ShapeConfig{}, errParam(fmt.Sprintf("shapeConfig.memoryInGBs must be between %g and %g for %g OCPUs",
			lo, hi, ocpus))
	}

	return ShapeConfig{Processor: shape.Processor, Ocpus: ocpus, Memory: mem, Bandwidth: shape.bandwidth(ocpus)}, nil
}

func (b *InMemoryBackend) buildParts(inst *Instance, in createIn) *APIError {
	if in.Graceful != nil && *in.Graceful < 0 {
		return errParam("gracefulShutdownTimeoutInSeconds must not be negative")
	}

	if aerr := checkSecurity(in.SecurityCtx); aerr != nil {
		return aerr
	}

	if aerr := checkDNS(in.DNS); aerr != nil {
		return aerr
	}

	if aerr := b.buildVnics(inst, in.Vnics); aerr != nil {
		return aerr
	}

	if aerr := buildVolumes(inst, in.Volumes); aerr != nil {
		return aerr
	}

	if aerr := buildSecrets(inst, in.Secrets); aerr != nil {
		return aerr
	}

	return b.buildContainers(inst, in.Containers)
}

func (b *InMemoryBackend) buildVnics(inst *Instance, vnics []createVnicIn) *APIError {
	if len(vnics) == 0 {
		return errMissing("vnics is required and needs one entry")
	}

	if len(vnics) > maxVnics {
		return errParam("a container instance supports exactly one VNIC")
	}

	for _, v := range vnics {
		if v.SubnetID == "" {
			return errMissing("vnics.subnetId is required")
		}

		if b.subnet != nil && !b.subnet(v.SubnetID) {
			return errNotFound(v.SubnetID)
		}

		inst.Vnics = append(inst.Vnics, Vnic{VnicID: newOCID("vnic"), SubnetID: v.SubnetID})
	}

	return nil
}

func checkSecurity(raw json.RawMessage) *APIError {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	var sc struct {
		Type string `json:"securityContextType"`
	}

	if err := json.Unmarshal(raw, &sc); err != nil || sc.Type != "LINUX" {
		return errParam("securityContext.securityContextType must be LINUX")
	}

	return nil
}

func checkDNS(d *DNSConfig) *APIError {
	if d == nil {
		return nil
	}

	for _, ns := range d.Nameservers {
		if _, err := netip.ParseAddr(ns); err != nil {
			return errParam("dnsConfig.nameservers must be IP addresses: " + ns)
		}
	}

	return nil
}

func buildVolumes(inst *Instance, vols []createVolumeIn) *APIError {
	if len(vols) > maxVolumes {
		return errParam(fmt.Sprintf("a container instance supports up to %d volumes", maxVolumes))
	}

	seen := map[string]bool{}

	for _, v := range vols {
		if v.Name == "" || seen[v.Name] {
			return errParam("volume names are required and must be unique")
		}

		seen[v.Name] = true
		vol := Volume{Name: v.Name, Type: v.VolumeType}

		switch v.VolumeType {
		case volEmptyDir:
			if v.BackingStore != "" && v.BackingStore != backingMemory && v.BackingStore != backingEphem {
				return errParam("volume backingStore must be EPHEMERAL_STORAGE or MEMORY")
			}

			vol.BackingStore = v.BackingStore
		case volConfigFile:
			if aerr := checkConfigs(v.Name, v.Configs); aerr != nil {
				return aerr
			}

			vol.Configs = v.Configs
		default:
			return errParam("volumeType " + v.VolumeType + " is not supported; use EMPTYDIR or CONFIGFILE")
		}

		inst.Volumes = append(inst.Volumes, vol)
	}

	return nil
}

func checkConfigs(vol string, cfgs []ConfigFile) *APIError {
	seen := map[string]bool{}

	for _, c := range cfgs {
		if c.FileName == "" || strings.ContainsAny(c.FileName, "/\\") || c.FileName == "." || c.FileName == ".." {
			return errParam("volume " + vol + ": config fileName must be a plain file name")
		}

		if _, err := base64.StdEncoding.DecodeString(c.Data); err != nil {
			return errParam("volume " + vol + ": config data for " + c.FileName + " must be base64")
		}

		if c.Path != "" && !relClean(c.Path) {
			return errParam("volume " + vol + ": config path must be relative and stay inside the volume")
		}

		key := path.Join(c.Path, c.FileName)
		if seen[key] {
			return errParam("volume " + vol + ": duplicate config file " + key)
		}

		seen[key] = true
	}

	return nil
}

func relClean(p string) bool {
	return !path.IsAbs(p) && !strings.Contains(p, "\\") && path.Clean(p) != ".." &&
		!strings.HasPrefix(path.Clean(p), "../")
}

func buildSecrets(inst *Instance, secrets []createSecretIn) *APIError {
	seen := map[string]bool{}

	for _, s := range secrets {
		if s.Endpoint == "" {
			return errMissing("imagePullSecrets.registryEndpoint is required")
		}

		if seen[strs.Fold(s.Endpoint)] {
			return errParam("duplicate imagePullSecrets for registry " + s.Endpoint)
		}

		seen[strs.Fold(s.Endpoint)] = true

		switch s.SecretType {
		case secretBasic:
			if s.Username == "" || s.Password == "" {
				return errMissing("BASIC imagePullSecrets need username and password")
			}
		case secretVault:
			if s.SecretID == "" {
				return errMissing("VAULT imagePullSecrets need secretId")
			}
		default:
			return errParam("imagePullSecrets.secretType must be BASIC or VAULT")
		}

		inst.PullSecrets = append(inst.PullSecrets, PullSecret{
			Type: s.SecretType, Endpoint: s.Endpoint, Username: s.Username, Password: s.Password, SecretID: s.SecretID,
		})
	}

	return nil
}

func (b *InMemoryBackend) buildContainers(inst *Instance, cs []createContainerIn) *APIError {
	if len(cs) == 0 {
		return errMissing("containers is required and needs at least one entry")
	}

	if len(cs) > maxContainers {
		return errParam(fmt.Sprintf("a container instance supports up to %d containers", maxContainers))
	}

	names := map[string]bool{}

	for i, in := range cs {
		c, aerr := buildContainer(inst, in, i)
		if aerr != nil {
			return aerr
		}

		if names[strs.Fold(c.DisplayName)] {
			return errParam("duplicate container displayName " + c.DisplayName)
		}

		names[strs.Fold(c.DisplayName)] = true
		inst.Containers = append(inst.Containers, c)
	}

	return nil
}

func buildContainer(inst *Instance, in createContainerIn, idx int) (*Container, *APIError) {
	if in.ImageURL == "" {
		return nil, errMissing("containers.imageUrl is required")
	}

	if in.WorkingDir != "" && !path.IsAbs(in.WorkingDir) {
		return nil, errParam("containers.workingDirectory must be an absolute path")
	}

	if aerr := checkSecurity(in.SecurityCtx); aerr != nil {
		return nil, aerr
	}

	name := in.DisplayName
	if name == "" {
		name = fmt.Sprintf("%s-container-%d", inst.DisplayName, idx+1)
	}

	c := &Container{
		ID: newOCID("computecontainer"), DisplayName: name, ImageURL: in.ImageURL, State: stateCreating,
		Created: inst.Created, Command: in.Command, Args: in.Arguments, WorkingDir: in.WorkingDir, Env: in.Env,
		Freeform: in.Freeform, Defined: in.Defined, Security: in.SecurityCtx, Mounts: in.VolumeMounts,
	}

	if in.NoResourcePrn != nil {
		c.NoResourcePrn = *in.NoResourcePrn
	}

	if rc := in.ResourceConfig; rc != nil {
		if (rc.VcpusLimit != nil && (*rc.VcpusLimit <= 0 || *rc.VcpusLimit > inst.ShapeConfig.Ocpus)) ||
			(rc.MemoryLimitInGBs != nil && (*rc.MemoryLimitInGBs <= 0 || *rc.MemoryLimitInGBs > inst.ShapeConfig.Memory)) {
			return nil, errParam("containers.resourceConfig limits must be positive and within the instance shapeConfig")
		}

		c.Resource = rc
	}

	if aerr := checkMounts(inst, c.Mounts); aerr != nil {
		return nil, aerr
	}

	for _, h := range in.HealthChecks {
		hc, aerr := buildCheck(h)
		if aerr != nil {
			return nil, aerr
		}

		c.Health = append(c.Health, hc)
	}

	return c, nil
}

func checkMounts(inst *Instance, mounts []VolumeMount) *APIError {
	seen := map[string]bool{}

	for _, m := range mounts {
		if !slices.ContainsFunc(inst.Volumes, func(v Volume) bool { return v.Name == m.VolumeName }) {
			return errParam("volumeMounts.volumeName " + m.VolumeName + " does not match any volume")
		}

		if !path.IsAbs(m.MountPath) || seen[path.Clean(m.MountPath)] {
			return errParam("volumeMounts.mountPath must be an absolute path, unique within the container")
		}

		seen[path.Clean(m.MountPath)] = true

		if m.SubPath != "" && !relClean(m.SubPath) {
			return errParam("volumeMounts.subPath must be relative and stay inside the volume")
		}
	}

	return nil
}

func buildCheck(h createCheckIn) (*HealthCheck, *APIError) {
	if h.Type != hcHTTP && h.Type != hcTCP {
		return nil, errParam("healthChecks.healthCheckType must be HTTP or TCP")
	}

	if h.Port == nil || *h.Port < 1 || *h.Port > maxPort {
		return nil, errParam("healthChecks.port must be between 1 and 65535")
	}

	if h.Type == hcHTTP && !strings.HasPrefix(h.Path, "/") {
		return nil, errParam("HTTP healthChecks.path must start with /")
	}

	hc := &HealthCheck{
		Name: h.Name, Type: h.Type, Port: *h.Port, Path: h.Path, Headers: h.Headers, Status: healthUnknown,
		FailureAction: h.FailureAction, Interval: orDefault(h.Interval, defInterval),
		FailureThresh: orDefault(h.FailureThresh, defFailure), SuccessThresh: orDefault(h.SuccessThresh, defSuccess),
		Timeout: orDefault(h.Timeout, defTimeout), InitialDelay: orDefault(h.InitialDelay, 0),
	}

	if hc.FailureAction == "" {
		hc.FailureAction = actionKill
	}

	if hc.FailureAction != actionKill && hc.FailureAction != actionNone {
		return nil, errParam("healthChecks.failureAction must be KILL or NONE")
	}

	if hc.Interval < 1 || hc.Timeout < 1 || hc.FailureThresh < 1 || hc.SuccessThresh < 1 || hc.InitialDelay < 0 {
		return nil, errParam("healthChecks intervals, timeout and thresholds must be positive")
	}

	return hc, nil
}

func orDefault(p *int, def int) int {
	if p == nil {
		return def
	}

	return *p
}

package ocicontainerinstances

import (
	"encoding/json"
	"time"
)

type (
	createShapeConfigIn struct {
		Ocpus       *float32 `json:"ocpus"`
		MemoryInGBs *float32 `json:"memoryInGBs"`
	}
	createVnicIn struct {
		SubnetID    string `json:"subnetId"`
		DisplayName string `json:"displayName"`
	}
	createVolumeIn struct {
		Name         string       `json:"name"`
		VolumeType   string       `json:"volumeType"`
		BackingStore string       `json:"backingStore"`
		Configs      []ConfigFile `json:"configs"`
	}
	createSecretIn struct {
		SecretType string `json:"secretType"`
		Endpoint   string `json:"registryEndpoint"`
		Username   string `json:"username"`
		Password   string `json:"password"`
		SecretID   string `json:"secretId"`
	}
	createCheckIn struct {
		Port          *int           `json:"port"`
		InitialDelay  *int           `json:"initialDelayInSeconds"`
		Interval      *int           `json:"intervalInSeconds"`
		FailureThresh *int           `json:"failureThreshold"`
		SuccessThresh *int           `json:"successThreshold"`
		Timeout       *int           `json:"timeoutInSeconds"`
		Name          string         `json:"name"`
		Type          string         `json:"healthCheckType"`
		Path          string         `json:"path"`
		FailureAction string         `json:"failureAction"`
		Headers       []HealthHeader `json:"headers"`
	}
	createContainerIn struct {
		ResourceConfig *ResourceConfig   `json:"resourceConfig"`
		NoResourcePrn  *bool             `json:"isResourcePrincipalDisabled"`
		Env            map[string]string `json:"environmentVariables"`
		Freeform       map[string]string `json:"freeformTags"`
		Defined        map[string]any    `json:"definedTags"`
		ImageURL       string            `json:"imageUrl"`
		DisplayName    string            `json:"displayName"`
		WorkingDir     string            `json:"workingDirectory"`
		SecurityCtx    json.RawMessage   `json:"securityContext"`
		Command        []string          `json:"command"`
		Arguments      []string          `json:"arguments"`
		VolumeMounts   []VolumeMount     `json:"volumeMounts"`
		HealthChecks   []createCheckIn   `json:"healthChecks"`
	}
	createIn struct {
		ShapeConfig *createShapeConfigIn `json:"shapeConfig"`
		DNS         *DNSConfig           `json:"dnsConfig"`
		Graceful    *int64               `json:"gracefulShutdownTimeoutInSeconds"`
		Freeform    map[string]string    `json:"freeformTags"`
		Defined     map[string]any       `json:"definedTags"`
		SecurityCtx json.RawMessage      `json:"securityContext"`
		Compartment string               `json:"compartmentId"`
		AD          string               `json:"availabilityDomain"`
		Shape       string               `json:"shape"`
		DisplayName string               `json:"displayName"`
		FaultDomain string               `json:"faultDomain"`
		Restart     string               `json:"containerRestartPolicy"`
		Containers  []createContainerIn  `json:"containers"`
		Vnics       []createVnicIn       `json:"vnics"`
		Volumes     []createVolumeIn     `json:"volumes"`
		Secrets     []createSecretIn     `json:"imagePullSecrets"`
	}
	updateIn struct {
		DisplayName *string           `json:"displayName"`
		Freeform    map[string]string `json:"freeformTags"`
		Defined     map[string]any    `json:"definedTags"`
	}
	compartmentIn struct {
		Compartment string `json:"compartmentId"`
	}
)

type containerRefWire struct {
	ContainerID string `json:"containerId"`
	DisplayName string `json:"displayName"`
}

type vnicWire struct {
	VnicID string `json:"vnicId"`
}

type shapeConfigWire struct {
	Processor string  `json:"processorDescription"`
	Ocpus     float32 `json:"ocpus"`
	Memory    float32 `json:"memoryInGBs"`
	Bandwidth float32 `json:"networkingBandwidthInGbps"`
}

type instanceSummaryWire struct {
	TimeCreated time.Time         `json:"timeCreated"`
	Defined     map[string]any    `json:"definedTags,omitempty"`
	TimeUpdated *time.Time        `json:"timeUpdated,omitempty"`
	Graceful    *int64            `json:"gracefulShutdownTimeoutInSeconds,omitempty"`
	Freeform    map[string]string `json:"freeformTags,omitempty"`
	DisplayName string            `json:"displayName"`
	ID          string            `json:"id"`
	Compartment string            `json:"compartmentId"`
	AD          string            `json:"availabilityDomain"`
	FaultDomain string            `json:"faultDomain,omitempty"`
	State       string            `json:"lifecycleState"`
	Details     string            `json:"lifecycleDetails,omitempty"`
	Shape       string            `json:"shape"`
	Restart     string            `json:"containerRestartPolicy"`
	Security    json.RawMessage   `json:"securityContext,omitempty"`
	ShapeConfig shapeConfigWire   `json:"shapeConfig"`
	Count       int               `json:"containerCount"`
	VolumeCount int               `json:"volumeCount"`
}

type instanceWire struct {
	DNS        *DNSConfig         `json:"dnsConfig,omitempty"`
	Containers []containerRefWire `json:"containers"`
	Vnics      []vnicWire         `json:"vnics"`
	Volumes    []map[string]any   `json:"volumes"`
	Secrets    []map[string]any   `json:"imagePullSecrets"`
	instanceSummaryWire
}

func summaryOf(i *Instance) instanceSummaryWire {
	return instanceSummaryWire{
		ID: i.ID, DisplayName: i.DisplayName, Compartment: i.Compartment, AD: i.AD, FaultDomain: i.FaultDomain,
		State: i.State, Details: i.Details, Shape: i.Shape, Restart: i.Restart, TimeCreated: i.Created,
		TimeUpdated: i.Updated, ShapeConfig: shapeConfigWire(i.ShapeConfig), Count: len(i.Containers),
		VolumeCount: len(i.Volumes), Graceful: i.Graceful, Freeform: i.Freeform, Defined: i.Defined,
		Security: i.Security,
	}
}

func instanceView(i *Instance) instanceWire {
	w := instanceWire{
		instanceSummaryWire: summaryOf(i), DNS: i.DNS, Containers: make([]containerRefWire, 0, len(i.Containers)),
		Vnics: make([]vnicWire, 0, len(i.Vnics)), Volumes: make([]map[string]any, 0, len(i.Volumes)),
		Secrets: make([]map[string]any, 0, len(i.PullSecrets)),
	}

	for _, c := range i.Containers {
		w.Containers = append(w.Containers, containerRefWire{ContainerID: c.ID, DisplayName: c.DisplayName})
	}

	for _, v := range i.Vnics {
		w.Vnics = append(w.Vnics, vnicWire{VnicID: v.VnicID})
	}

	for _, v := range i.Volumes {
		w.Volumes = append(w.Volumes, volumeView(v))
	}

	for _, s := range i.PullSecrets {
		m := map[string]any{"secretType": s.Type, "registryEndpoint": s.Endpoint}
		if s.Type == secretVault {
			m["secretId"] = s.SecretID
		}

		w.Secrets = append(w.Secrets, m)
	}

	return w
}

func volumeView(v Volume) map[string]any {
	m := map[string]any{"name": v.Name, "volumeType": v.Type}

	switch v.Type {
	case volEmptyDir:
		if v.BackingStore != "" {
			m["backingStore"] = v.BackingStore
		}
	case volConfigFile:
		cfgs := make([]ConfigFile, 0, len(v.Configs))
		cfgs = append(cfgs, v.Configs...)
		m["configs"] = cfgs
	}

	return m
}

type checkWire struct {
	Name          string         `json:"name,omitempty"`
	Type          string         `json:"healthCheckType"`
	Path          string         `json:"path,omitempty"`
	Status        string         `json:"status"`
	StatusDetails string         `json:"statusDetails,omitempty"`
	FailureAction string         `json:"failureAction"`
	Headers       []HealthHeader `json:"headers,omitempty"`
	Port          int            `json:"port"`
	InitialDelay  int            `json:"initialDelayInSeconds"`
	Interval      int            `json:"intervalInSeconds"`
	FailureThresh int            `json:"failureThreshold"`
	SuccessThresh int            `json:"successThreshold"`
	Timeout       int            `json:"timeoutInSeconds"`
}

type containerSummaryWire struct {
	TimeCreated time.Time         `json:"timeCreated"`
	TimeUpdated *time.Time        `json:"timeUpdated,omitempty"`
	Resource    *ResourceConfig   `json:"resourceConfig,omitempty"`
	Freeform    map[string]string `json:"freeformTags,omitempty"`
	Defined     map[string]any    `json:"definedTags,omitempty"`
	Compartment string            `json:"compartmentId"`
	ID          string            `json:"id"`
	DisplayName string            `json:"displayName"`
	AD          string            `json:"availabilityDomain"`
	FaultDomain string            `json:"faultDomain,omitempty"`
	State       string            `json:"lifecycleState"`
	Details     string            `json:"lifecycleDetails,omitempty"`
	InstanceID  string            `json:"containerInstanceId"`
	ImageURL    string            `json:"imageUrl"`
	Security    json.RawMessage   `json:"securityContext,omitempty"`
	NoResource  bool              `json:"isResourcePrincipalDisabled"`
}

type containerWire struct {
	TimeTerminated *time.Time        `json:"timeTerminated,omitempty"`
	ExitCode       *int              `json:"exitCode,omitempty"`
	Env            map[string]string `json:"environmentVariables,omitempty"`
	WorkingDir     string            `json:"workingDirectory,omitempty"`
	Command        []string          `json:"command,omitempty"`
	Arguments      []string          `json:"arguments,omitempty"`
	VolumeMounts   []VolumeMount     `json:"volumeMounts,omitempty"`
	HealthChecks   []checkWire       `json:"healthChecks,omitempty"`
	containerSummaryWire
	RestartCount int `json:"containerRestartAttemptCount"`
}

func containerSummaryOf(i *Instance, c *Container) containerSummaryWire {
	return containerSummaryWire{
		ID: c.ID, DisplayName: c.DisplayName, Compartment: i.Compartment, AD: i.AD, FaultDomain: i.FaultDomain,
		State: c.State, Details: c.Details, InstanceID: i.ID, ImageURL: c.ImageURL, TimeCreated: c.Created,
		TimeUpdated: c.Updated, Resource: c.Resource, Freeform: c.Freeform, Defined: c.Defined, Security: c.Security,
		NoResource: c.NoResourcePrn,
	}
}

func containerView(i *Instance, c *Container) containerWire {
	w := containerWire{
		containerSummaryWire: containerSummaryOf(i, c), TimeTerminated: c.Terminated, ExitCode: c.ExitCode,
		Env: c.Env, WorkingDir: c.WorkingDir, Command: c.Command, Arguments: c.Args, VolumeMounts: c.Mounts,
		RestartCount: c.RestartCount,
	}

	for _, h := range c.Health {
		w.HealthChecks = append(w.HealthChecks, checkWire{
			Name: h.Name, Type: h.Type, Path: h.Path, Status: h.Status, StatusDetails: h.StatusDetails,
			FailureAction: h.FailureAction, Headers: h.Headers, Port: h.Port, InitialDelay: h.InitialDelay,
			Interval: h.Interval, FailureThresh: h.FailureThresh, SuccessThresh: h.SuccessThresh, Timeout: h.Timeout,
		})
	}

	return w
}

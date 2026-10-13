package batch

import (
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"
)

const reservedEnvPrefix = "AWS_BATCH"

// NodePropertyOverride overrides the properties of a node range at SubmitJob time.
type NodePropertyOverride struct {
	ConsumableOverride *ConsumableResourceProperties `json:"consumableResourcePropertiesOverride,omitempty"`
	ContainerOverrides *ContainerOverrides           `json:"containerOverrides,omitempty"`
	EcsOverride        map[string]any                `json:"ecsPropertiesOverride,omitempty"`
	EksOverride        *EksPropertiesOverride        `json:"eksPropertiesOverride,omitempty"`
	TargetNodes        string                        `json:"targetNodes"`
	InstanceTypes      []string                      `json:"instanceTypes,omitempty"`
}

// NodeOverrides overrides a multi-node job definition at SubmitJob time.
type NodeOverrides struct {
	NodePropertyOverrides []NodePropertyOverride `json:"nodePropertyOverrides,omitempty"`
	NumNodes              int32                  `json:"numNodes,omitempty"`
}

// EksContainerOverride overrides one EKS container.
type EksContainerOverride struct {
	Resources *EksContainerResources `json:"resources,omitempty"`
	Name      string                 `json:"name,omitempty"`
	Image     string                 `json:"image,omitempty"`
	Args      []string               `json:"args,omitempty"`
	Command   []string               `json:"command,omitempty"`
	Env       []EksContainerEnv      `json:"env,omitempty"`
}

// EksPodPropertiesOverride overrides the pod of an EKS job.
type EksPodPropertiesOverride struct {
	Metadata       *EksMetadata           `json:"metadata,omitempty"`
	Containers     []EksContainerOverride `json:"containers,omitempty"`
	InitContainers []EksContainerOverride `json:"initContainers,omitempty"`
}

// EksPropertiesOverride is the SubmitJob eksPropertiesOverride member.
type EksPropertiesOverride struct {
	PodProperties *EksPodPropertiesOverride `json:"podProperties,omitempty"`
}

func deepCopy[T any](v T) T {
	var out T

	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}

	if err = json.Unmarshal(raw, &out); err != nil {
		return v
	}

	return out
}

func validateEnvNames(env []KeyValuePair) error {
	for _, kv := range env {
		if strings.HasPrefix(kv.Name, reservedEnvPrefix) {
			return fmt.Errorf("%w: environment variable names cannot start with %s", ErrValidation, reservedEnvPrefix)
		}
	}

	return nil
}

func validateJobOverrides(co *ContainerOverrides, eks *EksPropertiesOverride, no *NodeOverrides) error {
	if co != nil {
		if err := validateEnvNames(co.Environment); err != nil {
			return err
		}
	}

	if err := validateEksEnvNames(eks); err != nil {
		return err
	}

	return validateNodeOverrides(no)
}

func validateEksEnvNames(eks *EksPropertiesOverride) error {
	if eks == nil || eks.PodProperties == nil {
		return nil
	}

	for _, c := range append(slicesClone(eks.PodProperties.Containers), eks.PodProperties.InitContainers...) {
		for _, e := range c.Env {
			if strings.HasPrefix(e.Name, reservedEnvPrefix) {
				return fmt.Errorf(
					"%w: environment variable names cannot start with %s",
					ErrValidation,
					reservedEnvPrefix,
				)
			}
		}
	}

	return nil
}

func validateNodeOverrides(no *NodeOverrides) error {
	if no == nil {
		return nil
	}

	for _, o := range no.NodePropertyOverrides {
		if o.TargetNodes == "" {
			return fmt.Errorf("%w: nodeOverrides.nodePropertyOverrides.targetNodes is required", ErrValidation)
		}

		if o.ContainerOverrides == nil {
			continue
		}

		if err := validateEnvNames(o.ContainerOverrides.Environment); err != nil {
			return err
		}
	}

	return nil
}

func slicesClone[T any](s []T) []T { return append([]T(nil), s...) }

func mergeEnv(base, over []KeyValuePair) []KeyValuePair {
	out := append([]KeyValuePair(nil), base...)

	for _, o := range over {
		replaced := false

		for i := range out {
			if out[i].Name == o.Name {
				out[i] = o
				replaced = true

				break
			}
		}

		if !replaced {
			out = append(out, o)
		}
	}

	return out
}

func mergeResources(base, over []ResourceRequirement) []ResourceRequirement {
	out := append([]ResourceRequirement(nil), base...)

	for _, o := range over {
		replaced := false

		for i := range out {
			if out[i].Type == o.Type {
				out[i] = o
				replaced = true

				break
			}
		}

		if !replaced {
			out = append(out, o)
		}
	}

	return out
}

// applyContainerPropertiesOverrides merges SubmitJob container overrides onto cp.
func applyContainerPropertiesOverrides(cp *ContainerProperties, o *ContainerOverrides) {
	if cp == nil || o == nil {
		return
	}

	if o.InstanceType != "" {
		cp.InstanceType = o.InstanceType
	}

	if len(o.Command) > 0 {
		cp.Command = o.Command
	}

	if o.Vcpus > 0 {
		cp.Vcpus = o.Vcpus
	}

	if o.Memory > 0 {
		cp.Memory = o.Memory
	}

	cp.Environment = mergeEnv(cp.Environment, o.Environment)
	cp.ResourceRequirements = mergeResources(cp.ResourceRequirements, o.ResourceRequirements)
}

func mergeEksEnv(base, over []EksContainerEnv) []EksContainerEnv {
	out := append([]EksContainerEnv(nil), base...)

	for _, o := range over {
		replaced := false

		for i := range out {
			if out[i].Name == o.Name {
				out[i] = o
				replaced = true

				break
			}
		}

		if !replaced {
			out = append(out, o)
		}
	}

	return out
}

func mergeStringMap(base, over map[string]string) map[string]string {
	if len(over) == 0 {
		return base
	}

	out := maps.Clone(base)
	if out == nil {
		out = map[string]string{}
	}

	maps.Copy(out, over)

	return out
}

func applyEksContainerOverrides(containers []EksContainer, overrides []EksContainerOverride) {
	for _, o := range overrides {
		for i := range containers {
			if o.Name != "" && containers[i].Name == o.Name {
				applyEksContainerOverride(&containers[i], o)
			}
		}
	}
}

func applyEksContainerOverride(c *EksContainer, o EksContainerOverride) {
	if o.Image != "" {
		c.Image = o.Image
	}

	if len(o.Command) > 0 {
		c.Command = o.Command
	}

	if len(o.Args) > 0 {
		c.Args = o.Args
	}

	c.Env = mergeEksEnv(c.Env, o.Env)

	if o.Resources == nil {
		return
	}

	if c.Resources == nil {
		c.Resources = &EksContainerResources{}
	}

	c.Resources.Limits = mergeStringMap(c.Resources.Limits, o.Resources.Limits)
	c.Resources.Requests = mergeStringMap(c.Resources.Requests, o.Resources.Requests)
}

// applyEksOverride merges an EKS pod override onto ep.
func applyEksOverride(ep *EksProperties, o *EksPropertiesOverride) {
	if ep == nil || ep.PodProperties == nil || o == nil || o.PodProperties == nil {
		return
	}

	pod := ep.PodProperties
	applyEksContainerOverrides(pod.Containers, o.PodProperties.Containers)
	applyEksContainerOverrides(pod.InitContainers, o.PodProperties.InitContainers)

	if m := o.PodProperties.Metadata; m != nil {
		if pod.Metadata == nil {
			pod.Metadata = &EksMetadata{}
		}

		pod.Metadata.Labels = mergeStringMap(pod.Metadata.Labels, m.Labels)
		pod.Metadata.Annotations = mergeStringMap(pod.Metadata.Annotations, m.Annotations)
	}
}

// applyEcsOverride merges ecsPropertiesOverride.taskProperties[].containers[] onto the
// matching task and container (by position and name) of ecs.
func applyEcsOverride(ecs, override map[string]any) {
	tasks, _ := ecs["taskProperties"].([]any)
	overTasks, _ := override["taskProperties"].([]any)

	for i, ot := range overTasks {
		if i >= len(tasks) {
			break
		}

		task, _ := tasks[i].(map[string]any)
		containers, _ := task["containers"].([]any)
		overContainers, _ := asMap(ot)["containers"].([]any)

		for _, oc := range overContainers {
			applyEcsContainerOverride(containers, asMap(oc))
		}
	}
}

func applyEcsContainerOverride(containers []any, o map[string]any) {
	name, _ := o["name"].(string)

	for _, raw := range containers {
		c, _ := raw.(map[string]any)
		if name == "" || c["name"] != name {
			continue
		}

		if cmd, ok := o["command"]; ok {
			c["command"] = cmd
		}

		c["environment"] = mergeAnyKeyed(c["environment"], o["environment"], "name")
		c["resourceRequirements"] = mergeAnyKeyed(c["resourceRequirements"], o["resourceRequirements"], "type")
	}
}

func mergeAnyKeyed(base, over any, key string) any {
	b, _ := base.([]any)
	o, _ := over.([]any)

	if len(o) == 0 {
		return base
	}

	out := append([]any(nil), b...)

	for _, ov := range o {
		replaced := false

		for i := range out {
			if asMap(out[i])[key] == asMap(ov)[key] {
				out[i] = ov
				replaced = true

				break
			}
		}

		if !replaced {
			out = append(out, ov)
		}
	}

	return out
}

// nodeRange is an inclusive [lo, hi] node index range.
type nodeRange struct{ lo, hi int32 }

// parseTargetNodes parses "lo:hi", ":hi", "lo:" and "n" against numNodes.
func parseTargetNodes(target string, numNodes int32) (nodeRange, error) {
	bad := fmt.Errorf("%w: invalid targetNodes %q", ErrValidation, target)
	lo, hi := int32(0), numNodes-1

	loS, hiS, isRange := strings.Cut(target, ":")
	if !isRange {
		n, err := strconv.ParseInt(target, 10, 32)
		if err != nil || n < 0 {
			return nodeRange{}, bad
		}

		return nodeRange{int32(n), int32(n)}, nil
	}

	if loS != "" {
		n, err := strconv.ParseInt(loS, 10, 32)
		if err != nil || n < 0 {
			return nodeRange{}, bad
		}

		lo = int32(n)
	}

	if hiS != "" {
		n, err := strconv.ParseInt(hiS, 10, 32)
		if err != nil || n < 0 {
			return nodeRange{}, bad
		}

		hi = int32(n)
	}

	if lo > hi {
		return nodeRange{}, bad
	}

	return nodeRange{lo, hi}, nil
}

func (r nodeRange) String() string { return fmt.Sprintf("%d:%d", r.lo, r.hi) }

// hasOpenUpperBound reports whether a targetNodes string has an open upper boundary and its lower bound.
func hasOpenUpperBound(target string) (int32, bool) {
	loS, hiS, isRange := strings.Cut(target, ":")
	if !isRange || hiS != "" {
		return 0, false
	}

	n, _ := strconv.ParseInt(loS, 10, 32)

	return int32(n), true
}

// effectiveNodeProperties applies nodeOverrides to a copy of np.
func effectiveNodeProperties(np *NodeProperties, no *NodeOverrides) (*NodeProperties, error) {
	out := deepCopy(np)
	if out == nil || no == nil {
		return out, nil
	}

	if no.NumNodes > 0 {
		if err := validateNumNodesOverride(out, no.NumNodes); err != nil {
			return nil, err
		}

		out.NumNodes = no.NumNodes
	}

	for _, o := range no.NodePropertyOverrides {
		target, err := parseTargetNodes(o.TargetNodes, out.NumNodes)
		if err != nil {
			return nil, err
		}

		out.NodeRangeProperties = overrideRanges(out, target, o)
	}

	return out, nil
}

func validateNumNodesOverride(np *NodeProperties, numNodes int32) error {
	for _, r := range np.NodeRangeProperties {
		if lo, open := hasOpenUpperBound(r.TargetNodes); open && lo < numNodes {
			return nil
		}
	}

	return fmt.Errorf(
		"%w: nodeOverrides.numNodes needs a node range with an open upper boundary whose lower boundary is below numNodes",
		ErrValidation,
	)
}

// overrideRanges splits np's ranges around target and applies o to the covered slices.
func overrideRanges(np *NodeProperties, target nodeRange, o NodePropertyOverride) []NodeRangeProperty {
	var out []NodeRangeProperty

	for _, r := range np.NodeRangeProperties {
		rng, err := parseTargetNodes(r.TargetNodes, np.NumNodes)
		if err != nil || rng.hi < target.lo || rng.lo > target.hi {
			out = append(out, r)

			continue
		}

		if rng.lo < target.lo {
			before := deepCopy(r)
			before.TargetNodes = nodeRange{rng.lo, target.lo - 1}.String()
			out = append(out, before)
		}

		covered := deepCopy(r)
		covered.TargetNodes = nodeRange{max(rng.lo, target.lo), min(rng.hi, target.hi)}.String()
		applyNodeOverride(&covered, o)
		out = append(out, covered)

		if rng.hi > target.hi {
			after := deepCopy(r)
			after.TargetNodes = nodeRange{target.hi + 1, rng.hi}.String()
			out = append(out, after)
		}
	}

	return out
}

func applyNodeOverride(r *NodeRangeProperty, o NodePropertyOverride) {
	applyContainerPropertiesOverrides(r.ContainerProperties, o.ContainerOverrides)
	applyEksOverride(r.EksProperties, o.EksOverride)

	if r.EcsProperties != nil && o.EcsOverride != nil {
		applyEcsOverride(r.EcsProperties, o.EcsOverride)
	}

	if len(o.InstanceTypes) > 0 {
		r.InstanceTypes = o.InstanceTypes
	}

	if o.ConsumableOverride != nil {
		r.ConsumableResourceProperties = o.ConsumableOverride
	}
}

// effectiveNodePropertiesLocked resolves a job's definition node properties with its overrides applied.
func (b *InMemoryBackend) effectiveNodePropertiesLocked(j *Job) *NodeProperties {
	jd, ok := b.jobDefinitions.Get(regionKey(j.region, j.JobDefinition))
	if !ok || jd.NodeProperties == nil {
		return nil
	}

	np, err := effectiveNodeProperties(jd.NodeProperties, j.NodeOverrides)
	if err != nil {
		return deepCopy(jd.NodeProperties)
	}

	return np
}

// effectiveEksPropertiesLocked resolves a job's definition eksProperties with its override applied.
func (b *InMemoryBackend) effectiveEksPropertiesLocked(j *Job) *EksProperties {
	jd, ok := b.jobDefinitions.Get(regionKey(j.region, j.JobDefinition))
	if !ok || jd.EksProperties == nil {
		return nil
	}

	ep := deepCopy(jd.EksProperties)
	applyEksOverride(ep, j.EksOverride)

	return ep
}

// effectiveEcsPropertiesLocked resolves a job's definition ecsProperties with its override applied.
func (b *InMemoryBackend) effectiveEcsPropertiesLocked(j *Job) map[string]any {
	jd, ok := b.jobDefinitions.Get(regionKey(j.region, j.JobDefinition))
	if !ok || jd.EcsProperties == nil {
		return nil
	}

	ecs := deepCopy(jd.EcsProperties)
	if j.EcsOverride != nil {
		applyEcsOverride(ecs, j.EcsOverride)
	}

	return ecs
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

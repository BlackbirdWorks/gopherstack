package ssm

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func (b *InMemoryBackend) instancePatchStatesStore(region string) *store.Table[InstancePatchState] {
	return getOrCreateTable(b, b.instancePatchStates, "instancePatchStates", region, instancePatchStateKeyFn)
}
func (b *InMemoryBackend) instancePatchesStore(region string) map[string][]PatchComplianceData {
	return b.instancePatches[region]
}
func (b *InMemoryBackend) instancePropertiesStore(region string) *store.Table[InstanceProperty] {
	return getOrCreateTable(b, b.instanceProperties, "instanceProperties", region, instancePropertyKeyFn)
}

// buildNodeInfos derives managed nodes from the activations store, sorted by
// InstanceID. Shared by ListNodes and ListNodesSummary.
func (b *InMemoryBackend) buildNodeInfos(region string) []NodeInfo {
	activations := b.activationsStore(region)
	nodes := make([]NodeInfo, 0, activations.Len())
	for _, act := range activations.All() {
		nodes = append(nodes, NodeInfo{
			InstanceID:       act.ActivationID,
			PlatformType:     platformTypeLinux,
			AgentVersion:     defaultAgentVersionSSM,
			RegistrationDate: act.CreatedDate,
		})
	}

	sort.Slice(nodes, func(i, k int) bool {
		return nodes[i].InstanceID < nodes[k].InstanceID
	})

	return nodes
}

// ListNodes returns managed nodes derived from the activations store.
func (b *InMemoryBackend) ListNodes(
	ctx context.Context,
	_ *ListNodesInput,
) (*ListNodesOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	return &ListNodesOutputFull{Nodes: b.buildNodeInfos(region)}, nil
}

// nodeAttributeValue returns the value of a NodeAttributeName/NodeFilterKey
// on a node. This backend only tracks InstanceId, PlatformType and
// AgentVersion (see NodeInfo); every other attribute
// (PlatformName/PlatformVersion/Region/ResourceType/SourceType/
// AvailabilityZone/...) has no backing state and returns "" rather than a
// fabricated value.
func nodeAttributeValue(n NodeInfo, attr string) string {
	switch attr {
	case "InstanceId":
		return n.InstanceID
	case "PlatformType":
		return n.PlatformType
	case "AgentVersion":
		return n.AgentVersion
	default:
		return ""
	}
}

// matchesNodeFilter reports whether a node satisfies a single NodeFilter.
// Only the attributes nodeAttributeValue tracks can be meaningfully
// filtered; filters on any other key match every node (accept-and-echo,
// since this backend has no real data to filter against).
func matchesNodeFilter(n NodeInfo, f NodeFilter) bool {
	value := nodeAttributeValue(n, f.Key)
	if value == "" && f.Key != "InstanceId" && f.Key != "PlatformType" && f.Key != "AgentVersion" {
		return true
	}

	switch f.Type {
	case "NotEqual":
		return !slices.Contains(f.Values, value)
	case "BeginWith":
		for _, v := range f.Values {
			if strings.HasPrefix(value, v) {
				return true
			}
		}

		return false
	default: // "Equal" and the unset default (real API default is Equal).
		return slices.Contains(f.Values, value)
	}
}

// nodeSummaryCountKey is the Summary entry key holding a group's node count.
const nodeSummaryCountKey = "Count"

// aggregateNodes groups nodes by a NodeAggregator's AttributeName and
// returns one Summary entry per distinct value, each carrying the group's
// Count. Nested sub-aggregators (NodeAggregator.Aggregators) are accepted on
// the wire but not applied -- see NodeAggregator's doc comment.
func aggregateNodes(nodes []NodeInfo, agg NodeAggregator) []map[string]string {
	counts := make(map[string]int)

	var order []string

	for _, n := range nodes {
		v := nodeAttributeValue(n, agg.AttributeName)
		if _, seen := counts[v]; !seen {
			order = append(order, v)
		}

		counts[v]++
	}

	sort.Strings(order)

	out := make([]map[string]string, 0, len(order))
	for _, v := range order {
		out = append(out, map[string]string{
			agg.AttributeName:   v,
			nodeSummaryCountKey: strconv.Itoa(counts[v]),
		})
	}

	return out
}

// ListNodesSummary returns real per-attribute node counts grouped by the
// caller's Aggregators, matching ListNodesSummaryInput/Output
// (api_op_ListNodesSummary.go:31-78, ssm@v1.73.4): Aggregators is required
// and actually drives the grouping instead of being ignored in favor of a
// synthetic constant.
func (b *InMemoryBackend) ListNodesSummary(
	ctx context.Context,
	input *ListNodesSummaryInput,
) (*ListNodesSummaryOutputFull, error) {
	if len(input.Aggregators) == 0 {
		return nil, fmt.Errorf("%w: Aggregators is required", ErrInvalidAggregator)
	}

	region := getRegion(ctx)
	b.mu.RLock("ListNodesSummary")
	defer b.mu.RUnlock()

	nodes := b.buildNodeInfos(region)

	filtered := nodes[:0:0]
	for _, n := range nodes {
		matched := true

		for _, f := range input.Filters {
			if !matchesNodeFilter(n, f) {
				matched = false

				break
			}
		}

		if matched {
			filtered = append(filtered, n)
		}
	}

	summary := make([]map[string]string, 0, len(input.Aggregators))
	for _, agg := range input.Aggregators {
		summary = append(summary, aggregateNodes(filtered, agg)...)
	}

	return &ListNodesSummaryOutputFull{Summary: summary}, nil
}

// DescribeEffectiveInstanceAssociations returns associations targeting an instance.
func (b *InMemoryBackend) DescribeEffectiveInstanceAssociations(
	ctx context.Context,
	input *DescribeEffectiveInstanceAssociationsInput,
) (*DescribeEffectiveInstanceAssociationsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeEffectiveInstanceAssociations")
	defer b.mu.RUnlock()

	var result []InstanceAssociationInfo

	for _, assocPtr := range b.associationsStore(region).All() {
		assoc := *assocPtr
		if assoc.InstanceID == input.InstanceID {
			result = append(result, InstanceAssociationInfo{
				AssociationID:      assoc.AssociationID,
				Name:               assoc.Name,
				DocumentVersion:    assoc.DocumentVersion,
				AssociationVersion: "1",
			})
		}
	}

	if result == nil {
		result = []InstanceAssociationInfo{}
	}

	return &DescribeEffectiveInstanceAssociationsOutputFull{Associations: result}, nil
}

// DescribeInstanceAssociationsStatus returns status of associations on an instance.
func (b *InMemoryBackend) DescribeInstanceAssociationsStatus(
	ctx context.Context,
	input *DescribeInstanceAssociationsStatusInput,
) (*DescribeInstanceAssociationsStatusOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstanceAssociationsStatus")
	defer b.mu.RUnlock()

	var result []InstanceAssociationStatusInfo

	for _, assocPtr := range b.associationsStore(region).All() {
		assoc := *assocPtr
		if assoc.InstanceID == input.InstanceID {
			status := commandStatusSuccess
			if assoc.Overview != nil {
				status = assoc.Overview.Status
			}

			result = append(result, InstanceAssociationStatusInfo{
				AssociationID: assoc.AssociationID,
				Name:          assoc.Name,
				Status:        status,
				ExecutionDate: assoc.LastUpdateAssociationDate,
			})
		}
	}

	if result == nil {
		result = []InstanceAssociationStatusInfo{}
	}

	return &DescribeInstanceAssociationsStatusOutputFull{
		InstanceAssociationStatusInfos: result,
	}, nil
}

// DescribeInstanceInformation returns information about managed instances from activations.
func (b *InMemoryBackend) DescribeInstanceInformation(
	ctx context.Context,
	_ *DescribeInstanceInformationInput,
) (*DescribeInstanceInformationOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstanceInformation")
	defer b.mu.RUnlock()

	activations := b.activationsStore(region)
	list := make([]InstanceInformation, 0, activations.Len())
	for _, act := range activations.All() {
		list = append(list, InstanceInformation{
			InstanceID:       act.ActivationID,
			PingStatus:       "Online",
			AgentVersion:     defaultAgentVersionSSM,
			PlatformType:     platformTypeLinux,
			RegistrationDate: act.CreatedDate,
		})
	}

	return &DescribeInstanceInformationOutputFull{InstanceInformationList: list}, nil
}

// DescribeInstancePatchStates returns patch compliance state for instances.
func (b *InMemoryBackend) DescribeInstancePatchStates(
	ctx context.Context,
	input *DescribeInstancePatchStatesInput,
) (*DescribeInstancePatchStatesOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstancePatchStates")
	defer b.mu.RUnlock()

	patchStates := b.instancePatchStatesStore(region)
	states := make([]InstancePatchState, 0)

	if len(input.InstanceIDs) == 0 {
		for _, s := range patchStates.All() {
			states = append(states, *s)
		}
	} else {
		for _, instanceID := range input.InstanceIDs {
			if s, exists := patchStates.Get(instanceID); exists {
				states = append(states, *s)
			}
		}
	}

	return &DescribeInstancePatchStatesOutputFull{InstancePatchStates: states}, nil
}

// DescribeInstancePatchStatesForPatchGroup returns patch states filtered by patch group.
func (b *InMemoryBackend) DescribeInstancePatchStatesForPatchGroup(
	ctx context.Context,
	input *DescribeInstancePatchStatesForPatchGroupInput,
) (*DescribeInstancePatchStatesForPatchGroupOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstancePatchStatesForPatchGroup")
	defer b.mu.RUnlock()

	patchStates := b.instancePatchStatesStore(region)
	states := make([]InstancePatchState, 0)
	for _, s := range patchStates.All() {
		if s.PatchGroup == input.PatchGroup {
			states = append(states, *s)
		}
	}

	return &DescribeInstancePatchStatesForPatchGroupOutput{InstancePatchStates: states}, nil
}

// DescribeInstancePatches returns patch compliance data for an instance.
func (b *InMemoryBackend) DescribeInstancePatches(
	ctx context.Context,
	input *DescribeInstancePatchesInput,
) (*DescribeInstancePatchesOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstancePatches")
	defer b.mu.RUnlock()

	store := b.instancePatches[region]
	patches := store[input.InstanceID]
	if patches == nil {
		patches = []PatchComplianceData{}
	}

	result := make([]PatchComplianceData, len(patches))
	copy(result, patches)

	return &DescribeInstancePatchesOutput{Patches: result}, nil
}

// DescribeInstanceProperties returns properties for managed instances.
// DescribeInstanceProperties returns properties for managed instances. Any
// explicitly-stored InstanceProperty (from an earlier UpdateInstanceInformation-
// style write) wins; every other registered managed instance (i.e. every
// activation, mirroring DescribeInstanceInformation) is reported too, so the
// response reflects real registered instances rather than a permanently-empty
// map.
func (b *InMemoryBackend) DescribeInstanceProperties(
	ctx context.Context,
	_ *DescribeInstancePropertiesInput,
) (*DescribeInstancePropertiesOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstanceProperties")
	defer b.mu.RUnlock()

	instancePropsTable := b.instancePropertiesStore(region)
	activationsTable := b.activationsStore(region)
	props := make([]InstanceProperty, 0, instancePropsTable.Len()+activationsTable.Len())
	seen := make(map[string]struct{}, instancePropsTable.Len())

	for _, p := range instancePropsTable.All() {
		props = append(props, *p)
		seen[p.InstanceID] = struct{}{}
	}

	for _, act := range activationsTable.All() {
		if _, ok := seen[act.ActivationID]; ok {
			continue
		}

		props = append(props, InstanceProperty{
			InstanceID:      act.ActivationID,
			Name:            act.DefaultInstanceName,
			PlatformType:    platformTypeLinux,
			PlatformName:    "Amazon Linux",
			PlatformVersion: "2",
			PingStatus:      "Online",
			AgentVersion:    defaultAgentVersionSSM,
			ActivationID:    act.ActivationID,
		})
	}

	return &DescribeInstancePropertiesOutput{InstanceProperties: props}, nil
}

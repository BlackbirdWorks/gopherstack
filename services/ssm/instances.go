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

// nodeToWire converts the internal NodeInfo into the real ListNodesOutput
// element (types.Node). Owner is always nil: this backend has no
// account/OU tracking to report.
func nodeToWire(n NodeInfo, region string) Node {
	return Node{
		ID:          n.InstanceID,
		CaptureTime: n.RegistrationDate,
		Region:      region,
		NodeType: &NodeType{
			Instance: &NodeInstanceInfo{
				AgentVersion: n.AgentVersion,
				PlatformType: n.PlatformType,
			},
		},
	}
}

// filterNodes returns the nodes matching every filter (AND semantics, same
// as the real API). Shared by ListNodes and ListNodesSummary.
func filterNodes(nodes []NodeInfo, filters []NodeFilter) []NodeInfo {
	filtered := nodes[:0:0]

	for _, n := range nodes {
		matched := true

		for _, f := range filters {
			if !matchesNodeFilter(n, f) {
				matched = false

				break
			}
		}

		if matched {
			filtered = append(filtered, n)
		}
	}

	return filtered
}

// defaultListNodesMaxResults is used when the caller omits MaxResults. The
// pinned SDK's ListNodesInput doc does not state a default, so this mirrors
// the 50-item default already established for this service's other
// NextToken-paginated list ops (e.g. DescribeOpsItems).
const defaultListNodesMaxResults = 50

// ListNodes returns managed nodes derived from the activations store,
// filtered by input.Filters and paginated by input.MaxResults/NextToken --
// all real, optional ListNodesInput members (api_op_ListNodes.go:31-53)
// that a literal struct{} input previously discarded from every request.
func (b *InMemoryBackend) ListNodes(
	ctx context.Context,
	input *ListNodesInput,
) (*ListNodesOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	filtered := filterNodes(b.buildNodeInfos(region), input.Filters)

	startIdx := parseNextToken(input.NextToken)
	if startIdx >= len(filtered) {
		return &ListNodesOutputFull{Nodes: []Node{}}, nil
	}

	maxResults := int32(defaultListNodesMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(filtered) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(filtered)
	}

	wire := make([]Node, 0, end-startIdx)
	for _, n := range filtered[startIdx:end] {
		wire = append(wire, nodeToWire(n, region))
	}

	return &ListNodesOutputFull{Nodes: wire, NextToken: nextToken}, nil
}

// nodeAttributeValue returns the value of a NodeAttributeName/NodeFilterKey
// on a node. This backend only tracks InstanceId, PlatformType and
// AgentVersion (see NodeInfo); every other attribute
// (PlatformName/PlatformVersion/Region/ResourceType/SourceType/
// AvailabilityZone/...) has no backing state and returns "" rather than a
// fabricated value.
func nodeAttributeValue(n NodeInfo, attr string) string {
	switch attr {
	case filterKeyInstanceID:
		return n.InstanceID
	case "PlatformType":
		return n.PlatformType
	case filterKeyAgentVersion:
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
	if value == "" && f.Key != filterKeyInstanceID && f.Key != "PlatformType" && f.Key != filterKeyAgentVersion {
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

	filtered := filterNodes(b.buildNodeInfos(region), input.Filters)

	summary := make([]map[string]string, 0, len(input.Aggregators))
	for _, agg := range input.Aggregators {
		summary = append(summary, aggregateNodes(filtered, agg)...)
	}

	maxResults := 0
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(summary, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &ListNodesSummaryOutputFull{Summary: page, NextToken: next}, nil
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
				AssociationVersion: "1",
				InstanceID:         assoc.InstanceID,
			})
		}
	}

	if result == nil {
		result = []InstanceAssociationInfo{}
	}

	sort.Slice(
		result,
		func(i, k int) bool { return result[i].AssociationID < result[k].AssociationID },
	)

	maxResults := 0
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(result, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &DescribeEffectiveInstanceAssociationsOutputFull{
		Associations: page,
		NextToken:    next,
	}, nil
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
				AssociationID:      assoc.AssociationID,
				AssociationName:    assoc.AssociationName,
				AssociationVersion: assoc.AssociationVersion,
				DocumentVersion:    assoc.DocumentVersion,
				InstanceID:         assoc.InstanceID,
				Name:               assoc.Name,
				Status:             status,
				ExecutionDate:      assoc.LastUpdateAssociationDate,
			})
		}
	}

	if result == nil {
		result = []InstanceAssociationStatusInfo{}
	}

	sort.Slice(
		result,
		func(i, k int) bool { return result[i].AssociationID < result[k].AssociationID },
	)

	maxResults := 0
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(result, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &DescribeInstanceAssociationsStatusOutputFull{
		InstanceAssociationStatusInfos: page,
		NextToken:                      next,
	}, nil
}

// instanceInformationAttr returns the value of an InstanceInformation
// attribute by its filter key name. Only the attributes InstanceInformation
// itself tracks can be meaningfully filtered; every other key
// (IamRole/ResourceType/AssociationStatus/...) returns "" and is treated as
// unmatched by the caller unless the key itself is also unrecognized (see
// matchesInstanceInformationFilter).
func instanceInformationAttr(info InstanceInformation, key string) (string, bool) {
	switch key {
	case "InstanceIds", "ActivationIds":
		return info.InstanceID, true
	case filterKeyAgentVersion:
		return info.AgentVersion, true
	case "PingStatus":
		return info.PingStatus, true
	case "PlatformTypes":
		return info.PlatformType, true
	default:
		return "", false
	}
}

// matchesInstanceInformationFilter reports whether an instance satisfies a
// single key/values filter. Unrecognized keys match every instance
// (accept-and-echo, mirroring ListNodes' unknown-key handling,
// instances.go); recognized keys require the tracked attribute to be one of
// the supplied values.
func matchesInstanceInformationFilter(info InstanceInformation, key string, values []string) bool {
	value, tracked := instanceInformationAttr(info, key)
	if !tracked {
		return true
	}

	return slices.Contains(values, value)
}

// DescribeInstanceInformation returns information about managed instances
// from activations, filtered by input.Filters/InstanceInformationFilterList
// and paginated by input.MaxResults/NextToken -- real, optional
// DescribeInstanceInformationInput members
// (api_op_DescribeInstanceInformation.go) a literal struct{} input
// previously discarded from every request.
func (b *InMemoryBackend) DescribeInstanceInformation(
	ctx context.Context,
	input *DescribeInstanceInformationInput,
) (*DescribeInstanceInformationOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstanceInformation")
	defer b.mu.RUnlock()

	activations := b.activationsStore(region)
	list := make([]InstanceInformation, 0, activations.Len())

	for _, act := range activations.All() {
		info := InstanceInformation{
			InstanceID:       act.ActivationID,
			PingStatus:       "Online",
			AgentVersion:     defaultAgentVersionSSM,
			PlatformType:     platformTypeLinux,
			RegistrationDate: act.CreatedDate,
		}

		matched := true

		for _, f := range input.Filters {
			if !matchesInstanceInformationFilter(info, f.Key, f.Values) {
				matched = false

				break
			}
		}

		for _, f := range input.InstanceInformationFilterList {
			if !matchesInstanceInformationFilter(info, f.Key, f.ValueSet) {
				matched = false

				break
			}
		}

		if matched {
			list = append(list, info)
		}
	}

	var maxResults int
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(list, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &DescribeInstanceInformationOutputFull{InstanceInformationList: page, NextToken: next}, nil
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

		sort.Slice(
			states,
			func(i, j int) bool { return states[i].InstanceID < states[j].InstanceID },
		)
	} else {
		for _, instanceID := range input.InstanceIDs {
			if s, exists := patchStates.Get(instanceID); exists {
				states = append(states, *s)
			}
		}
	}

	maxResults := 0
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(states, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &DescribeInstancePatchStatesOutputFull{InstancePatchStates: page, NextToken: next}, nil
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

	sort.Slice(states, func(i, j int) bool { return states[i].InstanceID < states[j].InstanceID })

	maxResults := 0
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(states, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &DescribeInstancePatchStatesForPatchGroupOutput{
		InstancePatchStates: page,
		NextToken:           next,
	}, nil
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

	maxResults := 0
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(result, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &DescribeInstancePatchesOutput{Patches: page, NextToken: next}, nil
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
	input *DescribeInstancePropertiesInput,
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

	sort.Slice(props, func(i, k int) bool { return props[i].InstanceID < props[k].InstanceID })

	maxResults := 0
	if input.MaxResults != nil {
		maxResults = int(*input.MaxResults)
	}

	page, next := paginateSlice(props, input.NextToken, maxResults, defaultDescribeMaxResults)

	return &DescribeInstancePropertiesOutput{InstanceProperties: page, NextToken: next}, nil
}

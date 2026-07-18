package ssm

import (
	"context"
	"sort"
	"strconv"
	"time"

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

// ListNodes returns managed nodes derived from the activations store.
func (b *InMemoryBackend) ListNodes(
	ctx context.Context,
	_ *ListNodesInput,
) (*ListNodesOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	activations := b.activationsStore(region)
	nodes := make([]NodeInfo, 0, activations.Len())
	for _, act := range activations.All() {
		nodes = append(nodes, NodeInfo{
			InstanceID:       act.ActivationID,
			PlatformType:     platformTypeLinux,
			AgentVersion:     defaultAgentVersionSSM,
			RegistrationDate: time.Unix(int64(act.CreatedDate), 0).UTC(),
		})
	}

	sort.Slice(nodes, func(i, k int) bool {
		return nodes[i].InstanceID < nodes[k].InstanceID
	})

	return &ListNodesOutputFull{Nodes: nodes}, nil
}

// ListNodesSummary returns a summary of managed nodes.
func (b *InMemoryBackend) ListNodesSummary(
	ctx context.Context,
	_ *ListNodesSummaryInput,
) (*ListNodesSummaryOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListNodesSummary")
	defer b.mu.RUnlock()

	return &ListNodesSummaryOutputFull{
		Summary: []map[string]string{
			{"NodeCount": strconv.Itoa(b.activationsStore(region).Len())},
		},
	}, nil
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
				ExecutionDate: time.Unix(int64(assoc.LastUpdateAssociationDate), 0).UTC(),
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
			RegistrationDate: time.Unix(int64(act.CreatedDate), 0).UTC(),
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

package dms

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createReplicationConfigInput struct {
	ReplicationConfigIdentifier *string    `json:"ReplicationConfigIdentifier"`
	ReplicationType             *string    `json:"ReplicationType"`
	SourceEndpointArn           *string    `json:"SourceEndpointArn"`
	TargetEndpointArn           *string    `json:"TargetEndpointArn"`
	Tags                        []tagEntry `json:"Tags"`
}

type replicationConfigJSON struct {
	ReplicationConfigIdentifier string `json:"ReplicationConfigIdentifier"`
	ReplicationConfigArn        string `json:"ReplicationConfigArn"`
	ReplicationType             string `json:"ReplicationType"`
	SourceEndpointArn           string `json:"SourceEndpointArn"`
	TargetEndpointArn           string `json:"TargetEndpointArn"`
}

type createReplicationConfigOutput struct {
	ReplicationConfig replicationConfigJSON `json:"ReplicationConfig"`
}

func rcToJSON(rc *ReplicationConfig) replicationConfigJSON {
	return replicationConfigJSON{
		ReplicationConfigIdentifier: rc.ReplicationConfigIdentifier,
		ReplicationConfigArn:        rc.ReplicationConfigArn,
		ReplicationType:             rc.ReplicationType,
		SourceEndpointArn:           rc.SourceEndpointArn,
		TargetEndpointArn:           rc.TargetEndpointArn,
	}
}

func (h *Handler) handleCreateReplicationConfig(
	ctx context.Context, in *createReplicationConfigInput,
) (*createReplicationConfigOutput, error) {
	identifier := ptrconv.String(in.ReplicationConfigIdentifier)
	if identifier == "" {
		return nil, fmt.Errorf("%w: ReplicationConfigIdentifier is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	rc, err := h.Backend.CreateReplicationConfig(
		ctx,
		identifier,
		ptrconv.String(in.ReplicationType),
		ptrconv.String(in.SourceEndpointArn),
		ptrconv.String(in.TargetEndpointArn),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createReplicationConfigOutput{ReplicationConfig: rcToJSON(rc)}, nil
}

type deleteReplicationConfigInput struct {
	ReplicationConfigArn *string `json:"ReplicationConfigArn"`
}

type deleteReplicationConfigOutput struct {
	ReplicationConfig replicationConfigJSON `json:"ReplicationConfig"`
}

func (h *Handler) handleDeleteReplicationConfig(
	ctx context.Context, in *deleteReplicationConfigInput,
) (*deleteReplicationConfigOutput, error) {
	identifierOrArn := ptrconv.String(in.ReplicationConfigArn)

	configs, _ := h.Backend.DescribeReplicationConfigs(ctx)
	var found *ReplicationConfig
	for _, rc := range configs {
		if rc.ReplicationConfigArn == identifierOrArn ||
			rc.ReplicationConfigIdentifier == identifierOrArn {
			found = rc

			break
		}
	}

	if err := h.Backend.DeleteReplicationConfig(ctx, identifierOrArn); err != nil {
		return nil, err
	}

	if found == nil {
		return &deleteReplicationConfigOutput{}, nil
	}

	return &deleteReplicationConfigOutput{ReplicationConfig: rcToJSON(found)}, nil
}

type describeReplicationConfigsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationConfigsOutput struct {
	Marker             *string                 `json:"Marker,omitempty"`
	ReplicationConfigs []replicationConfigJSON `json:"ReplicationConfigs"`
}

func (h *Handler) handleDescribeReplicationConfigs(
	ctx context.Context, in *describeReplicationConfigsInput,
) (*describeReplicationConfigsOutput, error) {
	list, err := h.Backend.DescribeReplicationConfigs(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationConfigIdentifier < list[j].ReplicationConfigIdentifier
	})

	all := make([]replicationConfigJSON, 0, len(list))
	for _, rc := range list {
		all = append(all, rcToJSON(rc))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeReplicationConfigsOutput{ReplicationConfigs: data, Marker: nextMarker}, nil
}

type describeReplicationsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationsOutput struct {
	Marker       *string           `json:"Marker,omitempty"`
	Replications []replicationJSON `json:"Replications"`
}

func (h *Handler) handleDescribeReplications(
	ctx context.Context, in *describeReplicationsInput,
) (*describeReplicationsOutput, error) {
	arnOrID := extractFilterValue(in.Filters, "replication-config-arn", "replication-config-id")

	list, err := h.Backend.DescribeReplications(ctx, arnOrID)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationConfigIdentifier < list[j].ReplicationConfigIdentifier
	})

	all := make([]replicationJSON, 0, len(list))
	for _, rc := range list {
		all = append(all, replToJSON(rc))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeReplicationsOutput{Replications: data, Marker: nextMarker}, nil
}

type modifyReplicationConfigInput struct {
	ReplicationConfigArn *string `json:"ReplicationConfigArn"`
	ReplicationType      *string `json:"ReplicationType"`
}

type modifyReplicationConfigOutput struct {
	ReplicationConfig replicationConfigJSON `json:"ReplicationConfig"`
}

func (h *Handler) handleModifyReplicationConfig(
	ctx context.Context, in *modifyReplicationConfigInput,
) (*modifyReplicationConfigOutput, error) {
	rc, err := h.Backend.ModifyReplicationConfig(
		ctx,
		ptrconv.String(in.ReplicationConfigArn),
		ptrconv.String(in.ReplicationType),
	)
	if err != nil {
		return nil, err
	}

	return &modifyReplicationConfigOutput{ReplicationConfig: rcToJSON(rc)}, nil
}

type startReplicationInput struct {
	ReplicationConfigArn *string `json:"ReplicationConfigArn"`
	StartReplicationType *string `json:"StartReplicationType"`
}

// replicationJSON represents the DMS Serverless "Replication" runtime
// resource returned by StartReplication, StopReplication, and
// DescribeReplications. This is distinct from the "ReplicationConfig"
// resource (replicationConfigJSON) which has no Status field.
type replicationJSON struct {
	ReplicationConfigArn        string `json:"ReplicationConfigArn"`
	ReplicationConfigIdentifier string `json:"ReplicationConfigIdentifier"`
	ReplicationType             string `json:"ReplicationType,omitempty"`
	SourceEndpointArn           string `json:"SourceEndpointArn,omitempty"`
	TargetEndpointArn           string `json:"TargetEndpointArn,omitempty"`
	StartReplicationType        string `json:"StartReplicationType,omitempty"`
	Status                      string `json:"Status"`
}

func replToJSON(rc *ReplicationConfig) replicationJSON {
	return replicationJSON{
		ReplicationConfigArn:        rc.ReplicationConfigArn,
		ReplicationConfigIdentifier: rc.ReplicationConfigIdentifier,
		ReplicationType:             rc.ReplicationType,
		SourceEndpointArn:           rc.SourceEndpointArn,
		TargetEndpointArn:           rc.TargetEndpointArn,
		StartReplicationType:        rc.StartReplicationType,
		Status:                      rc.Status,
	}
}

type startReplicationOutput struct {
	Replication replicationJSON `json:"Replication"`
}

func (h *Handler) handleStartReplication(
	ctx context.Context, in *startReplicationInput,
) (*startReplicationOutput, error) {
	configArn := ptrconv.String(in.ReplicationConfigArn)
	if configArn == "" {
		return nil, fmt.Errorf("%w: ReplicationConfigArn is required", ErrValidation)
	}

	startType := ptrconv.String(in.StartReplicationType)
	if startType == "" {
		return nil, fmt.Errorf("%w: StartReplicationType is required", ErrValidation)
	}

	if !isValidStartReplicationTaskType(startType) {
		return nil, fmt.Errorf(
			"%w: invalid StartReplicationType %q; valid: start-replication, resume-processing, reload-target",
			ErrValidation,
			startType,
		)
	}

	rc, err := h.Backend.StartReplication(ctx, configArn, startType)
	if err != nil {
		return nil, err
	}

	return &startReplicationOutput{Replication: replToJSON(rc)}, nil
}

type stopReplicationInput struct {
	ReplicationConfigArn *string `json:"ReplicationConfigArn"`
}

type stopReplicationOutput struct {
	Replication replicationJSON `json:"Replication"`
}

func (h *Handler) handleStopReplication(
	ctx context.Context, in *stopReplicationInput,
) (*stopReplicationOutput, error) {
	configArn := ptrconv.String(in.ReplicationConfigArn)
	if configArn == "" {
		return nil, fmt.Errorf("%w: ReplicationConfigArn is required", ErrValidation)
	}

	rc, err := h.Backend.StopReplication(ctx, configArn)
	if err != nil {
		return nil, err
	}

	return &stopReplicationOutput{Replication: replToJSON(rc)}, nil
}

// opsReplicationConfigs returns the dispatch-table entries for the replication_configs operation family.
func (h *Handler) opsReplicationConfigs() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateReplicationConfig: service.WrapOp(
			h.handleCreateReplicationConfig,
		),
		opDeleteReplicationConfig: service.WrapOp(
			h.handleDeleteReplicationConfig,
		),
		opDescribeReplicationConfigs: service.WrapOp(
			h.handleDescribeReplicationConfigs,
		),
		opDescribeReplications: service.WrapOp(
			h.handleDescribeReplications,
		),
		opModifyReplicationConfig: service.WrapOp(
			h.handleModifyReplicationConfig,
		),
		opStartReplication: service.WrapOp(h.handleStartReplication),
		opStopReplication:  service.WrapOp(h.handleStopReplication),
	}
}

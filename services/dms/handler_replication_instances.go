package dms

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createReplicationInstanceInput struct {
	ReplicationInstanceIdentifier *string    `json:"ReplicationInstanceIdentifier"`
	ReplicationInstanceClass      *string    `json:"ReplicationInstanceClass"`
	EngineVersion                 *string    `json:"EngineVersion"`
	AvailabilityZone              *string    `json:"AvailabilityZone"`
	AllocatedStorage              *int32     `json:"AllocatedStorage"`
	MultiAZ                       *bool      `json:"MultiAZ"`
	AutoMinorVersionUpgrade       *bool      `json:"AutoMinorVersionUpgrade"`
	PubliclyAccessible            *bool      `json:"PubliclyAccessible"`
	KmsKeyID                      *string    `json:"KmsKeyId"`
	DNSNameServers                *string    `json:"DnsNameServers"`
	NetworkType                   *string    `json:"NetworkType"`
	PreferredMaintenanceWindow    *string    `json:"PreferredMaintenanceWindow"`
	Tags                          []tagEntry `json:"Tags"`
}

type createReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleCreateReplicationInstance(
	ctx context.Context, in *createReplicationInstanceInput,
) (*createReplicationInstanceOutput, error) {
	identifier := ptrconv.String(in.ReplicationInstanceIdentifier)
	class := ptrconv.String(in.ReplicationInstanceClass)

	if identifier == "" {
		return nil, fmt.Errorf("%w: ReplicationInstanceIdentifier is required", ErrValidation)
	}

	if class == "" {
		return nil, fmt.Errorf("%w: ReplicationInstanceClass is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	ri, err := h.Backend.CreateReplicationInstance(
		ctx,
		identifier, class,
		ptrconv.String(in.EngineVersion),
		ptrconv.String(in.AvailabilityZone),
		ptrInt32(in.AllocatedStorage),
		ptrconv.Bool(in.MultiAZ),
		ptrconv.Bool(in.AutoMinorVersionUpgrade),
		ptrconv.Bool(in.PubliclyAccessible),
		kv,
		ReplicationInstanceSettings{
			KmsKeyID:                   ptrconv.String(in.KmsKeyID),
			DNSNameServers:             ptrconv.String(in.DNSNameServers),
			NetworkType:                ptrconv.String(in.NetworkType),
			PreferredMaintenanceWindow: ptrconv.String(in.PreferredMaintenanceWindow),
		},
	)
	if err != nil {
		return nil, err
	}

	return &createReplicationInstanceOutput{
		ReplicationInstance: riToJSON(ri),
	}, nil
}

type describeReplicationInstancesInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationInstancesOutput struct {
	Marker               *string                   `json:"Marker,omitempty"`
	ReplicationInstances []replicationInstanceJSON `json:"ReplicationInstances"`
}

//nolint:dupl // same HMAC-paginate pattern as handleDescribeEndpoints
func (h *Handler) handleDescribeReplicationInstances(
	ctx context.Context,
	in *describeReplicationInstancesInput,
) (*describeReplicationInstancesOutput, error) {
	identifier := extractFilterValue(in.Filters, "replication-instance-id")
	arnFilter := extractFilterValue(in.Filters, "replication-instance-arn")

	// ARN filter takes precedence when present.
	lookup := identifier
	if arnFilter != "" {
		lookup = arnFilter
	}

	list, err := h.Backend.DescribeReplicationInstances(ctx, lookup)
	if err != nil {
		return nil, err
	}

	// Sort for stable pagination.
	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationInstanceIdentifier < list[j].ReplicationInstanceIdentifier
	})

	all := make([]replicationInstanceJSON, 0, len(list))
	for _, ri := range list {
		all = append(all, riToJSON(ri))
	}

	data, nextMarker := dmsHMACPaginate(all, in.Marker, in.MaxRecords, h.Backend.PaginationSecret())

	return &describeReplicationInstancesOutput{ReplicationInstances: data, Marker: nextMarker}, nil
}

type deleteReplicationInstanceInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
}

type deleteReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleDeleteReplicationInstance(
	ctx context.Context, in *deleteReplicationInstanceInput,
) (*deleteReplicationInstanceOutput, error) {
	arnOrID := ptrconv.String(in.ReplicationInstanceArn)
	// Retrieve before deletion to return it in the response.
	instances, err := h.Backend.DescribeReplicationInstances(ctx, arnOrID)
	if err != nil {
		// Try ARN lookup via delete directly.
		if delErr := h.Backend.DeleteReplicationInstance(ctx, arnOrID); delErr != nil {
			return nil, delErr
		}

		return &deleteReplicationInstanceOutput{}, nil
	}

	if delErr := h.Backend.DeleteReplicationInstance(ctx, arnOrID); delErr != nil {
		return nil, delErr
	}

	if len(instances) == 0 {
		return &deleteReplicationInstanceOutput{}, nil
	}

	return &deleteReplicationInstanceOutput{ReplicationInstance: riToJSON(instances[0])}, nil
}

type replicationInstanceJSON struct {
	ReplicationSubnetGroup                replicationSubnetGroupJSON `json:"ReplicationSubnetGroup"`
	DNSNameServers                        string                     `json:"DnsNameServers,omitempty"`
	KmsKeyID                              string                     `json:"KmsKeyId,omitempty"`
	ReplicationInstanceClass              string                     `json:"ReplicationInstanceClass"`
	EngineVersion                         string                     `json:"EngineVersion"`
	AvailabilityZone                      string                     `json:"AvailabilityZone"`
	ReplicationInstanceStatus             string                     `json:"ReplicationInstanceStatus"`
	PreferredMaintenanceWindow            string                     `json:"PreferredMaintenanceWindow,omitempty"`
	NetworkType                           string                     `json:"NetworkType,omitempty"`
	ReplicationInstanceArn                string                     `json:"ReplicationInstanceArn"`
	ReplicationInstanceIdentifier         string                     `json:"ReplicationInstanceIdentifier"`
	VpcSecurityGroups                     []any                      `json:"VpcSecurityGroups"`
	ReplicationInstancePublicIPAddresses  []string                   `json:"ReplicationInstancePublicIpAddresses"`
	ReplicationInstancePrivateIPAddresses []string                   `json:"ReplicationInstancePrivateIpAddresses"`
	InstanceCreateTime                    float64                    `json:"InstanceCreateTime,omitempty"`
	AllocatedStorage                      int32                      `json:"AllocatedStorage"`
	MultiAZ                               bool                       `json:"MultiAZ"`
	AutoMinorVersionUpgrade               bool                       `json:"AutoMinorVersionUpgrade"`
	PubliclyAccessible                    bool                       `json:"PubliclyAccessible"`
}

func riToJSON(ri *ReplicationInstance) replicationInstanceJSON {
	emptyID := ""

	privateIPs := []string{ri.PrivateIPAddress}
	publicIPs := []string{}

	return replicationInstanceJSON{
		// ReplicationSubnetGroup must always be present with a non-nil Identifier.
		// The Terraform AWS provider accesses ReplicationSubnetGroup.ReplicationSubnetGroupIdentifier
		// directly (no nil check), so a nil pointer causes a panic.
		ReplicationSubnetGroup: replicationSubnetGroupJSON{
			ReplicationSubnetGroupIdentifier: &emptyID,
		},
		ReplicationInstanceIdentifier:         ri.ReplicationInstanceIdentifier,
		ReplicationInstanceArn:                ri.ReplicationInstanceArn,
		ReplicationInstanceClass:              ri.ReplicationInstanceClass,
		EngineVersion:                         ri.EngineVersion,
		AvailabilityZone:                      ri.AvailabilityZone,
		ReplicationInstanceStatus:             ri.ReplicationInstanceStatus,
		ReplicationInstancePrivateIPAddresses: privateIPs,
		ReplicationInstancePublicIPAddresses:  publicIPs,
		VpcSecurityGroups:                     []any{},
		InstanceCreateTime:                    awstime.Epoch(ri.CreationTime),
		KmsKeyID:                              ri.KmsKeyID,
		DNSNameServers:                        ri.DNSNameServers,
		NetworkType:                           ri.NetworkType,
		PreferredMaintenanceWindow:            ri.PreferredMaintenanceWindow,
		AllocatedStorage:                      ri.AllocatedStorage,
		MultiAZ:                               ri.MultiAZ,
		AutoMinorVersionUpgrade:               ri.AutoMinorVersionUpgrade,
		PubliclyAccessible:                    ri.PubliclyAccessible,
	}
}

type describeOrderableReplicationInstancesInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type orderableReplicationInstanceJSON struct {
	ReplicationInstanceClass string `json:"ReplicationInstanceClass"`
	StorageType              string `json:"StorageType"`
	ReleaseStatus            string `json:"ReleaseStatus"`
	DefaultAllocatedStorage  int32  `json:"DefaultAllocatedStorage"`
	MinAllocatedStorage      int32  `json:"MinAllocatedStorage"`
	MaxAllocatedStorage      int32  `json:"MaxAllocatedStorage"`
}

type describeOrderableReplicationInstancesOutput struct {
	Marker                        *string                            `json:"Marker,omitempty"`
	OrderableReplicationInstances []orderableReplicationInstanceJSON `json:"OrderableReplicationInstances"`
}

type orderableInstanceSpec struct {
	class          string
	defaultStorage int32
	minStorage     int32
	maxStorage     int32
}

const (
	t3DefaultStorage   int32 = 50
	t3MaxStorage       int32 = 200
	c5r5DefaultStorage int32 = 100
	c5r5MaxStorage     int32 = 1024
	minStorageAll      int32 = 5
)

func dmsOrderableInstanceList() []orderableInstanceSpec {
	return []orderableInstanceSpec{
		{class: "dms.t3.micro", defaultStorage: t3DefaultStorage, minStorage: minStorageAll, maxStorage: t3MaxStorage},
		{class: "dms.t3.small", defaultStorage: t3DefaultStorage, minStorage: minStorageAll, maxStorage: t3MaxStorage},
		{class: "dms.t3.medium", defaultStorage: t3DefaultStorage, minStorage: minStorageAll, maxStorage: t3MaxStorage},
		{class: "dms.t3.large", defaultStorage: t3DefaultStorage, minStorage: minStorageAll, maxStorage: t3MaxStorage},
		{
			class:          "dms.c5.large",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.c5.xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.c5.2xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.c5.4xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.large",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.2xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.4xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.8xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
		{
			class:          "dms.r5.16xlarge",
			defaultStorage: c5r5DefaultStorage,
			minStorage:     minStorageAll,
			maxStorage:     c5r5MaxStorage,
		},
	}
}

func (h *Handler) handleDescribeOrderableReplicationInstances(
	_ context.Context, in *describeOrderableReplicationInstancesInput,
) (*describeOrderableReplicationInstancesOutput, error) {
	specs := dmsOrderableInstanceList()
	all := make([]orderableReplicationInstanceJSON, 0, len(specs))

	for _, spec := range specs {
		all = append(all, orderableReplicationInstanceJSON{
			ReplicationInstanceClass: spec.class,
			StorageType:              "gp2",
			DefaultAllocatedStorage:  spec.defaultStorage,
			MinAllocatedStorage:      spec.minStorage,
			MaxAllocatedStorage:      spec.maxStorage,
			// types.ReleaseStatusValues only has "beta"/"prod"
			// (databasemigrationservice@v1.66.4, types/enums.go:628-634);
			// "GA" is not a real value of this enum.
			ReleaseStatus: "prod",
		})
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeOrderableReplicationInstancesOutput{
		OrderableReplicationInstances: data,
		Marker:                        nextMarker,
	}, nil
}

type describeReplicationInstanceTaskLogsInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	Marker                 *string `json:"Marker"`
	MaxRecords             *int32  `json:"MaxRecords"`
}

type describeReplicationInstanceTaskLogsOutput struct {
	Marker                      *string          `json:"Marker,omitempty"`
	ReplicationInstanceArn      string           `json:"ReplicationInstanceArn,omitempty"`
	ReplicationInstanceTaskLogs []map[string]any `json:"ReplicationInstanceTaskLogs"`
}

func (h *Handler) handleDescribeReplicationInstanceTaskLogs(
	_ context.Context, in *describeReplicationInstanceTaskLogsInput,
) (*describeReplicationInstanceTaskLogsOutput, error) {
	return &describeReplicationInstanceTaskLogsOutput{
		ReplicationInstanceArn:      ptrconv.String(in.ReplicationInstanceArn),
		ReplicationInstanceTaskLogs: []map[string]any{},
	}, nil
}

type modifyReplicationInstanceInput struct {
	ReplicationInstanceArn     *string `json:"ReplicationInstanceArn"`
	ReplicationInstanceClass   *string `json:"ReplicationInstanceClass"`
	EngineVersion              *string `json:"EngineVersion"`
	MultiAZ                    *bool   `json:"MultiAZ"`
	AutoMinorVersionUpgrade    *bool   `json:"AutoMinorVersionUpgrade"`
	AllocatedStorage           *int32  `json:"AllocatedStorage"`
	NetworkType                *string `json:"NetworkType"`
	PreferredMaintenanceWindow *string `json:"PreferredMaintenanceWindow"`
}

type modifyReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleModifyReplicationInstance(
	ctx context.Context, in *modifyReplicationInstanceInput,
) (*modifyReplicationInstanceOutput, error) {
	ri, err := h.Backend.ModifyReplicationInstance(
		ctx,
		ptrconv.String(in.ReplicationInstanceArn),
		ptrconv.String(in.ReplicationInstanceClass),
		ptrconv.String(in.EngineVersion),
		in.MultiAZ,
		in.AutoMinorVersionUpgrade,
		in.AllocatedStorage,
		ReplicationInstanceSettings{
			NetworkType:                ptrconv.String(in.NetworkType),
			PreferredMaintenanceWindow: ptrconv.String(in.PreferredMaintenanceWindow),
		},
	)
	if err != nil {
		return nil, err
	}

	return &modifyReplicationInstanceOutput{ReplicationInstance: riToJSON(ri)}, nil
}

type rebootReplicationInstanceInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	ForcePlannedFailover   *bool   `json:"ForcePlannedFailover"`
	ForceFailover          *bool   `json:"ForceFailover"`
}

type rebootReplicationInstanceOutput struct {
	ReplicationInstance replicationInstanceJSON `json:"ReplicationInstance"`
}

func (h *Handler) handleRebootReplicationInstance(
	ctx context.Context, in *rebootReplicationInstanceInput,
) (*rebootReplicationInstanceOutput, error) {
	ri, err := h.Backend.RebootReplicationInstance(ctx, ptrconv.String(in.ReplicationInstanceArn))
	if err != nil {
		return nil, err
	}

	return &rebootReplicationInstanceOutput{ReplicationInstance: riToJSON(ri)}, nil
}

// opsReplicationInstances returns the dispatch-table entries for the replication_instances operation family.
func (h *Handler) opsReplicationInstances() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateReplicationInstance: service.WrapOp(
			h.handleCreateReplicationInstance,
		),
		opDescribeReplicationInstances: service.WrapOp(
			h.handleDescribeReplicationInstances,
		),
		opDeleteReplicationInstance: service.WrapOp(
			h.handleDeleteReplicationInstance,
		),
		opDescribeOrderableReplicationInstances: service.WrapOp(
			h.handleDescribeOrderableReplicationInstances,
		),
		opDescribeReplicationInstanceTaskLogs: service.WrapOp(
			h.handleDescribeReplicationInstanceTaskLogs,
		),
		opModifyReplicationInstance: service.WrapOp(
			h.handleModifyReplicationInstance,
		),
		opRebootReplicationInstance: service.WrapOp(
			h.handleRebootReplicationInstance,
		),
	}
}

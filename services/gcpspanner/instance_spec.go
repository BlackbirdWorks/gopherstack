package gcpspanner

import (
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	minDisplayName    = 4
	maxDisplayName    = 30
	maxLabels         = 64
	puPerNode         = 1000
	puStep            = 100
	maxProcessingUnit = 1_000_000
)

var mutableInstanceFields = []string{ //nolint:gochecknoglobals // immutable catalog
	"display_name", "node_count", "processing_units", "labels", "edition", "default_backup_schedule_type",
}

func validateInstanceID(id string) error {
	if !instanceIDRe.MatchString(id) {
		return errInvalid("instance_id %q must be 2-64 lowercase letters, digits or hyphens, starting with a "+
			"letter and not ending with a hyphen.", id)
	}

	return nil
}

func validateCommon(pb *instancepb.Instance) error {
	if n := len(pb.GetDisplayName()); n < minDisplayName || n > maxDisplayName {
		return errInvalid("display_name must be %d-%d characters.", minDisplayName, maxDisplayName)
	}

	if len(pb.GetLabels()) > maxLabels {
		return errInvalid("An instance can have at most %d labels.", maxLabels)
	}

	if pb.GetAutoscalingConfig() != nil || len(pb.GetReplicaComputeCapacity()) > 0 {
		return errUnimplemented("Autoscaling and replica compute capacity are not supported by this emulator.")
	}

	return nil
}

// normalizeCapacity requires exactly one capacity field and fills in the other.
func normalizeCapacity(pb *instancepb.Instance, nodeSet, puSet bool) error {
	switch {
	case nodeSet && puSet:
		return errInvalid("Only one of node_count and processing_units may be set.")
	case !nodeSet && !puSet:
		return errInvalid("One of node_count or processing_units must be set.")
	case nodeSet:
		if pb.GetNodeCount() <= 0 || int(pb.GetNodeCount())*puPerNode > maxProcessingUnit {
			return errInvalid("node_count %d is out of range.", pb.GetNodeCount())
		}

		pb.ProcessingUnits = pb.GetNodeCount() * puPerNode
	default:
		pu := pb.GetProcessingUnits()
		if pu <= 0 || pu > maxProcessingUnit || (pu < puPerNode && pu%puStep != 0) ||
			(pu >= puPerNode && pu%puPerNode != 0) {
			return errInvalid("processing_units must be a multiple of 100 below 1000, or a multiple of 1000.")
		}

		pb.NodeCount = pu / puPerNode
	}

	return nil
}

func buildInstance(project, id string, in *instancepb.Instance, now time.Time) (*instancepb.Instance, error) {
	pb, _ := proto.Clone(in).(*instancepb.Instance)

	if err := validateCommon(pb); err != nil {
		return nil, err
	}

	if want := configName(project, ""); !strings.HasPrefix(pb.GetConfig(), want) {
		return nil, errInvalid("config must be in the form %s{config}.", want)
	}

	if _, ok := lookupConfig(project, strings.TrimPrefix(pb.GetConfig(), configName(project, ""))); !ok {
		return nil, errNotFound("Instance config not found: %s", pb.GetConfig())
	}

	if err := normalizeCapacity(pb, in.GetNodeCount() != 0, in.GetProcessingUnits() != 0); err != nil {
		return nil, err
	}

	pb.Name = instanceName(project, id)
	pb.State = instancepb.Instance_CREATING
	pb.InstanceType = instancepb.Instance_PROVISIONED
	pb.EndpointUris, pb.FreeInstanceMetadata = nil, nil
	pb.CreateTime, pb.UpdateTime = timestamppb.New(now), timestamppb.New(now)

	if pb.GetEdition() == instancepb.Instance_EDITION_UNSPECIFIED {
		pb.Edition = instancepb.Instance_STANDARD
	}

	if pb.GetDefaultBackupScheduleType() == instancepb.Instance_DEFAULT_BACKUP_SCHEDULE_TYPE_UNSPECIFIED {
		pb.DefaultBackupScheduleType = instancepb.Instance_NONE
	}

	return pb, nil
}

// applyInstanceMask copies the masked top-level fields of src onto dst.
func applyInstanceMask(dst, src *instancepb.Instance, paths []string) error {
	d, s := dst.ProtoReflect(), src.ProtoReflect()

	for _, p := range paths {
		name, _, _ := strings.Cut(p, ".")
		if !slices.Contains(mutableInstanceFields, name) {
			return errInvalid("Update mask path %q is not an updatable field.", p)
		}

		fd := d.Descriptor().Fields().ByName(protoreflect.Name(name))
		if s.Has(fd) {
			d.Set(fd, s.Get(fd))
		} else {
			d.Clear(fd)
		}
	}

	return nil
}

func maskHas(paths []string, field string) bool {
	return slices.ContainsFunc(paths, func(p string) bool {
		name, _, _ := strings.Cut(p, ".")

		return name == field
	})
}

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
	fieldDisplayName, "node_count", "processing_units", "labels", "edition", "default_backup_schedule_type",
	"autoscaling_config",
}

func validateInstanceID(id string) error {
	if !instanceIDRe.MatchString(id) {
		return errInvalid("instance_id %q must be 2-64 lowercase letters, digits or hyphens, starting with a "+
			"letter and not ending with a hyphen.", id)
	}

	return nil
}

func validateCommon(pb *instancepb.Instance, cfg *instancepb.InstanceConfig) error {
	if n := len(pb.GetDisplayName()); n < minDisplayName || n > maxDisplayName {
		return errInvalid("display_name must be %d-%d characters.", minDisplayName, maxDisplayName)
	}

	if len(pb.GetLabels()) > maxLabels {
		return errInvalid("An instance can have at most %d labels.", maxLabels)
	}

	return validateAutoscaling(pb.GetAutoscalingConfig(), cfg)
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

type configLookup func(name string) (*instancepb.InstanceConfig, bool)

func buildInstance(
	project, id string, in *instancepb.Instance, now time.Time, cfg configLookup,
) (*instancepb.Instance, error) {
	pb, _ := proto.Clone(in).(*instancepb.Instance)

	if want := configName(project, ""); !strings.HasPrefix(pb.GetConfig(), want) {
		return nil, errInvalid("config must be in the form %s{config}.", want)
	}

	c, ok := cfg(pb.GetConfig())
	if !ok {
		return nil, errNotFound("Instance config not found: %s", pb.GetConfig())
	}

	if c.GetState() != instancepb.InstanceConfig_READY {
		return nil, errPrecondition("Instance config %s is not ready.", pb.GetConfig())
	}

	if err := validateCommon(pb, c); err != nil {
		return nil, err
	}

	if pb.GetAutoscalingConfig() != nil {
		setInstanceCapacity(pb, autoscaleMin(pb.GetAutoscalingConfig()))
	} else if err := normalizeCapacity(pb, in.GetNodeCount() != 0, in.GetProcessingUnits() != 0); err != nil {
		return nil, err
	}

	replicaCapacity(pb, c)

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

// applyInstanceMask copies the masked fields of src onto dst; a path below a message field copies only that leaf.
func applyInstanceMask(dst, src *instancepb.Instance, paths []string) error {
	for _, p := range paths {
		name, _, _ := strings.Cut(p, ".")
		if !slices.Contains(mutableInstanceFields, name) {
			return errInvalid("Update mask path %q is not an updatable field.", p)
		}

		if !copyPath(dst.ProtoReflect(), src.ProtoReflect(), strings.Split(p, ".")) {
			return errInvalid("Update mask path %q is not a field of Instance.", p)
		}
	}

	return nil
}

func copyPath(d, s protoreflect.Message, path []string) bool {
	fd := d.Descriptor().Fields().ByName(protoreflect.Name(path[0]))
	if fd == nil {
		return false
	}

	if len(path) == 1 || fd.Message() == nil || fd.IsMap() || fd.IsList() {
		if s.Has(fd) {
			d.Set(fd, s.Get(fd))
		} else {
			d.Clear(fd)
		}

		return true
	}

	return copyPath(d.Mutable(fd).Message(), s.Get(fd).Message(), path[1:])
}

func maskHas(paths []string, field string) bool {
	return slices.ContainsFunc(paths, func(p string) bool {
		name, _, _ := strings.Cut(p, ".")

		return name == field
	})
}

// applyCapacity settles node_count and processing_units after a masked update. With autoscaling on, both are
// output-only: the current capacity is kept within the limits.
func (b *InMemoryBackend) applyCapacity(next, prev *instancepb.Instance, paths []string) error {
	nodeSet, puSet := maskHas(paths, "node_count"), maskHas(paths, "processing_units")

	if next.GetAutoscalingConfig() != nil {
		if nodeSet || puSet {
			return errInvalid("node_count and processing_units are output-only while autoscaling is enabled.")
		}

		cur := capacity{units: prev.GetProcessingUnits()}
		setInstanceCapacity(next, cur.clamp(next.GetAutoscalingConfig()))

		return nil
	}

	if nodeSet || puSet {
		return normalizeCapacity(next, nodeSet, puSet)
	}

	if prev.GetAutoscalingConfig() != nil {
		setInstanceCapacity(next, capacity{units: prev.GetProcessingUnits()})
	}

	return nil
}

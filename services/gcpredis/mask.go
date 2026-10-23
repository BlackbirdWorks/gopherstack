package gcpredis

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var mutableFields = []string{ //nolint:gochecknoglobals // immutable catalog
	"display_name", "labels", "memory_size_gb", "redis_configs", "replica_count", "maintenance_policy",
	"auth_enabled", "persistence_config", "read_replicas_mode",
}

// applyMask copies the masked fields of src onto dst. Dotted paths address the whole top-level field, since
// Memorystore updates replace maps and nested messages as a unit.
func applyMask(dst, src proto.Message, paths []string) error {
	d, s := dst.ProtoReflect(), src.ProtoReflect()

	for _, p := range paths {
		name, _, _ := strings.Cut(p, ".")
		if !slices.Contains(mutableFields, name) {
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

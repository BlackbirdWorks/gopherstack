package gcprun

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// applyMask copies the masked top-level fields of src onto dst. An empty mask copies every populated mutable
// field; a nested path replaces its whole top-level field.
func applyMask(dst, src proto.Message, paths, mutable []string) error {
	d, s := dst.ProtoReflect(), clone(src).ProtoReflect()
	fields := d.Descriptor().Fields()

	if len(paths) == 0 {
		s.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			if slices.Contains(mutable, string(fd.Name())) {
				d.Set(fd, v)
			}

			return true
		})

		return nil
	}

	if slices.Contains(paths, "*") {
		paths = mutable
	}

	for _, p := range paths {
		top, _, _ := strings.Cut(p, ".")

		fd := fields.ByName(protoreflect.Name(top))
		if fd == nil || !slices.Contains(mutable, top) {
			return errInvalid("update_mask path %q is not a mutable field.", p)
		}

		if s.Has(fd) {
			d.Set(fd, s.Get(fd))
		} else {
			d.Clear(fd)
		}
	}

	return nil
}

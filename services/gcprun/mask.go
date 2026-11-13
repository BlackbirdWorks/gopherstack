package gcprun

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// applyMask copies the masked fields of src onto dst. An empty mask copies every populated mutable top-level
// field; a dotted path copies only that nested field through singular message fields.
func applyMask(dst, src proto.Message, paths, mutable []string) error {
	d, s := dst.ProtoReflect(), clone(src).ProtoReflect()

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
		segs := strings.Split(p, ".")
		if !slices.Contains(mutable, segs[0]) {
			return errInvalid("update_mask path %q is not a mutable field.", p)
		}

		if err := copyPath(d, s, segs, p); err != nil {
			return err
		}
	}

	return nil
}

func copyPath(d, s protoreflect.Message, segs []string, full string) error {
	fd := d.Descriptor().Fields().ByName(protoreflect.Name(segs[0]))
	if fd == nil {
		return errInvalid("update_mask path %q is not a mutable field.", full)
	}

	if len(segs) == 1 {
		if s.Has(fd) {
			d.Set(fd, s.Get(fd))
		} else {
			d.Clear(fd)
		}

		return nil
	}

	if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
		return errInvalid("update_mask path %q is not a mutable field.", full)
	}

	return copyPath(d.Mutable(fd).Message(), s.Get(fd).Message(), segs[1:], full)
}

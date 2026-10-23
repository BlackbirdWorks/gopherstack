package gcpcloudbuild

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// applyMask copies the masked fields of src onto dst. Dotted paths address the whole top-level field.
// immutable lists fields that cannot be updated.
func applyMask(dst, src proto.Message, paths, immutable []string) error {
	d, s := dst.ProtoReflect(), src.ProtoReflect()

	for _, p := range paths {
		name, _, _ := strings.Cut(p, ".")

		fd := d.Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil || containsString(immutable, name) {
			return errInvalid("Update mask path %q is not an updatable field.", p)
		}

		if s.Has(fd) {
			d.Set(fd, s.Get(fd))
		} else {
			d.Clear(fd)
		}
	}

	return nil
}

func containsString(list []string, s string) bool {
	return slices.Contains(list, s)
}

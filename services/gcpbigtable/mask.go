package gcpbigtable

import (
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// applyMask copies the masked (dotted) paths from src onto dst; a path absent in src is cleared on dst.
func applyMask(dst, src proto.Message, paths []string) error {
	for _, p := range paths {
		if err := copyPath(dst.ProtoReflect(), src.ProtoReflect(), strings.Split(p, ".")); err != nil {
			return errInvalid("Unsupported update_mask path %q.", p)
		}
	}

	return nil
}

func copyPath(dst, src protoreflect.Message, parts []string) error {
	fd := dst.Descriptor().Fields().ByName(protoreflect.Name(parts[0]))
	if fd == nil {
		return errBadFilter
	}

	if len(parts) == 1 {
		if !src.Has(fd) {
			dst.Clear(fd)

			return nil
		}

		v := src.Get(fd)
		if fd.Message() != nil && !fd.IsList() && !fd.IsMap() {
			v = protoreflect.ValueOfMessage(proto.Clone(v.Message().Interface()).ProtoReflect())
		}

		dst.Set(fd, v)

		return nil
	}

	if fd.Message() == nil || fd.IsList() || fd.IsMap() {
		return errBadFilter
	}

	return copyPath(dst.Mutable(fd).Message(), src.Get(fd).Message(), parts[1:])
}

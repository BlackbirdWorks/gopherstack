package gcpfunctions

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func functionMutable() []string {
	return []string{"description", "build_config", "service_config", "event_trigger", "labels", "kms_key_name"}
}

// applyMask copies the masked fields of src onto dst. Dotted paths descend into nested messages; an empty mask
// copies every populated mutable top-level field.
func applyMask(dst, src proto.Message, paths []string) error {
	d, s := dst.ProtoReflect(), clone(src).ProtoReflect()

	if len(paths) == 0 {
		s.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			if slices.Contains(functionMutable(), string(fd.Name())) {
				d.Set(fd, v)
			}

			return true
		})

		return nil
	}

	for _, p := range paths {
		parts := strings.Split(p, ".")
		if !slices.Contains(functionMutable(), parts[0]) || applyPath(d, s, parts) != nil {
			return errInvalid("update_mask path %q is not a mutable field.", p)
		}
	}

	return nil
}

func applyPath(d, s protoreflect.Message, path []string) error {
	fd := d.Descriptor().Fields().ByName(protoreflect.Name(path[0]))
	if fd == nil {
		return errInvalid("unknown field")
	}

	if len(path) == 1 {
		if s.Has(fd) {
			d.Set(fd, s.Get(fd))
		} else {
			d.Clear(fd)
		}

		return nil
	}

	if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
		return errInvalid("not a message field")
	}

	return applyPath(d.Mutable(fd).Message(), s.Get(fd).Message(), path[1:])
}

func paginate[T any](all []T, token string, size int32) ([]T, string, error) {
	if size < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	if err := page.ValidateToken(token); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	pg := page.New(all, token, int(size), defaultPageSize)

	return pg.Data, pg.Next, nil
}

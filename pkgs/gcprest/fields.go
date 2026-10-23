package gcprest

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func fieldByName(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	if fd := md.Fields().ByJSONName(name); fd != nil {
		return fd
	}

	return md.Fields().ByName(protoreflect.Name(name))
}

// setPath assigns vals to the (possibly dotted) field path on m. Unknown paths are ignored, like Google's frontends do.
func setPath(m protoreflect.Message, path string, vals []string) error {
	head, tail, nested := strings.Cut(path, ".")

	fd := fieldByName(m.Descriptor(), head)
	if fd == nil || len(vals) == 0 {
		return nil
	}

	if !nested {
		return setLeaf(m, fd, vals)
	}

	if fd.IsMap() {
		return setMapEntry(m, fd, tail, vals[len(vals)-1])
	}

	if fd.Kind() != protoreflect.MessageKind || fd.IsList() {
		return nil
	}

	return setPath(m.Mutable(fd).Message(), tail, vals)
}

func setMapEntry(m protoreflect.Message, fd protoreflect.FieldDescriptor, key, val string) error {
	mk, err := convertScalar(fd.MapKey(), key)
	if err != nil {
		return err
	}

	mv, err := convertScalar(fd.MapValue(), val)
	if err != nil {
		return err
	}

	m.Mutable(fd).Map().Set(mk.MapKey(), mv)

	return nil
}

func setLeaf(m protoreflect.Message, fd protoreflect.FieldDescriptor, vals []string) error {
	if fd.IsMap() {
		return nil
	}

	if fd.IsList() {
		list := m.Mutable(fd).List()

		for _, s := range vals {
			v, err := convert(m, fd, s)
			if err != nil {
				return err
			}

			list.Append(v)
		}

		return nil
	}

	v, err := convert(m, fd, vals[len(vals)-1])
	if err != nil {
		return err
	}

	m.Set(fd, v)

	return nil
}

func convert(parent protoreflect.Message, fd protoreflect.FieldDescriptor, s string) (protoreflect.Value, error) {
	if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
		return convertScalar(fd, s)
	}

	var msg protoreflect.Message
	if fd.IsList() {
		msg = parent.Mutable(fd).List().NewElement().Message()
	} else {
		msg = parent.NewField(fd).Message()
	}

	if err := protojson.Unmarshal([]byte(strconv.Quote(s)), msg.Interface()); err != nil {
		return protoreflect.Value{}, fmt.Errorf("field %s: %w", fd.Name(), err)
	}

	return protoreflect.ValueOfMessage(msg), nil
}

func convertScalar(fd protoreflect.FieldDescriptor, s string) (protoreflect.Value, error) {
	v, err := scalar(fd, s)
	if err != nil {
		return protoreflect.Value{}, fmt.Errorf("field %s: invalid value %q: %w", fd.Name(), s, err)
	}

	return v, nil
}

func scalar(fd protoreflect.FieldDescriptor, s string) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(s), nil
	case protoreflect.BoolKind:
		b, err := strconv.ParseBool(s)

		return protoreflect.ValueOfBool(b), err
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, err := strconv.ParseInt(s, 10, 32)

		return protoreflect.ValueOfInt32(int32(n)), err
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, err := strconv.ParseInt(s, 10, 64)

		return protoreflect.ValueOfInt64(n), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, err := strconv.ParseUint(s, 10, 32)

		return protoreflect.ValueOfUint32(uint32(n)), err
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, err := strconv.ParseUint(s, 10, 64)

		return protoreflect.ValueOfUint64(n), err
	case protoreflect.FloatKind:
		f, err := strconv.ParseFloat(s, 32)

		return protoreflect.ValueOfFloat32(float32(f)), err
	case protoreflect.DoubleKind:
		f, err := strconv.ParseFloat(s, 64)

		return protoreflect.ValueOfFloat64(f), err
	case protoreflect.BytesKind:
		b, err := decodeBase64(s)

		return protoreflect.ValueOfBytes(b), err
	case protoreflect.EnumKind:
		return enumValue(fd.Enum(), s)
	default:
		return protoreflect.Value{}, fmt.Errorf("%w: %s", errUnsupportedKind, fd.Kind())
	}
}

func enumValue(ed protoreflect.EnumDescriptor, s string) (protoreflect.Value, error) {
	if ev := ed.Values().ByName(protoreflect.Name(s)); ev != nil {
		return protoreflect.ValueOfEnum(ev.Number()), nil
	}

	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return protoreflect.Value{}, fmt.Errorf("unknown enum value: %w", err)
	}

	return protoreflect.ValueOfEnum(protoreflect.EnumNumber(n)), nil
}

func decodeBase64(s string) ([]byte, error) {
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.RawURLEncoding,
	} {
		if b, err := enc.DecodeString(s); err == nil {
			return b, nil
		}
	}

	return nil, errBadBase64
}

func newMessage(md protoreflect.MessageDescriptor) proto.Message {
	if mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName()); err == nil {
		return mt.New().Interface()
	}

	return dynamicpb.NewMessage(md)
}

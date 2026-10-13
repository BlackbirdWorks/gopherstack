package iot

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	protoArgCount   = 6
	protoBucketPos  = 2
	protoKeyPos     = 3
	protoFilePos    = 4
	protoMessagePos = 5
)

// DescriptorReader reads a FileDescriptorSet object from S3 for decode(..., 'proto', ...).
type DescriptorReader interface {
	GetDescriptorFile(ctx context.Context, region, bucket, key string) ([]byte, error)
}

// decodeProto implements decode(value, 'proto', bucket, key, protoFile, messageType).
func decodeProto(c *sqlCtx, args []any) any {
	m, h, ok := lookupEnv(c)
	if !ok {
		return nil
	}

	if len(args) != protoArgCount {
		return failed(m, "decode")
	}

	bucket, _ := strArg(args, protoBucketPos)
	key, _ := strArg(args, protoKeyPos)
	file, _ := strArg(args, protoFilePos)
	msgType, _ := strArg(args, protoMessagePos)

	payload, pok := strArg(args, 0)
	reader := h.backend.actionTargets().Descriptors

	if !pok || reader == nil {
		return failed(m, "decode")
	}

	set, err := reader.GetDescriptorFile(h.ctx, m.region, bucket, key)
	if err != nil {
		return failed(m, "decode")
	}

	doc, err := protoToJSON(set, file, msgType, []byte(payload))
	if err != nil {
		return failed(m, "decode")
	}

	return jsonOrFail(m, "decode", doc, c.v2016)
}

func protoToJSON(descriptorSet []byte, file, msgType string, payload []byte) ([]byte, error) {
	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(descriptorSet, &fds); err != nil {
		return nil, fmt.Errorf("descriptor set: %w", err)
	}

	files, err := protodesc.NewFiles(&fds)
	if err != nil {
		return nil, fmt.Errorf("descriptor set: %w", err)
	}

	md, err := findProtoMessage(files, file, msgType)
	if err != nil {
		return nil, err
	}

	msg := dynamicpb.NewMessage(md)
	if err = proto.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("payload: %w", err)
	}

	return protojson.MarshalOptions{UseProtoNames: true}.Marshal(msg)
}

type protoFileFinder interface {
	FindFileByPath(path string) (protoreflect.FileDescriptor, error)
	FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error)
}

func findProtoMessage(files protoFileFinder, file, msgType string) (protoreflect.MessageDescriptor, error) {
	for _, p := range []string{file, file + ".proto"} {
		fd, err := files.FindFileByPath(p)
		if err != nil {
			continue
		}

		if md := messageInFile(fd, msgType); md != nil {
			return md, nil
		}
	}

	d, err := files.FindDescriptorByName(protoreflect.FullName(strings.TrimPrefix(msgType, ".")))
	if err == nil {
		if md, ok := d.(protoreflect.MessageDescriptor); ok {
			return md, nil
		}
	}

	return nil, fmt.Errorf("%w: message type %q in %q", errSQLFunction, msgType, file)
}

func messageInFile(fd protoreflect.FileDescriptor, msgType string) protoreflect.MessageDescriptor {
	msgs := fd.Messages()
	name := strings.TrimPrefix(msgType, ".")
	name = strings.TrimPrefix(name, string(fd.Package())+".")

	parts := strings.Split(name, ".")

	md := msgs.ByName(protoreflect.Name(parts[0]))
	for _, p := range parts[1:] {
		if md == nil {
			return nil
		}

		md = md.Messages().ByName(protoreflect.Name(p))
	}

	return md
}

package gcpartifactregistry

import (
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const uploadPkg = "google.devtools.artifactregistry.v1"

var errUploadType = errors.New("unknown upload response type")

// uploadResponse names a media-upload response and the artifact message its repeated field carries.
type uploadResponse struct {
	name, field, elem string
}

var uploadResponses = []uploadResponse{ //nolint:gochecknoglobals // immutable catalog
	{"UploadAptArtifactResponse", "apt_artifacts", "AptArtifact"},
	{"UploadYumArtifactResponse", "yum_artifacts", "YumArtifact"},
	{"UploadGoModuleResponse", "go_modules", "GoModule"},
}

// registerUploadTypes defines the upload responses at runtime: the REST discovery document has them but the
// generated Go protos do not. The shapes follow google.golang.org/api/artifactregistry/v1 (UploadAptArtifactResponse
// and UploadYumArtifactResponse); UploadGoModuleResponse's shape is a guess (unverified).
var registerUploadTypes = sync.OnceValue(func() error { //nolint:gochecknoglobals // one-time type registration
	fd := &descriptorpb.FileDescriptorProto{
		Name: new("gopherstack/artifactregistry/upload_responses.proto"), Package: new(uploadPkg),
		Syntax: new("proto3"),
		Dependency: []string{
			artifactregistrypb.File_google_devtools_artifactregistry_v1_apt_artifact_proto.Path(),
			artifactregistrypb.File_google_devtools_artifactregistry_v1_yum_artifact_proto.Path(),
			artifactregistrypb.File_google_devtools_artifactregistry_v1_go_proto.Path(),
		},
	}

	for _, u := range uploadResponses {
		if _, err := protoregistry.GlobalTypes.FindMessageByName(
			protoreflect.FullName(uploadPkg + "." + u.name),
		); err == nil {
			continue
		}

		fd.MessageType = append(fd.MessageType, &descriptorpb.DescriptorProto{
			Name: new(u.name),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: new(u.field), Number: proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: new("." + uploadPkg + "." + u.elem),
			}},
		})
	}

	if len(fd.GetMessageType()) == 0 {
		return nil
	}

	file, err := protodesc.NewFile(fd, protoregistry.GlobalFiles)
	if err != nil {
		return fmt.Errorf("describe upload responses: %w", err)
	}

	msgs := file.Messages()
	for i := range msgs.Len() {
		if err = protoregistry.GlobalTypes.RegisterMessage(dynamicpb.NewMessageType(msgs.Get(i))); err != nil {
			return fmt.Errorf("register upload response: %w", err)
		}
	}

	return nil
})

func newUploadResponse(name string, items []proto.Message) (proto.Message, error) {
	if err := registerUploadTypes(); err != nil {
		return nil, err
	}

	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(uploadPkg + "." + name))
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errUploadType, name)
	}

	msg := mt.New()
	list := msg.Mutable(msg.Descriptor().Fields().ByNumber(1)).List()

	for _, it := range items {
		list.Append(protoreflect.ValueOfMessage(it.ProtoReflect()))
	}

	return msg.Interface(), nil
}

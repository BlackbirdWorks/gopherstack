package gcpcloudbuild

import (
	"encoding/base64"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

// buildOpName names the operation of a build: operations/build/{project}/{id} when global, else regional.
func buildOpName(r ref) string {
	if r.location == globalLocation {
		return "operations/build/" + r.project + "/" + base64.RawURLEncoding.EncodeToString(
			[]byte(r.id),
		)
	}

	return "projects/" + r.project + "/locations/" + r.location + "/operations/" + r.id
}

// putBuildOp records the operation of rec: pending until the build is terminal, then done with the Build.
func (b *InMemoryBackend) putBuildOp(rec *buildRec) *longrunningpb.Operation {
	meta := &cloudbuildpb.BuildOperationMetadata{Build: cloneBuild(rec.pb)}

	if terminal(rec.pb.GetStatus()) {
		if op, err := gcplro.Done(rec.opName, meta, rec.pb); err == nil {
			return b.ops.Put(op)
		}
	}

	return b.ops.Put(&longrunningpb.Operation{Name: rec.opName, Metadata: mustAny(meta)})
}

func mustAny(m proto.Message) *anypb.Any {
	a, err := anypb.New(m)
	if err != nil {
		return nil
	}

	return a
}

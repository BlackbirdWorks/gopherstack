package gcpartifactregistry

import (
	"context"
	"path"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ObjectStore reads Cloud Storage objects for the import RPCs; the in-process GCS emulator implements it.
type ObjectStore interface {
	ReadObject(bucket, object string) ([]byte, error)
	ListObjects(bucket, prefix string) ([]string, error)
}

// SetObjectStore connects the import RPCs to a Cloud Storage emulator.
func (b *InMemoryBackend) SetObjectStore(s ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()

	b.objects = s
}

func splitGCSURI(uri string) (string, string, bool) {
	rest, ok := strings.CutPrefix(uri, "gs://")
	if !ok {
		return "", "", false
	}

	bucket, object, ok := strings.Cut(rest, "/")

	return bucket, object, ok && bucket != "" && object != ""
}

// expandSource resolves one URI, with wildcards when allowed, to the objects it names.
func expandSource(store ObjectStore, uri string, wildcards bool) ([]string, error) {
	bucket, object, ok := splitGCSURI(uri)
	if !ok {
		return nil, errInvalid("%q is not a gs://bucket/object URI.", uri)
	}

	if !wildcards || !strings.ContainsAny(object, "*?[") {
		return []string{object}, nil
	}

	prefix, _, _ := strings.Cut(object, "*")
	if i := strings.IndexAny(prefix, "?["); i >= 0 {
		prefix = prefix[:i]
	}

	names, err := store.ListObjects(bucket, prefix)
	if err != nil {
		return nil, err
	}

	var out []string

	for _, n := range names {
		if matched, _ := path.Match(object, n); matched || globStar(object, n) {
			out = append(out, n)
		}
	}

	return out, nil
}

// globStar lets * match across slashes, as Cloud Storage wildcards do.
func globStar(pattern, name string) bool {
	if !strings.Contains(pattern, "*") {
		return false
	}

	parts := strings.Split(pattern, "*")
	pos := 0

	for i, part := range parts {
		idx := strings.Index(name[pos:], part)
		if idx < 0 || (i == 0 && idx != 0) {
			return false
		}

		pos += idx + len(part)
	}

	return strings.HasSuffix(name, parts[len(parts)-1])
}

type importResult struct {
	err error
	uri string
}

// importObjects reads each source object and hands its bytes to add.
func (b *InMemoryBackend) importObjects(
	uris []string, wildcards bool, add func(data []byte) error,
) ([]importResult, error) {
	b.mu.RLock("importObjects")
	store := b.objects
	b.mu.RUnlock()

	if store == nil {
		return nil, errPrecondition("The Cloud Storage emulator is not available to Artifact Registry.")
	}

	if len(uris) == 0 {
		return nil, errInvalid("gcs_source.uris is required.")
	}

	var out []importResult

	for _, u := range uris {
		bucket, _, _ := splitGCSURI(u)

		objs, err := expandSource(store, u, wildcards)
		if err != nil {
			out = append(out, importResult{uri: u, err: err})

			continue
		}

		for _, o := range objs {
			uri := "gs://" + bucket + "/" + o

			data, rerr := store.ReadObject(bucket, o)
			if rerr == nil {
				rerr = add(data)
			}

			out = append(out, importResult{uri: uri, err: rerr})
		}
	}

	return out, nil
}

// importFailure is one source that could not be imported.
type importFailure struct {
	status *status.Status
	uri    string
}

// errStatus converts an error to a google.rpc.Status; errors that are not gRPC statuses read as NOT_FOUND, the
// failure to read the object.
func errStatus(err error) *status.Status {
	if st, ok := grpcstatus.FromError(err); ok {
		return st.Proto()
	}

	return grpcstatus.New(codes.NotFound, err.Error()).Proto()
}

// importSource is the gcs_source of an import request.
type importSource struct {
	uris      []string
	wildcards bool
}

// runImport imports every source object into the repository of the format. It returns the artifacts that were
// created and, through failure, one error entry per source that failed.
func runImport[A, E proto.Message](
	b *InMemoryBackend, parent string, format artifactregistrypb.Repository_Format, src importSource,
	add func(ref repoRef, data []byte) (A, error), failure func(importFailure) E,
) (repoRef, []A, []E, error) {
	ref, err := b.requireFormat(parent, format)
	if err != nil {
		return ref, nil, nil, err
	}

	var created []A

	results, err := b.importObjects(src.uris, src.wildcards, func(data []byte) error {
		art, aerr := add(ref, data)
		if aerr == nil {
			created = append(created, art)
		}

		return aerr
	})
	if err != nil {
		return ref, nil, nil, err
	}

	var failed []E

	for _, r := range results {
		if r.err != nil {
			failed = append(failed, failure(importFailure{uri: r.uri, status: errStatus(r.err)}))
		}
	}

	return ref, created, failed, nil
}

// ImportAptArtifacts imports .deb files from Cloud Storage.
func (b *InMemoryBackend) ImportAptArtifacts(
	_ context.Context, in *artifactregistrypb.ImportAptArtifactsRequest,
) (*longrunningpb.Operation, error) {
	gcs := in.GetGcsSource()

	ref, arts, errs, err := runImport(b, in.GetParent(), artifactregistrypb.Repository_APT,
		importSource{uris: gcs.GetUris(), wildcards: gcs.GetUseWildcards()}, b.addDeb,
		func(f importFailure) *artifactregistrypb.ImportAptArtifactsErrorInfo {
			return &artifactregistrypb.ImportAptArtifactsErrorInfo{
				Source: &artifactregistrypb.ImportAptArtifactsErrorInfo_GcsSource{
					GcsSource: &artifactregistrypb.ImportAptArtifactsGcsSource{Uris: []string{f.uri}},
				},
				Error: f.status,
			}
		})
	if err != nil {
		return nil, err
	}

	resp := &artifactregistrypb.ImportAptArtifactsResponse{AptArtifacts: arts, Errors: errs}

	return b.doneWith(ref.parent(), &artifactregistrypb.ImportAptArtifactsMetadata{}, resp)
}

// ImportYumArtifacts imports .rpm files from Cloud Storage.
func (b *InMemoryBackend) ImportYumArtifacts(
	_ context.Context, in *artifactregistrypb.ImportYumArtifactsRequest,
) (*longrunningpb.Operation, error) {
	gcs := in.GetGcsSource()

	ref, arts, errs, err := runImport(b, in.GetParent(), artifactregistrypb.Repository_YUM,
		importSource{uris: gcs.GetUris(), wildcards: gcs.GetUseWildcards()}, b.addRPM,
		func(f importFailure) *artifactregistrypb.ImportYumArtifactsErrorInfo {
			return &artifactregistrypb.ImportYumArtifactsErrorInfo{
				Source: &artifactregistrypb.ImportYumArtifactsErrorInfo_GcsSource{
					GcsSource: &artifactregistrypb.ImportYumArtifactsGcsSource{Uris: []string{f.uri}},
				},
				Error: f.status,
			}
		})
	if err != nil {
		return nil, err
	}

	resp := &artifactregistrypb.ImportYumArtifactsResponse{YumArtifacts: arts, Errors: errs}

	return b.doneWith(ref.parent(), &artifactregistrypb.ImportYumArtifactsMetadata{}, resp)
}

// doneWith finishes an operation with its own metadata type.
func (b *InMemoryBackend) doneWith(parent string, meta, resp proto.Message) (*longrunningpb.Operation, error) {
	op, err := gcplroDone(parent, meta, resp)
	if err != nil {
		return nil, err
	}

	return b.ops.Put(op), nil
}

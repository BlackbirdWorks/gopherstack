package gcpartifactregistry

import (
	"context"
	"errors"
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
		if ok, _ := path.Match(object, n); ok || globStar(object, n) {
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
	uri string
	err error
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

func rpcStatus(err error) *status.Status {
	st, _ := grpcstatus.FromError(err)

	return &status.Status{Code: int32(st.Code()), Message: st.Message()}
}

func importCode(err error) codes.Code {
	if st, ok := grpcstatus.FromError(err); ok && st.Code() != codes.Unknown {
		return st.Code()
	}

	if errors.Is(err, context.Canceled) {
		return codes.Canceled
	}

	return codes.NotFound
}

// ImportAptArtifacts imports .deb files from Cloud Storage.
func (b *InMemoryBackend) ImportAptArtifacts(
	_ context.Context, in *artifactregistrypb.ImportAptArtifactsRequest,
) (*longrunningpb.Operation, error) {
	ref, err := b.requireFormat(in.GetParent(), artifactregistrypb.Repository_APT)
	if err != nil {
		return nil, err
	}

	src := in.GetGcsSource()
	resp := &artifactregistrypb.ImportAptArtifactsResponse{}

	results, err := b.importObjects(src.GetUris(), src.GetUseWildcards(), func(data []byte) error {
		art, aerr := b.addDeb(ref, data)
		if aerr == nil {
			resp.AptArtifacts = append(resp.AptArtifacts, art)
		}

		return aerr
	})

	if err != nil {
		return nil, err
	}

	for _, r := range results {
		if r.err != nil {
			resp.Errors = append(resp.Errors, &artifactregistrypb.ImportAptArtifactsErrorInfo{
				Source: &artifactregistrypb.ImportAptArtifactsErrorInfo_GcsSource{
					GcsSource: &artifactregistrypb.ImportAptArtifactsGcsSource{Uris: []string{r.uri}},
				},
				Error: &status.Status{Code: int32(importCode(r.err)), Message: rpcStatus(r.err).GetMessage()},
			})
		}
	}

	return b.doneWith(ref.parent(), &artifactregistrypb.ImportAptArtifactsMetadata{}, resp)
}

// ImportYumArtifacts imports .rpm files from Cloud Storage.
func (b *InMemoryBackend) ImportYumArtifacts(
	_ context.Context, in *artifactregistrypb.ImportYumArtifactsRequest,
) (*longrunningpb.Operation, error) {
	ref, err := b.requireFormat(in.GetParent(), artifactregistrypb.Repository_YUM)
	if err != nil {
		return nil, err
	}

	src := in.GetGcsSource()
	resp := &artifactregistrypb.ImportYumArtifactsResponse{}

	results, err := b.importObjects(src.GetUris(), src.GetUseWildcards(), func(data []byte) error {
		art, aerr := b.addRPM(ref, data)
		if aerr == nil {
			resp.YumArtifacts = append(resp.YumArtifacts, art)
		}

		return aerr
	})
	if err != nil {
		return nil, err
	}

	for _, r := range results {
		if r.err != nil {
			resp.Errors = append(resp.Errors, &artifactregistrypb.ImportYumArtifactsErrorInfo{
				Source: &artifactregistrypb.ImportYumArtifactsErrorInfo_GcsSource{
					GcsSource: &artifactregistrypb.ImportYumArtifactsGcsSource{Uris: []string{r.uri}},
				},
				Error: &status.Status{Code: int32(importCode(r.err)), Message: rpcStatus(r.err).GetMessage()},
			})
		}
	}

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

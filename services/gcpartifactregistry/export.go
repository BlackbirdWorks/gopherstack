package gcpartifactregistry

import (
	"crypto/sha256"
	"sort"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
)

// exportFile is one stored file of the exported version.
type exportFile struct {
	name   string
	object string
}

// exportObjectName is the object name of a file below the destination directory: the file name part of ids
// shaped {package}:{version}:{filename}, else the id itself (Maven and Go keep their repository paths).
func exportObjectName(id string) string {
	if strings.Contains(id, "/") {
		return id
	}

	if i := strings.LastIndex(id, ":"); i >= 0 && i+1 < len(id) {
		return id[i+1:]
	}

	return id
}

func (b *InMemoryBackend) exportSourceVersion(in *artifactregistrypb.ExportArtifactRequest) (string, error) {
	switch {
	case in.GetSourceVersion() != "":
		return in.GetSourceVersion(), nil
	case in.GetSourceTag() != "":
		tag, err := b.GetTag(in.GetSourceTag())
		if err != nil {
			return "", err
		}

		return tag.GetVersion(), nil
	default:
		return "", errInvalid("One of source_version or source_tag is required.")
	}
}

// ExportArtifact copies the files of a version to a Cloud Storage path when the returned operation finishes.
func (b *InMemoryBackend) ExportArtifact(
	in *artifactregistrypb.ExportArtifactRequest,
) (*longrunningpb.Operation, error) {
	ref, err := parseRepo(in.GetRepository())
	if err != nil {
		return nil, err
	}

	bucket, dir, _ := strings.Cut(strings.Trim(in.GetGcsPath(), "/"), "/")
	if bucket == "" {
		return nil, errInvalid("gcs_path is required: a bucket name with an optional directory.")
	}

	format, err := b.formatOf(ref)
	if err != nil {
		return nil, err
	}

	if !storesFormat(format) {
		return nil, errPrecondition("Only repositories whose artifacts are stored as files can be exported; %s is not.",
			format)
	}

	versionName, err := b.exportSourceVersion(in)
	if err != nil {
		return nil, err
	}

	l, err := parseLeaf(versionName, "version")
	if err != nil {
		return nil, err
	}

	if l.pkg.repo != ref {
		return nil, errInvalid("%q does not belong to %q.", versionName, ref.full())
	}

	version, err := b.GetVersion(b.ctxOrBackground(), &artifactregistrypb.GetVersionRequest{Name: versionName})
	if err != nil {
		return nil, err
	}

	b.mu.RLock("ExportArtifact")
	store := b.objects
	b.mu.RUnlock()

	if store == nil {
		return nil, errPrecondition("The Cloud Storage emulator is not available to Artifact Registry.")
	}

	files, meta := b.exportPlan(ref, versionName, bucket, dir)

	return b.startOp(ref.parent(), meta, true, func() (proto.Message, error) {
		for _, f := range files {
			b.mu.RLock("ExportArtifact")
			src, ok := b.genFiles[f.name]
			b.mu.RUnlock()

			if !ok {
				return nil, errNotFound("File %q not found.", f.name)
			}

			if werr := store.WriteObject(bucket, f.object, src.Data, ""); werr != nil {
				return nil, errNotFound("Cannot write gs://%s/%s: %v", bucket, f.object, werr)
			}
		}

		return &artifactregistrypb.ExportArtifactResponse{ExportedVersion: version}, nil
	})
}

// exportPlan lists the version's files with their destination objects and the operation metadata describing them.
func (b *InMemoryBackend) exportPlan(
	ref repoRef, versionName, bucket, dir string,
) ([]exportFile, *artifactregistrypb.ExportArtifactMetadata) {
	b.mu.RLock("exportPlan")
	defer b.mu.RUnlock()

	meta := &artifactregistrypb.ExportArtifactMetadata{}

	var files []exportFile

	for name, f := range b.genFiles {
		if f.Owner != versionName {
			continue
		}

		id, _ := unescapeImage(strings.TrimPrefix(name, ref.full()+"/files/"))
		object := exportObjectName(id)

		if dir != "" {
			object = dir + "/" + object
		}

		files = append(files, exportFile{name: name, object: object})
	}

	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })

	for _, f := range files {
		sum := sha256.Sum256(b.genFiles[f.name].Data)

		meta.ExportedFiles = append(meta.ExportedFiles, &artifactregistrypb.ExportArtifactMetadata_ExportedFile{
			Name: f.name, Hashes: []*artifactregistrypb.Hash{{Type: artifactregistrypb.Hash_SHA256, Value: sum[:]}},
			Destination: &artifactregistrypb.ExportArtifactMetadata_ExportedFile_GcsObjectPath{
				GcsObjectPath: bucket + "/" + f.object,
			},
		})
	}

	return files, meta
}

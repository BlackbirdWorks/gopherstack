package gcpartifactregistry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"sort"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxIndexDepth = 4

// exportFile is one stored file of the exported version; read returns its bytes when the operation finishes.
type exportFile struct {
	read   func() ([]byte, error)
	name   string
	object string
	sum    [sha256.Size]byte
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

	if !storesFormat(format) && format != artifactregistrypb.Repository_DOCKER {
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

	files, err := b.exportFiles(format, ref, l, dir)
	if err != nil {
		return nil, err
	}

	meta := exportMetadata(files, bucket)

	return b.startOp(ref.parent(), meta, true, func() (proto.Message, error) {
		for _, f := range files {
			data, rerr := f.read()
			if rerr != nil {
				return nil, rerr
			}

			if werr := store.WriteObject(bucket, f.object, data, ""); werr != nil {
				return nil, errNotFound("Cannot write gs://%s/%s: %v", bucket, f.object, werr)
			}
		}

		return &artifactregistrypb.ExportArtifactResponse{ExportedVersion: version}, nil
	})
}

func (b *InMemoryBackend) exportFiles(
	format artifactregistrypb.Repository_Format, ref repoRef, l leafRef, dir string,
) ([]exportFile, error) {
	var files []exportFile

	if format == artifactregistrypb.Repository_DOCKER {
		var err error

		if files, err = b.dockerExportFiles(ref, l); err != nil {
			return nil, err
		}
	} else {
		files = b.genericExportFiles(ref, l.pkg.full()+"/versions/"+l.leaf)
	}

	for i := range files {
		if dir != "" {
			files[i].object = dir + "/" + files[i].object
		}
	}

	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })

	return files, nil
}

func (b *InMemoryBackend) genericExportFiles(ref repoRef, versionName string) []exportFile {
	b.mu.RLock("genericExportFiles")
	defer b.mu.RUnlock()

	var files []exportFile

	for name, f := range b.genFiles {
		if f.Owner != versionName {
			continue
		}

		id, _ := unescapeImage(strings.TrimPrefix(name, ref.full()+"/files/"))
		files = append(files, exportFile{
			name: name, object: exportObjectName(id), sum: sha256.Sum256(f.Data),
			read: func() ([]byte, error) {
				b.mu.RLock("ExportArtifact")
				defer b.mu.RUnlock()

				src, ok := b.genFiles[name]
				if !ok {
					return nil, errNotFound("File %q not found.", name)
				}

				return src.Data, nil
			},
		})
	}

	return files
}

type exportIndex struct {
	Manifests []blobDesc `json:"manifests"`
}

// dockerExporter collects the files of a Docker version.
type dockerExporter struct {
	b     *InMemoryBackend
	seen  map[string]bool
	ref   repoRef
	repo  string
	files []exportFile
}

func (x *dockerExporter) add(digest string, read func() ([]byte, error)) {
	raw, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	if x.seen[digest] || err != nil || len(raw) != sha256.Size || !strings.HasPrefix(digest, "sha256:") {
		return
	}

	x.seen[digest] = true

	f := exportFile{name: x.ref.fileName(digest), object: digest, read: read}
	copy(f.sum[:], raw)
	x.files = append(x.files, f)
}

func (x *dockerExporter) addBlob(ctx context.Context, digest string) {
	x.add(digest, func() ([]byte, error) {
		data, err := x.b.client().GetBlob(ctx, x.repo, digest, maxDockerBody)
		if err != nil {
			return nil, errNotFound("File %q not found.", digest)
		}

		return data, nil
	})
}

func (x *dockerExporter) walk(ctx context.Context, digest string, depth int) error {
	m, err := x.b.client().GetManifest(ctx, x.repo, digest)
	if err != nil {
		return errNotFound("Version %q not found.", digest)
	}

	x.add(digest, func() ([]byte, error) { return m.Body, nil })

	var doc manifestBlobs
	if json.Unmarshal(m.Body, &doc, jsonx.V1()) == nil {
		for _, blob := range append([]blobDesc{doc.Config}, doc.Layers...) {
			x.addBlob(ctx, blob.Digest)
		}
	}

	var idx exportIndex
	if depth < maxIndexDepth && json.Unmarshal(m.Body, &idx, jsonx.V1()) == nil {
		for _, child := range idx.Manifests {
			if err = x.walk(ctx, child.Digest, depth+1); err != nil {
				return err
			}
		}
	}

	return nil
}

// dockerExportFiles lists the manifest of a version, the config and layer blobs it references and, for an
// index, its child manifests with theirs; each is exported under its digest, as ListFiles names them.
func (b *InMemoryBackend) dockerExportFiles(ref repoRef, l leafRef) ([]exportFile, error) {
	x := &dockerExporter{
		b: b, seen: map[string]bool{}, ref: ref, repo: ref.dockerKey() + "/" + l.pkg.image,
	}

	if err := x.walk(b.ctxOrBackground(), l.leaf, 0); err != nil {
		return nil, err
	}

	return x.files, nil
}

func exportMetadata(files []exportFile, bucket string) *artifactregistrypb.ExportArtifactMetadata {
	meta := &artifactregistrypb.ExportArtifactMetadata{}

	for _, f := range files {
		meta.ExportedFiles = append(meta.ExportedFiles, &artifactregistrypb.ExportArtifactMetadata_ExportedFile{
			Name: f.name, Hashes: []*artifactregistrypb.Hash{{Type: artifactregistrypb.Hash_SHA256, Value: f.sum[:]}},
			Destination: &artifactregistrypb.ExportArtifactMetadata_ExportedFile_GcsObjectPath{
				GcsObjectPath: bucket + "/" + f.object,
			},
		})
	}

	return meta
}

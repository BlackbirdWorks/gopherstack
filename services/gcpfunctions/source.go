package gcpfunctions

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxSourceBytes = 100 << 20
	maxUnpacked    = 512 << 20
	dirMode        = 0o755
	fileMode       = 0o644
	uploadPrefix   = "/_gcf/upload/"
	downloadPrefix = "/_gcf/download/"
)

var (
	errUnsafePath = errors.New("archive entry escapes the source directory")
	errTooLarge   = errors.New("archive is too large when unpacked")
)

func uploadBucket(n resName) string {
	return "gcf-v2-uploads-" + shortHash(n.project, n.location) + "-" + n.location
}

// resolved is a source archive together with the storage object it came from.
type resolved struct {
	src  *functionspb.StorageSource
	data []byte
}

// resolveSource loads the archive named by src from the upload store or the object reader.
func (b *InMemoryBackend) resolveSource(n resName, src *functionspb.Source) (*resolved, error) {
	ss := src.GetStorageSource()
	if ss == nil {
		return nil, errInvalid("build_config.source.storage_source is required; repo_source and git_uri are not " +
			"supported by the emulator.")
	}

	if ss.GetBucket() == "" || ss.GetObject() == "" {
		return nil, errInvalid("storage_source.bucket and storage_source.object are required.")
	}

	b.mu.RLock("resolveSource")
	defer b.mu.RUnlock()

	if ss.GetBucket() == uploadBucket(n) {
		up := b.uploads[ss.GetBucket()+"/"+ss.GetObject()]
		if up == nil || up.data == nil {
			return nil, errInvalid("Could not find source %s/%s; upload it to the URL from GenerateUploadUrl first.",
				ss.GetBucket(), ss.GetObject())
		}

		return &resolved{
			data: up.data,
			src:  &functionspb.StorageSource{Bucket: ss.GetBucket(), Object: ss.GetObject(), Generation: up.generation},
		}, nil
	}

	if b.objects == nil {
		return nil, errPrecondition("Source bucket %q is not served by the emulator.", ss.GetBucket())
	}

	data, err := b.objects.ReadObject(ss.GetBucket(), ss.GetObject())
	if err != nil {
		return nil, errInvalid("Could not read source gs://%s/%s: %v", ss.GetBucket(), ss.GetObject(), err)
	}

	return &resolved{data: data, src: &functionspb.StorageSource{
		Bucket: ss.GetBucket(), Object: ss.GetObject(), Generation: ss.GetGeneration(),
	}}, nil
}

// GenerateUploadURL issues a one-shot upload slot in the location's source bucket.
func (b *InMemoryBackend) GenerateUploadURL(in *functionspb.GenerateUploadUrlRequest) (
	*functionspb.GenerateUploadUrlResponse, error,
) {
	n, err := parseParent(in.GetParent())
	if err != nil || n.location == allLocs {
		return nil, errInvalid("Invalid parent %q.", in.GetParent())
	}

	if env := in.GetEnvironment(); env == functionspb.Environment_GEN_1 {
		return nil, errInvalid("Only the GEN_2 environment is supported.")
	}

	base, err := b.ingressBase()
	if err != nil {
		return nil, err
	}

	bucket, object := uploadBucket(n), uuid.NewString()+".zip"

	b.mu.Lock("GenerateUploadURL")
	b.uploads[bucket+"/"+object] = &uploadRec{generation: b.nextGenLocked()}
	b.mu.Unlock()

	return &functionspb.GenerateUploadUrlResponse{
		UploadUrl:     base + uploadPrefix + bucket + "/" + object,
		StorageSource: &functionspb.StorageSource{Bucket: bucket, Object: object},
	}, nil
}

// GenerateDownloadURL returns a URL serving the deployed source archive of a function.
func (b *InMemoryBackend) GenerateDownloadURL(in *functionspb.GenerateDownloadUrlRequest) (
	*functionspb.GenerateDownloadUrlResponse, error,
) {
	n, err := parseName(in.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GenerateDownloadURL")
	rec, ok := b.functions[n.full()]
	hasZip := ok && len(rec.zip) > 0
	b.mu.RUnlock()

	if !ok {
		return nil, errNotFound(n)
	}

	if !hasZip {
		return nil, errPrecondition("Function %q has no deployed source.", n.full())
	}

	base, err := b.ingressBase()
	if err != nil {
		return nil, err
	}

	return &functionspb.GenerateDownloadUrlResponse{DownloadUrl: base + downloadPrefix + n.full() + ".zip"}, nil
}

// storeUpload records the archive PUT to an issued upload slot.
func (b *InMemoryBackend) storeUpload(key string, data []byte) bool {
	b.mu.Lock("storeUpload")
	defer b.mu.Unlock()

	up := b.uploads[key]
	if up == nil {
		return false
	}

	up.data = data
	up.generation = b.nextGenLocked()

	return true
}

func (b *InMemoryBackend) functionZip(name string) []byte {
	b.mu.RLock("functionZip")
	defer b.mu.RUnlock()

	if rec := b.functions[name]; rec != nil {
		return rec.zip
	}

	return nil
}

// checkArchive verifies data is a zip carrying the files runtime needs.
func checkArchive(data []byte, rt *runtimeSpec) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "Source is not a valid zip archive: %v", err)
	}

	names := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		names = append(names, path.Clean(f.Name))
	}

	if msg := rt.missing(names); msg != "" {
		return status.Errorf(codes.FailedPrecondition, "Build failed: %s", msg)
	}

	return nil
}

// extractArchive unpacks data into dir, refusing entries that escape it and skipping links.
func extractArchive(data []byte, dir string) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}

	var total uint64

	for _, f := range zr.File {
		total += f.UncompressedSize64
		if total > maxUnpacked {
			return errTooLarge
		}

		if err = extractEntry(f, dir); err != nil {
			return err
		}
	}

	return nil
}

func extractEntry(f *zip.File, dir string) error {
	target := filepath.Join(dir, filepath.FromSlash(f.Name))
	if target != dir && !strings.HasPrefix(target, dir+string(os.PathSeparator)) {
		return fmt.Errorf("%w: %s", errUnsafePath, f.Name)
	}

	if f.FileInfo().IsDir() {
		return os.MkdirAll(target, dirMode)
	}

	if !f.Mode().IsRegular() {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(target), dirMode); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("read %s: %w", f.Name, err)
	}
	defer rc.Close()

	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fileMode)
	if err != nil {
		return fmt.Errorf("create %s: %w", f.Name, err)
	}

	_, err = io.Copy(out, io.LimitReader(rc, maxUnpacked))
	if cerr := out.Close(); err == nil {
		err = cerr
	}

	return err
}

func removeDir(dir string) {
	if dir == "" {
		return
	}

	if err := os.RemoveAll(dir); err != nil {
		logger.Load(context.Background()).Warn("gcpfunctions: remove source dir failed", "dir", dir, "error", err)
	}
}

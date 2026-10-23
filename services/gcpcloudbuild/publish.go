package gcpcloudbuild

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxArtifactBytes = 256 << 20
	maxArtifactFiles = 10_000
	sha256Type       = 1
	arHostSuffix     = "docker.pkg.dev"
)

var (
	errBadLocation  = errors.New("invalid Cloud Storage location")
	errNoPusher     = errors.New("the container runtime cannot push images")
	errNoRegistry   = errors.New("the Artifact Registry emulator is not available to this build")
	errRegistryHost = errors.New(
		"images must be pushed to <location>-docker.pkg.dev/<project>/<repository>/<image>",
	)
	errUnsafeArtifact = errors.New("artifact path must be relative to the workspace")
	errTooManyFiles   = errors.New("too many artifact files")
	errBigArtifact    = errors.New("artifact file is too large")
)

// publish pushes the built images and uploads the artifacts of a successful build.
func (r *runner) publish(ctx context.Context) *stepFailure {
	res := &cloudbuildpb.Results{}

	var failure *stepFailure

	if len(r.build.GetImages()) > 0 {
		r.log.system("PUSH")

		failure = r.pushImages(ctx, res)
	}

	if failure == nil && r.build.GetArtifacts().GetObjects() != nil {
		r.log.system("ARTIFACTS")

		failure = r.uploadArtifacts(ctx, res)
	}

	r.b.update(r.ref, func(pb *cloudbuildpb.Build) { pb.Results = res })

	return failure
}

func (r *runner) pushImages(ctx context.Context, res *cloudbuildpb.Results) *stepFailure {
	for _, img := range r.build.GetImages() {
		start := r.b.ts()

		digest, err := r.pushImage(ctx, img)
		if err != nil {
			r.log.system("ERROR: failed to push " + img + ": " + err.Error())

			ft := cloudbuildpb.Build_FailureInfo_PUSH_FAILED
			if strings.Contains(err.Error(), "No such image") {
				ft = cloudbuildpb.Build_FailureInfo_PUSH_IMAGE_NOT_FOUND
			}

			return &stepFailure{
				status: cloudbuildpb.Build_FAILURE,
				detail: fmt.Sprintf("failed to push image %q: %v", img, err),
				info:   &cloudbuildpb.Build_FailureInfo{Type: ft, Detail: err.Error()},
			}
		}

		r.log.system("Pushed " + img)

		res.Images = append(res.Images, &cloudbuildpb.BuiltImage{
			Name: img, Digest: digest, PushTiming: &cloudbuildpb.TimeSpan{StartTime: start, EndTime: r.b.ts()},
		})
	}

	return nil
}

func (r *runner) pushImage(ctx context.Context, image string) (string, error) {
	if r.svc.RegistryAddr == "" {
		return "", errNoRegistry
	}

	target, err := registryTarget(r.svc.RegistryAddr, image)
	if err != nil {
		return "", err
	}

	p, ok := r.rt.(container.ImagePusher)
	if !ok {
		return "", errNoPusher
	}

	digest, err := p.TagAndPush(ctx, withTag(image), target)
	if err != nil {
		return "", err //nolint:wrapcheck // the runtime error already names the image
	}

	return digest, nil
}

// withTag adds :latest to an image reference that has no tag or digest.
func withTag(image string) string {
	last := image[strings.LastIndex(image, "/")+1:]
	if strings.ContainsAny(last, ":@") {
		return image
	}

	return image + ":latest"
}

// registryTarget maps <location>-docker.pkg.dev/<project>/<repo>/<image> onto the emulator's address.
func registryTarget(addr, image string) (string, error) {
	host, rest, ok := strings.Cut(image, "/")
	if !ok || (host != arHostSuffix && !strings.HasSuffix(host, "-"+arHostSuffix)) ||
		strings.Count(rest, "/") < 2 {
		return "", fmt.Errorf("%w: got %q", errRegistryHost, image)
	}

	return addr + "/" + withTag(rest), nil
}

type manifestLine struct {
	Location string             `json:"location"`
	FileHash []manifestFileHash `json:"file_hash"`
}

type manifestFileHash struct {
	FileHash []manifestHash `json:"file_hash"`
}

type manifestHash struct {
	Value string `json:"value"`
	Type  int    `json:"type"`
}

func (r *runner) uploadArtifacts(_ context.Context, res *cloudbuildpb.Results) *stepFailure {
	start := r.b.ts()

	n, manifest, err := r.copyArtifacts(r.build.GetArtifacts().GetObjects())
	if err != nil {
		r.log.system("ERROR: failed to upload artifacts: " + err.Error())

		return &stepFailure{
			status: cloudbuildpb.Build_FAILURE,
			detail: "failed to upload artifacts: " + err.Error(),
		}
	}

	res.NumArtifacts, res.ArtifactManifest = int64(n), manifest
	res.ArtifactTiming = &cloudbuildpb.TimeSpan{StartTime: start, EndTime: r.b.ts()}

	return nil
}

// copyArtifacts uploads the files matching objects.paths and writes a manifest next to them.
func (r *runner) copyArtifacts(objs *cloudbuildpb.Artifacts_ArtifactObjects) (int, string, error) {
	if r.svc.Store == nil {
		return 0, "", errNoStore
	}

	bucket, prefix, err := splitGS(objs.GetLocation())
	if err != nil {
		return 0, "", err
	}

	files, err := r.matchArtifacts(objs.GetPaths())
	if err != nil {
		return 0, "", err
	}

	lines := make([]manifestLine, 0, len(files))

	for _, rel := range files {
		data, rErr := readBounded(filepath.Join(r.work, rel))
		if rErr != nil {
			return 0, "", rErr
		}

		object := prefix + filepath.ToSlash(rel)

		if wErr := r.svc.Store.WriteObject(bucket, object, data, ""); wErr != nil {
			return 0, "", fmt.Errorf("write gs://%s/%s: %w", bucket, object, wErr)
		}

		sum := sha256.Sum256(data)
		lines = append(lines, manifestLine{
			Location: "gs://" + bucket + "/" + object,
			FileHash: []manifestFileHash{
				{
					FileHash: []manifestHash{
						{Type: sha256Type, Value: base64.StdEncoding.EncodeToString(sum[:])},
					},
				},
			},
		})
	}

	manifest, err := r.writeManifest(bucket, prefix, lines)

	return len(files), manifest, err
}

func (r *runner) writeManifest(bucket, prefix string, lines []manifestLine) (string, error) {
	var sb strings.Builder

	for _, l := range lines {
		raw, err := json.Marshal(l)
		if err != nil {
			return "", fmt.Errorf("encode manifest: %w", err)
		}

		sb.Write(raw)
		sb.WriteByte('\n')
	}

	object := prefix + "artifacts-" + r.ref.id + ".json"

	if err := r.svc.Store.WriteObject(bucket, object, []byte(sb.String()), "application/json"); err != nil {
		return "", fmt.Errorf("write manifest gs://%s/%s: %w", bucket, object, err)
	}

	return "gs://" + bucket + "/" + object, nil
}

func splitGS(location string) (string, string, error) {
	u, err := url.Parse(location)
	if err != nil || u.Scheme != "gs" || u.Host == "" {
		return "", "", fmt.Errorf("%w: %q", errBadLocation, location)
	}

	prefix := strings.TrimPrefix(u.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return u.Host, prefix, nil
}

// matchArtifacts expands the glob patterns (relative to the workspace) into regular files, sorted.
func (r *runner) matchArtifacts(patterns []string) ([]string, error) {
	seen := map[string]bool{}

	for _, p := range patterns {
		clean := path.Clean(strings.TrimPrefix(p, workspace+"/"))
		if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
			return nil, fmt.Errorf("%w: %q", errUnsafeArtifact, p)
		}

		matches, err := filepath.Glob(filepath.Join(r.work, filepath.FromSlash(clean)))
		if err != nil {
			return nil, fmt.Errorf("invalid artifact pattern %q: %w", p, err)
		}

		for _, m := range matches {
			if rel, ok := r.regularFile(m); ok {
				seen[rel] = true
			}
		}

		if len(seen) > maxArtifactFiles {
			return nil, errTooManyFiles
		}
	}

	out := make([]string, 0, len(seen))
	for rel := range seen {
		out = append(out, rel)
	}

	sort.Strings(out)

	return out, nil
}

// regularFile returns the workspace-relative path of m when it is a regular file that stays inside the workspace.
func (r *runner) regularFile(m string) (string, bool) {
	resolved, err := filepath.EvalSymlinks(m)
	if err != nil {
		return "", false
	}

	root, err := filepath.EvalSymlinks(r.work)
	if err != nil {
		return "", false
	}

	rel, err := filepath.Rel(root, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}

	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() {
		return "", false
	}

	return rel, true
}

func readBounded(file string) ([]byte, error) {
	info, err := os.Stat(file)
	if err != nil {
		return nil, fmt.Errorf("stat artifact: %w", err)
	}

	if info.Size() > maxArtifactBytes {
		return nil, fmt.Errorf("%w: %s", errBigArtifact, filepath.Base(file))
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read artifact: %w", err)
	}

	return data, nil
}

// uploadLog stores the build log as log-<id>.txt in the logs bucket.
func (r *runner) uploadLog() {
	l := r.log
	if !l.toBucket || r.svc.Store == nil {
		return
	}

	bucket := strings.TrimPrefix(r.build.GetLogsBucket(), "gs://")
	bucket, _, _ = strings.Cut(bucket, "/")

	if bucket == "" {
		return
	}

	ctx := r.b.ctx

	if bucket == strings.TrimPrefix(r.b.logsBucketDefault(r.ref.project), "gs://") {
		if err := r.svc.Store.EnsureBucket(r.ref.project, bucket); err != nil {
			logger.Load(ctx).
				WarnContext(ctx, "gcpcloudbuild: create default logs bucket failed", "bucket", bucket, "error", err)
		}
	}

	if err := r.svc.Store.WriteObject(bucket, "log-"+r.ref.id+".txt", l.text(), "text/plain"); err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "gcpcloudbuild: write build log failed", "bucket", bucket, "error", err)
	}
}

package gcpartifactregistry

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"net/http"
	"path"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	kfpMaxTags     = 8
	kfpSpecFile    = "pipeline_spec.yaml"
	kfpDigestPref  = "sha256:"
	kfpFormMemory  = 4 << 20
	kfpHeaderDescr = "Description"
)

var (
	kfpPackageRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{3,127}$`)
	kfpTagRe     = regexp.MustCompile(`^[a-zA-Z0-9\-._~:@+]{1,128}$`)

	errKFPSpec = errors.New("content is not a pipeline spec with a pipeline name")
)

// serveKFP serves the KFP registry protocol of the Artifact Registry docs: POST to the repository root uploads a
// pipeline template (multipart "content" and a comma-separated "tags") and answers {package}/{version}; GET of
// {package}/{version-or-tag} returns the template.
func (b *InMemoryBackend) serveKFP(w http.ResponseWriter, r *http.Request, rc repoCtx, p string) {
	switch {
	case r.Method == http.MethodPost && p == "":
		b.uploadKFP(w, r, rc)
	case isRead(r) && p != "":
		b.downloadKFP(w, r, rc, p)
	case isRead(r) || r.Method == http.MethodPost:
		writePlain(w, http.StatusNotFound, "Not found: "+p)
	default:
		writePlain(w, http.StatusMethodNotAllowed, "Method not allowed.")
	}
}

// kfpPipelineName reads pipeline_info.name from a pipeline spec; a .zip package holds the spec as its first
// .yaml or .yml file.
func kfpPipelineName(spec []byte) (string, error) {
	var doc struct {
		Info struct {
			Name string `yaml:"name"`
		} `yaml:"pipelineInfo"`
		InfoSnake struct {
			Name string `yaml:"name"`
		} `yaml:"pipeline_info"`
	}

	if err := yaml.Unmarshal(spec, &doc); err != nil {
		return "", errKFPSpec
	}

	name := doc.Info.Name
	if name == "" {
		name = doc.InfoSnake.Name
	}

	if name == "" {
		return "", errKFPSpec
	}

	return name, nil
}

func kfpSpecFromZip(data []byte) ([]byte, bool) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, false
	}

	for _, f := range zr.File {
		if ext := path.Ext(f.Name); ext != ".yaml" && ext != ".yml" {
			continue
		}

		rc, openErr := f.Open()
		if openErr != nil {
			return nil, false
		}

		out, readErr := io.ReadAll(io.LimitReader(rc, maxGenericBytes))
		_ = rc.Close()

		return out, readErr == nil
	}

	return nil, false
}

func parseKFPTags(raw string) ([]string, bool) {
	if raw == "" {
		return nil, true
	}

	tags := strings.Split(raw, ",")
	if len(tags) > kfpMaxTags {
		return nil, false
	}

	for i, t := range tags {
		tags[i] = strings.TrimSpace(t)
		if !kfpTagRe.MatchString(tags[i]) {
			return nil, false
		}
	}

	return tags, true
}

func (b *InMemoryBackend) uploadKFP(w http.ResponseWriter, r *http.Request, rc repoCtx) {
	r.Body = http.MaxBytesReader(w, r.Body, maxGenericBytes+1<<20)

	if err := r.ParseMultipartForm(
		kfpFormMemory,
	); err != nil { //nolint:gosec // r.Body is wrapped in MaxBytesReader above
		writePlain(w, http.StatusBadRequest, "Upload must be multipart/form-data with a content file.")

		return
	}

	file, _, err := r.FormFile("content")
	if err != nil {
		writePlain(w, http.StatusBadRequest, "The multipart field \"content\" is required.")

		return
	}

	defer func() { _ = file.Close() }()

	content, err := io.ReadAll(io.LimitReader(file, maxGenericBytes+1))
	if err != nil || len(content) > maxGenericBytes {
		writePlain(w, http.StatusRequestEntityTooLarge, "The pipeline template is too large.")

		return
	}

	tags, ok := parseKFPTags(r.FormValue("tags"))
	if !ok {
		writePlain(w, http.StatusBadRequest,
			"tags must be at most 8 comma-separated values matching ^[a-zA-Z0-9\\-._~:@+]{1,128}$.")

		return
	}

	spec := content
	if z, isZip := kfpSpecFromZip(content); isZip {
		spec = z
	}

	name, err := kfpPipelineName(spec)
	if err != nil || !kfpPackageRe.MatchString(name) {
		writePlain(w, http.StatusBadRequest,
			"The pipeline name (pipeline_info.name) must match ^[a-z0-9][a-z0-9-]{3,127}$.")

		return
	}

	version := kfpDigestPref + sha256Hex(content)
	b.storeKFP(rc.ref, name, version, r.Header.Get(kfpHeaderDescr), content, tags)

	writePlain(w, http.StatusOK, name+"/"+version)
}

// storeKFP records the template under its digest and moves the given tags onto it; uploading identical content
// again only refreshes the description and tags.
func (b *InMemoryBackend) storeKFP(ref repoRef, name, version, description string, content []byte, tags []string) {
	_ = b.putPackageFile(ref, pkgFile{
		pkgID: name, display: name, version: version, fileID: genFileID(name, version, kfpSpecFile), data: content,
	}, true)

	pkg := ref.packageName(name)

	b.mu.Lock("storeKFP")
	defer b.mu.Unlock()

	if m := b.genVers[pkg+"/versions/"+version]; m != nil && description != "" {
		m.Description = description
	}

	for _, t := range tags {
		if b.pkgTags[pkg] == nil {
			b.pkgTags[pkg] = map[string]string{}
		}

		b.pkgTags[pkg][t] = version
	}
}

func (b *InMemoryBackend) downloadKFP(w http.ResponseWriter, r *http.Request, rc repoCtx, p string) {
	pkgID, target, ok := strings.Cut(p, "/")
	if !ok || target == "" || strings.Contains(target, "/") {
		writePlain(w, http.StatusNotFound, "Not found: "+p)

		return
	}

	pkg := rc.ref.packageName(pkgID)

	b.mu.RLock("downloadKFP")

	version := target
	if v, tagged := b.pkgTags[pkg][target]; tagged {
		version = v
	}

	f, found := b.genFiles[rc.ref.fileName(genFileID(pkgID, version, kfpSpecFile))]
	b.mu.RUnlock()

	if !found {
		writePlain(w, http.StatusNotFound, "Not found: "+p)

		return
	}

	if b.packageDenied(r.Context(), rc, pkgID) {
		writePlain(w, http.StatusForbidden, "A download rule denies access to this package.")

		return
	}

	writeBytes(w, "application/x-yaml", f.Data)
}

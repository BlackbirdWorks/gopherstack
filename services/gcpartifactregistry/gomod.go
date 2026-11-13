package gcpartifactregistry

import (
	"archive/zip"
	"bytes"
	"encoding/json/v2"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxGoModFile = 16 << 20

var zipRootRe = regexp.MustCompile(`^(.+?)@(v[^/]+)/`)

func moduleFileIDs(path, version string) (string, string, string) {
	base := path + "/@v/" + version

	return base + ".zip", base + ".mod", base + ".info"
}

// moduleZipID reads the module path and version from the root directory of a module zip.
func moduleZipID(zr *zip.Reader) (string, string, bool) {
	if len(zr.File) == 0 {
		return "", "", false
	}

	m := zipRootRe.FindStringSubmatch(zr.File[0].Name)
	if m == nil {
		return "", "", false
	}

	return m[1], m[2], true
}

func goModFromZip(zr *zip.Reader, path, version string) []byte {
	for _, f := range zr.File {
		if f.Name != path+"@"+version+"/go.mod" {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			break
		}

		defer func() { _ = rc.Close() }()

		if data, rerr := io.ReadAll(io.LimitReader(rc, maxGoModFile)); rerr == nil {
			return data
		}
	}

	return []byte("module " + path + "\n")
}

type goInfo struct {
	Time    time.Time `json:"Time"`
	Version string    `json:"Version"`
}

func (b *InMemoryBackend) addGoModule(ref repoRef, data []byte) (*artifactregistrypb.GoModule, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, errInvalid("The file is not a valid module zip: %v", err)
	}

	path, version, ok := moduleZipID(zr)
	if !ok {
		return nil, errInvalid("The zip must contain files under <module>@<version>/.")
	}

	if err = module.CheckPath(path); err != nil {
		return nil, errInvalid("Invalid module path %q: %v", path, err)
	}

	if !semver.IsValid(version) || semver.Canonical(version) != version {
		return nil, errInvalid("Version %q is not a canonical semantic version.", version)
	}

	now := b.now().UTC()
	info, _ := json.Marshal(goInfo{Version: version, Time: now}, jsonx.V1())
	zipID, modID, infoID := moduleFileIDs(path, version)

	for _, f := range []pkgFile{
		{fileID: zipID, data: data},
		{fileID: modID, data: goModFromZip(zr, path, version)},
		{fileID: infoID, data: info},
	} {
		f.pkgID, f.display, f.version = path, path, version
		if err = b.putPackageFile(ref, f, false); err != nil {
			return nil, err
		}
	}

	return &artifactregistrypb.GoModule{
		Name:       ref.full() + "/goModules/" + escapeImage(path) + ":" + version,
		Version:    version,
		CreateTime: timestamppb.New(now), UpdateTime: timestamppb.New(now),
	}, nil
}

func storeGoModule(b *InMemoryBackend, parent string, _, data []byte) (*longrunningpb.Operation, error) {
	ref, err := b.requireFormat(parent, artifactregistrypb.Repository_GO)
	if err != nil {
		return nil, err
	}

	mod, err := b.addGoModule(ref, data)
	if err != nil {
		return nil, err
	}

	return b.doneAny(ref.parent(), "UploadGoModuleResponse", mod)
}

func (b *InMemoryBackend) goVersions(rc repoCtx, path string) []string {
	var out []string

	for _, v := range b.versionsOfPackage(rc.ref, path) {
		if len(v.files) > 0 {
			out = append(out, v.version)
		}
	}

	sort.Slice(out, func(i, j int) bool { return semver.Compare(out[i], out[j]) < 0 })

	return out
}

func splitGoPath(p string) (string, string, bool) {
	if mod, ok := strings.CutSuffix(p, "/@latest"); ok {
		return mod, "@latest", true
	}

	mod, tail, ok := strings.Cut(p, "/@v/")

	return mod, tail, ok && mod != "" && tail != ""
}

func (b *InMemoryBackend) serveGo(w http.ResponseWriter, r *http.Request, rc repoCtx, p string) {
	if !isRead(r) {
		writePlain(w, http.StatusMethodNotAllowed, "Go repositories are populated with the upload API.")

		return
	}

	escMod, tail, ok := splitGoPath(p)
	if !ok {
		writePlain(w, http.StatusNotFound, "not found: "+p)

		return
	}

	mod, err := module.UnescapePath(escMod)
	if err != nil {
		writePlain(w, http.StatusBadRequest, "invalid module path: "+err.Error())

		return
	}

	versions := b.goVersions(rc, mod)
	if len(versions) == 0 {
		writePlain(w, http.StatusNotFound, "not found: module "+mod+": no matching versions")

		return
	}

	switch tail {
	case "list":
		var rel []string

		for _, v := range versions {
			if module.IsPseudoVersion(v) {
				continue
			}

			rel = append(rel, v)
		}

		writeBytes(w, "text/plain; charset=utf-8", []byte(strings.Join(rel, "\n")+"\n"))
	case "@latest":
		b.serveGoFile(w, r, rc, mod, versions[len(versions)-1], ".info")
	default:
		b.serveGoVersion(w, r, rc, mod, tail)
	}
}

func (b *InMemoryBackend) serveGoVersion(w http.ResponseWriter, r *http.Request, rc repoCtx, mod, tail string) {
	dot := strings.LastIndex(tail, ".")
	if dot < 0 {
		writePlain(w, http.StatusNotFound, "not found: "+tail)

		return
	}

	version, err := module.UnescapeVersion(tail[:dot])
	if err != nil {
		writePlain(w, http.StatusBadRequest, "invalid version: "+err.Error())

		return
	}

	b.serveGoFile(w, r, rc, mod, version, tail[dot:])
}

func (b *InMemoryBackend) serveGoFile(w http.ResponseWriter, r *http.Request, rc repoCtx, mod, version, ext string) {
	zipID, modID, infoID := moduleFileIDs(mod, version)

	var id, ctype string

	switch ext {
	case ".zip":
		id, ctype = zipID, "application/zip"
	case ".mod":
		id, ctype = modID, "text/plain; charset=utf-8"
	case ".info":
		id, ctype = infoID, "application/json"
	default:
		writePlain(w, http.StatusNotFound, "not found: "+version+ext)

		return
	}

	f, ok := b.packageFile(rc.ref, id)
	if !ok {
		writePlain(w, http.StatusNotFound, "not found: "+mod+"@"+version+": unknown revision")

		return
	}

	if b.packageDenied(r.Context(), rc, mod) {
		writePlain(w, http.StatusForbidden, "A download rule denies access to this package.")

		return
	}

	writeBytes(w, ctype, f.Data)
}

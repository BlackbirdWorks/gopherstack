package gcpartifactregistry

import (
	"encoding/json/v2"
	"errors"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

var (
	errPublishInput = errors.New("invalid package")
)

// publishRepo finds a repository of the format for an in-process publisher such as Cloud Build.
func (b *InMemoryBackend) publishRepo(
	project, id string,
	format artifactregistrypb.Repository_Format,
) (repoCtx, error) {
	rc, ok := b.repoOfFormat(project, id, "", format)
	if !ok {
		return repoCtx{}, errNotFound(
			"No %s repository %q in project %q.",
			strings.ToLower(format.String()),
			id,
			project,
		)
	}

	if rc.pb.GetMode() != artifactregistrypb.Repository_STANDARD_REPOSITORY &&
		rc.pb.GetMode() != artifactregistrypb.Repository_MODE_UNSPECIFIED {
		return repoCtx{}, errPrecondition("Repository %q is not a standard repository.", id)
	}

	return rc, nil
}

// PublishMaven stores one file of a Maven artifact and returns its package resource name. The file is stored as
// <group path>/<artifactId>/<version>/<artifactId>-<version><ext>, honouring the repository's version policy.
func (b *InMemoryBackend) PublishMaven(
	project, repo, group, artifact, version, ext string,
	data []byte,
) (string, error) {
	rc, err := b.publishRepo(project, repo, artifactregistrypb.Repository_MAVEN)
	if err != nil {
		return "", err
	}

	if group == "" || artifact == "" || version == "" {
		return "", errInvalid("groupId, artifactId and version are required: %v", errPublishInput)
	}

	if msg := mavenPolicyError(rc.pb.GetMavenConfig(), version); msg != "" {
		return "", errPrecondition("%s", msg)
	}

	fileID := strings.ReplaceAll(
		group,
		".",
		"/",
	) + "/" + artifact + "/" + version + "/" + artifact + "-" + version + ext
	pkgID := group + ":" + artifact
	overwrite := strings.HasSuffix(version, snapshotSfx) && rc.pb.GetMavenConfig().GetAllowSnapshotOverwrites()

	err = b.putPackageFile(
		rc.ref,
		pkgFile{pkgID: pkgID, display: pkgID, version: version, fileID: fileID, data: data},
		overwrite,
	)
	if err != nil {
		return "", err
	}

	return rc.ref.packageName(pkgID), nil
}

// PublishPython stores a wheel or sdist and returns its package resource name.
func (b *InMemoryBackend) PublishPython(project, repo, name, version, filename string, data []byte) (string, error) {
	rc, err := b.publishRepo(project, repo, artifactregistrypb.Repository_PYTHON)
	if err != nil {
		return "", err
	}

	if name == "" || version == "" || !pyFileRe.MatchString(filename) {
		return "", errInvalid("Invalid distribution %q: %v", filename, errPublishInput)
	}

	norm := pyNormalize(name)
	doc, _ := json.Marshal(pyDoc{Name: name, SHA256: sha256Hex(data)}, jsonx.V1())

	err = b.putPackageFile(rc.ref, pkgFile{
		pkgID: norm, display: name, version: version, fileID: pyFileID(norm, version, filename), data: data, doc: doc,
	}, false)
	if err != nil {
		return "", err
	}

	return rc.ref.packageName(norm), nil
}

// PublishNPM stores an npm tarball (as `npm pack` writes it) and tags it latest. It returns the package resource
// name, the package name and the version.
func (b *InMemoryBackend) PublishNPM(project, repo string, tarball []byte) (string, string, string, error) {
	rc, err := b.publishRepo(project, repo, artifactregistrypb.Repository_NPM)
	if err != nil {
		return "", "", "", err
	}

	manifest, err := tgzPackageJSON(tarball)
	if err != nil {
		return "", "", "", errInvalid("The tarball has no package/package.json: %v", err)
	}

	var m struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	if err = json.Unmarshal(
		manifest,
		&m,
		jsonx.V1(),
	); err != nil || m.Name == "" ||
		!npmVersionRe.MatchString(m.Version) {
		return "", "", "", errInvalid("package.json needs a name and a semver version: %v", errPublishInput)
	}

	pf := pkgFile{
		pkgID: m.Name, display: m.Name, version: m.Version, fileID: npmFileID(m.Name, npmTarball(m.Name, m.Version)),
		data: tarball, doc: manifest,
	}

	if err = b.putPackageFile(rc.ref, pf, false); err != nil {
		return "", "", "", err
	}

	b.setNPMTags(rc, m.Name, map[string]string{"latest": m.Version}, false)

	return rc.ref.packageName(m.Name), m.Name, m.Version, nil
}

// PublishGoModule stores a module zip (golang.org/x/mod/zip layout) and returns its package resource name, the
// module path and the version.
func (b *InMemoryBackend) PublishGoModule(project, repo string, zipData []byte) (string, string, string, error) {
	rc, err := b.publishRepo(project, repo, artifactregistrypb.Repository_GO)
	if err != nil {
		return "", "", "", err
	}

	mod, err := b.addGoModule(rc.ref, zipData)
	if err != nil {
		return "", "", "", err
	}

	pkgID, version, _ := strings.Cut(strings.TrimPrefix(mod.GetName(), rc.ref.full()+"/goModules/"), ":")
	path, _ := unescapeImage(pkgID)

	return rc.ref.packageName(path), path, version, nil
}

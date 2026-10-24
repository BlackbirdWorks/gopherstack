package gcpartifactregistry

import (
	"net/url"
	"regexp"
	"strings"
)

var idRe = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)

const (
	allLocs      = "-"
	parentLen    = 4
	repoLen      = 6
	childLen     = 8
	leafLen      = 10
	segLocs      = 2
	segRepos     = 4
	segChild     = 6
	segLeaf      = 8
	escapedSlash = "%2F"

	segProjects   = "projects"
	segLocations  = "locations"
	fieldName     = "name"
	fieldAnnot    = "annotations"
	fieldUpdated  = "updateTime"
	fieldUpdSnake = "update_time"
)

// repoRef is a parsed projects/{p}/locations/{l}/repositories/{r} name.
type repoRef struct {
	project  string
	location string
	id       string
}

func (r repoRef) parent() string { return "projects/" + r.project + "/locations/" + r.location }

func (r repoRef) full() string { return r.parent() + "/repositories/" + r.id }

// dockerKey is the registry path prefix (without trailing slash) of the repository's images.
func (r repoRef) dockerKey() string { return r.project + "/" + r.id }

func (r repoRef) packageName(image string) string {
	return r.full() + "/packages/" + escapeImage(image)
}

func (r repoRef) fileName(id string) string { return r.full() + "/files/" + escapeImage(id) }

func (r repoRef) ruleName(id string) string { return r.full() + "/rules/" + id }

func escapeImage(image string) string {
	return strings.ReplaceAll(url.PathEscape(image), "/", escapedSlash)
}

func unescapeImage(seg string) (string, bool) {
	image, err := url.PathUnescape(seg)

	return image, err == nil && image != ""
}

func parseParent(name string) (repoRef, error) {
	p := strings.Split(name, "/")
	if len(p) != parentLen || p[0] != segProjects || p[segLocs] != segLocations || p[1] == "" || p[3] == "" {
		return repoRef{}, errInvalid("Invalid resource name %q.", name)
	}

	return repoRef{project: p[1], location: p[3]}, nil
}

func splitName(name string, n int, kind string) ([]string, error) {
	p := strings.Split(name, "/")
	if len(p) < n || p[0] != segProjects || p[segLocs] != segLocations || p[segRepos] != "repositories" ||
		p[1] == "" || p[3] == "" || p[5] == "" || p[3] == allLocs {
		return nil, errInvalid("Invalid %s name %q.", kind, name)
	}

	return p, nil
}

func refOf(p []string) repoRef { return repoRef{project: p[1], location: p[3], id: p[5]} }

func parseRepo(name string) (repoRef, error) {
	p, err := splitName(name, repoLen, "repository")
	if err != nil || len(p) != repoLen {
		return repoRef{}, errInvalid("Invalid repository name %q.", name)
	}

	return refOf(p), nil
}

// pkgRef is a parsed .../packages/{image} name.
type pkgRef struct {
	image string
	repo  repoRef
}

func (p pkgRef) full() string { return p.repo.packageName(p.image) }

func parsePackage(name string) (pkgRef, error) {
	p, err := splitName(name, childLen, "package")
	if err != nil || len(p) != childLen || p[segChild] != "packages" {
		return pkgRef{}, errInvalid("Invalid package name %q.", name)
	}

	image, ok := unescapeImage(p[7])
	if !ok {
		return pkgRef{}, errInvalid("Invalid package name %q.", name)
	}

	return pkgRef{repo: refOf(p), image: image}, nil
}

// leafRef is a parsed .../packages/{image}/{versions|tags}/{leaf} name.
type leafRef struct {
	leaf string
	pkg  pkgRef
}

func parseLeaf(name, kind string) (leafRef, error) {
	p, err := splitName(name, leafLen, kind)
	if err != nil || len(p) != leafLen || p[segChild] != "packages" || p[segLeaf] != kind+"s" || p[9] == "" {
		return leafRef{}, errInvalid("Invalid %s name %q.", kind, name)
	}

	image, ok := unescapeImage(p[7])
	if !ok {
		return leafRef{}, errInvalid("Invalid %s name %q.", kind, name)
	}

	return leafRef{pkg: pkgRef{repo: refOf(p), image: image}, leaf: p[9]}, nil
}

// parseImageName parses .../dockerImages/{image}@{digest} (or {image}:{tag}).
func parseImageName(name string) (repoRef, string, string, error) {
	p, err := splitName(name, childLen, "docker image")
	if err != nil || len(p) != childLen || p[segChild] != "dockerImages" {
		return repoRef{}, "", "", errInvalid("Invalid docker image name %q.", name)
	}

	raw, err2 := url.PathUnescape(p[7])
	if err2 != nil {
		return repoRef{}, "", "", errInvalid("Invalid docker image name %q.", name)
	}

	for _, sep := range []string{"@", ":"} {
		if image, ref, ok := strings.Cut(raw, sep); ok && image != "" && ref != "" {
			return refOf(p), image, ref, nil
		}
	}

	return repoRef{}, "", "", errInvalid("Invalid docker image name %q.", name)
}

func validateID(id string) error {
	if !idRe.MatchString(id) {
		return errInvalid("repository_id %q must be 1-63 lowercase letters, digits or hyphens, starting with a "+
			"letter and not ending with a hyphen.", id)
	}

	return nil
}

// fileRef is a parsed .../files/{id} name; slashes in the id are escaped.
type fileRef struct {
	id   string
	repo repoRef
}

func parseFile(name string) (fileRef, error) {
	p, err := splitName(name, childLen, "file")
	if err != nil || p[segChild] != "files" {
		return fileRef{}, errInvalid("Invalid file name %q.", name)
	}

	id, ok := unescapeImage(strings.Join(p[7:], "/"))
	if !ok {
		return fileRef{}, errInvalid("Invalid file name %q.", name)
	}

	return fileRef{repo: refOf(p), id: id}, nil
}

func parseRule(name string) (repoRef, string, error) {
	p, err := splitName(name, childLen, "rule")
	if err != nil || len(p) != childLen || p[segChild] != "rules" || p[7] == "" {
		return repoRef{}, "", errInvalid("Invalid rule name %q.", name)
	}

	return refOf(p), p[7], nil
}

package gcpartifactregistry

import (
	"net/http"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
)

// standardUpstreams lists the virtual repository's upstreams that hold their packages locally, by descending
// priority; the indices of REMOTE upstreams are the upstream's own files and cannot be merged.
func (b *InMemoryBackend) standardUpstreams(rc repoCtx) []repoCtx {
	var out []repoCtx

	for _, up := range b.upstreamsOf(rc) {
		if up.pb.GetMode() != artifactregistrypb.Repository_REMOTE_REPOSITORY {
			out = append(out, up)
		}
	}

	return out
}

// serveVirtualOSIndex answers the apt and yum index files of a virtual repository from the packages of all its
// local upstreams, signed with the virtual repository's own key; a package present in several upstreams comes
// from the one with the higher priority. Package downloads stay with the first upstream that has the file.
func (b *InMemoryBackend) serveVirtualOSIndex(w http.ResponseWriter, r *http.Request, rc repoCtx, path string) bool {
	switch rc.pb.GetFormat() {
	case artifactregistrypb.Repository_APT:
		if !strings.HasPrefix(path, "dists/"+rc.ref.id+"/") {
			return false
		}

		ups := b.standardUpstreams(rc)
		if len(ups) == 0 {
			return false
		}

		b.serveAptPath(w, r, rc, mergeDebEntries(b, ups), path)

		return true
	case artifactregistrypb.Repository_YUM:
		if !strings.HasPrefix(path, "repodata/") {
			return false
		}

		ups := b.standardUpstreams(rc)
		if len(ups) == 0 {
			return false
		}

		b.serveYumPath(w, r, rc, mergeRPMEntries(b, ups), path)

		return true
	default:
		return false
	}
}

func mergeDebEntries(b *InMemoryBackend, ups []repoCtx) []debEntry {
	seen := map[string]bool{}

	var out []debEntry

	for _, up := range ups {
		for _, e := range b.debEntries(up) {
			key := e.ctl.Package + "\x00" + e.ctl.Version + "\x00" + e.ctl.Arch
			if !seen[key] {
				seen[key] = true

				out = append(out, e)
			}
		}
	}

	sortDebEntries(out)

	return out
}

func mergeRPMEntries(b *InMemoryBackend, ups []repoCtx) []rpmEntry {
	seen := map[string]bool{}

	var out []rpmEntry

	for _, up := range ups {
		for _, e := range b.rpmEntries(up) {
			if !seen[e.path] {
				seen[e.path] = true

				out = append(out, e)
			}
		}
	}

	sortRPMEntries(out)

	return out
}

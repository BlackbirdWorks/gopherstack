package gcpartifactregistry

import (
	"bytes"
	"maps"
	"net/http"
	"sort"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
)

// bufResponse records a response so a virtual repository can try the next upstream when this one misses.
type bufResponse struct {
	header http.Header
	body   bytes.Buffer
	code   int
}

func newBufResponse() *bufResponse { return &bufResponse{header: http.Header{}} }

func (b *bufResponse) Header() http.Header { return b.header }

func (b *bufResponse) WriteHeader(code int) {
	if b.code == 0 {
		b.code = code
	}
}

func (b *bufResponse) Write(p []byte) (int, error) {
	b.WriteHeader(http.StatusOK)

	return b.body.Write(p)
}

func (b *bufResponse) status() int {
	if b.code == 0 {
		return http.StatusOK
	}

	return b.code
}

func (b *bufResponse) replay(w http.ResponseWriter) {
	maps.Copy(w.Header(), b.header)

	w.WriteHeader(b.status())
	_, _ = w.Write(b.body.Bytes())
}

// upstreamsOf lists a virtual repository's upstream repositories, highest priority first.
func (b *InMemoryBackend) upstreamsOf(rc repoCtx) []repoCtx {
	policies := append(
		[]*artifactregistrypb.UpstreamPolicy(nil),
		rc.pb.GetVirtualRepositoryConfig().GetUpstreamPolicies()...)

	sort.SliceStable(policies, func(i, j int) bool {
		if policies[i].GetPriority() != policies[j].GetPriority() {
			return policies[i].GetPriority() > policies[j].GetPriority()
		}

		return policies[i].GetId() < policies[j].GetId()
	})

	view := rc.ref
	if rc.view != nil {
		view = *rc.view
	}

	var out []repoCtx

	for _, p := range policies {
		ref, err := parseRepo(p.GetRepository())
		if err != nil {
			continue
		}

		up, ok := b.repoOfFormat(ref.project, ref.id, rc.pb.GetFormat())
		if !ok || up.pb.GetMode() == artifactregistrypb.Repository_VIRTUAL_REPOSITORY {
			continue
		}

		up.view = &view
		out = append(out, up)
	}

	return out
}

// serveVirtual answers a read with the first upstream, by descending priority, that has the artifact.
func (b *InMemoryBackend) serveVirtual(w http.ResponseWriter, r *http.Request, rc repoCtx, path string) {
	if !isRead(r) {
		writePlain(w, http.StatusMethodNotAllowed, "Virtual repositories are read-only; upload to an upstream.")

		return
	}

	for _, up := range b.upstreamsOf(rc) {
		rec := newBufResponse()
		b.dispatchRepo(rec, r, up, path)

		if st := rec.status(); st < http.StatusBadRequest || st == http.StatusForbidden {
			rec.replay(w)

			return
		}
	}

	writePlain(w, http.StatusNotFound, "Not found in any upstream of "+rc.ref.id+": "+path)
}

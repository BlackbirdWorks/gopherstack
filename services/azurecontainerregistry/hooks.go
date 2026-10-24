package azurecontainerregistry

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

type manifestDoc struct {
	MediaType string `json:"mediaType"`
	Config    struct {
		MediaType string `json:"mediaType"`
	} `json:"config"`
}

func requestInfo(r *http.Request) RequestInfo {
	return RequestInfo{ID: uuid.NewString(), Host: r.Host, Method: r.Method, Useragent: r.UserAgent()}
}

// loadChange reads the manifest at repo:ref into a ManifestChange.
func loadChange(app http.Handler, r *http.Request, repo, ref string) (ManifestChange, bool) {
	m, err := ociregistry.NewClient(app).GetManifest(r.Context(), repo, ref)
	if err != nil {
		return ManifestChange{}, false
	}

	c := ManifestChange{
		Req: requestInfo(r), Repository: repo, Digest: m.Digest, MediaType: m.MediaType, Size: int64(len(m.Body)),
	}

	if !ociregistry.IsDigest(ref) {
		c.Tag = ref
	}

	var doc manifestDoc
	if json.Unmarshal(m.Body, &doc) == nil {
		c.ConfigType = doc.Config.MediaType

		if doc.MediaType != "" {
			c.MediaType = doc.MediaType
		}
	}

	return c, true
}

// observer wires manifest permits, ledger bookkeeping and webhook events into the registry traffic of r.
func (h *Handler) observer(reg string, app http.Handler, r *http.Request) ociregistry.Observer {
	var before ManifestChange

	var haveBefore bool

	return ociregistry.Observer{
		Permit: func(w http.ResponseWriter, repo, ref, op string) bool {
			if !h.Backend.Allowed(reg, repo, ref, op) {
				writeRegistryError(w, http.StatusForbidden, "DENIED", "The operation is disabled.")

				return false
			}

			if op == ociregistry.OpDelete && h.Backend.HasWebhooks(reg) {
				before, haveBefore = loadChange(app, r, repo, ref)
				before.Delete = true
			}

			return true
		},
		Put: func(repo, ref, digest string) {
			h.Backend.RecordManifestPut(reg, repo, ref, digest)

			if !h.Backend.HasWebhooks(reg) {
				return
			}

			if c, ok := loadChange(app, r, repo, digest); ok {
				c.Tag = ""
				if !ociregistry.IsDigest(ref) {
					c.Tag = ref
				}

				h.Backend.FireManifest(reg, c)
			}
		},
		Delete: func(repo, ref string) {
			h.Backend.RecordManifestDelete(reg, repo, ref)

			if haveBefore {
				h.Backend.FireManifest(reg, before)
			}
		},
	}
}

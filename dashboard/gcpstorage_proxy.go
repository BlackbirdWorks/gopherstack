package dashboard

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	gcpStorageSeg          = "storage"
	errServiceUnavailable  = "ServiceUnavailable"
	gcpStorageProxyPrefix  = "/dashboard/api/v1/gcp/storage"
	gcpStorageMaxBodyBytes = 64 << 20
	gcpStorageMaxJSONSegs  = 6
	gcpStorageIAMSegs      = 5
	gcpStorageObjectSegs   = 7
	gcpStorageUploadSegs   = 6
	gcpStorageBucketIdx    = 3
)

// registerGCPStorageRoutes exposes the GCS emulator to the SPA by dispatching
// in-process to its handler, avoiding its separate listener and CORS.
func (h *DashboardHandler) registerGCPStorageRoutes() {
	var router *echo.Echo

	if h.config.GCPStorageOps != nil {
		router = echo.New()
		router.Any("/*", h.config.GCPStorageOps.Handler())
	}

	serve := func(c *echo.Context) error {
		if router == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{keyError: errServiceUnavailable})
		}

		r := c.Request()

		escaped := strings.TrimPrefix(r.URL.EscapedPath(), gcpStorageProxyPrefix)

		p, ok := gcpStorageProxyPath(escaped)
		if !ok {
			return c.String(http.StatusBadRequest, "unsupported GCS proxy request")
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path = p
		fwd.URL.RawPath = escaped
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, gcpStorageMaxBodyBytes)
		router.ServeHTTP(c.Response(), fwd)

		return nil
	}

	for _, method := range []string{
		http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
	} {
		h.SubRouter.Add(method, gcpStorageProxyPrefix+"/*", serve)
	}
}

// gcpStorageProxyPath admits only the JSON API (/storage/v1/b[/B[/o[/O]|/iam]]),
// media upload (/upload/storage/v1/b/B/o) and download (/download/storage/v1/b/B/o/O).
// It returns the decoded path; the escaped form is forwarded as RawPath.
func gcpStorageProxyPath(escaped string) (string, bool) {
	if !strings.HasPrefix(escaped, "/") {
		return "", false
	}

	parts := strings.Split(escaped[1:], "/")
	segs := make([]string, len(parts))

	for i, part := range parts {
		s, err := url.PathUnescape(part)
		if err != nil || s == "" || s == "." || s == ".." {
			return "", false
		}

		segs[i] = s
	}

	if !gcpStorageShapeOK(segs) {
		return "", false
	}

	return "/" + strings.Join(segs, "/"), true
}

func gcpStorageShapeOK(segs []string) bool {
	switch segs[0] {
	case gcpStorageSeg:
		return gcpStorageJSONShapeOK(segs)
	case "upload":
		return len(segs) == gcpStorageUploadSegs && gcpStorageV1Bucket(segs) && segs[5] == "o"
	case "download":
		return len(segs) == gcpStorageObjectSegs && gcpStorageV1Bucket(segs) && segs[5] == "o"
	default:
		return false
	}
}

func gcpStorageV1Bucket(segs []string) bool {
	return segs[1] == gcpStorageSeg && segs[2] == "v1" && segs[3] == "b"
}

func gcpStorageJSONShapeOK(segs []string) bool {
	if len(segs) < gcpStorageBucketIdx || segs[1] != "v1" || segs[2] != "b" {
		return false
	}

	if len(segs) > gcpStorageBucketIdx+1 && segs[4] != "o" && segs[4] != "iam" {
		return false
	}

	if len(segs) > gcpStorageIAMSegs && segs[4] == "iam" {
		return false
	}

	return len(segs) <= gcpStorageMaxJSONSegs
}

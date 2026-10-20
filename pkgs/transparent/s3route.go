package transparent

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
)

const (
	accessPointLookupPath = "/v20180820/accesspoint/"
	websitePathPrefix     = "/_gopherstack/website/"
	accountHeader         = "X-Amz-Account-Id"
)

// S3HostRouter maps S3 access-point, website and S3 Control hostnames onto the
// path-based routes next serves; other requests pass through untouched.
func S3HostRouter(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, ok := ParseS3Host(r.Host)
		if !ok {
			next.ServeHTTP(w, r)

			return
		}

		switch h.Kind {
		case S3HostControl:
			routeControl(next, w, r, h)
		case S3HostWebsite:
			routeWebsite(next, w, r, h)
		case S3HostAccessPoint:
			routeAccessPoint(next, w, r, h)
		}
	})
}

func routeControl(next http.Handler, w http.ResponseWriter, r *http.Request, h S3Host) {
	if r.Header.Get(accountHeader) == "" {
		r.Header.Set(accountHeader, h.Account)
	}

	next.ServeHTTP(w, r)
}

func routeWebsite(next http.Handler, w http.ResponseWriter, r *http.Request, h S3Host) {
	if r.Method == http.MethodGet {
		rewritePath(r, websitePathPrefix+h.Name+r.URL.Path)
	}

	next.ServeHTTP(w, r)
}

func routeAccessPoint(next http.Handler, w http.ResponseWriter, r *http.Request, h S3Host) {
	bucket := lookupAccessPointBucket(next, r, h)
	if bucket == "" {
		writeNoSuchAccessPoint(w, h.Name)

		return
	}

	rewritePath(r, "/"+bucket+r.URL.Path)
	next.ServeHTTP(w, r)
}

func rewritePath(r *http.Request, path string) {
	r.URL.Path = path
	r.URL.RawPath = ""
}

// lookupAccessPointBucket resolves the access point through the S3 Control route of next.
func lookupAccessPointBucket(next http.Handler, r *http.Request, h S3Host) string {
	req := r.Clone(r.Context())
	req.Method = http.MethodGet
	req.Body = http.NoBody
	req.ContentLength = 0
	req.URL = &url.URL{Path: accessPointLookupPath + h.Name}
	req.RequestURI = req.URL.Path
	req.Header = http.Header{accountHeader: []string{h.Account}}

	rec := httptest.NewRecorder()
	next.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		return ""
	}

	var out struct {
		Bucket string `xml:"Bucket"`
	}

	if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		return ""
	}

	return out.Bucket
}

func writeNoSuchAccessPoint(w http.ResponseWriter, name string) {
	var b bytes.Buffer

	_ = xml.EscapeText(&b, []byte(name))

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusNotFound)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<Error><Code>NoSuchAccessPoint</Code><Message>The specified access point does not exist</Message>`+
		`<AccessPointName>%s</AccessPointName></Error>`, b.String())
}

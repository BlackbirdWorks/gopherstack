package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	ociArtPrefix      = ociProxyPrefix + "/artifacts"
	ociArtAPIPrefix   = "/20160918"
	ociArtRepos       = "containerRepositories"
	ociArtImages      = "containerImages"
	ociArtSigs        = "containerImageSignatures"
	ociArtConfig      = "containerConfiguration"
	ociArtGenRepos    = "genericRepositories"
	ociArtArtifacts   = "genericArtifacts"
	ociArtRestore     = "restore"
	ociArtContent     = "content"
	ociArtMaxName     = 255
	ociArtMaxReadme   = 1 << 16
	ociArtMaxDesc     = 1024
	ociArtMaxQuery    = 512
	ociArtMaxDownload = 64 << 20
	ociArtItemSegs    = 2
	ociArtSubSegs     = 3
	ociArtMarkdown    = "TEXT_MARKDOWN"
	ociArtPlain       = "TEXT_PLAIN"
	ociArtFileFallbk  = "artifact"
	ociArtKeyName     = "displayName"
	ociArtKeyPage     = "page"
	ociArtKeyLimit    = "limit"
	ociArtMiBShift    = 20
)

var (
	ociArtIDRe   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
	ociArtFileRe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)
)

// ociArtQueryKeys are the only query parameters each listing forwards; everything else is dropped.
//
//nolint:gochecknoglobals // immutable allow-list
var ociArtQueryKeys = map[string][]string{
	ociArtRepos:     {ociKeyCompartment, ociArtKeyName, ociArtKeyPage, ociArtKeyLimit},
	ociArtImages:    {ociKeyCompartment, "repositoryId", "lifecycleState", ociArtKeyPage, ociArtKeyLimit},
	ociArtSigs:      {ociKeyCompartment, "imageId", ociArtKeyPage, ociArtKeyLimit},
	ociArtConfig:    {ociKeyCompartment},
	ociArtGenRepos:  {ociKeyCompartment, ociArtKeyPage, ociArtKeyLimit},
	ociArtArtifacts: {ociKeyCompartment, "repositoryId", ociArtKeyPage, ociArtKeyLimit},
}

// ociArtCall is one allow-listed request: the emulator path, a rebuilt body, and how to post-process the answer.
type ociArtCall struct {
	body     func(c *echo.Context) ([]byte, bool)
	path     string
	coll     string
	download bool
	scrub    bool
}

// registerOCIArtifactsRoutes serves the Artifacts and Container Registry page by dispatching in-process to the
// emulator's control plane without request signing. Only the container repository, image, signature, container
// configuration and generic repository and artifact operations the page uses are reachable, each write rebuilt
// from fixed fields. The Docker Registry v2 data plane, the registry token endpoint and the admin key endpoints are
// not routed, so no auth token or bearer token can pass through. Signature message and signature blobs are
// stripped, and artifact content is only served as a size-capped attachment.
func (h *DashboardHandler) registerOCIArtifactsRoutes() {
	var inner *echo.Echo

	ops := h.config.OCIArtifactsOps
	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI Artifacts", next) }

	h.SubRouter.Add(http.MethodGet, ociArtPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: ops.Backend.Compartments()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociArtPrefix+"/api")

		parts, ok := ociSplitPath(raw)
		if !ok {
			return ociInvalid(c, "unsupported Artifacts proxy request")
		}

		call, ok := ociArtResolve(r.Method, parts)
		if !ok {
			return ociInvalid(c, "unsupported Artifacts proxy request")
		}

		var body []byte

		if call.body != nil {
			if body, ok = call.body(c); !ok {
				return ociInvalid(c, "invalid Artifacts request")
			}
		}

		r.Header.Del("Authorization")
		r.URL.RawQuery = ociArtQuery(r.URL.Query(), ociArtQueryKeys[call.coll])

		if call.download {
			return ociArtDownload(c, inner, call.path)
		}

		rec := ociArtForward(c, inner, call.path, body)
		if call.scrub && rec.Code == http.StatusOK {
			rec.Body = bytes.NewBuffer(ociArtScrubSignatures(rec.Body.Bytes()))
		}

		return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id")
	})

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		h.SubRouter.Add(m, ociArtPrefix+"/api/*", serve)
	}
}

// ociArtIDOK checks the resource id segment, the only caller-chosen part of a path.
func ociArtIDOK(parts []string) bool {
	return len(parts) < ociArtItemSegs || ociArtIDRe.MatchString(unescapedPath(parts[1]))
}

// ociArtResolve maps a proxy request to its emulator call; false means the request is outside the allow-list.
func ociArtResolve(method string, parts []string) (ociArtCall, bool) {
	if len(parts) > ociArtSubSegs || !ociArtIDOK(parts) {
		return ociArtCall{}, false
	}

	var (
		call ociArtCall
		ok   bool
	)

	switch parts[0] {
	case ociArtRepos:
		call, ok = ociArtRepoCall(method, parts)
	case ociArtImages:
		call, ok = ociArtImageCall(method, parts)
	case ociArtSigs:
		call, ok = ociArtCall{
			path:  "/container/imageSignatures",
			scrub: true,
		}, method == http.MethodGet &&
			len(parts) == 1
	case ociArtConfig:
		call, ok = ociArtConfigCall(method, parts)
	case ociArtGenRepos:
		call, ok = ociArtGenRepoCall(method, parts)
	case ociArtArtifacts:
		call, ok = ociArtArtifactCall(method, parts)
	}

	call.coll = parts[0]
	call.path = ociArtAPIPrefix + call.path

	return call, ok
}

func ociArtRepoCall(method string, parts []string) (ociArtCall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociArtCall{path: "/container/repositories"}, true
	case len(parts) == 1 && method == http.MethodPost:
		return ociArtCall{path: "/container/repositories", body: ociArtCreateRepoBody}, true
	case len(parts) != ociArtItemSegs:
		return ociArtCall{}, false
	}

	item := "/container/repositories/" + parts[1]

	switch method {
	case http.MethodGet, http.MethodDelete:
		return ociArtCall{path: item}, true
	case http.MethodPut:
		return ociArtCall{path: item, body: ociArtUpdateRepoBody}, true
	}

	return ociArtCall{}, false
}

func ociArtImageCall(method string, parts []string) (ociArtCall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociArtCall{path: "/container/images"}, true
	case len(parts) == ociArtItemSegs && (method == http.MethodGet || method == http.MethodDelete):
		return ociArtCall{path: "/container/images/" + parts[1]}, true
	case len(parts) == ociArtSubSegs && parts[2] == ociArtRestore && method == http.MethodPost:
		return ociArtCall{path: "/container/images/" + parts[1] + "/actions/restore", body: ociArtRestoreBody}, true
	}

	return ociArtCall{}, false
}

func ociArtConfigCall(method string, parts []string) (ociArtCall, bool) {
	if len(parts) != 1 {
		return ociArtCall{}, false
	}

	switch method {
	case http.MethodGet:
		return ociArtCall{path: "/container/configuration"}, true
	case http.MethodPut:
		return ociArtCall{path: "/container/configuration", body: ociArtConfigBody}, true
	}

	return ociArtCall{}, false
}

func ociArtGenRepoCall(method string, parts []string) (ociArtCall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociArtCall{path: "/repositories"}, true
	case len(parts) == 1 && method == http.MethodPost:
		return ociArtCall{path: "/repositories", body: ociArtCreateGenRepoBody}, true
	case len(parts) == ociArtItemSegs && (method == http.MethodGet || method == http.MethodDelete):
		return ociArtCall{path: "/repositories/" + parts[1]}, true
	}

	return ociArtCall{}, false
}

func ociArtArtifactCall(method string, parts []string) (ociArtCall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociArtCall{path: "/generic/artifacts"}, true
	case len(parts) == ociArtItemSegs && (method == http.MethodGet || method == http.MethodDelete):
		return ociArtCall{path: "/generic/artifacts/" + parts[1]}, true
	case len(parts) == ociArtSubSegs && parts[2] == ociArtContent && method == http.MethodGet:
		return ociArtCall{path: "/generic/artifacts/" + parts[1] + "/content", download: true}, true
	}

	return ociArtCall{}, false
}

// ociArtQuery keeps only the allowed parameters, each bounded in length.
func ociArtQuery(in url.Values, keys []string) string {
	out := url.Values{}

	for _, k := range keys {
		if v := in.Get(k); v != "" && len(v) <= ociArtMaxQuery {
			out.Set(k, v)
		}
	}

	return out.Encode()
}

// ociArtForward runs one call against the emulator and records the answer. A nil body forwards no body.
func ociArtForward(c *echo.Context, inner *echo.Echo, apiPath string, body []byte) *httptest.ResponseRecorder {
	r := c.Request()
	fwd := r.Clone(r.Context())
	fwd.URL.Path, fwd.URL.RawPath = unescapedPath(apiPath), apiPath
	fwd.RequestURI, fwd.Body, fwd.ContentLength = "", http.NoBody, 0
	fwd.Header = fwd.Header.Clone()

	if len(body) > 0 {
		fwd.Body, fwd.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		fwd.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	inner.ServeHTTP(rec, fwd)

	return rec
}

func ociArtName(s string) bool { return s != "" && len(s) <= ociArtMaxName }

type ociArtReadmeIn struct {
	Content *string `json:"content"`
	Format  string  `json:"format"`
}

// build returns the wire readme, or false when it is oversized or has an unknown format.
func (in *ociArtReadmeIn) build() (map[string]any, bool) {
	if in == nil || in.Content == nil {
		return nil, true
	}

	format := in.Format
	if format == "" {
		format = ociArtMarkdown
	}

	if len(*in.Content) > ociArtMaxReadme || (format != ociArtMarkdown && format != ociArtPlain) {
		return nil, false
	}

	return map[string]any{"content": *in.Content, "format": format}, true
}

func ociArtMarshal(v map[string]any) ([]byte, bool) {
	b, err := json.Marshal(v)

	return b, err == nil
}

func ociArtCreateRepoBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		IsPublic    *bool           `json:"isPublic"`
		IsImmutable *bool           `json:"isImmutable"`
		Readme      *ociArtReadmeIn `json:"readme"`
		Compartment string          `json:"compartmentId"`
		DisplayName string          `json:"displayName"`
	}

	if !ociDecodeBody(c, &in) || !ociArtName(in.Compartment) || !ociArtName(in.DisplayName) {
		return nil, false
	}

	readme, ok := in.Readme.build()
	if !ok {
		return nil, false
	}

	out := map[string]any{
		ociKeyCompartment: in.Compartment, ociArtKeyName: in.DisplayName,
		"isPublic": in.IsPublic != nil && *in.IsPublic, "isImmutable": in.IsImmutable != nil && *in.IsImmutable,
	}

	if readme != nil {
		out["readme"] = readme
	}

	return ociArtMarshal(out)
}

func ociArtUpdateRepoBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		IsPublic    *bool           `json:"isPublic"`
		IsImmutable *bool           `json:"isImmutable"`
		Readme      *ociArtReadmeIn `json:"readme"`
	}

	if !ociDecodeBody(c, &in) {
		return nil, false
	}

	readme, ok := in.Readme.build()
	if !ok {
		return nil, false
	}

	out := map[string]any{}

	if in.IsPublic != nil {
		out["isPublic"] = *in.IsPublic
	}

	if in.IsImmutable != nil {
		out["isImmutable"] = *in.IsImmutable
	}

	if readme != nil {
		out["readme"] = readme
	}

	if len(out) == 0 {
		return nil, false
	}

	return ociArtMarshal(out)
}

func ociArtRestoreBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		Version *string `json:"version"`
	}

	err := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, ociInprocMaxBody)).Decode(&in)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false
	}

	if in.Version == nil {
		return nil, true
	}

	if !ociArtName(*in.Version) {
		return nil, false
	}

	return ociArtMarshal(map[string]any{"version": *in.Version})
}

func ociArtConfigBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		OnFirstPush *bool `json:"isRepositoryCreatedOnFirstPush"`
	}

	if !ociDecodeBody(c, &in) || in.OnFirstPush == nil {
		return nil, false
	}

	return ociArtMarshal(map[string]any{"isRepositoryCreatedOnFirstPush": *in.OnFirstPush})
}

func ociArtCreateGenRepoBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		IsImmutable *bool  `json:"isImmutable"`
		Compartment string `json:"compartmentId"`
		DisplayName string `json:"displayName"`
		Description string `json:"description"`
	}

	if !ociDecodeBody(c, &in) || !ociArtName(in.Compartment) || !ociArtName(in.DisplayName) ||
		len(in.Description) > ociArtMaxDesc {
		return nil, false
	}

	out := map[string]any{
		ociKeyCompartment: in.Compartment, ociArtKeyName: in.DisplayName, "repositoryType": "GENERIC",
		"isImmutable": in.IsImmutable != nil && *in.IsImmutable,
	}

	if in.Description != "" {
		out["description"] = in.Description
	}

	return ociArtMarshal(out)
}

// ociArtScrubSignatures drops the base64 message and signature of every signature in a listing.
func ociArtScrubSignatures(raw []byte) []byte {
	var env struct {
		Items []map[string]any `json:"items"`
	}

	if json.Unmarshal(raw, &env) != nil {
		return []byte("{}")
	}

	for _, it := range env.Items {
		delete(it, "message")
		delete(it, "signature")
	}

	if env.Items == nil {
		env.Items = []map[string]any{}
	}

	out, err := json.Marshal(env)
	if err != nil {
		return []byte("{}")
	}

	return out
}

// ociArtDownload answers an artifact content call as a size-capped attachment. The cap is checked on the artifact
// record before its bytes are fetched.
func ociArtDownload(c *echo.Context, inner *echo.Echo, contentPath string) error {
	meta := ociArtForward(c, inner, strings.TrimSuffix(contentPath, "/"+ociArtContent), nil)
	if meta.Code != http.StatusOK {
		return ociRelay(c, meta, "opc-request-id")
	}

	var art struct {
		Path string `json:"artifactPath"`
		Size int64  `json:"sizeInBytes"`
	}

	_ = json.Unmarshal(meta.Body.Bytes(), &art)

	if art.Size > ociArtMaxDownload {
		return cloudJSONFail(c, http.StatusRequestEntityTooLarge, "PayloadTooLarge",
			"artifact exceeds the "+strconv.Itoa(ociArtMaxDownload>>ociArtMiBShift)+" MiB dashboard download limit")
	}

	content := ociArtForward(c, inner, contentPath, nil)
	if content.Code != http.StatusOK {
		return ociRelay(c, content, "opc-request-id")
	}

	h := c.Response().Header()
	h.Set("Content-Disposition", `attachment; filename="`+ociArtFileName(art.Path)+`"`)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "sandbox; default-src 'none'")
	h.Set("Cache-Control", "no-store")

	return c.Blob(http.StatusOK, ociMimeOctet, content.Body.Bytes())
}

// ociArtFileName reduces an artifact path to a safe download file name.
func ociArtFileName(artifactPath string) string {
	base := artifactPath[strings.LastIndex(artifactPath, "/")+1:]
	base = strings.Trim(ociArtFileRe.ReplaceAllString(base, "_"), "._")

	if base == "" {
		return ociArtFileFallbk
	}

	if len(base) > ociArtMaxName {
		base = base[:ociArtMaxName]
	}

	return base
}

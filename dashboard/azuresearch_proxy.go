package dashboard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	azureSearchPrefix     = "/dashboard/api/v1/azure/search"
	azureSearchKeysPrefix = "/dashboard/api/v1/azure/searchkeys"
	azureSearchDataPrefix = "/dashboard/api/v1/azure/searchdata"

	srchPxNamespace   = "microsoft.search"
	srchPxARMNS       = "Microsoft.Search"
	srchPxServices    = "searchservices"
	srchPxARMServices = "searchServices"
	srchPxARMVersion  = "2025-05-01"
	srchPxDataVersion = "2024-07-01"
	srchPxHostSuffix  = ".search.windows.net"
	srchPxRGList      = 7
	srchPxBasePath    = 9
	srchPxMaxBody     = 1 << 16
	srchPxMaxReply    = 8 << 20
	srchPxMaxText     = 4096
	srchPxMaxList     = 32
	srchPxMaxTop      = 1000
	srchPxMaxSkip     = 100000
	srchPxMaskTail    = 4
	srchPxFPBytes     = 4
	srchPxNone        = "<redacted>"
	srchPxMask        = "••••••••"
	srchPxDocsSearch  = "docs/search.post.search"
	srchPxKeyQuery    = "query"
	srchPxKeyFP       = "fp"
)

var (
	srchPxSubRe  = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
	srchPxNameRe = regexp.MustCompile(`^[A-Za-z0-9._()-]{1,128}$`)
	srchPxItemRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	srchPxKeyRe  = regexp.MustCompile(`^[A-Za-z0-9 _-]{1,64}$`)
	srchPxFPRe   = regexp.MustCompile(`^[0-9a-f]{8}$`)
)

// srchPxSecretFields are the string properties of data plane definitions that never leave the server.
//
//nolint:gochecknoglobals // immutable allow-list
var srchPxSecretFields = []string{
	"connectionstring", "apikey", "key", "accountkey", "sharedaccesssignature", "secret", "clientsecret",
	"applicationsecret", "authsecret", "sastoken",
}

// srchPxReadShapes are the data plane calls the explorer may make; item names are written as *.
//
//nolint:gochecknoglobals // immutable allow-list
var srchPxReadShapes = []string{
	"indexes", "indexes/*", "indexes/*/search.stats", "indexers", "indexers/*", "indexers/*/search.status",
	"datasources", "datasources/*", "skillsets", "skillsets/*", "synonymmaps", "synonymmaps/*", "servicestats",
}

//nolint:gochecknoglobals // immutable allow-list
var srchPxPostShapes = []string{"indexers/*/search.run", "indexers/*/search.reset", "indexes/*/" + srchPxDocsSearch}

// registerAzureSearchRoutes exposes Azure AI Search by dispatching in-process. Service reads go to the Microsoft.Search
// ARM provider; the data plane is reached only through allow-listed shapes with the admin key held on the server, and
// keys leave the server only through the explicit reveal routes.
func (h *DashboardHandler) registerAzureSearchRoutes() {
	h.registerARMProxy(armProxy{
		name: "Azure AI Search", prefix: azureSearchPrefix, version: srchPxARMVersion,
		methods: []string{http.MethodGet}, allowed: searchProxyAllowed,
	})

	a := searchAccess{}

	if h.config.AzureARMOps != nil && h.config.AzureSearchOps != nil {
		a.arm = echo.New()
		a.arm.Any("/*", h.config.AzureARMOps.Handler())
		a.data = echo.New()
		a.data.Any("/*", h.config.AzureSearchOps.Handler())
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if a.arm == nil {
				return azureACIFail(c, http.StatusServiceUnavailable, errServiceUnavailable,
					"Azure AI Search is not enabled")
			}

			return next(c)
		}
	}

	const ids = "/:sub/:rg/:svc"

	h.SubRouter.Add(http.MethodGet, azureSearchKeysPrefix+ids, guard(a.listKeys))
	h.SubRouter.Add(http.MethodPost, azureSearchKeysPrefix+ids+"/admin/reveal", guard(a.revealAdmin))
	h.SubRouter.Add(http.MethodPost, azureSearchKeysPrefix+ids+"/query", guard(a.createQueryKey))
	h.SubRouter.Add(http.MethodPost, azureSearchKeysPrefix+ids+"/query/:fp/reveal", guard(a.revealQueryKey))
	h.SubRouter.Add(http.MethodDelete, azureSearchKeysPrefix+ids+"/query/:fp", guard(a.deleteQueryKey))
	h.SubRouter.Add(http.MethodGet, azureSearchDataPrefix+ids+"/*", guard(a.dataPlane))
	h.SubRouter.Add(http.MethodPost, azureSearchDataPrefix+ids+"/*", guard(a.dataPlane))
}

func searchProxyAllowed(method string, p []string) bool {
	if method != http.MethodGet {
		return false
	}

	if len(p) == srchPxRGList {
		return p[0] == armPxSubsSeg && p[2] == armPxRGsSeg && p[4] == armPxProvSeg && p[5] == srchPxNamespace &&
			p[6] == srchPxServices
	}

	typ, name, rest, ok := armScope(p, srchPxNamespace)

	return ok && typ == srchPxServices && (name == "" || len(rest) == 0)
}

type searchAccess struct {
	arm  *echo.Echo
	data *echo.Echo
}

type searchIDs struct{ sub, rg, svc string }

func (i searchIDs) armPath(rest ...string) string {
	parts := make([]string, 0, srchPxBasePath+len(rest))
	parts = append(parts, "", armPxSubsSeg, url.PathEscape(i.sub), "resourceGroups", url.PathEscape(i.rg), armPxProvSeg,
		srchPxARMNS, srchPxARMServices, url.PathEscape(i.svc))

	for _, r := range rest {
		parts = append(parts, url.PathEscape(r))
	}

	return strings.Join(parts, "/")
}

func searchParams(c *echo.Context) (searchIDs, bool) {
	ids := searchIDs{sub: c.Param("sub"), rg: c.Param("rg"), svc: c.Param("svc")}

	return ids, srchPxSubRe.MatchString(ids.sub) && srchPxNameRe.MatchString(ids.rg) &&
		srchPxNameRe.MatchString(ids.svc)
}

func (a searchAccess) armDo(ctx context.Context, method, path string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(ctx, method, path+"?api-version="+srchPxARMVersion, bytes.NewReader(body))
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	a.arm.ServeHTTP(rec, req)

	return rec
}

func searchBad(c *echo.Context) error {
	return azureACIFail(c, http.StatusBadRequest, "InvalidRequestUri", "invalid Azure AI Search request")
}

func searchNoStore(c *echo.Context) { c.Response().Header().Set("Cache-Control", "no-store") }

// service checks that the service exists in the given subscription and resource group.
func (a searchAccess) service(c *echo.Context, ids searchIDs) bool {
	rec := a.armDo(c.Request().Context(), http.MethodGet, ids.armPath(), nil)
	if rec.Code == http.StatusOK {
		return true
	}

	_ = azureACIFail(c, rec.Code, "ResourceNotFound", "the search service was not found")

	return false
}

type searchKey struct{ name, key string }

func (k searchKey) fingerprint() string {
	sum := sha256.Sum256([]byte(k.key))

	return hex.EncodeToString(sum[:srchPxFPBytes])
}

func searchMask(key string) string {
	if len(key) <= srchPxMaskTail {
		return srchPxMask
	}

	return srchPxMask + key[len(key)-srchPxMaskTail:]
}

func (k searchKey) view() map[string]string {
	return map[string]string{netPxNameKey: k.name, srchPxKeyFP: k.fingerprint(), "masked": searchMask(k.key)}
}

func (a searchAccess) adminKeys(c *echo.Context, ids searchIDs) (string, string, bool) {
	rec := a.armDo(c.Request().Context(), http.MethodPost, ids.armPath("listAdminKeys"), []byte("{}"))
	if rec.Code != http.StatusOK {
		_ = azureACIFail(c, rec.Code, "KeysUnavailable", "the admin keys could not be read")

		return "", "", false
	}

	var doc struct {
		Primary   string `json:"primaryKey"`
		Secondary string `json:"secondaryKey"`
	}
	if json.Unmarshal(rec.Body.Bytes(), &doc, jsonx.V1()) != nil {
		_ = azureACIFail(c, http.StatusBadGateway, "KeysUnavailable", "unreadable admin keys")

		return "", "", false
	}

	return doc.Primary, doc.Secondary, true
}

func (a searchAccess) queryKeys(c *echo.Context, ids searchIDs) ([]searchKey, bool) {
	rec := a.armDo(c.Request().Context(), http.MethodPost, ids.armPath("listQueryKeys"), []byte("{}"))
	if rec.Code != http.StatusOK {
		_ = azureACIFail(c, rec.Code, "KeysUnavailable", "the query keys could not be read")

		return nil, false
	}

	var doc struct {
		Value []struct {
			Name string `json:"name"`
			Key  string `json:"key"`
		} `json:"value"`
	}
	if json.Unmarshal(rec.Body.Bytes(), &doc, jsonx.V1()) != nil {
		_ = azureACIFail(c, http.StatusBadGateway, "KeysUnavailable", "unreadable query keys")

		return nil, false
	}

	out := make([]searchKey, 0, len(doc.Value))
	for _, k := range doc.Value {
		out = append(out, searchKey{name: k.Name, key: k.Key})
	}

	return out, true
}

// listKeys answers masked keys only.
func (a searchAccess) listKeys(c *echo.Context) error {
	ids, ok := searchParams(c)
	if !ok {
		return searchBad(c)
	}

	if !a.service(c, ids) {
		return nil
	}

	primary, secondary, ok := a.adminKeys(c, ids)
	if !ok {
		return nil
	}

	keys, ok := a.queryKeys(c, ids)
	if !ok {
		return nil
	}

	views := make([]map[string]string, 0, len(keys))
	for _, k := range keys {
		views = append(views, k.view())
	}

	searchNoStore(c)

	return c.JSON(http.StatusOK, map[string]any{
		"admin":        map[string]string{"primary": searchMask(primary), "secondary": searchMask(secondary)},
		srchPxKeyQuery: views,
	})
}

// revealAdmin returns the admin key pair. It is the only route that sends them out.
func (a searchAccess) revealAdmin(c *echo.Context) error {
	ids, ok := searchParams(c)
	if !ok {
		return searchBad(c)
	}

	if !a.service(c, ids) {
		return nil
	}

	primary, secondary, ok := a.adminKeys(c, ids)
	if !ok {
		return nil
	}

	searchNoStore(c)

	return c.JSON(http.StatusOK, map[string]string{"primaryKey": primary, "secondaryKey": secondary})
}

func (a searchAccess) createQueryKey(c *echo.Context) error {
	ids, ok := searchParams(c)
	if !ok {
		return searchBad(c)
	}

	var in struct {
		Name string `json:"name"`
	}

	dec := jsontext.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, srchPxMaxBody))

	if json.UnmarshalDecode(dec, &in, jsonx.V1(), json.RejectUnknownMembers(true)) != nil ||
		!srchPxKeyRe.MatchString(in.Name) {
		return azureACIFail(c, http.StatusBadRequest, "InvalidRequestContent", "the key name is invalid")
	}

	if !a.service(c, ids) {
		return nil
	}

	rec := a.armDo(c.Request().Context(), http.MethodPost, ids.armPath("createQueryKey", in.Name), []byte("{}"))
	if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
		return azureACIFail(c, rec.Code, "CreateFailed", "the query key could not be created")
	}

	var made struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	}
	if json.Unmarshal(rec.Body.Bytes(), &made, jsonx.V1()) != nil {
		return azureACIFail(c, http.StatusBadGateway, "CreateFailed", "unreadable query key")
	}

	searchNoStore(c)

	return c.JSON(http.StatusCreated, searchKey{name: made.Name, key: made.Key}.view())
}

func (a searchAccess) keyByFingerprint(c *echo.Context, ids searchIDs) (searchKey, bool) {
	fp := c.Param(srchPxKeyFP)
	if !srchPxFPRe.MatchString(fp) {
		_ = searchBad(c)

		return searchKey{}, false
	}

	if !a.service(c, ids) {
		return searchKey{}, false
	}

	keys, ok := a.queryKeys(c, ids)
	if !ok {
		return searchKey{}, false
	}

	for _, k := range keys {
		if k.fingerprint() == fp {
			return k, true
		}
	}

	_ = azureACIFail(c, http.StatusNotFound, "KeyNotFound", "the query key was not found")

	return searchKey{}, false
}

// revealQueryKey returns one query key value. It is the only route that sends one out.
func (a searchAccess) revealQueryKey(c *echo.Context) error {
	ids, ok := searchParams(c)
	if !ok {
		return searchBad(c)
	}

	k, found := a.keyByFingerprint(c, ids)
	if !found {
		return nil
	}

	searchNoStore(c)

	return c.JSON(http.StatusOK, map[string]string{netPxNameKey: k.name, "key": k.key})
}

func (a searchAccess) deleteQueryKey(c *echo.Context) error {
	ids, ok := searchParams(c)
	if !ok {
		return searchBad(c)
	}

	k, found := a.keyByFingerprint(c, ids)
	if !found {
		return nil
	}

	rec := a.armDo(c.Request().Context(), http.MethodDelete, ids.armPath("deleteQueryKey", k.key), nil)
	if rec.Code >= http.StatusMultipleChoices {
		return azureACIFail(c, rec.Code, "DeleteFailed", "the query key could not be deleted")
	}

	return c.NoContent(http.StatusNoContent)
}

// searchShape turns data plane segments into a shape with item names written as *.
func searchShape(segs []string) string {
	parts := make([]string, len(segs))
	copy(parts, segs)

	if len(parts) > 1 {
		parts[1] = "*"
	}

	return strings.Join(parts, "/")
}

func searchDataAllowed(method string, segs []string) bool {
	if len(segs) == 0 {
		return false
	}

	for _, s := range segs {
		if !srchPxItemRe.MatchString(s) {
			return false
		}
	}

	shape := searchShape(segs)

	switch method {
	case http.MethodGet:
		return slices.Contains(srchPxReadShapes, shape)
	case http.MethodPost:
		return slices.Contains(srchPxPostShapes, shape)
	}

	return false
}

type searchQueryIn struct {
	Top          *int     `json:"top"`
	Skip         *int     `json:"skip"`
	Search       string   `json:"search"`
	Filter       string   `json:"filter"`
	OrderBy      string   `json:"orderby"`
	Select       string   `json:"select"`
	QueryType    string   `json:"queryType"`
	SearchMode   string   `json:"searchMode"`
	SearchFields string   `json:"searchFields"`
	Highlight    string   `json:"highlight"`
	Facets       []string `json:"facets"`
	Count        bool     `json:"count"`
}

func (q searchQueryIn) valid() bool {
	text := []string{q.Search, q.Filter, q.OrderBy, q.Select, q.SearchFields, q.Highlight}
	if slices.ContainsFunc(text, func(s string) bool { return len(s) > srchPxMaxText }) {
		return false
	}

	if len(q.Facets) > srchPxMaxList ||
		slices.ContainsFunc(q.Facets, func(s string) bool { return len(s) > srchPxMaxText }) {
		return false
	}

	if q.Top != nil && (*q.Top < 0 || *q.Top > srchPxMaxTop) ||
		q.Skip != nil && (*q.Skip < 0 || *q.Skip > srchPxMaxSkip) {
		return false
	}

	return slices.Contains([]string{"", "simple", "full"}, q.QueryType) &&
		slices.Contains([]string{"", "any", "all"}, q.SearchMode)
}

// searchQueryBody rebuilds an explorer request from fixed fields; anything else the caller sent is rejected.
func searchQueryBody(raw []byte) ([]byte, bool) {
	var in searchQueryIn

	dec := jsontext.NewDecoder(bytes.NewReader(raw))

	if json.UnmarshalDecode(dec, &in, jsonx.V1(), json.RejectUnknownMembers(true)) != nil || !in.valid() {
		return nil, false
	}

	out := map[string]any{"search": in.Search, "count": in.Count}

	for k, v := range map[string]string{
		"filter": in.Filter, "orderby": in.OrderBy, "select": in.Select, "queryType": in.QueryType,
		"searchMode": in.SearchMode, "searchFields": in.SearchFields, "highlight": in.Highlight,
	} {
		if v != "" {
			out[k] = v
		}
	}

	if len(in.Facets) > 0 {
		out["facets"] = in.Facets
	}

	if in.Top != nil {
		out["top"] = *in.Top
	}

	if in.Skip != nil {
		out["skip"] = *in.Skip
	}

	body, err := json.Marshal(out, jsonx.V1())

	return body, err == nil
}

// searchScrub hides the secret strings of a data plane definition (data sources, skillsets, indexers).
func searchScrub(v any) {
	switch t := v.(type) {
	case []any:
		for _, x := range t {
			searchScrub(x)
		}
	case map[string]any:
		for k, x := range t {
			if _, isStr := x.(string); isStr && slices.Contains(srchPxSecretFields, strings.ToLower(k)) {
				t[k] = srchPxNone

				continue
			}

			searchScrub(x)
		}
	}
}

func (a searchAccess) dataPlane(c *echo.Context) error {
	ids, ok := searchParams(c)
	if !ok {
		return searchBad(c)
	}

	r := c.Request()
	segs := strings.Split(strings.Trim(c.Param("*"), "/"), "/")

	if !searchDataAllowed(r.Method, segs) {
		return azureACIFail(c, http.StatusBadRequest, "InvalidRequestUri", "unsupported Azure AI Search request")
	}

	var body []byte

	isQuery := r.Method == http.MethodPost && strings.HasSuffix(strings.Join(segs, "/"), srchPxDocsSearch)

	if isQuery {
		raw, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, srchPxMaxBody))
		if err != nil {
			return azureACIFail(c, http.StatusBadRequest, "InvalidRequestContent", "the query is too large")
		}

		if body, ok = searchQueryBody(raw); !ok {
			return azureACIFail(c, http.StatusBadRequest, "InvalidRequestContent", "the query is invalid")
		}
	}

	if !a.service(c, ids) {
		return nil
	}

	primary, _, ok := a.adminKeys(c, ids)
	if !ok {
		return nil
	}

	req := httptest.NewRequestWithContext(r.Context(), r.Method,
		"/"+strings.Join(segs, "/")+"?api-version="+srchPxDataVersion, bytes.NewReader(body))
	req.Host = ids.svc + srchPxHostSuffix
	req.Header.Set("Api-Key", primary)

	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	a.data.ServeHTTP(rec, req)

	return searchRelay(c, rec, isQuery)
}

func searchRelay(c *echo.Context, rec *httptest.ResponseRecorder, isQuery bool) error {
	reply := rec.Body.Bytes()
	if len(reply) > srchPxMaxReply {
		return azureACIFail(c, http.StatusBadGateway, "ReplyTooLarge", "the reply is too large")
	}

	if len(reply) == 0 {
		return c.NoContent(rec.Code)
	}

	if !isQuery && rec.Code < http.StatusMultipleChoices {
		var doc any
		if json.Unmarshal(reply, &doc, jsonx.V1()) == nil {
			searchScrub(doc)

			if out, err := json.Marshal(doc, jsonx.V1()); err == nil {
				reply = out
			}
		}
	}

	return c.JSONBlob(rec.Code, reply)
}

// registerSearchAndWebRoutes registers the Azure AI Search and Lambda Web Functions pages.
func (h *DashboardHandler) registerSearchAndWebRoutes() {
	h.registerAzureSearchRoutes()
	h.registerLambdaWebRoutes()
}

package dashboard

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	idbackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

const (
	ociDBPrefix       = ociProxyPrefix + "/database"
	ociDBAPIPrefix    = "/20160918"
	ociDBDatabases    = "autonomousDatabases"
	ociDBBackups      = "autonomousDatabaseBackups"
	ociDBActions      = "actions"
	ociDBWallet       = "wallet"
	ociDBMaxBody      = 1 << 14
	ociDBMaxName      = 255
	ociDBMaxACL       = 64
	ociDBMaxPassword  = 256
	ociDBItemSegs     = 2
	ociDBSubSegs      = 3
	ociDBActionSegs   = 4
	ociDBKeyComp      = "compartmentId"
	ociDBKeyDisplay   = "displayName"
	ociDBKeyLimit     = "limit"
	ociDBKeyPage      = "page"
	ociDBKeyState     = "lifecycleState"
	ociDBKeyPassword  = "password"
	ociDBKeyAdminPass = "adminPassword"
	ociDBKeyACL       = "whitelistedIps"
	ociDBKeyCompute   = "computeCount"
	ociDBKeyStorage   = "dataStorageSizeInTBs"
	ociDBKeyDBID      = "autonomousDatabaseId"
	ociDBWalletFile   = "Wallet_"
)

var (
	ociDBIDRe   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
	ociDBNameRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,29}$`)
	ociDBACLRe  = regexp.MustCompile(`^[A-Za-z0-9.:/_-]{1,255}$`)
	ociDBFileRe = regexp.MustCompile(`[^A-Za-z0-9]`)
)

// ociDBQueryKeys are the only query parameters each listing forwards; everything else is dropped.
//
//nolint:gochecknoglobals // immutable allow-list
var ociDBQueryKeys = map[string][]string{
	ociDBDatabases: {
		ociDBKeyComp, ociDBKeyState, ociDBKeyDisplay, "dbWorkload", "isFreeTier", ociDBKeyLimit, ociDBKeyPage,
		ociDNSKeySortBy, ociDNSKeySortOrd,
	},
	ociDBBackups: {
		ociDBKeyDBID, ociDBKeyComp, ociDBKeyState, ociDBKeyLimit, ociDBKeyPage, ociDNSKeySortBy, ociDNSKeySortOrd,
	},
}

type ociDBCall struct {
	body   func(c *echo.Context) ([]byte, bool)
	path   string
	coll   string
	wallet bool
}

// registerOCIDatabaseRoutes serves the Autonomous Database page by dispatching in-process to the emulator's control
// plane without request signing. Only the database, action, backup and wallet operations the page uses are
// reachable, each write rebuilt from fixed fields. The admin and wallet passwords travel only in the request body
// and are stripped from every answer; the wallet zip is streamed back as an attachment.
func (h *DashboardHandler) registerOCIDatabaseRoutes() {
	var inner *echo.Echo

	ops := h.config.OCIDatabaseOps
	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(inner != nil, "OCI Autonomous Database", next)
	}

	h.SubRouter.Add(http.MethodGet, ociDBPrefix+"/compartments", guard(func(c *echo.Context) error {
		comps := ops.Backend.Compartments()
		if !slices.Contains(comps, idbackend.TenancyID) {
			comps = slices.Insert(comps, 0, idbackend.TenancyID)
		}

		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps})
	}))

	h.SubRouter.Add(http.MethodGet, ociDBPrefix+"/engine", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{"engine": ops.Backend.EngineMode()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()

		parts, ok := ociSplitPath(strings.TrimPrefix(r.URL.EscapedPath(), ociDBPrefix+"/api"))
		if !ok {
			return ociInvalid(c, "unsupported Autonomous Database proxy request")
		}

		call, ok := ociDBResolve(r.Method, parts)
		if !ok {
			return ociInvalid(c, "unsupported Autonomous Database proxy request")
		}

		var body []byte

		if call.body != nil {
			if body, ok = call.body(c); !ok {
				return ociInvalid(c, "invalid Autonomous Database request")
			}
		}

		query := ociArtQuery(r.URL.Query(), ociDBQueryKeys[call.coll])
		r.Header = http.Header{}
		r.URL.RawQuery = query

		if call.wallet {
			return ociDBWalletDownload(c, inner, parts[1], body)
		}

		rec := ociArtForward(c, inner, call.path, body)
		if rec.Code < http.StatusMultipleChoices {
			rec.Body = bytes.NewBuffer(ociDBScrubJSON(rec.Body.Bytes()))
		}

		return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id", "opc-work-request-id")
	})

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		h.SubRouter.Add(m, ociDBPrefix+"/api/*", serve)
	}
}

// ociDBResolve maps a proxy request to its emulator call; false means the request is outside the allow-list.
func ociDBResolve(method string, parts []string) (ociDBCall, bool) {
	if len(parts) > ociDBActionSegs || (len(parts) >= ociDBItemSegs && !ociDBIDOK(parts[1])) {
		return ociDBCall{}, false
	}

	var (
		call ociDBCall
		ok   bool
	)

	switch parts[0] {
	case ociDBDatabases:
		call, ok = ociDBDatabaseCall(method, parts)
	case ociDBBackups:
		call, ok = ociDBBackupCall(method, parts)
	}

	if !ok {
		return ociDBCall{}, false
	}

	call.coll = parts[0]
	call.path = ociDBAPIPrefix + call.path

	return call, true
}

func ociDBDatabaseCall(method string, parts []string) (ociDBCall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociDBCall{path: "/autonomousDatabases"}, true
	case len(parts) == 1 && method == http.MethodPost:
		return ociDBCall{path: "/autonomousDatabases", body: ociDBCreateBody}, true
	case len(parts) == ociDBItemSegs:
		item := "/autonomousDatabases/" + parts[1]

		switch method {
		case http.MethodGet, http.MethodDelete:
			return ociDBCall{path: item}, true
		case http.MethodPut:
			return ociDBCall{path: item, body: ociDBUpdateBody}, true
		}
	case len(parts) == ociDBSubSegs && parts[2] == ociDBWallet && method == http.MethodPost:
		return ociDBCall{
			path: "/autonomousDatabases/" + parts[1] + "/actions/generateWallet", body: ociDBWalletBody, wallet: true,
		}, true
	case len(parts) == ociDBActionSegs && parts[2] == ociDBActions && method == http.MethodPost && ociDBActionOK(parts[3]):
		return ociDBCall{path: "/autonomousDatabases/" + parts[1] + "/actions/" + parts[3]}, true
	}

	return ociDBCall{}, false
}

func ociDBBackupCall(method string, parts []string) (ociDBCall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociDBCall{path: "/autonomousDatabaseBackups"}, true
	case len(parts) == 1 && method == http.MethodPost:
		return ociDBCall{path: "/autonomousDatabaseBackups", body: ociDBBackupBody}, true
	}

	return ociDBCall{}, false
}

func ociDBIDOK(seg string) bool { return ociDBIDRe.MatchString(unescapedPath(seg)) }

func ociDBActionOK(a string) bool { return a == aciPxStart || a == aciPxStop || a == aciPxRestart }

func ociDBACLBad(s string) bool { return !ociDBACLRe.MatchString(s) }

func ociDBText(s string) bool { return s != "" && len(s) <= ociDBMaxName }

func ociDBWorkloadOK(w string) bool { return slices.Contains([]string{"OLTP", "DW", "AJD", "APEX"}, w) }

func ociDBModelOK(m string) bool { return m == "" || m == "ECPU" || m == "OCPU" }

type ociDBCreateIn struct {
	Compute  *float64 `json:"computeCount"`
	Storage  *int     `json:"dataStorageSizeInTBs"`
	FreeTier *bool    `json:"isFreeTier"`
	Mtls     *bool    `json:"isMtlsConnectionRequired"`
	Comp     string   `json:"compartmentId"`
	Display  string   `json:"displayName"`
	DBName   string   `json:"dbName"`
	Workload string   `json:"dbWorkload"`
	Model    string   `json:"computeModel"`
	Password string   `json:"adminPassword"`
}

func (in ociDBCreateIn) valid() bool {
	return ociDBText(in.Comp) && len(in.Display) <= ociDBMaxName && ociDBNameRe.MatchString(in.DBName) &&
		ociDBWorkloadOK(in.Workload) && ociDBModelOK(in.Model) && in.Password != "" &&
		len(in.Password) <= ociDBMaxPassword
}

func (in ociDBCreateIn) build() map[string]any {
	out := map[string]any{
		ociDBKeyComp: in.Comp, "dbName": in.DBName, "dbWorkload": in.Workload, ociDBKeyAdminPass: in.Password,
	}

	if in.Display != "" {
		out[ociDBKeyDisplay] = in.Display
	}

	if in.Model != "" {
		out["computeModel"] = in.Model
	}

	if in.Compute != nil {
		out[ociDBKeyCompute] = *in.Compute
	}

	if in.Storage != nil {
		out[ociDBKeyStorage] = *in.Storage
	}

	if in.FreeTier != nil {
		out["isFreeTier"] = *in.FreeTier
	}

	if in.Mtls != nil {
		out["isMtlsConnectionRequired"] = *in.Mtls
	}

	return out
}

func ociDBCreateBody(c *echo.Context) ([]byte, bool) {
	var in ociDBCreateIn

	if !ociDecodeBodyN(c, &in, ociDBMaxBody) || !in.valid() {
		return nil, false
	}

	return ociArtMarshal(in.build())
}

func ociDBUpdateBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		Display *string   `json:"displayName"`
		ACL     *[]string `json:"whitelistedIps"`
		ACLOn   *bool     `json:"isAccessControlEnabled"`
		Compute *float64  `json:"computeCount"`
	}

	if !ociDecodeBodyN(c, &in, ociDBMaxBody) {
		return nil, false
	}

	out := map[string]any{}

	if in.Display != nil {
		if !ociDBText(*in.Display) {
			return nil, false
		}

		out[ociDBKeyDisplay] = *in.Display
	}

	if in.ACL != nil {
		if len(*in.ACL) > ociDBMaxACL || slices.ContainsFunc(*in.ACL, ociDBACLBad) {
			return nil, false
		}

		out[ociDBKeyACL] = *in.ACL
	}

	if in.ACLOn != nil {
		out["isAccessControlEnabled"] = *in.ACLOn
	}

	if in.Compute != nil {
		out[ociDBKeyCompute] = *in.Compute
	}

	if len(out) == 0 {
		return nil, false
	}

	return ociArtMarshal(out)
}

func ociDBBackupBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		DBID    string `json:"autonomousDatabaseId"`
		Display string `json:"displayName"`
	}

	if !ociDecodeBodyN(c, &in, ociDBMaxBody) || !ociDBIDRe.MatchString(in.DBID) || len(in.Display) > ociDBMaxName {
		return nil, false
	}

	out := map[string]any{ociDBKeyDBID: in.DBID}
	if in.Display != "" {
		out[ociDBKeyDisplay] = in.Display
	}

	return ociArtMarshal(out)
}

func ociDBWalletBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		Password string `json:"password"`
	}

	if !ociDecodeBodyN(c, &in, ociDBMaxBody) || in.Password == "" || len(in.Password) > ociDBMaxPassword {
		return nil, false
	}

	return ociArtMarshal(map[string]any{ociDBKeyPassword: in.Password, "generateType": "ALL"})
}

// ociDBScrubJSON removes password fields from any answer.
func ociDBScrubJSON(raw []byte) []byte {
	if len(bytes.TrimSpace(raw)) == 0 {
		return raw
	}

	var doc any
	if json.Unmarshal(raw, &doc) != nil {
		return []byte("{}")
	}

	ociDBStrip(doc)

	out, err := json.Marshal(doc)
	if err != nil {
		return []byte("{}")
	}

	return out
}

func ociDBStrip(v any) {
	switch t := v.(type) {
	case []any:
		for _, x := range t {
			ociDBStrip(x)
		}
	case map[string]any:
		delete(t, ociDBKeyPassword)
		delete(t, ociDBKeyAdminPass)

		for _, x := range t {
			ociDBStrip(x)
		}
	}
}

// ociDBWalletDownload answers GenerateAutonomousDatabaseWallet as a zip attachment. The wallet password is only
// ever in the forwarded body; failures keep the emulator's JSON error envelope.
func ociDBWalletDownload(c *echo.Context, inner *echo.Echo, id string, body []byte) error {
	c.Request().Method = http.MethodGet

	meta := ociArtForward(c, inner, ociDBAPIPrefix+"/autonomousDatabases/"+id, nil)
	if meta.Code != http.StatusOK {
		return ociRelay(c, meta, "opc-request-id")
	}

	var db struct {
		DBName string `json:"dbName"`
	}

	_ = json.Unmarshal(meta.Body.Bytes(), &db)

	c.Request().Method = http.MethodPost

	rec := ociArtForward(c, inner, ociDBAPIPrefix+"/autonomousDatabases/"+id+"/actions/generateWallet", body)
	if rec.Code != http.StatusOK {
		return ociRelay(c, ociDBScrubRec(rec), "opc-request-id")
	}

	h := c.Response().Header()
	h.Set("Content-Disposition", `attachment; filename="`+ociDBWalletName(db.DBName)+`"`)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "sandbox; default-src 'none'")
	h.Set("Cache-Control", "no-store")

	return c.Blob(http.StatusOK, "application/zip", rec.Body.Bytes())
}

func ociDBScrubRec(rec *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	rec.Body = bytes.NewBuffer(ociDBScrubJSON(rec.Body.Bytes()))

	return rec
}

// ociDBWalletName builds the download file name from the database name, reduced to letters and digits.
func ociDBWalletName(dbName string) string {
	name := ociDBFileRe.ReplaceAllString(dbName, "")
	if name == "" {
		name = "database"
	}

	return ociDBWalletFile + name + ".zip"
}

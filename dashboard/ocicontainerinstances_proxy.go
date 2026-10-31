package dashboard

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v5"

	idbackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

const (
	ociCIPrefix       = ociProxyPrefix + "/containerinstances"
	ociCIAPIPrefix    = "/20210415"
	ociCICoreAPI      = "/20160918"
	ociCIInstances    = "containerInstances"
	ociCIShapes       = "containerInstanceShapes"
	ociCIContainers   = "containers"
	ociCIWorkReqs     = "workRequests"
	ociCIActions      = "actions"
	ociCIRetrieveLogs = "retrieveLogs"
	ociCIErrors       = "errors"
	ociCIMaxBody      = 1 << 16
	ociCILogCap       = 256 << 10
	ociCIMaxContainer = 8
	ociCIMaxEntries   = 64
	ociCIMaxArg       = 1024
	ociCIMaxEnvValue  = 4096
	ociCIMaxName      = 255
	ociCIItemSegs     = 2
	ociCIActionSegs   = 4
	ociCISubSegs      = 3
	ociCIKeyInstance  = "containerInstanceId"
	ociCIKeyOcpus     = "ocpus"
	ociCIKeyMemory    = "memoryInGBs"
	ociCIKeyEnv       = "environmentVariables"
	ociCIKeyEnvNames  = "environmentVariableNames"
	ociCIKeyDisplay   = "displayName"
	ociCIKeyIsPrev    = "isPrevious"
	ociCIKeyState     = "lifecycleState"
	ociCIKeyItems     = "items"
	ociCIKeyName      = "name"
	ociCIKeyLimit     = "limit"
	ociCIKeyPage      = "page"
	ociCIKeyStatus    = "status"
)

var (
	ociCIIDRe    = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
	ociCIEnvNmRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
)

func ociCIRestartOK(p string) bool {
	switch p {
	case "", "ALWAYS", "NEVER", "ON_FAILURE":
		return true
	}

	return false
}

// ociCIQueryKeys are the only query parameters each listing forwards; everything else is dropped.
//
//nolint:gochecknoglobals // immutable allow-list
var ociCIQueryKeys = map[string][]string{
	ociCIInstances:  {ociKeyCompartment, ociCIKeyState, ociCIKeyDisplay, ociCIKeyLimit, ociCIKeyPage},
	ociCIShapes:     {ociKeyCompartment, ociCIKeyLimit, ociCIKeyPage},
	ociCIContainers: {ociKeyCompartment, ociCIKeyInstance, ociCIKeyState, ociCIKeyLimit, ociCIKeyPage},
	ociCIWorkReqs:   {ociKeyCompartment, "resourceId", ociCIKeyStatus, ociCIKeyLimit, ociCIKeyPage},
	ociCIErrors:     {ociCIKeyLimit, ociCIKeyPage},
	ociCIRetrieveLogs: {
		ociCIKeyIsPrev,
	},
}

type ociCICall struct {
	body func(c *echo.Context) ([]byte, bool)
	path string
	coll string
	logs bool
}

// registerOCIContainerInstancesRoutes serves the Container Instances page by dispatching in-process to the
// emulator's control plane without request signing. Only the instance, container, shape, work request and log
// operations the page uses are reachable, each write rebuilt from fixed fields. Environment variable values,
// image pull secret credentials and config file contents are removed from every answer before it leaves.
func (h *DashboardHandler) registerOCIContainerInstancesRoutes() {
	var inner *echo.Echo

	ops := h.config.OCIContainerInstancesOps
	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(inner != nil, "OCI Container Instances", next)
	}

	h.SubRouter.Add(http.MethodGet, ociCIPrefix+"/compartments", guard(func(c *echo.Context) error {
		comps := ops.Backend.Compartments()
		if !slices.Contains(comps, idbackend.TenancyID) {
			comps = slices.Insert(comps, 0, idbackend.TenancyID)
		}

		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps})
	}))

	h.SubRouter.Add(http.MethodGet, ociCIPrefix+"/engine", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{"engine": ops.Backend.EngineMode()})
	}))

	h.SubRouter.Add(http.MethodGet, ociCIPrefix+"/availabilitydomains", guard(h.ociCIAvailabilityDomains))
	h.SubRouter.Add(http.MethodGet, ociCIPrefix+"/subnets", guard(h.ociCISubnets))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()

		parts, ok := ociSplitPath(strings.TrimPrefix(r.URL.EscapedPath(), ociCIPrefix+"/api"))
		if !ok {
			return ociInvalid(c, "unsupported Container Instances proxy request")
		}

		call, ok := ociCIResolve(r.Method, parts)
		if !ok {
			return ociInvalid(c, "unsupported Container Instances proxy request")
		}

		var body []byte

		if call.body != nil {
			if body, ok = call.body(c); !ok {
				return ociInvalid(c, "invalid Container Instances request")
			}
		}

		r.Header.Del("Authorization")
		r.URL.RawQuery = ociCIQuery(call, r.URL.Query())

		rec := ociArtForward(c, inner, call.path, body)

		if call.logs {
			return ociCILogs(c, rec)
		}

		if rec.Code < http.StatusMultipleChoices {
			rec.Body = bytes.NewBuffer(ociCIScrubJSON(rec.Body.Bytes()))
		}

		return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id", "opc-work-request-id")
	})

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		h.SubRouter.Add(m, ociCIPrefix+"/api/*", serve)
	}
}

func ociCIQuery(call ociCICall, in url.Values) string {
	keys := ociCIQueryKeys[call.coll]
	if call.logs {
		keys = ociCIQueryKeys[ociCIRetrieveLogs]
	}

	return ociArtQuery(in, keys)
}

// ociCIResolve maps a proxy request to its emulator call; false means the request is outside the allow-list.
func ociCIResolve(method string, parts []string) (ociCICall, bool) {
	if len(parts) > ociCIActionSegs || (len(parts) >= ociCIItemSegs && !ociCIIDOK(parts[1])) {
		return ociCICall{}, false
	}

	var (
		call ociCICall
		ok   bool
	)

	switch parts[0] {
	case ociCIInstances:
		call, ok = ociCIInstanceCall(method, parts)
	case ociCIShapes:
		call, ok = ociCICall{path: "/containerInstanceShapes"}, method == http.MethodGet && len(parts) == 1
	case ociCIContainers:
		call, ok = ociCIContainerCall(method, parts)
	case ociCIWorkReqs:
		call, ok = ociCIWorkReqCall(method, parts)
	}

	call.coll = parts[0]
	if len(parts) == ociCISubSegs && parts[2] == ociCIErrors {
		call.coll = ociCIErrors
	}

	call.path = ociCIAPIPrefix + call.path

	return call, ok
}

func ociCIInstanceCall(method string, parts []string) (ociCICall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociCICall{path: "/containerInstances"}, true
	case len(parts) == 1 && method == http.MethodPost:
		return ociCICall{path: "/containerInstances", body: ociCICreateBody}, true
	case len(parts) == ociCIItemSegs:
		item := "/containerInstances/" + parts[1]

		switch method {
		case http.MethodGet, http.MethodDelete:
			return ociCICall{path: item}, true
		case http.MethodPut:
			return ociCICall{path: item, body: ociCIUpdateBody}, true
		}
	case len(parts) == ociCISubSegs+1 && method == http.MethodPost && parts[2] == ociCIActions &&
		ociCIActionOK(parts[3]):
		return ociCICall{path: "/containerInstances/" + parts[1] + "/actions/" + parts[3]}, true
	}

	return ociCICall{}, false
}

func ociCIContainerCall(method string, parts []string) (ociCICall, bool) {
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return ociCICall{path: "/containers"}, true
	case len(parts) == ociCIItemSegs && method == http.MethodGet:
		return ociCICall{path: "/containers/" + parts[1]}, true
	case len(parts) == ociCISubSegs+1 && method == http.MethodPost && parts[2] == ociCIActions &&
		parts[3] == ociCIRetrieveLogs:
		return ociCICall{path: "/containers/" + parts[1] + "/actions/retrieveLogs", logs: true}, true
	}

	return ociCICall{}, false
}

func ociCIWorkReqCall(method string, parts []string) (ociCICall, bool) {
	if method != http.MethodGet {
		return ociCICall{}, false
	}

	switch {
	case len(parts) == 1:
		return ociCICall{path: "/workRequests"}, true
	case len(parts) == ociCIItemSegs:
		return ociCICall{path: "/workRequests/" + parts[1]}, true
	case len(parts) == ociCISubSegs && parts[2] == ociCIErrors:
		return ociCICall{path: "/workRequests/" + parts[1] + "/errors"}, true
	}

	return ociCICall{}, false
}

func ociCIIDOK(seg string) bool { return ociCIIDRe.MatchString(unescapedPath(seg)) }

func ociCIActionOK(a string) bool { return a == aciPxStart || a == aciPxStop || a == "restart" }

func ociCIName(s string) bool { return s != "" && len(s) <= ociCIMaxName }

func ociCIStrings(in []string) bool {
	return len(in) <= ociCIMaxEntries && !slices.ContainsFunc(in, func(s string) bool { return len(s) > ociCIMaxArg })
}

type ociCIContainerIn struct {
	Env      map[string]string `json:"environmentVariables"`
	Resource *struct {
		Vcpus *float64 `json:"vcpusLimit"`
		Mem   *float64 `json:"memoryLimitInGBs"`
	} `json:"resourceConfig"`
	ImageURL    string   `json:"imageUrl"`
	DisplayName string   `json:"displayName"`
	Command     []string `json:"command"`
	Arguments   []string `json:"arguments"`
}

// build returns the rebuilt wire container, or false when a field is out of bounds.
func (in ociCIContainerIn) build() (map[string]any, bool) {
	if !ociCIName(in.ImageURL) || len(in.DisplayName) > ociCIMaxName || !ociCIStrings(in.Command) ||
		!ociCIStrings(in.Arguments) || len(in.Env) > ociCIMaxEntries {
		return nil, false
	}

	out := map[string]any{"imageUrl": in.ImageURL}
	if in.DisplayName != "" {
		out[ociCIKeyDisplay] = in.DisplayName
	}

	if len(in.Command) > 0 {
		out["command"] = in.Command
	}

	if len(in.Arguments) > 0 {
		out["arguments"] = in.Arguments
	}

	if len(in.Env) > 0 {
		if !ociCIEnvOK(in.Env) {
			return nil, false
		}

		out[ociCIKeyEnv] = in.Env
	}

	if rc := in.resource(); rc != nil {
		out["resourceConfig"] = rc
	}

	return out, true
}

func (in ociCIContainerIn) resource() map[string]any {
	if in.Resource == nil {
		return nil
	}

	rc := map[string]any{}
	if in.Resource.Vcpus != nil {
		rc["vcpusLimit"] = *in.Resource.Vcpus
	}

	if in.Resource.Mem != nil {
		rc["memoryLimitInGBs"] = *in.Resource.Mem
	}

	if len(rc) == 0 {
		return nil
	}

	return rc
}

func ociCIEnvOK(env map[string]string) bool {
	for k, v := range env {
		if !ociCIEnvNmRe.MatchString(k) || len(v) > ociCIMaxEnvValue {
			return false
		}
	}

	return true
}

type ociCICreateIn struct {
	ShapeConfig *struct {
		Ocpus  *float64 `json:"ocpus"`
		Memory *float64 `json:"memoryInGBs"`
	} `json:"shapeConfig"`
	Graceful    *int64             `json:"gracefulShutdownTimeoutInSeconds"`
	Compartment string             `json:"compartmentId"`
	AD          string             `json:"availabilityDomain"`
	DisplayName string             `json:"displayName"`
	Shape       string             `json:"shape"`
	Restart     string             `json:"containerRestartPolicy"`
	Containers  []ociCIContainerIn `json:"containers"`
	Vnics       []struct {
		SubnetID string `json:"subnetId"`
	} `json:"vnics"`
}

func (in *ociCICreateIn) valid() bool {
	return ociCIName(in.Compartment) && ociCIName(in.Shape) && ociCIName(in.AD) &&
		len(in.DisplayName) <= ociCIMaxName && ociCIRestartOK(in.Restart) &&
		in.ShapeConfig != nil && in.ShapeConfig.Ocpus != nil &&
		len(in.Containers) > 0 && len(in.Containers) <= ociCIMaxContainer &&
		len(in.Vnics) == 1 && ociCIName(in.Vnics[0].SubnetID)
}

func (in *ociCICreateIn) build() (map[string]any, bool) {
	sc := map[string]any{ociCIKeyOcpus: *in.ShapeConfig.Ocpus}
	if in.ShapeConfig.Memory != nil {
		sc[ociCIKeyMemory] = *in.ShapeConfig.Memory
	}

	ctrs := make([]map[string]any, 0, len(in.Containers))

	for _, ci := range in.Containers {
		built, ok := ci.build()
		if !ok {
			return nil, false
		}

		ctrs = append(ctrs, built)
	}

	out := map[string]any{
		ociKeyCompartment: in.Compartment, "availabilityDomain": in.AD, "shape": in.Shape, "shapeConfig": sc,
		"containers": ctrs, "vnics": []map[string]any{{"subnetId": in.Vnics[0].SubnetID}},
	}

	if in.DisplayName != "" {
		out[ociCIKeyDisplay] = in.DisplayName
	}

	if in.Restart != "" {
		out["containerRestartPolicy"] = in.Restart
	}

	if in.Graceful != nil && *in.Graceful >= 0 {
		out["gracefulShutdownTimeoutInSeconds"] = *in.Graceful
	}

	return out, true
}

func ociCICreateBody(c *echo.Context) ([]byte, bool) {
	var in ociCICreateIn

	if !ociDecodeBodyN(c, &in, ociCIMaxBody) || !in.valid() {
		return nil, false
	}

	out, ok := in.build()
	if !ok {
		return nil, false
	}

	return ociArtMarshal(out)
}

func ociCIUpdateBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		DisplayName *string `json:"displayName"`
	}

	if !ociDecodeBody(c, &in) || in.DisplayName == nil || !ociCIName(*in.DisplayName) {
		return nil, false
	}

	return ociArtMarshal(map[string]any{ociCIKeyDisplay: *in.DisplayName})
}

// ociCIScrubJSON removes secret-bearing fields from any answer: environment values become a sorted name list,
// config file data becomes its size, pull secrets keep only type, registry and vault secret id, and health check
// header values are dropped.
func ociCIScrubJSON(raw []byte) []byte {
	if len(bytes.TrimSpace(raw)) == 0 {
		return raw
	}

	var doc any
	if json.Unmarshal(raw, &doc) != nil {
		return []byte("{}")
	}

	ociCIScrub(doc)

	out, err := json.Marshal(doc)
	if err != nil {
		return []byte("{}")
	}

	return out
}

func ociCIScrub(v any) {
	switch t := v.(type) {
	case []any:
		for _, x := range t {
			ociCIScrub(x)
		}
	case map[string]any:
		ociCIScrubMap(t)

		for _, x := range t {
			ociCIScrub(x)
		}
	}
}

func ociCIScrubMap(m map[string]any) {
	if env, ok := m[ociCIKeyEnv].(map[string]any); ok {
		names := make([]string, 0, len(env))
		for k := range env {
			names = append(names, k)
		}

		slices.Sort(names)
		m[ociCIKeyEnvNames] = names
	}

	delete(m, ociCIKeyEnv)
	delete(m, "username")
	delete(m, "password")

	if cfgs, ok := m["configs"].([]any); ok {
		for i, f := range cfgs {
			cfgs[i] = ociCIConfigSummary(f)
		}
	}

	if hs, ok := m["headers"].([]any); ok {
		for i, f := range hs {
			fm, _ := f.(map[string]any)
			hs[i] = map[string]any{ociCIKeyName: fm[ociCIKeyName]}
		}
	}

	if secrets, ok := m["imagePullSecrets"].([]any); ok {
		for i, s := range secrets {
			sm, _ := s.(map[string]any)
			kept := map[string]any{}

			for _, k := range []string{"secretType", "registryEndpoint", "secretId"} {
				if val, has := sm[k]; has {
					kept[k] = val
				}
			}

			secrets[i] = kept
		}
	}
}

func ociCIConfigSummary(f any) map[string]any {
	fm, _ := f.(map[string]any)
	data, _ := fm["data"].(string)

	size := len(data)
	if dec, err := base64.StdEncoding.DecodeString(data); err == nil {
		size = len(dec)
	}

	out := map[string]any{"sizeInBytes": size}
	for _, k := range []string{"fileName", "path"} {
		if v, ok := fm[k]; ok {
			out[k] = v
		}
	}

	return out
}

// ociCILogs answers RetrieveLogs as size-capped plain text; failures keep the emulator's JSON error envelope.
func ociCILogs(c *echo.Context, rec *httptest.ResponseRecorder) error {
	if rec.Code != http.StatusOK {
		return ociRelay(c, rec, "opc-request-id")
	}

	logs := rec.Body.Bytes()
	total := len(logs)

	h := c.Response().Header()
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "sandbox; default-src 'none'")
	h.Set("Cache-Control", "no-store")

	if total > ociCILogCap {
		logs = logs[total-ociCILogCap:]
		for len(logs) > 0 && !utf8.RuneStart(logs[0]) {
			logs = logs[1:]
		}

		h.Set("X-Logs-Truncated", "true")
	}

	h.Set("X-Logs-Total-Bytes", strconv.Itoa(total))

	return c.Blob(http.StatusOK, "text/plain; charset=utf-8", logs)
}

func (h *DashboardHandler) ociCIAvailabilityDomains(c *echo.Context) error {
	items := []any{}

	if ops := h.config.OCIIdentityOps; ops != nil {
		ads, err := ops.Backend.ListAvailabilityDomains(idbackend.TenancyID)
		if err != nil {
			return ociFail(c, err)
		}

		for _, ad := range ads {
			items = append(items, map[string]any{ociCIKeyName: ad.Name})
		}
	}

	return c.JSON(http.StatusOK, map[string]any{ociCIKeyItems: items})
}

// ociCISubnets lists the subnets of one compartment from OCI Core, read only, reduced to what a picker needs.
func (h *DashboardHandler) ociCISubnets(c *echo.Context) error {
	ops := h.config.OCICoreOps
	comp := c.QueryParam(ociKeyCompartment)

	if ops == nil || !ociCIName(comp) {
		return c.JSON(http.StatusOK, map[string]any{ociCIKeyItems: []any{}})
	}

	inner := echo.New()
	inner.Any("/*", ops.Trusted())

	r := c.Request()
	fwd := r.Clone(r.Context())
	fwd.URL.Path, fwd.URL.RawPath = ociCICoreAPI+"/subnets", ""
	fwd.URL.RawQuery = url.Values{ociKeyCompartment: {comp}, ociCIKeyLimit: {"1000"}}.Encode()
	fwd.RequestURI, fwd.Method, fwd.Body, fwd.ContentLength = "", http.MethodGet, http.NoBody, 0
	fwd.Header = fwd.Header.Clone()
	fwd.Header.Del("Authorization")

	rec := httptest.NewRecorder()
	inner.ServeHTTP(rec, fwd)

	if rec.Code != http.StatusOK {
		return c.JSON(http.StatusOK, map[string]any{ociCIKeyItems: []any{}})
	}

	var subnets []map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &subnets) != nil {
		return c.JSON(http.StatusOK, map[string]any{ociCIKeyItems: []any{}})
	}

	items := make([]any, 0, len(subnets))

	for _, s := range subnets {
		items = append(items, map[string]any{
			"id": s["id"], ociCIKeyDisplay: s[ociCIKeyDisplay], "cidrBlock": s["cidrBlock"],
			"vcnId": s["vcnId"], ociCIKeyState: s[ociCIKeyState],
		})
	}

	return c.JSON(http.StatusOK, map[string]any{ociCIKeyItems: items})
}

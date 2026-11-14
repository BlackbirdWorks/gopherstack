package dashboard

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	functionsbackend "github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

const (
	azureFunctionsMasterKind  = "masterKey"
	azureFunctionsMasterName  = "_master"
	azureFunctionsKeyHeader   = "X-Functions-Key"
	azureFunctionsInvokeLimit = 1 << 20
	azureFunctionsInvokeTTL   = 60 * time.Second
	azureFunctionsAPIPrefix   = "/api/"
)

type azureFunctionKeyJSON struct {
	Kind  string `json:"kind"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

type azureFunctionInvokeIn struct {
	Function    string `json:"function"`
	Method      string `json:"method"`
	Query       string `json:"query"`
	Body        string `json:"body"`
	ContentType string `json:"contentType"`
}

type azureFunctionInvokeOut struct {
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
	Status  int               `json:"status"`
}

func (a *azureFunctionsAPI) hostKeys(c *echo.Context) error {
	keys, err := a.b.HostKeys(c.Param("name"))
	if err != nil {
		return a.fail(c, err)
	}

	items := []azureFunctionKeyJSON{{
		Kind: azureFunctionsMasterKind, Name: azureFunctionsMasterName, Value: azureFunctionsMask,
	}}
	items = appendMaskedKeys(items, functionsbackend.KeyTypeFunction, keys.FunctionKeys)
	items = appendMaskedKeys(items, functionsbackend.KeyTypeSystem, keys.SystemKeys)

	return c.JSON(http.StatusOK, map[string]any{"keys": items})
}

func appendMaskedKeys(items []azureFunctionKeyJSON, kind string, set map[string]string) []azureFunctionKeyJSON {
	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}

	slices.Sort(names)

	for _, n := range names {
		items = append(items, azureFunctionKeyJSON{Kind: kind, Name: n, Value: azureFunctionsMask})
	}

	return items
}

func (a *azureFunctionsAPI) revealKey(c *echo.Context) error {
	var in struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}

	if !cloudJSONDecode(c, &in) {
		return nil
	}

	keys, err := a.b.HostKeys(c.Param("name"))
	if err != nil {
		return a.fail(c, err)
	}

	var (
		value string
		ok    bool
	)

	switch in.Kind {
	case azureFunctionsMasterKind:
		value, ok = keys.MasterKey, true
	case functionsbackend.KeyTypeFunction:
		value, ok = keys.FunctionKeys[in.Name]
	case functionsbackend.KeyTypeSystem:
		value, ok = keys.SystemKeys[in.Name]
	}

	if !ok {
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, "key not found")
	}

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.JSON(http.StatusOK, azureFunctionKeyJSON{Kind: in.Kind, Name: in.Name, Value: value})
}

// bufferedResponse captures a handler's reply with the body capped.
type bufferedResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (r *bufferedResponse) Header() http.Header { return r.header }

func (r *bufferedResponse) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
}

func (r *bufferedResponse) Write(p []byte) (int, error) {
	r.WriteHeader(http.StatusOK)

	if room := azureFunctionsInvokeLimit - r.body.Len(); room > 0 {
		r.body.Write(p[:min(len(p), room)])
	}

	return len(p), nil
}

func invokeMethodAllowed(m string) bool {
	switch m {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}

	return false
}

// invoke calls an HTTP-triggered function through the app's own emulator host. The path comes from the
// function's route, never from the request, so the call cannot reach any other app or URL.
func (a *azureFunctionsAPI) invoke(c *echo.Context) error {
	var in azureFunctionInvokeIn

	if !cloudJSONDecode(c, &in) {
		return nil
	}

	in.Method = strings.ToUpper(in.Method)
	if !invokeMethodAllowed(in.Method) {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "unsupported method")
	}

	name := c.Param("name")

	ctx, cancel := context.WithTimeout(c.Request().Context(), azureFunctionsInvokeTTL)
	defer cancel()

	fn, err := a.b.Function(ctx, name, in.Function)
	if err != nil {
		return a.fail(c, err)
	}

	target, err := url.Parse(fn.InvokeURL)
	if err != nil || fn.InvokeURL == "" || !strings.HasPrefix(target.Path, azureFunctionsAPIPrefix) {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "function has no HTTP trigger")
	}

	keys, err := a.b.HostKeys(name)
	if err != nil {
		return a.fail(c, err)
	}

	req, err := http.NewRequestWithContext(ctx, in.Method, "http://"+a.b.HostName(name)+target.Path,
		strings.NewReader(in.Body))
	if err != nil {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "invalid request")
	}

	req.URL.RawQuery = strings.TrimPrefix(in.Query, "?")
	req.Host = a.b.HostName(name)
	req.Header.Set(azureFunctionsKeyHeader, keys.MasterKey)

	if in.ContentType != "" && !strings.ContainsAny(in.ContentType, "\r\n") {
		req.Header.Set("Content-Type", in.ContentType)
	}

	rec := &bufferedResponse{header: http.Header{}}
	a.b.ServeHTTP(rec, req)

	out := azureFunctionInvokeOut{Status: max(rec.status, http.StatusOK), Headers: map[string]string{}}
	for k := range rec.header {
		out.Headers[k] = rec.header.Get(k)
	}

	body, _ := io.ReadAll(&rec.body)
	out.Body = string(body)

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.JSON(http.StatusOK, out)
}

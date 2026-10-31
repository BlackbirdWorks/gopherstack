package stepfunctions

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const traceMask = "***"

type httpTraceKey struct{}

// httpTrace collects the HTTP Task request and response for a TestState call
// at inspection level TRACE (types.InspectionDataRequest/Response).
type httpTrace struct {
	request, response map[string]string
	secrets           []string
	reveal            bool
}

func withHTTPTrace(ctx context.Context, reveal bool) (context.Context, *httpTrace) {
	tr := &httpTrace{reveal: reveal}

	return context.WithValue(ctx, httpTraceKey{}, tr), tr
}

func httpTraceFrom(ctx context.Context) *httpTrace {
	tr, _ := ctx.Value(httpTraceKey{}).(*httpTrace)

	return tr
}

func connectionSecrets(c ConnectionCredentials) []string {
	secrets := []string{c.APIKeyValue, c.Password}

	if c.AuthType == authBasic {
		secrets = append(secrets, base64.StdEncoding.EncodeToString([]byte(c.Username+":"+c.Password)))
	}

	if c.OAuth != nil {
		secrets = append(secrets, c.OAuth.ClientSecret)
	}

	for _, group := range [][]NameValue{c.Headers, c.Query, c.Body} {
		for _, nv := range group {
			secrets = append(secrets, nv.Value)
		}
	}

	return secrets
}

func (t *httpTrace) scrub(s string) string {
	if t.reveal {
		return s
	}

	for _, secret := range t.secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, traceMask)
		}
	}

	return s
}

func (t *httpTrace) headers(h http.Header, dropAuth string) string {
	flat := make(map[string]string, len(h))

	for k, vs := range h {
		if !t.reveal && (strings.EqualFold(k, "Authorization") || strings.EqualFold(k, dropAuth)) {
			continue
		}

		flat[k] = t.scrub(strings.Join(vs, ","))
	}

	raw, _ := json.Marshal(flat)

	return string(raw)
}

func (t *httpTrace) recordRequest(req *http.Request, creds ConnectionCredentials) {
	t.secrets = connectionSecrets(creds)
	if creds.AuthType == authOAuth || creds.AuthType == authBasic {
		t.secrets = append(t.secrets, strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "))
	}

	var body string

	if req.GetBody != nil {
		if rc, err := req.GetBody(); err == nil {
			raw, _ := io.ReadAll(rc)
			body = string(raw)
		}
	}

	dropName := ""
	if creds.AuthType == authAPIKey {
		dropName = creds.APIKeyName
	}

	t.request = map[string]string{
		"protocol": req.Proto,
		"method":   req.Method,
		"url":      t.scrub(req.URL.String()),
		"headers":  t.headers(req.Header, dropName),
		"body":     t.scrub(body),
	}
}

func (t *httpTrace) recordResponse(resp *http.Response, data []byte) {
	status := strings.TrimSpace(strings.TrimPrefix(resp.Status, strconv.Itoa(resp.StatusCode)))

	t.response = map[string]string{
		"protocol":      resp.Proto,
		"statusCode":    strconv.Itoa(resp.StatusCode),
		"statusMessage": status,
		"headers":       t.headers(resp.Header, ""),
		"body":          t.scrub(string(data)),
	}
}

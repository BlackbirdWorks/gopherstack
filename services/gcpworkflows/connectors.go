package gcpworkflows

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

const (
	svcPubSub        = "pubsub"
	svcSecretManager = "secretmanager"
	svcFirestore     = "firestore"
	svcStorage       = "storage"
	maxConnectorBody = 8 << 20
	bodyArg          = "body"
)

// connDef maps one connector onto a REST method of an emulator. {+name} placeholders take a resource name
// whose slashes are kept, {name} placeholders are escaped as one path segment; every other argument is a
// query parameter, and body is the JSON request body.
type connDef struct {
	method  string
	service string
	path    string
}

const (
	pathName   = "/v1/{+name}"
	pathColl   = "/v1/{+parent}/{collectionId}"
	pathBucket = "/storage/v1/b/{bucket}"
	pathObject = pathBucket + "/o/{object}"
	smPrefix   = "googleapis.secretmanager.v1.projects.secrets."
	psPrefix   = "googleapis.pubsub.v1.projects.topics."
	fsPrefix   = "googleapis.firestore.v1.projects.databases.documents."
	storageV1  = "googleapis.storage.v1."
)

func connectors() map[string]connDef {
	const (
		get   = http.MethodGet
		post  = http.MethodPost
		del   = http.MethodDelete
		patch = http.MethodPatch
	)

	return map[string]connDef{
		psPrefix + "publish": {post, svcPubSub, "/v1/{+topic}:publish"},
		psPrefix + "create":  {http.MethodPut, svcPubSub, pathName},
		psPrefix + "get":     {get, svcPubSub, "/v1/{+topic}"},
		psPrefix + "delete":  {del, svcPubSub, "/v1/{+topic}"},

		smPrefix + "create":          {post, svcSecretManager, "/v1/{+parent}/secrets"},
		smPrefix + "get":             {get, svcSecretManager, pathName},
		smPrefix + "delete":          {del, svcSecretManager, pathName},
		smPrefix + "addVersion":      {post, svcSecretManager, "/v1/{+parent}:addVersion"},
		smPrefix + "versions.access": {get, svcSecretManager, "/v1/{+name}:access"},
		smPrefix + "versions.get":    {get, svcSecretManager, pathName},
		fsPrefix + "get":             {get, svcFirestore, pathName},
		fsPrefix + "list":            {get, svcFirestore, pathColl},
		fsPrefix + "patch":           {patch, svcFirestore, pathName},
		fsPrefix + "delete":          {del, svcFirestore, pathName},
		fsPrefix + "createDocument":  {post, svcFirestore, pathColl},
		storageV1 + "buckets.get":    {get, svcStorage, pathBucket},
		storageV1 + "buckets.insert": {post, svcStorage, "/storage/v1/b"},
		storageV1 + "objects.get":    {get, svcStorage, pathObject},
		storageV1 + "objects.list":   {get, svcStorage, pathBucket + "/o"},
		storageV1 + "objects.delete": {del, svcStorage, pathObject},
		storageV1 + "objects.insert": {post, svcStorage, "/upload/storage/v1/b/{bucket}/o"},
	}
}

func isConnector(name string) bool {
	_, ok := connectors()[name]

	return ok
}

var errMissingArg = errors.New("missing string argument")

var placeholderRe = regexp.MustCompile(`\{(\+?)([A-Za-z]+)\}`)

func expandPath(tmpl string, args map[string]any) (string, map[string]any, error) {
	rest := maps.Clone(args)

	var perr error

	out := placeholderRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		sub := placeholderRe.FindStringSubmatch(m)
		v, ok := rest[sub[2]].(string)

		if !ok || v == "" {
			perr = fmt.Errorf("%w %q", errMissingArg, sub[2])

			return ""
		}

		delete(rest, sub[2])

		if sub[1] == "+" {
			segs := strings.Split(v, "/")
			for i, s := range segs {
				segs[i] = url.PathEscape(s)
			}

			return strings.Join(segs, "/")
		}

		return url.PathEscape(v)
	})

	if perr != nil {
		return "", nil, perr
	}

	return out, rest, nil
}

func (b *InMemoryBackend) callConnector(
	ctx context.Context, info runInfo, name string, args map[string]any,
) (any, bool, error) {
	def, ok := connectors()[name]
	if !ok {
		return nil, false, nil
	}

	b.mu.RLock("callConnector")
	eps, client, clock := b.endpoints, b.client, b.clock
	b.mu.RUnlock()

	var base string
	if eps != nil {
		base = eps.BaseURL(def.service)
	}

	if base == "" {
		return nil, true, wfinterp.NewError(
			fmt.Sprintf(
				"%s is not available: no %s emulator is connected",
				name,
				def.service,
			),
			wfinterp.TagOperationError,
		)
	}

	path, rest, err := expandPath(def.path, args)
	if err != nil {
		return nil, true, wfinterp.NewError(fmt.Sprintf("%s: %v", name, err), wfinterp.TagTypeError)
	}

	req, err := buildConnectorRequest(ctx, def, base+path, name, rest)
	if err != nil {
		return nil, true, err
	}

	req.Header.Set("Authorization", "Bearer "+gcpdispatch.OAuthToken(accountEmail(info.account), ""))
	_ = clock

	res, err := doConnector(client, req)

	return res, true, err
}

func buildConnectorRequest(
	ctx context.Context,
	def connDef,
	target, name string,
	rest map[string]any,
) (*http.Request, error) {
	q := url.Values{}
	if def.service == svcStorage && strings.HasPrefix(def.path, "/upload/") {
		q.Set("uploadType", "media")
	}

	var payload any

	for k, v := range rest {
		if k == bodyArg {
			payload = v

			continue
		}

		if l, isList := v.([]any); isList {
			for _, e := range l {
				q.Add(k, fmt.Sprint(e))
			}

			continue
		}

		q.Set(k, fmt.Sprint(v))
	}

	var (
		body  io.Reader
		ctype string
	)

	switch p := payload.(type) {
	case nil:
	case string:
		body, ctype = strings.NewReader(p), "text/plain; charset=utf-8"
	case []byte:
		body, ctype = bytes.NewReader(p), "application/octet-stream"
	default:
		raw, err := wfinterp.ToJSON(p)
		if err != nil {
			return nil, wfinterp.NewError(fmt.Sprintf("%s: body: %v", name, err), wfinterp.TagValueError)
		}

		body, ctype = bytes.NewReader(raw), "application/json"
	}

	if len(q) > 0 {
		target += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, def.method, target, body)
	if err != nil {
		return nil, wfinterp.NewError(fmt.Sprintf("%s: %v", name, err), wfinterp.TagValueError)
	}

	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}

	return req, nil
}

func doConnector(client *http.Client, req *http.Request) (any, error) {
	resp, err := client.Do(req)
	if err != nil {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}

		return nil, wfinterp.NewError("connector connection failed: "+err.Error(), wfinterp.TagConnectionFailed)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxConnectorBody))
	if err != nil {
		return nil, wfinterp.NewError("connector read failed: "+err.Error(), wfinterp.TagConnectionError)
	}

	var body any

	switch ct := resp.Header.Get("Content-Type"); {
	case len(raw) == 0:
		body = map[string]any{}
	case strings.Contains(ct, "json"):
		if body, err = wfinterp.ParseJSON(raw); err != nil {
			return nil, wfinterp.NewError("connector returned invalid JSON", wfinterp.TagResponseTypeError)
		}
	case strings.HasPrefix(ct, "text/"):
		body = string(raw)
	default:
		body = raw
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, wfinterp.NewHTTPError(resp.StatusCode, resp.Header, body, errorMessage(body))
	}

	return body, nil
}

func errorMessage(body any) string {
	m, _ := body.(map[string]any)
	e, _ := m["error"].(map[string]any)
	msg, _ := e["message"].(string)

	return msg
}

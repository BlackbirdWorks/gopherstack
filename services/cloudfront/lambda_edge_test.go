package cloudfront_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

const (
	edgeAccount = "123456789012"
	edgeARNBase = "arn:aws:lambda:us-east-1:" + edgeAccount + ":function:"
)

var errFakeEdge = errors.New("fake edge failure")

type edgeFn func(cf map[string]any) (any, string, error)

type fakeEdge struct {
	fns    map[string]edgeFn
	events map[string][]map[string]any
	calls  map[string]int
	mu     sync.Mutex
	absent bool
}

func newFakeEdge(fns map[string]edgeFn) *fakeEdge {
	return &fakeEdge{fns: fns, events: map[string][]map[string]any{}, calls: map[string]int{}}
}

func (f *fakeEdge) InvokeEdge(_ context.Context, name, version string, payload []byte) ([]byte, string, error) {
	var in struct {
		Records []struct {
			CF map[string]any `json:"cf"`
		} `json:"Records"`
	}

	if err := json.Unmarshal(payload, &in); err != nil || len(in.Records) != 1 {
		return nil, "", fmt.Errorf("%w: bad event", errFakeEdge)
	}

	if version != "1" {
		return nil, "", fmt.Errorf("%w: unexpected version %s", errFakeEdge, version)
	}

	var snapshot struct {
		Records []struct {
			CF map[string]any `json:"cf"`
		} `json:"Records"`
	}

	_ = json.Unmarshal(payload, &snapshot)

	f.mu.Lock()
	f.calls[name]++
	f.events[name] = append(f.events[name], snapshot.Records[0].CF)
	fn := f.fns[name]
	f.mu.Unlock()

	out, fnErr, err := fn(in.Records[0].CF)
	if err != nil || fnErr != "" {
		return nil, fnErr, err
	}

	b, _ := json.Marshal(out)

	return b, "", nil
}

func (f *fakeEdge) EdgeVersionExists(_, version string) bool { return !f.absent && version == "1" }

func (f *fakeEdge) count(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls[name]
}

func (f *fakeEdge) lastEvent(t *testing.T, name string) map[string]any {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.NotEmpty(t, f.events[name])

	return f.events[name][len(f.events[name])-1]
}

func hv(v string) []any { return []any{map[string]any{"value": v}} }

func sub(cf map[string]any, key string) map[string]any {
	m, _ := cf[key].(map[string]any)

	return m
}

func hdrs(m map[string]any) map[string]any {
	h, _ := m["headers"].(map[string]any)

	return h
}

func passRequest(cf map[string]any) (any, string, error) { return sub(cf, "request"), "", nil }

func passResponse(cf map[string]any) (any, string, error) { return sub(cf, "response"), "", nil }

func edgeAssoc(name string, event types.EventType, includeBody bool) types.LambdaFunctionAssociation {
	return types.LambdaFunctionAssociation{
		EventType: event, LambdaFunctionARN: aws.String(edgeARNBase + name + ":1"), IncludeBody: aws.Bool(includeBody),
	}
}

func edgeBehavior(cachePolicy string, assocs ...types.LambdaFunctionAssociation) types.DefaultCacheBehavior {
	def := behavior("api", cachePolicy)
	def.AllowedMethods = allowed(types.MethodGet, types.MethodHead, types.MethodOptions, types.MethodPut,
		types.MethodPost, types.MethodPatch, types.MethodDelete)
	def.LambdaFunctionAssociations = &types.LambdaFunctionAssociations{
		Quantity: aws.Int32(int32(len(assocs))), Items: assocs,
	}

	return def
}

func TestLambdaEdge_DataPlane(t *testing.T) {
	t.Parallel()

	big := strings.Repeat("a", 41<<10)
	huge := strings.Repeat("a", 100<<10)

	tests := []struct {
		fn         edgeFn
		wantHeader [2]string
		wantOrigin [2]string
		wantBody   string
		method     string
		reqBody    string
		event      types.EventType
		wantXCache string
		name       string
		wantURI    string
		wantStatus int
		wantHits   int32
		includeBod bool
	}{
		{
			name: "viewer_request_rewrites_uri_and_query", event: types.EventTypeViewerRequest,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "request")
				r["uri"] = "/rewritten"
				r["querystring"] = "a=1"

				return r, "", nil
			},
			wantStatus: http.StatusOK, wantURI: "/rewritten?a=1", wantHits: 1,
		},
		{
			name: "viewer_request_generated_response", event: types.EventTypeViewerRequest,
			fn: func(map[string]any) (any, string, error) {
				return map[string]any{
					"status": "302", "statusDescription": "Found", "body": "moved",
					"headers": map[string]any{"location": hv("/elsewhere")},
				}, "", nil
			},
			wantStatus: http.StatusFound, wantBody: "moved", wantHeader: [2]string{"Location", "/elsewhere"},
			wantXCache: "LambdaGeneratedResponse from cloudfront",
		},
		{
			name: "generated_base64_body", event: types.EventTypeViewerRequest,
			fn: func(map[string]any) (any, string, error) {
				return map[string]any{
					"status": "200", "bodyEncoding": "base64", "body": base64.StdEncoding.EncodeToString([]byte("hi")),
				}, "", nil
			},
			wantStatus: http.StatusOK, wantBody: "hi",
		},
		{
			name: "origin_request_adds_header", event: types.EventTypeOriginRequest,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "request")
				hdrs(r)["x-edge"] = hv("added")

				return r, "", nil
			},
			wantStatus: http.StatusOK, wantOrigin: [2]string{"X-Edge", "added"}, wantHits: 1,
		},
		{
			name: "origin_request_rewrites_uri", event: types.EventTypeOriginRequest,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "request")
				r["uri"] = "/at-origin"

				return r, "", nil
			},
			wantStatus: http.StatusOK, wantURI: "/at-origin", wantHits: 1,
		},
		{
			name: "origin_request_generated_response", event: types.EventTypeOriginRequest,
			fn: func(map[string]any) (any, string, error) {
				return map[string]any{"status": "200", "body": huge[:1000]}, "", nil
			},
			wantStatus: http.StatusOK, wantBody: huge[:1000], wantXCache: "LambdaGeneratedResponse from cloudfront",
		},
		{
			name: "origin_response_adds_header", event: types.EventTypeOriginResponse,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "response")
				hdrs(r)["x-resp"] = hv("from-lambda")

				return r, "", nil
			},
			wantStatus: http.StatusOK, wantHeader: [2]string{"X-Resp", "from-lambda"}, wantHits: 1,
		},
		{
			name: "origin_response_replaces_status_and_body", event: types.EventTypeOriginResponse,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "response")
				r["status"] = "404"
				r["body"] = "gone"

				return r, "", nil
			},
			wantStatus: http.StatusNotFound, wantBody: "gone", wantHits: 1,
		},
		{
			name: "viewer_response_adds_header", event: types.EventTypeViewerResponse,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "response")
				hdrs(r)["x-viewer"] = hv("v")

				return r, "", nil
			},
			wantStatus: http.StatusOK, wantHeader: [2]string{"X-Viewer", "v"}, wantHits: 1,
		},
		{
			name: "viewer_response_cannot_change_status", event: types.EventTypeViewerResponse,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "response")
				r["status"] = "500"

				return r, "", nil
			},
			wantStatus: http.StatusOK, wantHits: 1,
		},
		{
			name: "viewer_request_read_only_header", event: types.EventTypeViewerRequest,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "request")
				hdrs(r)["content-length"] = hv("5")

				return r, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "viewer_request_host_is_read_only", event: types.EventTypeViewerRequest,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "request")
				hdrs(r)["host"] = hv("evil.example.com")

				return r, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "origin_request_read_only_header", event: types.EventTypeOriginRequest,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "request")
				hdrs(r)["via"] = hv("1.1 forged")

				return r, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "origin_response_read_only_header", event: types.EventTypeOriginResponse,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "response")
				hdrs(r)["transfer-encoding"] = hv("chunked")

				return r, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront", wantHits: 1,
		},
		{
			name: "viewer_response_read_only_header", event: types.EventTypeViewerResponse,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "response")
				hdrs(r)["content-encoding"] = hv("gzip")

				return r, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront", wantHits: 1,
		},
		{
			name: "disallowed_header", event: types.EventTypeViewerRequest,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "request")
				hdrs(r)["x-amz-cf-id"] = hv("forged")

				return r, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "uri_without_leading_slash", event: types.EventTypeViewerRequest,
			fn: func(cf map[string]any) (any, string, error) {
				r := sub(cf, "request")
				r["uri"] = "nope"

				return r, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "generated_status_out_of_range", event: types.EventTypeViewerRequest,
			fn: func(map[string]any) (any, string, error) {
				return map[string]any{"status": "199"}, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "generated_204_with_body", event: types.EventTypeViewerRequest,
			fn: func(map[string]any) (any, string, error) {
				return map[string]any{"status": "204", "body": "x"}, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "generated_invalid_base64", event: types.EventTypeViewerRequest,
			fn: func(map[string]any) (any, string, error) {
				return map[string]any{"status": "200", "bodyEncoding": "base64", "body": "!!!"}, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "viewer_generated_body_over_40kb", event: types.EventTypeViewerRequest,
			fn: func(map[string]any) (any, string, error) {
				return map[string]any{"status": "200", "body": big}, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "origin_generated_body_over_1mb", event: types.EventTypeOriginRequest,
			fn: func(map[string]any) (any, string, error) {
				return map[string]any{"status": "200", "body": strings.Repeat("a", 1<<20+1)}, "", nil
			},
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "invalid_result", event: types.EventTypeViewerRequest,
			fn:         func(map[string]any) (any, string, error) { return "just a string", "", nil },
			wantStatus: http.StatusBadGateway, wantXCache: "LambdaValidationError from cloudfront",
		},
		{
			name: "function_error", event: types.EventTypeViewerRequest,
			fn:         func(map[string]any) (any, string, error) { return nil, "Unhandled", nil },
			wantStatus: http.StatusServiceUnavailable, wantXCache: "LambdaExecutionError from cloudfront",
		},
		{
			name: "invoke_error", event: types.EventTypeOriginRequest,
			fn:         func(map[string]any) (any, string, error) { return nil, "", errFakeEdge },
			wantStatus: http.StatusServiceUnavailable, wantXCache: "LambdaExecutionError from cloudfront",
		},
		{
			name: "limit_exceeded", event: types.EventTypeViewerRequest,
			fn: func(map[string]any) (any, string, error) {
				return nil, "", cloudfront.ErrEdgeLimitExceeded
			},
			wantStatus: http.StatusServiceUnavailable, wantXCache: "LambdaLimitExceeded from cloudfront",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			edge := newFakeEdge(map[string]edgeFn{"fn": tt.fn})
			f.h.SetLambdaEdge(func() cloudfront.LambdaEdge { return edge })

			origin := newOriginServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(r.RequestURI))
			})

			def := edgeBehavior(f.cachePolicyQuery(t, 0, 0, types.CachePolicyQueryStringBehaviorAll),
				edgeAssoc("fn", tt.event, tt.includeBod))
			d := f.dist(t, distSpec{origins: []types.Origin{customOrigin("api", origin.hostPort())}, def: def})

			got := f.viewer(t, d, http.MethodGet, "/orig", nil)
			assert.Equal(t, tt.wantStatus, got.status, got.body)
			assert.Equal(t, tt.wantHits, origin.hits.Load())

			if tt.wantXCache != "" {
				assert.Equal(t, tt.wantXCache, got.xcache())
			}

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, got.body)
			}

			if tt.wantURI != "" {
				assert.Equal(t, tt.wantURI, origin.last(t).uri)
			}

			if tt.wantHeader[0] != "" {
				assert.Equal(t, tt.wantHeader[1], got.header.Get(tt.wantHeader[0]))
			}

			if tt.wantOrigin[0] != "" {
				assert.Equal(t, tt.wantOrigin[1], origin.last(t).header.Get(tt.wantOrigin[0]))
			}
		})
	}
}

func TestLambdaEdge_IncludeBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		event       types.EventType
		wantAtOrig  string
		includeBody bool
		replace     bool
		wantEvBody  bool
	}{
		{
			name:        "viewer_read_only",
			event:       types.EventTypeViewerRequest,
			includeBody: true,
			wantAtOrig:  "payload",
			wantEvBody:  true,
		},
		{
			name:        "viewer_replace",
			event:       types.EventTypeViewerRequest,
			includeBody: true,
			replace:     true,
			wantAtOrig:  "swapped",
			wantEvBody:  true,
		},
		{
			name:        "origin_replace",
			event:       types.EventTypeOriginRequest,
			includeBody: true,
			replace:     true,
			wantAtOrig:  "swapped",
			wantEvBody:  true,
		},
		{name: "not_requested", event: types.EventTypeViewerRequest, wantAtOrig: "payload"},
		{
			name:       "replace_ignored_without_include",
			event:      types.EventTypeOriginRequest,
			replace:    true,
			wantAtOrig: "payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			edge := newFakeEdge(map[string]edgeFn{"fn": func(cf map[string]any) (any, string, error) {
				r := sub(cf, "request")
				if tt.replace {
					r["body"] = map[string]any{
						"action": "replace", "encoding": "text", "data": "swapped",
					}
				}

				return r, "", nil
			}})
			f.h.SetLambdaEdge(func() cloudfront.LambdaEdge { return edge })

			origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			def := edgeBehavior(f.cachePolicy(t, 0), edgeAssoc("fn", tt.event, tt.includeBody))
			d := f.dist(t, distSpec{origins: []types.Origin{customOrigin("api", origin.hostPort())}, def: def})

			got := f.do(t, http.MethodPost, f.base+"/submit", d.domain, strings.NewReader("payload"), nil)
			require.Equal(t, http.StatusOK, got.status, got.body)
			assert.Equal(t, tt.wantAtOrig, origin.last(t).body)

			body := sub(sub(edge.lastEvent(t, "fn"), "request"), "body")
			has := body != nil

			require.Equal(t, tt.wantEvBody, has)

			if has {
				assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("payload")), body["data"])
				assert.Equal(t, "read-only", body["action"])
				assert.Equal(t, "base64", body["encoding"])
				assert.Equal(t, false, body["inputTruncated"])
			}
		})
	}
}

func TestLambdaEdge_EventShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event types.EventType
		fn    string
	}{
		{name: "viewer_request", event: types.EventTypeViewerRequest, fn: "vreq"},
		{name: "origin_request", event: types.EventTypeOriginRequest, fn: "oreq"},
		{name: "origin_response", event: types.EventTypeOriginResponse, fn: "oresp"},
		{name: "viewer_response", event: types.EventTypeViewerResponse, fn: "vresp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			fn := passRequest
			if strings.HasSuffix(tt.name, "response") {
				fn = passResponse
			}

			edge := newFakeEdge(map[string]edgeFn{tt.fn: fn})
			f.h.SetLambdaEdge(func() cloudfront.LambdaEdge { return edge })

			origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Origin", "yes")
				_, _ = w.Write([]byte("ok"))
			})
			def := edgeBehavior(f.cachePolicyQuery(t, 0, 0, types.CachePolicyQueryStringBehaviorAll),
				edgeAssoc(tt.fn, tt.event, false))
			d := f.dist(t, distSpec{origins: []types.Origin{customOrigin("api", origin.hostPort())}, def: def})

			got := f.viewer(t, d, http.MethodGet, "/p?q=1", map[string]string{"X-Custom": "c"})
			require.Equal(t, http.StatusOK, got.status, got.body)

			cf := edge.lastEvent(t, tt.fn)
			cfg := sub(cf, "config")
			assert.Equal(t, d.id, cfg["distributionId"])
			assert.Equal(t, d.domain, cfg["distributionDomainName"])
			assert.Equal(t, string(tt.event), cfg["eventType"])
			assert.NotEmpty(t, cfg["requestId"])

			req := sub(cf, "request")
			assert.Equal(t, "GET", req["method"])
			assert.Equal(t, "q=1", req["querystring"])
			assert.Equal(t, "/p", req["uri"])
			assert.NotEmpty(t, req["clientIp"])

			xc, _ := hdrs(req)["x-custom"].([]any)
			if tt.event == types.EventTypeViewerRequest || tt.event == types.EventTypeViewerResponse {
				require.Len(t, xc, 1)
				assert.Equal(t, map[string]any{"key": "X-Custom", "value": "c"}, xc[0])
			}

			_, hasOrigin := req["origin"]
			assert.Equal(t, strings.HasPrefix(string(tt.event), "origin"), hasOrigin)

			resp := sub(cf, "response")
			if strings.HasSuffix(tt.name, "response") {
				assert.Equal(t, "200", resp["status"])
				assert.Equal(t, "OK", resp["statusDescription"])
				assert.NotNil(t, hdrs(resp)["x-origin"])
			} else {
				assert.Nil(t, resp)
			}
		})
	}
}

func TestLambdaEdge_CacheSkipsOriginEvents(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	pass := func(fn edgeFn) edgeFn { return fn }
	edge := newFakeEdge(map[string]edgeFn{
		"vreq": pass(passRequest), "oreq": pass(passRequest), "oresp": pass(passResponse), "vresp": pass(passResponse),
	})
	f.h.SetLambdaEdge(func() cloudfront.LambdaEdge { return edge })

	origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("body")) })
	def := edgeBehavior(
		f.cachePolicy(t, 60),
		edgeAssoc("vreq", types.EventTypeViewerRequest, false),
		edgeAssoc("oreq", types.EventTypeOriginRequest, false),
		edgeAssoc(
			"oresp",
			types.EventTypeOriginResponse,
			false,
		),
		edgeAssoc("vresp", types.EventTypeViewerResponse, false),
	)
	d := f.dist(t, distSpec{origins: []types.Origin{customOrigin("api", origin.hostPort())}, def: def})

	first := f.viewer(t, d, http.MethodGet, "/cached", nil)
	second := f.viewer(t, d, http.MethodGet, "/cached", nil)

	assert.Equal(t, "Miss from cloudfront", first.xcache())
	assert.Equal(t, "Hit from cloudfront", second.xcache())
	assert.Equal(t, int32(1), origin.hits.Load())
	assert.Equal(t, 2, edge.count("vreq"))
	assert.Equal(t, 2, edge.count("vresp"))
	assert.Equal(t, 1, edge.count("oreq"))
	assert.Equal(t, 1, edge.count("oresp"))
}

func TestLambdaEdge_OriginRequestGeneratedResponseIsCached(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	edge := newFakeEdge(map[string]edgeFn{"oreq": func(map[string]any) (any, string, error) {
		return map[string]any{"status": "200", "body": "generated"}, "", nil
	}})
	f.h.SetLambdaEdge(func() cloudfront.LambdaEdge { return edge })

	origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("origin")) })
	def := edgeBehavior(f.cachePolicy(t, 60), edgeAssoc("oreq", types.EventTypeOriginRequest, false))
	d := f.dist(t, distSpec{origins: []types.Origin{customOrigin("api", origin.hostPort())}, def: def})

	first := f.viewer(t, d, http.MethodGet, "/g", nil)
	second := f.viewer(t, d, http.MethodGet, "/g", nil)

	assert.Equal(t, "generated", first.body)
	assert.Equal(t, "generated", second.body)
	assert.Equal(t, "Hit from cloudfront", second.xcache())
	assert.Equal(t, 1, edge.count("oreq"))
	assert.Zero(t, origin.hits.Load())
}

func TestLambdaEdge_OriginRequestSwitchesOrigin(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	other := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("other")) })
	edge := newFakeEdge(map[string]edgeFn{"oreq": func(cf map[string]any) (any, string, error) {
		r := sub(cf, "request")
		custom := sub(sub(r, "origin"), "custom")
		_, port, _ := strings.Cut(other.hostPort(), ":")
		custom["port"] = float64(atoi32(port))
		custom["protocol"] = "http"
		custom["path"] = "/pre"

		return r, "", nil
	}})
	f.h.SetLambdaEdge(func() cloudfront.LambdaEdge { return edge })

	first := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("first")) })
	def := edgeBehavior(f.cachePolicy(t, 0), edgeAssoc("oreq", types.EventTypeOriginRequest, false))
	d := f.dist(t, distSpec{origins: []types.Origin{customOrigin("api", first.hostPort())}, def: def})

	got := f.viewer(t, d, http.MethodGet, "/x", nil)
	assert.Equal(t, "other", got.body)
	assert.Equal(t, "/pre/x", other.last(t).uri)
	assert.Zero(t, first.hits.Load())

	custom := sub(sub(sub(edge.lastEvent(t, "oreq"), "request"), "origin"), "custom")
	assert.Equal(t, "127.0.0.1", custom["domainName"])
	assert.Equal(t, "http", custom["protocol"])
}

func TestLambdaEdge_Associations(t *testing.T) {
	t.Parallel()

	const validARN = edgeARNBase + "fn:1"

	tests := []struct {
		name      string
		arn       string
		event     types.EventType
		cfFn      types.EventType
		body      bool
		dup       bool
		absent    bool
		update    bool
		wantError bool
	}{
		{name: "valid", arn: validARN, event: types.EventTypeViewerRequest},
		{name: "valid_include_body", arn: validARN, event: types.EventTypeOriginRequest, body: true},
		{name: "valid_on_update", arn: validARN, event: types.EventTypeOriginResponse, update: true},
		{name: "latest", arn: edgeARNBase + "fn:$LATEST", event: types.EventTypeViewerRequest, wantError: true},
		{name: "alias", arn: edgeARNBase + "fn:prod", event: types.EventTypeViewerRequest, wantError: true},
		{name: "unqualified", arn: edgeARNBase + "fn", event: types.EventTypeViewerRequest, wantError: true},
		{
			name:      "latest_on_update",
			arn:       edgeARNBase + "fn:$LATEST",
			event:     types.EventTypeViewerRequest,
			update:    true,
			wantError: true,
		},
		{
			name: "wrong_region", arn: "arn:aws:lambda:eu-west-1:" + edgeAccount + ":function:fn:1",
			event: types.EventTypeViewerRequest, wantError: true,
		},
		{
			name: "wrong_account", arn: "arn:aws:lambda:us-east-1:999999999999:function:fn:1",
			event: types.EventTypeViewerRequest, wantError: true,
		},
		{name: "not_an_arn", arn: "fn:1", event: types.EventTypeViewerRequest, wantError: true},
		{name: "bad_event", arn: validARN, event: types.EventType("sometimes"), wantError: true},
		{
			name:      "body_on_viewer_response",
			arn:       validARN,
			event:     types.EventTypeViewerResponse,
			body:      true,
			wantError: true,
		},
		{
			name:      "body_on_origin_response",
			arn:       validARN,
			event:     types.EventTypeOriginResponse,
			body:      true,
			wantError: true,
		},
		{name: "duplicate_event", arn: validARN, event: types.EventTypeViewerRequest, dup: true, wantError: true},
		{name: "version_missing", arn: validARN, event: types.EventTypeViewerRequest, absent: true, wantError: true},
		{
			name: "cf_function_same_viewer_event", arn: validARN, event: types.EventTypeViewerRequest,
			cfFn: types.EventTypeViewerRequest, wantError: true,
		},
		{
			name: "cf_function_on_other_viewer_event", arn: validARN, event: types.EventTypeViewerRequest,
			cfFn: types.EventTypeViewerResponse,
		},
		{
			name: "cf_function_with_origin_event", arn: validARN, event: types.EventTypeOriginRequest,
			cfFn: types.EventTypeViewerRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			edge := newFakeEdge(nil)
			edge.absent = tt.absent
			f.h.SetLambdaEdge(func() cloudfront.LambdaEdge { return edge })

			origin := newOriginServer(t, func(http.ResponseWriter, *http.Request) {})
			def := behavior("api", f.cachePolicy(t, 0))
			assocs := []types.LambdaFunctionAssociation{{
				EventType: tt.event, LambdaFunctionARN: aws.String(tt.arn), IncludeBody: aws.Bool(tt.body),
			}}

			if tt.dup {
				assocs = append(assocs, assocs[0])
			}

			def.LambdaFunctionAssociations = &types.LambdaFunctionAssociations{
				Quantity: aws.Int32(int32(len(assocs))), Items: assocs,
			}

			if tt.cfFn != "" {
				cfARN := publishFn(t, f, "cffn", `function handler(e){return e.request;}`)
				def.FunctionAssociations = &types.FunctionAssociations{
					Quantity: aws.Int32(1),
					Items: []types.FunctionAssociation{{
						EventType: tt.cfFn, FunctionARN: aws.String(cfARN),
					}},
				}
			}

			var err error

			if tt.update {
				base := f.dist(t, distSpec{
					origins: []types.Origin{
						customOrigin("api", origin.hostPort()),
					}, def: behavior("api", f.cachePolicy(t, 0)),
				})
				err = updateWithBehavior(t, f.cf, base.id, def)
			} else {
				err = createWithBehavior(t, f, origin, def)
			}

			if !tt.wantError {
				require.NoError(t, err)

				return
			}

			var bad *types.InvalidLambdaFunctionAssociation

			require.ErrorAs(t, err, &bad)
		})
	}
}

func createWithBehavior(t *testing.T, f *dpFixture, origin *originServer, def types.DefaultCacheBehavior) error {
	t.Helper()

	f.callID++

	_, err := f.cf.CreateDistribution(t.Context(), &cfsdk.CreateDistributionInput{
		DistributionConfig: &types.DistributionConfig{
			CallerReference: aws.String("assoc-" + strings.Repeat("r", f.callID)),
			Comment:         aws.String("c"),
			Enabled:         aws.Bool(true),
			Origins: &types.Origins{
				Quantity: aws.Int32(1),
				Items:    []types.Origin{customOrigin("api", origin.hostPort())},
			},
			DefaultCacheBehavior: &def,
		},
	})

	return err
}

func updateWithBehavior(t *testing.T, c *cfsdk.Client, id string, def types.DefaultCacheBehavior) error {
	t.Helper()

	cur, err := c.GetDistributionConfig(t.Context(), &cfsdk.GetDistributionConfigInput{Id: aws.String(id)})
	require.NoError(t, err)

	cur.DistributionConfig.DefaultCacheBehavior = &def
	_, err = c.UpdateDistribution(t.Context(), &cfsdk.UpdateDistributionInput{
		Id: aws.String(id), IfMatch: cur.ETag, DistributionConfig: cur.DistributionConfig,
	})

	return err
}

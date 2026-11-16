package bedrockagentcorecontrol

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	targetAPIGateway = "apiGateway"
	apigwBaseURL     = "http://rest-api.invalid"
)

// ErrRestAPIHostUnavailable is returned by a RestAPIHost that has no API Gateway behind it.
var ErrRestAPIHostUnavailable = errors.New("API Gateway is not available")

// RestAPIHost reaches REST APIs of the emulated API Gateway: it exports a stage as OpenAPI 3 and serves a
// request through the stage's data-plane path.
type RestAPIHost interface {
	RestAPIExport(region, restAPIID, stage string) (map[string]any, error)
	InvokeRestAPI(ctx context.Context, region string, req RestAPIRequest) (RestAPIResponse, error)
}

// RestAPIRequest is an in-process call to a REST API stage.
type RestAPIRequest struct {
	Headers map[string]string
	Query   map[string]string
	APIID   string
	Stage   string
	Method  string
	Path    string
	Body    []byte
}

// RestAPIResponse is the stage's reply.
type RestAPIResponse struct {
	Body   []byte
	Status int
}

func (b *InMemoryBackend) restAPIHost() (RestAPIHost, bool) {
	b.mu.RLock("restAPIHost")
	defer b.mu.RUnlock()

	h, ok := b.refs.(RestAPIHost)

	return h, ok
}

// apiGatewayOps exports the target's REST API stage and selects the operations its tool configuration exposes.
func (b *InMemoryBackend) apiGatewayOps(t targetView, cfg map[string]any) ([]openAPIOp, error) {
	host, ok := b.restAPIHost()
	if !ok {
		return nil, ErrRestAPIHostUnavailable
	}

	export, err := host.RestAPIExport(t.sc.region, strOf(cfg, "restApiId"), strOf(cfg, "stage"))
	if err != nil {
		return nil, fmt.Errorf("export REST API: %w", err)
	}

	raw, err := json.Marshal(export, jsonx.V1())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errBadOpenAPI, err)
	}

	var spec map[string]any
	if err = json.Unmarshal(raw, &spec, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: %w", errBadOpenAPI, err)
	}

	return selectAPIGatewayOps(spec, objOf(cfg, "apiGatewayToolConfiguration")), nil
}

func filterMatches(filterPath, path string) bool {
	if base, wild := strings.CutSuffix(filterPath, "/*"); wild {
		return path == base || strings.HasPrefix(path, base+"/")
	}

	return filterPath == path
}

func selectAPIGatewayOps(spec, tc map[string]any) []openAPIOp {
	paths := mapOf(spec["paths"])
	keys := make([]string, 0, len(paths))

	for k := range paths {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	overrides := listOf(tc, "toolOverrides")
	filters := listOf(tc, "toolFilters")

	var ops []openAPIOp

	for _, p := range keys {
		item := mapOf(paths[p])

		for _, m := range httpMethods() {
			opm := mapOf(item[m])
			ov := overrideFor(overrides, p, m)
			if opm == nil || (ov == nil && !slices.ContainsFunc(filters, func(f any) bool {
				return filterAllows(mapOf(f), p, m)
			})) {
				continue
			}

			if ov != nil {
				opm = maps.Clone(opm)
				opm["operationId"] = strOf(ov, keyName)
			}

			op, built := buildOp(spec, apigwBaseURL, p, m, item, opm)
			if !built {
				continue
			}

			if d := strOf(ov, keyDescription); d != "" {
				op.desc = d
			}

			ops = append(ops, op)
		}
	}

	return ops
}

func filterAllows(f map[string]any, path, method string) bool {
	return filterMatches(strOf(f, "filterPath"), path) &&
		slices.ContainsFunc(stringsOf(f["methods"]), func(s string) bool { return strings.EqualFold(s, method) })
}

func overrideFor(overrides []any, path, method string) map[string]any {
	for _, o := range overrides {
		om := mapOf(o)
		if strOf(om, "path") == path && strings.EqualFold(strOf(om, "method"), method) {
			return om
		}
	}

	return nil
}

func (b *InMemoryBackend) callAPIGateway(
	ctx context.Context, t targetView, cfg map[string]any, tool string, args map[string]any,
) (map[string]any, error) {
	ops, err := b.apiGatewayOps(t, cfg)
	if err != nil {
		return toolFailure(err.Error())
	}

	idx := slices.IndexFunc(ops, func(o openAPIOp) bool { return o.id == tool })
	if idx < 0 {
		return nil, errUnknownTool
	}

	req, err := ops[idx].request(ctx, args)
	if err != nil {
		return toolFailure(err.Error())
	}

	apply, err := b.outboundCredentials(ctx, t)
	if err != nil {
		return toolFailure("credential provider error: " + err.Error())
	}

	if apply != nil {
		apply(req)
	}

	return b.invokeRestAPI(ctx, t, cfg, req)
}

func (b *InMemoryBackend) invokeRestAPI(
	ctx context.Context, t targetView, cfg map[string]any, req *http.Request,
) (map[string]any, error) {
	host, ok := b.restAPIHost()
	if !ok {
		return textResult(ErrRestAPIHostUnavailable.Error(), true), nil
	}

	var body []byte

	if req.Body != nil {
		body, _ = io.ReadAll(io.LimitReader(req.Body, maxOutboundBytes))
	}

	headers := make(map[string]string, len(req.Header))
	for k := range req.Header {
		headers[k] = req.Header.Get(k)
	}

	query := map[string]string{}

	for k, v := range req.URL.Query() {
		query[k] = strings.Join(v, ",")
	}

	resp, err := host.InvokeRestAPI(ctx, t.sc.region, RestAPIRequest{
		APIID: strOf(cfg, "restApiId"), Stage: strOf(cfg, "stage"), Method: req.Method,
		Path: pathOf(req.URL), Query: query, Headers: headers, Body: body,
	})
	if err != nil {
		return toolFailure("request to target failed: " + err.Error())
	}

	return textResult(string(resp.Body), resp.Status >= http.StatusBadRequest), nil
}

func pathOf(u *url.URL) string {
	if p := u.EscapedPath(); p != "" {
		return p
	}

	return "/"
}

// toolFailure reports a failed tool call as an isError result, which MCP carries as a successful response.
func toolFailure(msg string) (map[string]any, error) { return textResult(msg, true), nil }

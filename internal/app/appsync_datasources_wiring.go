package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	eventbridgebackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	rdsdatabackend "github.com/blackbirdworks/gopherstack/services/rdsdata"
)

var errNoOpenSearchDomain = errors.New("no OpenSearch domain serves endpoint")

const openSearchDomainRoot = "/2021-01-01/opensearch/domain/"

// wireAppSyncDataSources gives AppSync the EventBridge, OpenSearch and RDS Data API backends.
func wireAppSyncDataSources(byName map[string]service.Registerable) {
	asH, ok := byName["AppSync"].(*appsyncbackend.Handler)
	if !ok {
		return
	}

	asBk, ok := asH.Backend.(*appsyncbackend.InMemoryBackend)
	if !ok {
		return
	}

	if h, hok := byName["EventBridge"].(*eventbridgebackend.Handler); hok {
		asBk.SetEventBridge(appsyncEventBridge{backend: h.Backend})
	}

	if h, hok := byName["OpenSearch"].(*opensearchbackend.Handler); hok {
		asBk.SetOpenSearch(appsyncOpenSearch{handler: h})
	}

	if h, hok := byName["RDSData"].(*rdsdatabackend.Handler); hok {
		asBk.SetRDSData(appsyncRDSData{backend: h.Backend})
	}
}

type appsyncEventBridge struct {
	backend eventbridgebackend.StorageBackend
}

func (a appsyncEventBridge) PutEvents(
	ctx context.Context, entries []appsyncbackend.EventBridgeEntry,
) ([]appsyncbackend.EventBridgeResult, error) {
	in := make([]eventbridgebackend.EventEntry, len(entries))
	for i, e := range entries {
		in[i] = eventbridgebackend.EventEntry{
			Time: e.Time, Source: e.Source, DetailType: e.DetailType, Detail: e.Detail,
			EventBusName: e.EventBusName, Resources: e.Resources,
		}
	}

	res, err := a.backend.PutEvents(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("put events: %w", err)
	}

	out := make([]appsyncbackend.EventBridgeResult, len(res))
	for i, r := range res {
		out[i] = appsyncbackend.EventBridgeResult{
			EventID: r.EventID, ErrorCode: r.ErrorCode, ErrorMessage: r.ErrorMessage,
		}
	}

	return out, nil
}

type appsyncOpenSearch struct{ handler *opensearchbackend.Handler }

func (a appsyncOpenSearch) domainFor(endpoint string) (string, error) {
	host := endpoint
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		host = u.Host
	}

	names := a.handler.Backend.ListDomainNames()

	domains, err := a.handler.Backend.DescribeDomains(names)
	if err != nil {
		return "", fmt.Errorf("describe domains: %w", err)
	}

	for _, d := range domains {
		if d.Endpoint == host {
			return d.Name, nil
		}
	}

	return "", fmt.Errorf("%w: %q", errNoOpenSearchDomain, endpoint)
}

// emulatorPath maps a real OpenSearch REST path onto the emulator's domain-scoped data plane.
func emulatorPath(domain, method, path string) (string, string) {
	p := strings.Trim(path, "/")
	base := openSearchDomainRoot + domain

	index, rest, _ := strings.Cut(p, "/")
	if strings.HasPrefix(index, "_") || index == "" {
		return method, base + "/" + p
	}

	if method == http.MethodPut {
		method = http.MethodPost
	}

	out := base + "/index/" + index
	if rest != "" {
		out += "/" + rest
	}

	return method, out
}

func (a appsyncOpenSearch) OpenSearchRequest(
	ctx context.Context, req appsyncbackend.OpenSearchRequest,
) (int, []byte, error) {
	domain, err := a.domainFor(req.Endpoint)
	if err != nil {
		return 0, nil, err
	}

	method, path := emulatorPath(domain, req.Method, req.Path)

	r, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(req.Body))
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}

	r.URL.RawQuery = req.RawQuery

	for k, v := range req.Headers {
		r.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, r)

	body, err := io.ReadAll(rec.Result().Body)

	return rec.Code, body, err
}

type appsyncRDSData struct{ backend rdsdatabackend.StorageBackend }

func (a appsyncRDSData) ExecuteRDSStatement(
	ctx context.Context, st appsyncbackend.RDSStatement,
) (map[string]any, error) {
	cluster := st.ClusterID
	if !strings.HasPrefix(cluster, "arn:") {
		cluster = arn.Build("rds", a.backend.Region(), a.backend.AccountID(), "cluster:"+cluster)
	}

	records, cols, updated, generated, err := a.backend.ExecuteStatement(
		ctx, cluster, st.SQL, "", rdsParameters(st)...)
	if err != nil {
		return nil, fmt.Errorf("execute statement: %w", err)
	}

	raw, err := json.Marshal(map[string]any{
		"records": nonNil(records), "columnMetadata": nonNil(cols),
		"numberOfRecordsUpdated": updated, "generatedFields": nonNil(generated),
	})
	if err != nil {
		return nil, fmt.Errorf("encode result: %w", err)
	}

	var out map[string]any

	return out, json.Unmarshal(raw, &out)
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

func rdsParameters(st appsyncbackend.RDSStatement) []rdsdatabackend.SQLParameter {
	out := make([]rdsdatabackend.SQLParameter, 0, len(st.Variables))

	for k, v := range st.Variables {
		out = append(out, rdsdatabackend.SQLParameter{
			Name: strings.TrimPrefix(k, ":"), TypeHint: st.Hints[k], Value: rdsField(v),
		})
	}

	return out
}

func rdsField(v any) rdsdatabackend.Field {
	switch t := v.(type) {
	case nil:
		null := true

		return rdsdatabackend.Field{IsNull: &null}
	case bool:
		return rdsdatabackend.Field{BooleanValue: &t}
	case string:
		return rdsdatabackend.Field{StringValue: &t}
	case int:
		n := int64(t)

		return rdsdatabackend.Field{LongValue: &n}
	case int64:
		return rdsdatabackend.Field{LongValue: &t}
	case float64:
		if t == float64(int64(t)) {
			n := int64(t)

			return rdsdatabackend.Field{LongValue: &n}
		}

		return rdsdatabackend.Field{DoubleValue: &t}
	default:
		s := fmt.Sprint(t)

		return rdsdatabackend.Field{StringValue: &s}
	}
}

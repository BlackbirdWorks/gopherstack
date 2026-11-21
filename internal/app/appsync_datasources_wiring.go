package app

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	elasticsearchbackend "github.com/blackbirdworks/gopherstack/services/elasticsearch"
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
		es, _ := byName["Elasticsearch"].(*elasticsearchbackend.Handler)
		asBk.SetOpenSearch(appsyncOpenSearch{handler: h, es: es})
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

type appsyncOpenSearch struct {
	handler *opensearchbackend.Handler
	es      *elasticsearchbackend.Handler
}

const esMirrorTag = "gopherstack:elasticsearch-domain"

func endpointHost(endpoint string) string {
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		return u.Host
	}

	return endpoint
}

// domainFor resolves a data source endpoint to the OpenSearch engine domain that serves it. An endpoint of a
// domain created through the Elasticsearch API is served by an engine domain of the same name, as in AWS where
// both APIs manage the same domains.
func (a appsyncOpenSearch) domainFor(ctx context.Context, endpoint string) (string, error) {
	host := endpointHost(endpoint)

	a.sweepMirrors(ctx)

	if name, ok, err := a.elasticsearchDomain(ctx, host); ok || err != nil {
		return name, err
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

// endpointRegion reads the region out of an Elasticsearch endpoint host (<label>.<region>.es.<domain>).
func endpointRegion(host string) string {
	labels := strings.Split(host, ".")

	for i := 1; i+1 < len(labels); i++ {
		if labels[i+1] == "es" {
			return labels[i]
		}
	}

	return ""
}

func esScope(ctx context.Context, region string) context.Context {
	if region == "" {
		return ctx
	}

	return elasticsearchbackend.WithRegion(ctx, region)
}

// engineName is the engine domain mirroring an Elasticsearch domain. Account and region lead it, joined with
// underscores that Elasticsearch names, regions and account ids cannot contain, so no two Elasticsearch domains
// share an engine name.
func engineName(d *elasticsearchbackend.Domain) string {
	const (
		regionField  = 3
		accountField = 4
	)

	parts := strings.Split(d.ARN, ":")
	if len(parts) <= accountField {
		return "es_" + d.Name
	}

	return "es_" + parts[accountField] + "_" + parts[regionField] + "_" + d.Name
}

// stampOwner splits a mirror stamp into the ARN of the Elasticsearch domain it mirrors.
func stampOwner(stamp string) (string, string, string, bool) {
	owner, _, _ := strings.Cut(stamp, "|")
	parts := strings.Split(owner, ":")

	const resourceField = 5

	name, ok := strings.CutPrefix(parts[len(parts)-1], "domain/")
	if len(parts) <= resourceField || !ok {
		return "", "", "", false
	}

	return parts[3], parts[4], name, true
}

// sweepMirrors deletes the engine domains this wiring created for Elasticsearch domains that no longer exist or
// were recreated. A domain without the mirror tag is never touched.
func (a appsyncOpenSearch) sweepMirrors(ctx context.Context) {
	if a.es == nil {
		return
	}

	bk := a.handler.Backend

	for _, name := range bk.ListDomainNames() {
		od, err := bk.DescribeDomain(name)
		if err != nil {
			continue
		}

		tags, _ := bk.ListTags(od.ARN)

		have, mirrored := tags[esMirrorTag]
		if !mirrored {
			continue
		}

		region, _, esName, ok := stampOwner(have)
		if ok {
			d, derr := a.es.Backend.DescribeDomain(esScope(ctx, region), esName)
			if derr == nil && !d.Deleted && mirrorStamp(d) == have && engineName(d) == name {
				continue
			}
		}

		_ = bk.DeleteDomainNow(name)
	}
}

func mirrorStamp(d *elasticsearchbackend.Domain) string {
	return d.ARN + "|" + strconv.FormatInt(d.CreatedAt.UnixNano(), 10)
}

// esDomainAt finds the Elasticsearch domain serving host.
func (a appsyncOpenSearch) esDomainAt(ctx context.Context, host string) (*elasticsearchbackend.Domain, bool) {
	if a.es == nil {
		return nil, false
	}

	scoped := esScope(ctx, endpointRegion(host))

	for _, n := range a.es.Backend.ListDomainNames(scoped) {
		d, err := a.es.Backend.DescribeDomain(scoped, n)
		if err == nil && !d.Deleted && d.Endpoint == host {
			return d, true
		}
	}

	return nil, false
}

// ReleaseOpenSearch deletes the engine domain mirroring the Elasticsearch domain at endpoint.
func (a appsyncOpenSearch) ReleaseOpenSearch(ctx context.Context, endpoint string) {
	if d, ok := a.esDomainAt(ctx, endpointHost(endpoint)); ok {
		bk := a.handler.Backend
		name := engineName(d)

		if od, err := bk.DescribeDomain(name); err == nil {
			if tags, _ := bk.ListTags(od.ARN); tags[esMirrorTag] == mirrorStamp(d) {
				_ = bk.DeleteDomainNow(name)
			}
		}
	}

	a.sweepMirrors(ctx)
}

func (a appsyncOpenSearch) elasticsearchDomain(ctx context.Context, host string) (string, bool, error) {
	d, ok := a.esDomainAt(ctx, host)
	if !ok {
		return "", false, nil
	}

	return engineName(d), true, a.ensureEngineDomain(d)
}

// ensureEngineDomain backs an Elasticsearch domain with an engine domain, replacing a mirror left by an earlier
// domain of the same name. An engine domain that is not this domain's mirror is never replaced.
func (a appsyncOpenSearch) ensureEngineDomain(d *elasticsearchbackend.Domain) error {
	stamp := mirrorStamp(d)
	name := engineName(d)
	bk := a.handler.Backend

	if existing, err := bk.DescribeDomain(name); err == nil {
		tags, _ := bk.ListTags(existing.ARN)

		have, mirrored := tags[esMirrorTag]
		if !mirrored {
			return fmt.Errorf("%w: engine domain %s is not a mirror", errNoOpenSearchDomain, name)
		}

		if have == stamp {
			return nil
		}

		if err = bk.DeleteDomainNow(name); err != nil {
			return fmt.Errorf("replace stale engine domain: %w", err)
		}
	}

	_, err := bk.CreateDomain(opensearchbackend.CreateDomainInput{
		Name: name, EngineVersion: "Elasticsearch_" + d.ElasticsearchVersion,
		Tags: map[string]string{esMirrorTag: stamp},
	})
	if errors.Is(err, opensearchbackend.ErrDomainAlreadyExists) {
		if won, derr := bk.DescribeDomain(name); derr == nil {
			tags, _ := bk.ListTags(won.ARN)
			if tags[esMirrorTag] == stamp {
				return nil
			}
		}
	}

	if err != nil {
		return fmt.Errorf("create engine domain for %s: %w", d.Name, err)
	}

	return nil
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
	domain, err := a.domainFor(ctx, req.Endpoint)
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
	}, jsonx.V1())
	if err != nil {
		return nil, fmt.Errorf("encode result: %w", err)
	}

	var out map[string]any

	return out, json.Unmarshal(raw, &out, jsonx.V1())
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

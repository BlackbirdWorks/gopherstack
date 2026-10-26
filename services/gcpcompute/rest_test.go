package gcpcompute_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

type listDoc struct {
	Items         []map[string]any `json:"items"`
	Kind          string           `json:"kind"`
	ID            string           `json:"id"`
	SelfLink      string           `json:"selfLink"`
	NextPageToken string           `json:"nextPageToken"`
}

func getJSON[T any](t *testing.T, e *env, path string, want int) T {
	t.Helper()

	status, body := rawDo(t, e, http.MethodGet, path, "")
	require.Equal(t, want, status, body)

	var out T
	require.NoError(t, json.Unmarshal([]byte(body), &out), body)

	return out
}

func names(items []map[string]any) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i], _ = it["name"].(string)
	}

	return out
}

func seedInstances(t *testing.T, e *env) {
	t.Helper()
	e.baseNet(t)

	for _, v := range []struct {
		name, mt, env string
	}{
		{"alpha", "e2-medium", "prod"}, {"bravo", "e2-small", "dev"}, {"charlie", "n2-standard-2", "prod"}, {"delta", "e2-medium", "dev"},
	} {
		spec := vmSpec(v.name)
		spec.MachineType = s("zones/" + zone + "/machineTypes/" + v.mt)
		spec.Labels = map[string]string{"env": v.env}
		e.vm(t, spec)
	}

	east := vmSpec("echo")
	east.MachineType = s("zones/" + otherZone + "/machineTypes/e2-medium")
	east.NetworkInterfaces[0].Subnetwork = s("regions/" + otherRegion + "/subnetworks/east")
	east.NetworkInterfaces[0].AccessConfigs = nil

	op, err := e.instances(t).Insert(t.Context(), &computepb.InsertInstanceRequest{Project: project, Zone: otherZone, InstanceResource: east})
	e.done(t, op, err)
}

func TestInstanceListFilterOrderPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	seedInstances(t, e)

	base := "/compute/v1/projects/" + project + "/zones/" + zone + "/instances"

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"all by name", "", []string{"alpha", "bravo", "charlie", "delta"}},
		{"label equals", "filter=" + url.QueryEscape(`labels.env = "prod"`), []string{"alpha", "charlie"}},
		{"label bare value", "filter=" + url.QueryEscape(`labels.env=dev`), []string{"bravo", "delta"}},
		{"name regex", "filter=" + url.QueryEscape(`name eq "(alpha|delta)"`), []string{"alpha", "delta"}},
		{"not equal", "filter=" + url.QueryEscape(`name != "alpha"`), []string{"bravo", "charlie", "delta"}},
		{"and", "filter=" + url.QueryEscape(`labels.env = prod AND machineType = ".*e2-medium"`), []string{"alpha"}},
		{"or", "filter=" + url.QueryEscape(`name = alpha OR name = charlie`), []string{"alpha", "charlie"}},
		{"not", "filter=" + url.QueryEscape(`NOT labels.env = prod`), []string{"bravo", "delta"}},
		{"parentheses", "filter=" + url.QueryEscape(`(name = alpha OR name = bravo) AND labels.env = prod`), []string{"alpha"}},
		{"implicit and", "filter=" + url.QueryEscape(`(name = alpha) (labels.env = prod)`), []string{"alpha"}},
		{"nested field", "filter=" + url.QueryEscape(`networkInterfaces.networkIP = "10.10.0.2"`), []string{"alpha"}},
		{"has operator", "filter=" + url.QueryEscape(`labels.env:*`), []string{"alpha", "bravo", "charlie", "delta"}},
		{"has missing", "filter=" + url.QueryEscape(`labels.team:*`), nil},
		{"status", "filter=" + url.QueryEscape(`status = RUNNING`), []string{"alpha", "bravo", "charlie", "delta"}},
		{"order desc", "orderBy=" + url.QueryEscape("name desc"), []string{"delta", "charlie", "bravo", "alpha"}},
		{"order explicit asc", "orderBy=" + url.QueryEscape("name asc"), []string{"alpha", "bravo", "charlie", "delta"}},
		{"order by label", "orderBy=" + url.QueryEscape("labels.env desc") + "&filter=" + url.QueryEscape("name != delta"), []string{"alpha", "charlie", "bravo"}},
		{"max results", "maxResults=2", []string{"alpha", "bravo"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := getJSON[listDoc](t, e, base+"?"+tt.query, http.StatusOK)
			assert.Equal(t, "compute#instanceList", got.Kind)
			assert.Equal(t, "projects/"+project+"/zones/"+zone+"/instances", got.ID)
			assert.Equal(t, "https://www.googleapis.com/compute/v1/projects/"+project+"/zones/"+zone+"/instances", got.SelfLink)

			if tt.want == nil {
				assert.Empty(t, got.Items)

				return
			}

			assert.Equal(t, tt.want, names(got.Items))
		})
	}
}

func TestListPageTokenWalksAllPages(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	seedInstances(t, e)

	base := "/compute/v1/projects/" + project + "/zones/" + zone + "/instances"

	var seen []string

	token := ""

	for range 5 {
		path := base + "?maxResults=3"
		if token != "" {
			path += "&pageToken=" + url.QueryEscape(token)
		}

		page := getJSON[listDoc](t, e, path, http.StatusOK)
		seen = append(seen, names(page.Items)...)

		if token = page.NextPageToken; token == "" {
			break
		}
	}

	assert.Equal(t, []string{"alpha", "bravo", "charlie", "delta"}, seen)
}

func TestListQueryErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	seedInstances(t, e)

	base := "/compute/v1/projects/" + project + "/zones/" + zone + "/instances?"

	tests := []struct {
		name  string
		query string
	}{
		{"dangling operator", "filter=" + url.QueryEscape("name =")},
		{"unknown operator", "filter=" + url.QueryEscape("name ~ x")},
		{"unbalanced paren", "filter=" + url.QueryEscape("(name = x")},
		{"unterminated quote", "filter=" + url.QueryEscape(`name = "x`)},
		{"bad regex", "filter=" + url.QueryEscape(`name = "("`)},
		{"trailing and", "filter=" + url.QueryEscape("name = x AND")},
		{"bad max results", "maxResults=abc"},
		{"zero max results", "maxResults=0"},
		{"bad order", "orderBy=" + url.QueryEscape("name sideways")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := rawDo(t, e, http.MethodGet, base+tt.query, "")
			assert.Equal(t, http.StatusBadRequest, status, body)

			var doc struct {
				Error struct {
					Errors []struct{ Reason string } `json:"errors"`
					Code   int                       `json:"code"`
				} `json:"error"`
			}

			require.NoError(t, json.Unmarshal([]byte(body), &doc))
			assert.Equal(t, http.StatusBadRequest, doc.Error.Code)
			require.NotEmpty(t, doc.Error.Errors)
			assert.Equal(t, "invalid", doc.Error.Errors[0].Reason)
		})
	}
}

func TestAggregatedList(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	seedInstances(t, e)

	type scoped struct {
		Warning *struct{ Code string } `json:"warning"`
		Items   []map[string]any       `json:"instances"`
	}

	type agg struct {
		Items map[string]scoped `json:"items"`
		Kind  string            `json:"kind"`
		ID    string            `json:"id"`
	}

	got := getJSON[agg](t, e, "/compute/v1/projects/"+project+"/aggregated/instances", http.StatusOK)
	assert.Equal(t, "compute#instanceAggregatedList", got.Kind)
	assert.Equal(t, "projects/"+project+"/aggregated/instances", got.ID)
	assert.Len(t, got.Items, 46, "every zone is a scope")
	assert.Equal(t, []string{"alpha", "bravo", "charlie", "delta"}, names(got.Items["zones/"+zone].Items))
	assert.Equal(t, []string{"echo"}, names(got.Items["zones/"+otherZone].Items))
	require.NotNil(t, got.Items["zones/europe-west1-b"].Warning)
	assert.Equal(t, "NO_RESULTS_ON_PAGE", got.Items["zones/europe-west1-b"].Warning.Code)
	assert.Empty(t, got.Items["zones/europe-west1-b"].Items)

	filtered := getJSON[agg](t, e, "/compute/v1/projects/"+project+"/aggregated/instances?filter="+url.QueryEscape(`labels.env = prod`), http.StatusOK)
	assert.Equal(t, []string{"alpha", "charlie"}, names(filtered.Items["zones/"+zone].Items))
	assert.Empty(t, filtered.Items["zones/"+otherZone].Items)

	paged := getJSON[struct {
		agg
		NextPageToken string `json:"nextPageToken"`
	}](t, e, "/compute/v1/projects/"+project+"/aggregated/instances?maxResults=2", http.StatusOK)
	assert.Equal(t, []string{"alpha", "bravo"}, names(paged.Items["zones/"+zone].Items))
	assert.NotEmpty(t, paged.NextPageToken)
}

func TestAggregatedListThroughClient(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	seedInstances(t, e)

	it := e.instances(t).AggregatedList(t.Context(), &computepb.AggregatedListInstancesRequest{Project: project})
	found := map[string]int{}

	for {
		pair, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			break
		}

		found[pair.Key] = len(pair.Value.GetInstances())
	}

	assert.Equal(t, 4, found["zones/"+zone])
	assert.Equal(t, 1, found["zones/"+otherZone])
	assert.Zero(t, found["zones/us-west1-a"])
}

func TestAggregatedListsOtherKinds(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	for _, a := range []string{"a1", "a2"} {
		op, err := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{Project: project, Region: region, AddressResource: &computepb.Address{Name: s(a)}})
		e.done(t, op, err)
	}

	op, err := e.globalAddresses(t).Insert(t.Context(), &computepb.InsertGlobalAddressRequest{Project: project, AddressResource: &computepb.Address{Name: s("g1")}})
	e.done(t, op, err)

	op, err = e.disks(t).Insert(t.Context(), &computepb.InsertDiskRequest{Project: project, Zone: otherZone, DiskResource: &computepb.Disk{Name: s("d1"), SizeGb: proto.Int64(10)}})
	e.done(t, op, err)

	type scoped struct {
		Addresses []map[string]any `json:"addresses"`
		Disks     []map[string]any `json:"disks"`
		Subnets   []map[string]any `json:"subnetworks"`
		Routers   []map[string]any `json:"routers"`
		Machines  []map[string]any `json:"machineTypes"`
		Ops       []map[string]any `json:"operations"`
	}

	type agg struct {
		Items map[string]scoped `json:"items"`
		Kind  string            `json:"kind"`
	}

	tests := []struct {
		path  string
		kind  string
		pick  func(s scoped) []map[string]any
		scope string
		want  []string
	}{
		{"addresses", "compute#addressAggregatedList", func(s scoped) []map[string]any { return s.Addresses }, "regions/" + region, []string{"a1", "a2"}},
		{"addresses", "compute#addressAggregatedList", func(s scoped) []map[string]any { return s.Addresses }, "global", []string{"g1"}},
		{"disks", "compute#diskAggregatedList", func(s scoped) []map[string]any { return s.Disks }, "zones/" + otherZone, []string{"d1"}},
		{"subnetworks", "compute#subnetworkAggregatedList", func(s scoped) []map[string]any { return s.Subnets }, "regions/" + region, []string{"sub"}},
		{"machineTypes", "compute#machineTypeAggregatedList", func(s scoped) []map[string]any { return s.Machines }, "zones/" + zone, nil},
	}

	for _, tt := range tests {
		t.Run(tt.path+"/"+tt.scope, func(t *testing.T) {
			t.Parallel()

			q := ""
			if tt.path == "machineTypes" {
				q = "?filter=" + url.QueryEscape("name = e2-micro")
			}

			got := getJSON[agg](t, e, fmt.Sprintf("/compute/v1/projects/%s/aggregated/%s%s", project, tt.path, q), http.StatusOK)
			assert.Equal(t, tt.kind, got.Kind)

			if tt.want == nil {
				assert.Equal(t, []string{"e2-micro"}, names(tt.pick(got.Items[tt.scope])))

				return
			}

			assert.Equal(t, tt.want, names(tt.pick(got.Items[tt.scope])))
		})
	}
}

func TestRawRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct {
		name   string
		method string
		path   string
		status int
	}{
		{"unknown prefix", http.MethodGet, "/v1/projects/p/global/networks", http.StatusNotFound},
		{"no project", http.MethodGet, "/compute/v1/projects", http.StatusNotFound},
		{"unknown collection", http.MethodGet, "/compute/v1/projects/p/global/widgets", http.StatusNotFound},
		{"wrong scope", http.MethodGet, "/compute/v1/projects/p/zones/" + zone + "/networks", http.StatusNotFound},
		{"wrong method", http.MethodPut, "/compute/v1/projects/p/global/networks", http.StatusNotFound},
		{"action wrong method", http.MethodGet, "/compute/v1/projects/p/zones/" + zone + "/instances/x/start", http.StatusNotFound},
		{"unknown zone", http.MethodGet, "/compute/v1/projects/p/zones/mars-a/instances", http.StatusNotFound},
		{"unknown region", http.MethodGet, "/compute/v1/projects/p/regions/mars/subnetworks", http.StatusNotFound},
		{"beta prefix", http.MethodGet, "/compute/beta/projects/p/global/networks", http.StatusOK},
		{"alpha prefix", http.MethodGet, "/compute/alpha/projects/p/global/firewalls", http.StatusOK},
		{"malformed json", http.MethodPost, "/compute/v1/projects/p/global/networks", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := ""
			if tt.name == "malformed json" {
				body = "{not json"
			}

			status, _ := rawDo(t, e, tt.method, tt.path, body)
			assert.Equal(t, tt.status, status)
		})
	}
}

func TestErrorBodyShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	status, body := rawDo(t, e, http.MethodGet, "/compute/v1/projects/p/global/networks/ghost", "")
	assert.Equal(t, http.StatusNotFound, status)

	var doc struct {
		Error struct {
			Message string `json:"message"`
			Errors  []struct {
				Message, Domain, Reason string
			} `json:"errors"`
			Code int `json:"code"`
		} `json:"error"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &doc))
	assert.Equal(t, 404, doc.Error.Code)
	assert.Equal(t, "The resource 'projects/p/global/networks/ghost' was not found", doc.Error.Message)
	require.Len(t, doc.Error.Errors, 1)
	assert.Equal(t, "global", doc.Error.Errors[0].Domain)
	assert.Equal(t, "notFound", doc.Error.Errors[0].Reason)
}

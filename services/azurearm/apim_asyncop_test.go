package azurearm_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const apimSvcBody = `{"location":"westus","sku":{"name":"Developer","capacity":1},` +
	`"properties":{"publisherEmail":"a@b.io","publisherName":"P"}}`

type opCounter struct{ hits atomic.Int32 }

func (c *opCounter) Do(req *policy.Request) (*http.Response, error) {
	if req.Raw().Method == http.MethodGet && strings.Contains(req.Raw().URL.Path, "/operationresults/") {
		c.hits.Add(1)
	}

	return req.Next()
}

func (s *apimStack) countingFactory(t *testing.T) (*armapimanagement.ServiceClient, *opCounter) {
	t.Helper()

	cnt := &opCounter{}

	f, err := armapimanagement.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: s.url, Audience: "https://management.core.windows.net/"},
		}},
		Transport:       s.hc,
		Retry:           policy.RetryOptions{MaxRetries: -1},
		PerCallPolicies: []policy.Policy{cnt},
	})
	require.NoError(t, err)

	return f.NewServiceClient(), cnt
}

func statusDoc(t *testing.T, body string) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &m))

	return m
}

func TestAPIMService_OperationResultsHeaders(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	path := s.armPath("")

	code, _, hdr := s.raw(t, http.MethodPut, path, apimSvcBody, nil)
	require.Equal(t, http.StatusCreated, code)

	loc := hdr.Get("Location")
	assert.Equal(t, loc, hdr.Get("Azure-Asyncoperation"))
	assert.Contains(t, loc, "/providers/Microsoft.ApiManagement/service/"+apimSvc+"/operationresults/")
	assert.True(t, strings.HasSuffix(loc, "?api-version=2022-08-01&asyncResponse"), loc)
	assert.Equal(t, "1", hdr.Get("Retry-After"))

	u, err := url.Parse(loc)
	require.NoError(t, err)

	opPath := u.RequestURI()

	code, body, _ := s.raw(t, http.MethodGet, opPath, "", nil)
	assert.Equal(t, http.StatusAccepted, code)
	assert.Equal(t, "InProgress", statusDoc(t, body)["status"])

	s.clock.Advance(time.Hour)

	code, body, _ = s.raw(t, http.MethodGet, opPath, "", nil)
	require.Equal(t, http.StatusOK, code)

	doc := statusDoc(t, body)
	assert.Equal(t, "Succeeded", doc["status"])
	assert.Equal(t, apimSvc, doc["name"])
	assert.NotEmpty(t, doc["startTime"])
	assert.NotEmpty(t, doc["endTime"])

	code, _, hdr = s.raw(t, http.MethodPatch, path, `{"tags":{"a":"b"}}`, nil)
	assert.Equal(t, http.StatusAccepted, code)
	assert.NotEmpty(t, hdr.Get("Azure-Asyncoperation"))
	assert.Contains(t, hdr.Get("Location"), "/operationresults/")

	code, _, hdr = s.raw(t, http.MethodDelete, path, "", nil)
	require.Equal(t, http.StatusAccepted, code)
	assert.Empty(t, hdr.Get("Azure-Asyncoperation"))
	assert.NotContains(t, hdr.Get("Location"), "asyncResponse")

	u, err = url.Parse(hdr.Get("Location"))
	require.NoError(t, err)

	code, _, _ = s.raw(t, http.MethodGet, u.RequestURI(), "", nil)
	assert.Equal(t, http.StatusAccepted, code)

	s.clock.Advance(time.Hour)

	code, _, _ = s.raw(t, http.MethodGet, u.RequestURI(), "", nil)
	assert.Equal(t, http.StatusOK, code)

	code, _, _ = s.raw(t, http.MethodDelete, path, "", nil)
	assert.Equal(t, http.StatusNoContent, code)

	code, _, _ = s.raw(t, http.MethodGet, s.armPath("/operationresults/unknown"), "", nil)
	assert.Equal(t, http.StatusNotFound, code)
}

func TestAPIMService_PollersUseOperationResults(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	ctx := t.Context()
	svcs, cnt := s.countingFactory(t)
	opts := &runtime.PollUntilDoneOptions{Frequency: 5 * time.Millisecond}

	create, err := svcs.BeginCreateOrUpdate(ctx, apimRG, "polled",
		apimServiceBody(armapimanagement.SKUTypeDeveloper, 1, "a@b.io", "P"), nil)
	require.NoError(t, err)

	s.clock.Advance(time.Hour)

	_, err = create.PollUntilDone(ctx, opts)
	require.NoError(t, err)

	created := cnt.hits.Load()
	assert.Positive(t, created, "create poller must poll operationresults")

	upd, err := svcs.BeginUpdate(ctx, apimRG, "polled", armapimanagement.ServiceUpdateParameters{
		Tags: map[string]*string{"k": new("v")},
	}, nil)
	require.NoError(t, err)

	_, err = upd.PollUntilDone(ctx, opts)
	require.NoError(t, err)

	updated := cnt.hits.Load()
	assert.Greater(t, updated, created, "update poller must poll operationresults")

	del, err := svcs.BeginDelete(ctx, apimRG, "polled", nil)
	require.NoError(t, err)

	s.clock.Advance(time.Hour)

	_, err = del.PollUntilDone(ctx, opts)
	require.NoError(t, err)
	assert.Greater(t, cnt.hits.Load(), updated, "delete poller must poll operationresults")
}

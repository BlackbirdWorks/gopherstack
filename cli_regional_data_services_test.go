package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	mqbackend "github.com/blackbirdworks/gopherstack/services/mq"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func regionalHandler[H any](t *testing.T, byName map[string]service.Registerable, name string) H {
	t.Helper()

	h, ok := byName[name].(H)
	require.True(t, ok, name)

	return h
}

func TestInitializeServices_RegionalTaggingForDataServices(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	euCtx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: euRegion, Account: crossAcct})

	tests := []struct {
		create func(t *testing.T) string
		name   string
	}{
		{name: "eks", create: func(t *testing.T) string {
			t.Helper()

			h := regionalHandler[*eksbackend.Handler](t, byName, "EKS")
			c, err := h.BackendFor(euRegion).CreateCluster("tagged", "", "arn:aws:iam::"+crossAcct+":role/eks",
				&eksbackend.VpcConfig{SubnetIDs: []string{"subnet-1", "subnet-2"}}, nil, nil)
			require.NoError(t, err)

			return c.ARN
		}},
		{name: "mq", create: func(t *testing.T) string {
			t.Helper()

			h := regionalHandler[*mqbackend.Handler](t, byName, "MQ")
			bk, ok := h.BackendFor(euRegion).(*mqbackend.InMemoryBackend)
			require.True(t, ok)

			c, err := bk.CreateConfiguration("tagged", "", "ACTIVEMQ", "5.17.6", "", nil)
			require.NoError(t, err)

			return c.Arn
		}},
		{name: "opensearch", create: func(t *testing.T) string {
			t.Helper()

			h := regionalHandler[*opensearchbackend.Handler](t, byName, "OpenSearch")
			d, err := h.BackendFor(euRegion).CreateDomain(opensearchbackend.CreateDomainInput{Name: "tagged"})
			require.NoError(t, err)

			return d.ARN
		}},
		{name: "appsync", create: func(t *testing.T) string {
			t.Helper()

			h := regionalHandler[*appsyncbackend.Handler](t, byName, "AppSync")
			api, err := h.BackendFor(euRegion).CreateGraphqlAPI(
				"tagged", appsyncbackend.AuthenticationType("API_KEY"), false, "", "", nil, nil, nil)
			require.NoError(t, err)

			return api.ARN
		}},
		{name: "redshift", create: func(t *testing.T) string {
			t.Helper()

			h := regionalHandler[*redshiftbackend.Handler](t, byName, "Redshift")
			_, err := h.BackendFor(euRegion).CreateCluster(
				"tagged", "dc2.large", "dev", "admin", nil, "", redshiftbackend.CreateClusterOptions{})
			require.NoError(t, err)

			return arn.Build("redshift", euRegion, crossAcct, "cluster:tagged")
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resourceARN := tc.create(t)
			rgtH := regionalHandler[*rgtapibackend.Handler](t, byName, "ResourceGroupsTaggingAPI")

			_, err := rgtH.Backend.TagResources(euCtx, &rgtapibackend.TagResourcesInput{
				ResourceARNList: []string{resourceARN}, Tags: map[string]string{"Team": tc.name},
			})
			require.NoError(t, err)

			in := &rgtapibackend.GetResourcesInput{
				TagFilters: []rgtapibackend.TagFilter{{Key: "Team", Values: []string{tc.name}}},
			}

			euOut, err := rgtH.Backend.GetResources(euCtx, in)
			require.NoError(t, err)

			homeOut, err := rgtH.Backend.GetResources(t.Context(), in)
			require.NoError(t, err)

			assert.Contains(t, taggedARNs(euOut), resourceARN)
			assert.NotContains(t, taggedARNs(homeOut), resourceARN)
		})
	}
}

type fakeBrokerRuntime struct {
	mu    sync.Mutex
	count int
}

func (f *fakeBrokerRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.count++

	return "ctr-" + spec.Name, nil
}

func (f *fakeBrokerRuntime) StopAndRemove(context.Context, string) error { return nil }

func TestMQRegionResolver_ResolvesBrokerInARNRegion(t *testing.T) {
	t.Parallel()

	home := mqbackend.NewInMemoryBackend(crossAcct, crossHome)
	home.EnableBrokers(mqbackend.BrokerConfig{
		Runtime: &fakeBrokerRuntime{},
		Probe:   func(context.Context, string, string, string, string) error { return nil },
	})

	h := mqbackend.NewHandler(home)
	h.EnableRegions()
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	euBk, ok := h.BackendFor(euRegion).(*mqbackend.InMemoryBackend)
	require.True(t, ok)

	br, err := euBk.CreateBroker("b1", "", "ACTIVEMQ", "", "", false, false, nil, nil,
		[]*mqbackend.User{{Username: "admin", Password: "s3cretpassword12"}}, nil)
	require.NoError(t, err)

	r := &mqRegionResolver{handler: h}

	require.Eventually(t, func() bool {
		_, _, found := r.MQConsumerEndpoint(br.BrokerArn)

		return found
	}, time.Minute, time.Millisecond)

	_, _, found := home.MQConsumerEndpoint(br.BrokerArn)
	assert.False(t, found, "home region does not own the broker")
}

func TestFirehoseOpenSearchAdapter_IndexesInDomainARNRegion(t *testing.T) {
	t.Parallel()

	h := opensearchbackend.NewHandler(opensearchbackend.NewInMemoryBackend(crossAcct, crossHome))
	h.EnableRegions()

	euBk, ok := h.BackendFor(euRegion).(*opensearchbackend.InMemoryBackend)
	require.True(t, ok)

	_, err := euBk.CreateDomain(opensearchbackend.CreateDomainInput{Name: "logs"})
	require.NoError(t, err)

	a := &firehoseOpenSearchAdapter{handler: h}
	require.NoError(t, a.IndexDocumentInRegion(euRegion, "logs", "idx", map[string]any{"k": "v"}))

	homeBk, ok := h.BackendFor(crossHome).(*opensearchbackend.InMemoryBackend)
	require.True(t, ok)

	_, _, _, err = homeBk.IndexDocument("logs", "idx", "", map[string]any{"k": "v"})
	require.Error(t, err, "home region has no such domain")
}

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventgrid/armeventgrid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	egScopeRG  = "scope-rg"
	egOtherRG  = "other-rg"
	egSubScope = "/subscriptions/" + redisSDKSub
)

func (s *egDomainStack) armDo(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), method, s.armSrv.URL+path+"?api-version=2021-04-01",
		bytes.NewReader(raw))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.armSrv.Client().Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp
}

func (s *egDomainStack) mustARM(t *testing.T, method, path string, body any) {
	t.Helper()

	resp := s.armDo(t, method, path, body)
	require.Less(t, resp.StatusCode, http.StatusMultipleChoices, "%s %s", method, path)
}

func (s *egDomainStack) resourceGroup(t *testing.T, name string) {
	t.Helper()
	s.mustARM(t, http.MethodPut, egSubScope+"/resourceGroups/"+name, map[string]any{"location": "local"})
}

func (s *egDomainStack) subscribe(t *testing.T, scope, name, url string, types ...string) {
	t.Helper()

	sub := webhookSubscription(url, 1)
	if len(types) > 0 {
		sub.Properties.Filter = &armeventgrid.EventSubscriptionFilter{IncludedEventTypes: pointers(types)}
	}

	p, err := s.factory.NewEventSubscriptionsClient().BeginCreateOrUpdate(t.Context(), scope, name, *sub, nil)
	require.NoError(t, err)
	_, err = p.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)
}

// await delivers due events until one matching eventType and subject reaches r.
func (s *egDomainStack) await(
	ctx context.Context, t *testing.T, r *egReceiver, eventType, subject string,
) map[string]any {
	t.Helper()

	var found map[string]any

	require.Eventually(t, func() bool {
		s.backend.RunDue(ctx)

		evs, _ := r.events(t)
		for _, ev := range evs {
			if ev["eventType"] == eventType && ev["subject"] == subject {
				found = ev

				return true
			}
		}

		return false
	}, 5*time.Second, 20*time.Millisecond, "no %s event for %s", eventType, subject)

	return found
}

func TestAzureEventGridARMResourceEventsSDK(t *testing.T) {
	t.Parallel()

	widget := egSubScope + "/resourceGroups/" + egScopeRG + "/providers/Microsoft.Foo/widgets/w1"
	rgURI := egSubScope + "/resourceGroups/" + egOtherRG
	topicURI := egSubScope + "/resourceGroups/" + egScopeRG + "/providers/Microsoft.EventGrid/topics/t1"

	tests := []struct {
		act       func(t *testing.T, s *egDomainStack)
		name      string
		eventType string
		subject   string
		operation string
		rgScoped  bool
	}{
		{
			name: "write", eventType: "Microsoft.Resources.ResourceWriteSuccess", subject: widget,
			operation: "Microsoft.Foo/widgets/write", rgScoped: true,
			act: func(t *testing.T, s *egDomainStack) {
				t.Helper()
				s.mustARM(t, http.MethodPut, widget, map[string]any{"location": "local"})
			},
		},
		{
			name: "delete", eventType: "Microsoft.Resources.ResourceDeleteSuccess", subject: widget,
			operation: "Microsoft.Foo/widgets/delete", rgScoped: true,
			act: func(t *testing.T, s *egDomainStack) {
				t.Helper()
				s.mustARM(t, http.MethodPut, widget, map[string]any{"location": "local"})
				s.mustARM(t, http.MethodDelete, widget, nil)
			},
		},
		{
			name: "action", eventType: "Microsoft.Resources.ResourceActionSuccess", subject: topicURI,
			operation: "Microsoft.EventGrid/topics/listKeys/action", rgScoped: true,
			act: func(t *testing.T, s *egDomainStack) {
				t.Helper()

				p, err := s.factory.NewTopicsClient().BeginCreateOrUpdate(t.Context(), egScopeRG, "t1",
					armeventgrid.Topic{Location: new("local")}, nil)
				require.NoError(t, err)
				_, err = p.PollUntilDone(t.Context(), pollOpts())
				require.NoError(t, err)

				_, err = s.factory.NewTopicsClient().ListSharedAccessKeys(t.Context(), egScopeRG, "t1", nil)
				require.NoError(t, err)
			},
		},
		{
			name: "resource group write", eventType: "Microsoft.Resources.ResourceWriteSuccess", subject: rgURI,
			operation: "Microsoft.Resources/subscriptions/resourceGroups/write",
			act: func(t *testing.T, s *egDomainStack) {
				t.Helper()
				s.resourceGroup(t, egOtherRG)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newEGDomainStack(t)
			ctx := t.Context()
			s.resourceGroup(t, egScopeRG)

			subHook, rgHook, filtered := newEGReceiver(t), newEGReceiver(t), newEGReceiver(t)
			s.subscribe(t, egSubScope, "sub-all", subHook.srv.URL)
			s.subscribe(t, egSubScope+"/resourceGroups/"+egScopeRG, "rg-all", rgHook.srv.URL)
			s.subscribe(t, egSubScope, "sub-other", filtered.srv.URL, "Microsoft.Resources.ResourceDeleteSuccess")

			tt.act(t, s)

			ev := s.await(ctx, t, subHook, tt.eventType, tt.subject)
			assert.Equal(t, egSubScope, ev["topic"])
			assert.Equal(t, "2", ev["dataVersion"])
			assert.Equal(t, "1", ev["metadataVersion"])
			assert.NotEmpty(t, ev["id"])

			data, _ := ev["data"].(map[string]any)
			assert.Equal(t, tt.operation, data["operationName"])
			assert.Equal(t, tt.subject, data["resourceUri"])
			assert.Equal(t, "Succeeded", data["status"])
			assert.Equal(t, redisSDKSub, data["subscriptionId"])
			assert.NotEmpty(t, data["tenantId"])
			assert.NotEmpty(t, data["correlationId"])
			provider, _, _ := strings.Cut(tt.operation, "/")
			assert.Equal(t, provider, data["resourceProvider"])
			assert.Contains(t, data, "claims")
			assert.Contains(t, data, "authorization")
			assert.Contains(t, data, "httpRequest")

			if tt.rgScoped {
				rgEv := s.await(ctx, t, rgHook, tt.eventType, tt.subject)
				assert.Equal(t, egSubScope+"/resourceGroups/"+egScopeRG, rgEv["topic"])
			} else {
				s.backend.RunDue(ctx)

				evs, _ := rgHook.events(t)
				for _, e := range evs {
					assert.NotEqual(t, tt.subject, e["subject"], "the resource group's own event is subscription level")
				}
			}

			if tt.eventType != "Microsoft.Resources.ResourceDeleteSuccess" {
				s.backend.RunDue(ctx)

				evs, _ := filtered.events(t)
				for _, e := range evs {
					assert.Equal(t, "Microsoft.Resources.ResourceDeleteSuccess", e["eventType"])
				}
			}
		})
	}
}

func TestAzureEventGridARMScopeSubscriptionsSDK(t *testing.T) {
	t.Parallel()

	s := newEGDomainStack(t)
	ctx := t.Context()
	s.resourceGroup(t, egScopeRG)

	widget := egSubScope + "/resourceGroups/" + egScopeRG + "/providers/Microsoft.Foo/widgets/w1"
	s.mustARM(t, http.MethodPut, widget, map[string]any{"location": "local"})

	hook := newEGReceiver(t)
	client := s.factory.NewEventSubscriptionsClient()

	for _, scope := range []string{egSubScope, egSubScope + "/resourceGroups/" + egScopeRG, widget} {
		s.subscribe(t, scope, "scoped", hook.srv.URL)

		got, err := client.Get(ctx, scope, "scoped", nil)
		require.NoError(t, err)
		assert.Equal(t, scope+"/providers/Microsoft.EventGrid/eventSubscriptions/scoped", *got.ID)
		assert.Equal(t, "Microsoft.EventGrid/eventSubscriptions", *got.Type)
		assert.Equal(t, scope, *got.Properties.Topic)
		assert.Equal(t, armeventgrid.EventSubscriptionProvisioningStateSucceeded, *got.Properties.ProvisioningState)

		full, err := client.GetFullURL(ctx, scope, "scoped", nil)
		require.NoError(t, err)
		assert.Equal(t, hook.srv.URL, *full.EndpointURL)

		attrs, err := client.GetDeliveryAttributes(ctx, scope, "scoped", nil)
		require.NoError(t, err)
		assert.Empty(t, attrs.Value)

		del, err := client.BeginDelete(ctx, scope, "scoped", nil)
		require.NoError(t, err)
		_, err = del.PollUntilDone(ctx, pollOpts())
		require.NoError(t, err)

		_, err = client.Get(ctx, scope, "scoped", nil)
		require.Error(t, err, "a deleted subscription is gone")
	}

	missing := []struct{ name, scope string }{
		{"resource group", egSubScope + "/resourceGroups/nope"},
		{"resource", egSubScope + "/resourceGroups/" + egScopeRG + "/providers/Microsoft.Foo/widgets/nope"},
	}
	for _, m := range missing {
		_, err := client.BeginCreateOrUpdate(ctx, m.scope, "x", *webhookSubscription(hook.srv.URL, 1), nil)
		require.Error(t, err, "%s scope must exist", m.name)
	}
}

func names(subs []*armeventgrid.EventSubscription) []string {
	out := make([]string, 0, len(subs))
	for _, s := range subs {
		out = append(out, *s.Name)
	}

	return out
}

// drain reads every page, returning the subscription names and the page count.
func drain[R any](ctx context.Context, t *testing.T, pg *runtime.Pager[R]) ([]string, int) {
	t.Helper()

	out, pages := []string{}, 0

	for pg.More() {
		page, err := pg.NextPage(ctx)
		require.NoError(t, err)

		pages++

		subs, ok := reflect.ValueOf(page).FieldByName("Value").Interface().([]*armeventgrid.EventSubscription)
		require.True(t, ok)

		out = append(out, names(subs)...)
	}

	return out, pages
}

func TestAzureEventGridListEventSubscriptionsSDK(t *testing.T) {
	t.Parallel()

	s := newEGDomainStack(t)
	ctx := t.Context()
	s.resourceGroup(t, egScopeRG)
	s.resourceGroup(t, egOtherRG)

	hook := newEGReceiver(t)
	rgScope := egSubScope + "/resourceGroups/" + egScopeRG
	otherScope := egSubScope + "/resourceGroups/" + egOtherRG
	widget := rgScope + "/providers/Microsoft.Foo/widgets/w1"
	s.mustARM(t, http.MethodPut, widget, map[string]any{"location": "local"})

	tp, err := s.factory.NewTopicsClient().BeginCreateOrUpdate(ctx, egScopeRG, "t1",
		armeventgrid.Topic{Location: new("westus")}, nil)
	require.NoError(t, err)
	_, err = tp.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	dp, err := s.factory.NewDomainsClient().BeginCreateOrUpdate(ctx, egScopeRG, "d1",
		armeventgrid.Domain{Location: new("eastus")}, nil)
	require.NoError(t, err)
	_, err = dp.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	subscribe := func(scope, name string) { s.subscribe(t, scope, name, hook.srv.URL) }
	subscribe(egSubScope, "sub-a")
	subscribe(egSubScope, "sub-b")
	subscribe(rgScope, "rg-a")
	subscribe(otherScope, "other-a")
	subscribe(widget, "res-a")
	subscribe(rgScope+"/providers/Microsoft.EventGrid/topics/t1", "topic-a")
	subscribe(rgScope+"/providers/Microsoft.EventGrid/domains/d1", "domain-a")
	subscribe(rgScope+"/providers/Microsoft.EventGrid/domains/d1/topics/orders", "dtopic-a")

	c := s.factory.NewEventSubscriptionsClient()

	tests := []struct {
		list      func() ([]string, int)
		name      string
		want      []string
		wantPages int
	}{
		{
			name: "global by subscription",
			list: func() ([]string, int) {
				pg := c.NewListGlobalBySubscriptionPager(nil)

				return drain(ctx, t, pg)
			},
			want: []string{"other-a", "res-a", "rg-a", "sub-a", "sub-b"}, wantPages: 1,
		},
		{
			name: "global by subscription pages with top",
			list: func() ([]string, int) {
				pg := c.NewListGlobalBySubscriptionPager(&armeventgrid.EventSubscriptionsClientListGlobalBySubscriptionOptions{
					Top: new(int32(2)),
				})

				return drain(ctx, t, pg)
			},
			want: []string{"other-a", "res-a", "rg-a", "sub-a", "sub-b"}, wantPages: 3,
		},
		{
			name: "global by subscription filtered by name",
			list: func() ([]string, int) {
				pg := c.NewListGlobalBySubscriptionPager(&armeventgrid.EventSubscriptionsClientListGlobalBySubscriptionOptions{
					Filter: new("contains(name, 'SUB') and name ne 'sub-b' or name eq 'res-a'"),
				})

				return drain(ctx, t, pg)
			},
			want: []string{"res-a", "sub-a"}, wantPages: 1,
		},
		{
			name: "global by subscription with not",
			list: func() ([]string, int) {
				pg := c.NewListGlobalBySubscriptionPager(&armeventgrid.EventSubscriptionsClientListGlobalBySubscriptionOptions{
					Filter: new("not (contains(name, 'a'))"),
				})

				return drain(ctx, t, pg)
			},
			want: []string{"sub-b"}, wantPages: 1,
		},
		{
			name: "global by resource group",
			list: func() ([]string, int) {
				pg := c.NewListGlobalByResourceGroupPager(egScopeRG, nil)

				return drain(ctx, t, pg)
			},
			want: []string{"res-a", "rg-a"}, wantPages: 1,
		},
		{
			name: "global by subscription for topic type",
			list: func() ([]string, int) {
				pg := c.NewListGlobalBySubscriptionForTopicTypePager("Microsoft.Resources.ResourceGroups", nil)

				return drain(ctx, t, pg)
			},
			want: []string{"other-a", "rg-a"}, wantPages: 1,
		},
		{
			name: "global by resource group for topic type",
			list: func() ([]string, int) {
				pg := c.NewListGlobalByResourceGroupForTopicTypePager(egScopeRG, "Microsoft.Foo.Widgets", nil)

				return drain(ctx, t, pg)
			},
			want: []string{"res-a"}, wantPages: 1,
		},
		{
			name: "regional by subscription",
			list: func() ([]string, int) {
				pg := c.NewListRegionalBySubscriptionPager("westus", nil)

				return drain(ctx, t, pg)
			},
			want: []string{"topic-a"}, wantPages: 1,
		},
		{
			name: "regional by resource group",
			list: func() ([]string, int) {
				pg := c.NewListRegionalByResourceGroupPager(egScopeRG, "East US", nil)

				return drain(ctx, t, pg)
			},
			want: []string{"domain-a", "dtopic-a"}, wantPages: 1,
		},
		{
			name: "regional by subscription for topic type",
			list: func() ([]string, int) {
				pg := c.NewListRegionalBySubscriptionForTopicTypePager("eastus", "Microsoft.EventGrid.Domains", nil)

				return drain(ctx, t, pg)
			},
			want: []string{"domain-a", "dtopic-a"}, wantPages: 1,
		},
		{
			name: "regional by resource group for topic type",
			list: func() ([]string, int) {
				pg := c.NewListRegionalByResourceGroupForTopicTypePager(egScopeRG, "westus", "Microsoft.EventGrid.Domains", nil)

				return drain(ctx, t, pg)
			},
			want: []string{}, wantPages: 1,
		},
		{
			name: "by resource: custom topic",
			list: func() ([]string, int) {
				pg := c.NewListByResourcePager(egScopeRG, "Microsoft.EventGrid", "topics", "t1", nil)

				return drain(ctx, t, pg)
			},
			want: []string{"topic-a"}, wantPages: 1,
		},
		{
			name: "by resource: other resource",
			list: func() ([]string, int) {
				pg := c.NewListByResourcePager(egScopeRG, "Microsoft.Foo", "widgets", "w1", nil)

				return drain(ctx, t, pg)
			},
			want: []string{"res-a"}, wantPages: 1,
		},
		{
			name: "by domain topic",
			list: func() ([]string, int) {
				pg := c.NewListByDomainTopicPager(egScopeRG, "d1", "orders", nil)

				return drain(ctx, t, pg)
			},
			want: []string{"dtopic-a"}, wantPages: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, pages := tt.list()
			assert.ElementsMatch(t, tt.want, got)
			assert.Equal(t, tt.wantPages, pages)
		})
	}

	t.Run("invalid parameters are rejected", func(t *testing.T) {
		t.Parallel()

		for _, o := range []*armeventgrid.EventSubscriptionsClientListGlobalBySubscriptionOptions{
			{Filter: new("location eq 'westus'")},
			{Filter: new("contains(name, 'x'")},
			{Top: new(int32(101))},
		} {
			_, listErr := c.NewListGlobalBySubscriptionPager(o).NextPage(ctx)
			require.Error(t, listErr)
		}
	})
}

func pointers(in []string) []*string {
	out := make([]*string, 0, len(in))
	for i := range in {
		out = append(out, &in[i])
	}

	return out
}

package app

import (
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventgrid/armeventgrid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const egPatchWidgetID = "/subscriptions/" + redisSDKSub + "/resourceGroups/" + egScopeRG +
	"/providers/Microsoft.Foo/widgets/"

const egDeadLetterAccount = egSubScope + "/resourceGroups/" + egScopeRG +
	"/providers/Microsoft.Storage/storageAccounts/dl"

func (s *egDomainStack) patchStack(t *testing.T) map[string]string {
	t.Helper()

	ctx := t.Context()
	s.resourceGroup(t, egScopeRG)

	rgScope := egSubScope + "/resourceGroups/" + egScopeRG
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

	return map[string]string{
		"custom topic":   rgScope + "/providers/Microsoft.EventGrid/topics/t1",
		"domain":         rgScope + "/providers/Microsoft.EventGrid/domains/d1",
		"domain topic":   rgScope + "/providers/Microsoft.EventGrid/domains/d1/topics/orders",
		"subscription":   egSubScope,
		"resource group": rgScope,
		"resource":       widget,
	}
}

func TestAzureEventGridEventSubscriptionPatchSDK(t *testing.T) {
	t.Parallel()

	expiry := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []string{
		"custom topic", "domain", "domain topic", "subscription", "resource group", "resource",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := newEGDomainStack(t)
			ctx := t.Context()
			scope := s.patchStack(t)[name]
			hookA, hookB := newEGReceiver(t), newEGReceiver(t)
			client := s.factory.NewEventSubscriptionsClient()

			sub := webhookSubscription(hookA.srv.URL, 1)
			sub.Properties.Labels = pointers([]string{"orig"})
			sub.Properties.ExpirationTimeUTC = &expiry
			sub.Properties.Filter = &armeventgrid.EventSubscriptionFilter{
				SubjectBeginsWith:  new("/keep"),
				IncludedEventTypes: pointers([]string{"App.Thing"}),
			}
			sub.Properties.RetryPolicy = &armeventgrid.RetryPolicy{
				MaxDeliveryAttempts: new(int32(5)), EventTimeToLiveInMinutes: new(int32(60)),
			}

			cp, err := client.BeginCreateOrUpdate(ctx, scope, "patched", *sub, nil)
			require.NoError(t, err)
			_, err = cp.PollUntilDone(ctx, pollOpts())
			require.NoError(t, err)

			update := func(params armeventgrid.EventSubscriptionUpdateParameters) (armeventgrid.EventSubscription, error) {
				p, perr := client.BeginUpdate(ctx, scope, "patched", params, nil)
				if perr != nil {
					return armeventgrid.EventSubscription{}, perr
				}

				res, perr := p.PollUntilDone(ctx, pollOpts())

				return res.EventSubscription, perr
			}

			// only labels supplied: everything else is kept
			got, err := update(
				armeventgrid.EventSubscriptionUpdateParameters{Labels: pointers([]string{"new1", "new2"})},
			)
			require.NoError(t, err)
			require.NotNil(t, got.Properties)
			assert.Equal(t, scope+"/providers/Microsoft.EventGrid/eventSubscriptions/patched", *got.ID)
			assert.Equal(t, armeventgrid.EventSubscriptionProvisioningStateSucceeded, *got.Properties.ProvisioningState)
			assert.Equal(t, []string{"new1", "new2"}, derefStrings(got.Properties.Labels))
			assert.Equal(t, hookA.srv.URL, destURL(t, got))
			assert.Equal(t, "/keep", *got.Properties.Filter.SubjectBeginsWith)
			assert.Equal(t, []string{"App.Thing"}, derefStrings(got.Properties.Filter.IncludedEventTypes))
			assert.Equal(t, int32(5), *got.Properties.RetryPolicy.MaxDeliveryAttempts)
			assert.Equal(t, int32(60), *got.Properties.RetryPolicy.EventTimeToLiveInMinutes)
			assert.True(t, expiry.Equal(*got.Properties.ExpirationTimeUTC))

			// destination, filter and retry policy supplied: labels and expiry are kept
			newURL := hookB.srv.URL
			got, err = update(armeventgrid.EventSubscriptionUpdateParameters{
				Destination: &armeventgrid.WebHookEventSubscriptionDestination{
					EndpointType: new(armeventgrid.EndpointTypeWebHook),
					Properties:   &armeventgrid.WebHookEventSubscriptionDestinationProperties{EndpointURL: &newURL},
				},
				Filter:      &armeventgrid.EventSubscriptionFilter{SubjectEndsWith: new(".png")},
				RetryPolicy: &armeventgrid.RetryPolicy{MaxDeliveryAttempts: new(int32(7))},
			})
			require.NoError(t, err)
			assert.Equal(t, newURL, destURL(t, got))
			assert.Equal(t, ".png", *got.Properties.Filter.SubjectEndsWith)
			assert.Empty(t, got.Properties.Filter.IncludedEventTypes, "a supplied filter replaces the stored one")
			assert.Equal(t, int32(7), *got.Properties.RetryPolicy.MaxDeliveryAttempts)
			assert.Equal(t, []string{"new1", "new2"}, derefStrings(got.Properties.Labels))
			assert.True(t, expiry.Equal(*got.Properties.ExpirationTimeUTC))

			got, err = update(armeventgrid.EventSubscriptionUpdateParameters{
				DeadLetterDestination: &armeventgrid.StorageBlobDeadLetterDestination{
					EndpointType: new(armeventgrid.DeadLetterEndPointTypeStorageBlob),
					Properties: &armeventgrid.StorageBlobDeadLetterDestinationProperties{
						ResourceID:        new(egDeadLetterAccount),
						BlobContainerName: new("dead"),
					},
				},
			})
			require.NoError(t, err)

			dl, ok := got.Properties.DeadLetterDestination.(*armeventgrid.StorageBlobDeadLetterDestination)
			require.True(t, ok)
			assert.Equal(t, "dead", *dl.Properties.BlobContainerName)
			assert.Equal(t, newURL, destURL(t, got), "the destination is kept")

			read, err := client.Get(ctx, scope, "patched", nil)
			require.NoError(t, err)
			assert.Equal(t, newURL, destURL(t, read.EventSubscription))
			assert.Equal(t, []string{"new1", "new2"}, derefStrings(read.Properties.Labels))

			full, err := client.GetFullURL(ctx, scope, "patched", nil)
			require.NoError(t, err)
			assert.Equal(t, newURL, *full.EndpointURL)
		})
	}
}

func derefStrings(in []*string) []string {
	out := make([]string, 0, len(in))
	for _, p := range in {
		out = append(out, *p)
	}

	return out
}

func destURL(t *testing.T, es armeventgrid.EventSubscription) string {
	t.Helper()

	dest, ok := es.Properties.Destination.(*armeventgrid.WebHookEventSubscriptionDestination)
	require.True(t, ok)

	if dest.Properties.EndpointBaseURL != nil {
		return *dest.Properties.EndpointBaseURL
	}

	return *dest.Properties.EndpointURL
}

func TestAzureEventGridEventSubscriptionPatchErrorsSDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		patch armeventgrid.EventSubscriptionUpdateParameters
		name  string
		scope string
	}{
		{
			name: "retry attempts out of range", scope: egSubScope,
			patch: armeventgrid.EventSubscriptionUpdateParameters{
				RetryPolicy: &armeventgrid.RetryPolicy{MaxDeliveryAttempts: new(int32(99))},
			},
		},
		{
			name: "delivery schema differs from the input schema", scope: egSubScope,
			patch: armeventgrid.EventSubscriptionUpdateParameters{
				EventDeliverySchema: new(armeventgrid.EventDeliverySchemaCloudEventSchemaV10),
			},
		},
		{
			name: "dead letter destination without a container", scope: egSubScope,
			patch: armeventgrid.EventSubscriptionUpdateParameters{
				DeadLetterDestination: &armeventgrid.StorageBlobDeadLetterDestination{
					EndpointType: new(armeventgrid.DeadLetterEndPointTypeStorageBlob),
					Properties: &armeventgrid.StorageBlobDeadLetterDestinationProperties{
						ResourceID: new(egDeadLetterAccount),
					},
				},
			},
		},
		{
			name: "webhook destination without an http endpoint", scope: egSubScope,
			patch: armeventgrid.EventSubscriptionUpdateParameters{
				Destination: &armeventgrid.WebHookEventSubscriptionDestination{
					EndpointType: new(armeventgrid.EndpointTypeWebHook),
					Properties: &armeventgrid.WebHookEventSubscriptionDestinationProperties{
						EndpointURL: new("ftp://x"),
					},
				},
			},
		},
		{
			name: "resource identity delivery is not supported", scope: egSubScope,
			patch: armeventgrid.EventSubscriptionUpdateParameters{
				DeliveryWithResourceIdentity: &armeventgrid.DeliveryWithResourceIdentity{
					Identity: &armeventgrid.EventSubscriptionIdentity{
						Type: new(armeventgrid.EventSubscriptionIdentityTypeSystemAssigned),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newEGDomainStack(t)
			ctx := t.Context()
			hook := newEGReceiver(t)
			client := s.factory.NewEventSubscriptionsClient()

			s.subscribe(t, tt.scope, "patched", hook.srv.URL)

			_, err := client.BeginUpdate(ctx, tt.scope, "patched", tt.patch, nil)
			require.Error(t, err)

			read, err := client.Get(ctx, tt.scope, "patched", nil)
			require.NoError(t, err)
			assert.Equal(t, hook.srv.URL, destURL(t, read.EventSubscription), "a rejected patch changes nothing")
			assert.Equal(t, int32(30), *read.Properties.RetryPolicy.MaxDeliveryAttempts)
		})
	}
}

func TestAzureEventGridEventSubscriptionPatchMissingSDK(t *testing.T) {
	t.Parallel()

	s := newEGDomainStack(t)
	scopes := s.patchStack(t)
	client := s.factory.NewEventSubscriptionsClient()

	for name, scope := range scopes {
		_, err := client.BeginUpdate(t.Context(), scope, "ghost",
			armeventgrid.EventSubscriptionUpdateParameters{Labels: pointers([]string{"x"})}, nil)
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "404", name)
	}
}

func TestAzureEventGridEventSubscriptionPatchRedirectsDeliverySDK(t *testing.T) {
	t.Parallel()

	s := newEGDomainStack(t)
	ctx := t.Context()
	s.resourceGroup(t, egScopeRG)

	widget := egSubScope + "/resourceGroups/" + egScopeRG + "/providers/Microsoft.Foo/widgets/w1"
	oldHook, newHook := newEGReceiver(t), newEGReceiver(t)
	s.subscribe(t, egSubScope, "redirect", oldHook.srv.URL)

	url := newHook.srv.URL
	p, err := s.factory.NewEventSubscriptionsClient().BeginUpdate(ctx, egSubScope, "redirect",
		armeventgrid.EventSubscriptionUpdateParameters{Destination: &armeventgrid.WebHookEventSubscriptionDestination{
			EndpointType: new(armeventgrid.EndpointTypeWebHook),
			Properties:   &armeventgrid.WebHookEventSubscriptionDestinationProperties{EndpointURL: &url},
		}}, nil)
	require.NoError(t, err)
	_, err = p.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	s.mustARM(t, http.MethodPut, widget, map[string]any{"location": "local"})
	s.await(ctx, t, newHook, "Microsoft.Resources.ResourceWriteSuccess", widget)

	evs, _ := oldHook.events(t)
	assert.Empty(t, evs, "the replaced destination receives nothing")
}

func TestAzureEventGridResourceScopeEventsSDK(t *testing.T) {
	t.Parallel()

	acct, acctLonger := egPatchWidgetID+"acct1", egPatchWidgetID+"acct10"
	child := acct + "/gadgets/g1"

	tests := []struct {
		act       func(t *testing.T, s *egDomainStack)
		name      string
		eventType string
		subject   string
	}{
		{
			name: "write of the scoped resource", eventType: "Microsoft.Resources.ResourceWriteSuccess", subject: acct,
			act: func(t *testing.T, s *egDomainStack) {
				t.Helper()
				s.mustARM(t, http.MethodPut, acct, map[string]any{"location": "local"})
			},
		},
		{
			name: "write of a child resource", eventType: "Microsoft.Resources.ResourceWriteSuccess", subject: child,
			act: func(t *testing.T, s *egDomainStack) {
				t.Helper()
				s.mustARM(t, http.MethodPut, acct, map[string]any{"location": "local"})
				s.mustARM(t, http.MethodPut, child, map[string]any{})
			},
		},
		{
			name:      "delete of the scoped resource",
			eventType: "Microsoft.Resources.ResourceDeleteSuccess",
			subject:   acct,
			act: func(t *testing.T, s *egDomainStack) {
				t.Helper()
				s.mustARM(t, http.MethodPut, acct, map[string]any{"location": "local"})
				s.mustARM(t, http.MethodDelete, acct, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newEGDomainStack(t)
			ctx := t.Context()
			s.resourceGroup(t, egScopeRG)
			s.mustARM(t, http.MethodPut, acctLonger, map[string]any{"location": "local"})
			s.mustARM(t, http.MethodPut, egPatchWidgetID+"acct2", map[string]any{"location": "local"})
			s.mustARM(t, http.MethodPut, acct, map[string]any{"location": "local"})

			scoped, sibling := newEGReceiver(t), newEGReceiver(t)
			s.subscribe(t, acct, "on-acct", scoped.srv.URL)
			s.subscribe(t, egPatchWidgetID+"acct2", "on-acct2", sibling.srv.URL)

			tt.act(t, s)

			ev := s.await(ctx, t, scoped, tt.eventType, tt.subject)
			assert.Equal(t, acct, ev["topic"], "the topic of a resource scope subscription is the resource")

			data, _ := ev["data"].(map[string]any)
			assert.Equal(t, tt.subject, data["resourceUri"])
			assert.Equal(t, "Succeeded", data["status"])

			s.backend.RunDue(ctx)

			evs, _ := scoped.events(t)
			for _, e := range evs {
				assert.NotContains(
					t,
					e["subject"],
					"acct10",
					"a resource whose name only shares a prefix is not covered",
				)
			}

			evs, _ = sibling.events(t)
			assert.Empty(t, evs, "another resource's scope receives nothing")
		})
	}
}

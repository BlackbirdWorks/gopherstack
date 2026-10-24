package azureeventgrid_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

const (
	armSub = "11111111-1111-1111-1111-111111111111"
	armRG  = "rg1"
)

func queueProps(queue string, extra map[string]any) map[string]any {
	props := map[string]any{"destination": map[string]any{
		"endpointType": "StorageQueue",
		"properties":   map[string]any{"resourceId": storageID, "queueName": queue},
	}}
	for k, v := range extra {
		props[k] = v
	}

	return props
}

func armEvent(eventType, uri, rg string) azureeventgrid.ResourceEvent {
	return azureeventgrid.ResourceEvent{
		EventType: eventType, SubscriptionID: armSub, ResourceGroup: rg, ResourceURI: uri,
		Provider: "Microsoft.Storage", OperationName: "Microsoft.Storage/storageAccounts/write",
		CorrelationID: "corr-1", TenantID: "tenant-1", HTTPMethod: "PUT", HTTPURL: "https://arm" + uri,
		ClientRequestID: "client-1", ClientIP: "10.0.0.1", Claims: map[string]any{"aud": "arm"},
	}
}

func TestPublishResourceEvent(t *testing.T) {
	t.Parallel()

	subScope := "/subscriptions/" + armSub
	rgScope := subScope + "/resourceGroups/" + armRG
	uri := rgScope + "/providers/Microsoft.Storage/storageAccounts/acct"

	tests := []struct {
		name       string
		ev         azureeventgrid.ResourceEvent
		scopes     map[string]map[string]any
		wantTopics map[string]string
	}{
		{
			name: "subscription and resource group scopes both receive a resource event",
			ev:   armEvent(azureeventgrid.EventResourceWriteSuccess, uri, armRG),
			scopes: map[string]map[string]any{
				subScope: queueProps("sub-q", nil), rgScope: queueProps("rg-q", nil),
			},
			wantTopics: map[string]string{"sub-q": subScope, "rg-q": rgScope},
		},
		{
			name:       "an event without a resource group reaches only the subscription scope",
			ev:         armEvent(azureeventgrid.EventResourceDeleteSuccess, rgScope, ""),
			scopes:     map[string]map[string]any{subScope: queueProps("sub-q", nil), rgScope: queueProps("rg-q", nil)},
			wantTopics: map[string]string{"sub-q": subScope},
		},
		{
			name: "the event type filter applies",
			ev:   armEvent(azureeventgrid.EventResourceWriteSuccess, uri, armRG),
			scopes: map[string]map[string]any{
				subScope: queueProps("sub-q", map[string]any{
					"filter": map[string]any{"includedEventTypes": []string{azureeventgrid.EventResourceDeleteSuccess}},
				}),
			},
			wantTopics: map[string]string{},
		},
		{
			name: "the subject filter applies",
			ev:   armEvent(azureeventgrid.EventResourceWriteSuccess, uri, armRG),
			scopes: map[string]map[string]any{
				rgScope: queueProps("rg-q", map[string]any{"filter": map[string]any{"subjectEndsWith": "/other"}}),
			},
			wantTopics: map[string]string{},
		},
		{
			name:       "a scope for another subscription gets nothing",
			ev:         armEvent(azureeventgrid.EventResourceWriteSuccess, uri, armRG),
			scopes:     map[string]map[string]any{"/subscriptions/other": queueProps("x-q", nil)},
			wantTopics: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, rec := newBackend(t)

			for scope, props := range tt.scopes {
				_, err := b.PutScopedSubscription(t.Context(), azureeventgrid.Scope{Resource: scope}, "s1", props)
				require.NoError(t, err)
			}

			b.PublishResourceEvent(tt.ev)
			b.RunDue(t.Context())

			got := rec.all()
			require.Len(t, got, len(tt.wantTopics))

			for _, g := range got {
				var ev map[string]any

				require.NoError(t, json.Unmarshal([]byte(g.text), &ev))
				assert.Equal(t, tt.wantTopics[g.queue], ev["topic"])
				assert.Equal(t, tt.ev.ResourceURI, ev["subject"])
				assert.Equal(t, tt.ev.EventType, ev["eventType"])
				assert.Equal(t, "2", ev["dataVersion"])
				assert.Equal(t, "1", ev["metadataVersion"])
				assert.NotEmpty(t, ev["id"])
				assert.NotEmpty(t, ev["eventTime"])

				data, _ := ev["data"].(map[string]any)
				assert.Equal(t, tt.ev.ResourceURI, data["resourceUri"])
				assert.Equal(t, "Microsoft.Storage", data["resourceProvider"])
				assert.Equal(t, tt.ev.OperationName, data["operationName"])
				assert.Equal(t, "Succeeded", data["status"])
				assert.Equal(t, armSub, data["subscriptionId"])
				assert.Equal(t, "tenant-1", data["tenantId"])
				assert.Equal(t, "corr-1", data["correlationId"])
				assert.Equal(t, map[string]any{"aud": "arm"}, data["claims"])
				assert.Equal(t, map[string]any{
					"scope": tt.ev.ResourceURI, "action": tt.ev.OperationName,
				}, data["authorization"])
				assert.Equal(t, map[string]any{
					"clientRequestId": "client-1", "clientIpAddress": "10.0.0.1",
					"method": "PUT", "url": "https://arm" + tt.ev.ResourceURI,
				}, data["httpRequest"])
			}
		})
	}
}

func TestResourceScopeSubscriptionLifecycle(t *testing.T) {
	t.Parallel()

	scope := azureeventgrid.Scope{Resource: "/subscriptions/" + armSub + "/resourceGroups/" + armRG}

	b, _, _ := newBackend(t)

	_, err := b.ScopedSubscription(scope, "s1")
	require.ErrorIs(t, err, azureeventgrid.ErrSubscriptionNotFound)

	props, err := b.PutScopedSubscription(t.Context(), scope, "s1", queueProps("q", nil))
	require.NoError(t, err)
	assert.Equal(t, scope.Resource, props["topic"])
	assert.Equal(t, "EventGridSchema", props["eventDeliverySchema"])

	_, err = b.PutScopedSubscription(t.Context(), scope, "s1", map[string]any{
		"destination": map[string]any{"endpointType": "StorageQueue", "properties": map[string]any{}},
	})
	require.Error(t, err, "an invalid update is rejected")

	snap := b.Snapshot(t.Context())
	b2, _, _ := newBackend(t)
	require.NoError(t, b2.Restore(t.Context(), snap))

	again, err := b2.ScopedSubscription(scope, "s1")
	require.NoError(t, err)
	assert.Equal(t, props, again, "resource-scope subscriptions survive a snapshot")

	require.NoError(t, b2.DeleteScopedSubscription(scope, "s1"))
	_, err = b2.ScopedSubscription(scope, "s1")
	require.ErrorIs(t, err, azureeventgrid.ErrSubscriptionNotFound)
	assert.NotContains(t, string(b2.Snapshot(t.Context())), "armScopes", "an emptied scope is dropped")
}

package azureeventgrid_test

import (
	"encoding/json/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

func TestPublishStorageEvent(t *testing.T) {
	t.Parallel()

	rgScope := "/subscriptions/" + armSub + "/resourceGroups/" + armRG
	acct := rgScope + "/providers/Microsoft.Storage/storageAccounts/stacct"
	created := azureeventgrid.StorageEvent{
		Time: time.Unix(1700000000, 0), Account: "StAcct", Container: "c1", Blob: "dir/a.txt",
		EventType: "Microsoft.Storage.BlobCreated", API: "PutBlob", ContentType: "text/plain", ETag: "0x1",
		AccessTier: "Hot", RequestID: "req", ClientRequestID: "creq", BlobURL: "http://h/stacct/c1/dir/a.txt",
		ContentLength: 5,
	}
	deleted := created
	deleted.EventType, deleted.API = "Microsoft.Storage.BlobDeleted", "DeleteBlob"

	tests := []struct {
		scopes    map[string]map[string]any
		wantQueue map[string]string
		name      string
		ev        azureeventgrid.StorageEvent
	}{
		{
			name: "account scope receives created", ev: created,
			scopes:    map[string]map[string]any{acct: queueProps("acct-q", nil)},
			wantQueue: map[string]string{"acct-q": acct},
		},
		{
			name: "account scope receives deleted", ev: deleted,
			scopes:    map[string]map[string]any{acct: queueProps("acct-q", nil)},
			wantQueue: map[string]string{"acct-q": acct},
		},
		{
			name: "subject filter", ev: created,
			scopes: map[string]map[string]any{acct: queueProps("acct-q", map[string]any{
				"filter": map[string]any{"subjectBeginsWith": "/blobServices/default/containers/other/"},
			})},
			wantQueue: map[string]string{},
		},
		{
			name: "event type filter", ev: created,
			scopes: map[string]map[string]any{acct: queueProps("acct-q", map[string]any{
				"filter": map[string]any{"includedEventTypes": []string{"Microsoft.Storage.BlobDeleted"}},
			})},
			wantQueue: map[string]string{},
		},
		{
			name: "other account and wider scopes get nothing", ev: created,
			scopes: map[string]map[string]any{
				rgScope: queueProps("rg-q", nil),
				rgScope + "/providers/Microsoft.Storage/storageAccounts/stacct2": queueProps("o-q", nil),
			},
			wantQueue: map[string]string{},
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

			b.PublishStorageEvent(tt.ev)
			b.RunDue(t.Context())

			got := rec.all()
			require.Len(t, got, len(tt.wantQueue))

			for _, g := range got {
				var ev map[string]any

				require.NoError(t, json.Unmarshal([]byte(g.text), &ev, jsonx.V1()))
				assert.Equal(t, tt.wantQueue[g.queue], ev["topic"])
				assert.Equal(t, "/blobServices/default/containers/c1/blobs/dir/a.txt", ev["subject"])
				assert.Equal(t, tt.ev.EventType, ev["eventType"])
				assert.Empty(t, ev["dataVersion"])
				assert.Equal(t, "1", ev["metadataVersion"])

				data, _ := ev["data"].(map[string]any)
				assert.Equal(t, tt.ev.API, data["api"])
				assert.Equal(t, "BlockBlob", data["blobType"])
				assert.Equal(t, "http://h/stacct/c1/dir/a.txt", data["url"])
				assert.Len(t, data["sequencer"], 32)
				assert.NotEmpty(t, data["storageDiagnostics"])

				if tt.ev.EventType == "Microsoft.Storage.BlobCreated" {
					assert.Equal(t, "0x1", data["eTag"])
					assert.InDelta(t, 5, data["contentLength"], 0)
				} else {
					assert.NotContains(t, data, "eTag")
				}
			}
		})
	}
}

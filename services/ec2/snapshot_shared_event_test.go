package ec2_test

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestModifySnapshotAttributeSharedEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		wantShared []string
		wantCode   int
		wantEvents int
	}{
		{
			name: "structured add",
			form: url.Values{
				"CreateVolumePermission.Add.1.UserId": {"111111111111"},
				"CreateVolumePermission.Add.2.UserId": {"222222222222"},
			},
			wantCode: http.StatusOK, wantEvents: 1, wantShared: []string{"111111111111", "222222222222"},
		},
		{
			name: "legacy add",
			form: url.Values{
				"Attribute": {"createVolumePermission"}, "OperationType": {"add"}, "UserId.1": {"111111111111"},
			},
			wantCode: http.StatusOK, wantEvents: 1, wantShared: []string{"111111111111"},
		},
		{
			name: "legacy remove emits nothing",
			form: url.Values{
				"Attribute": {"createVolumePermission"}, "OperationType": {"remove"}, "UserId.1": {"111111111111"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "legacy group emits nothing",
			form: url.Values{
				"Attribute": {"createVolumePermission"}, "OperationType": {"add"}, "UserGroup.1": {"all"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "legacy invalid operation type",
			form: url.Values{
				"Attribute": {"createVolumePermission"}, "OperationType": {"grant"}, "UserId.1": {"111111111111"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "legacy missing operation type",
			form: url.Values{
				"Attribute": {"createVolumePermission"}, "UserId.1": {"111111111111"},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			vol, err := h.Backend.CreateVolume("us-east-1a", "gp3", 8, "")
			require.NoError(t, err)

			snap, err := h.Backend.CreateSnapshot(vol.ID, "d")
			require.NoError(t, err)

			var (
				mu     sync.Mutex
				events []ec2.SnapshotSharedEvent
			)

			h.SetSnapshotSharedListener(func(_ context.Context, evt ec2.SnapshotSharedEvent) {
				mu.Lock()
				defer mu.Unlock()

				events = append(events, evt)
			})

			tt.form.Set("Action", "ModifySnapshotAttribute")
			tt.form.Set("Version", "2016-11-15")
			tt.form.Set("SnapshotId", snap.SnapshotID)

			rec := postForm(t, h, tt.form.Encode())
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			mu.Lock()
			defer mu.Unlock()

			require.Len(t, events, tt.wantEvents)

			if tt.wantEvents > 0 {
				assert.Equal(t, tt.wantShared, events[0].SharedWith)
				assert.Equal(t, snap.SnapshotID, events[0].SnapshotID)
			}
		})
	}
}

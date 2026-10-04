package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iotanalyticsbackend "github.com/blackbirdworks/gopherstack/services/iotanalytics"
	iotdataplanebackend "github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

func TestIoTAnalyticsTarget_PutsInRuleRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home-region", region: regionA},
		{name: "other-region", region: regionB},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := iotanalyticsbackend.NewHandler(iotanalyticsbackend.NewInMemoryBackend())
			h.EnableRegions(regionA)

			for _, r := range []string{regionA, regionB} {
				bk, ok := h.BackendFor(r).(*iotanalyticsbackend.InMemoryBackend)
				require.True(t, ok)

				_, err := bk.CreateChannel(t.Context(), "events", nil, nil, nil)
				require.NoError(t, err)
			}

			target := &iotAnalyticsTarget{handler: h}
			require.NoError(t, target.PutChannelMessages(t.Context(), tc.region, "events", [][]byte{[]byte("m")}))

			for _, r := range []string{regionA, regionB} {
				bk, _ := h.BackendFor(r).(*iotanalyticsbackend.InMemoryBackend)
				got, err := bk.SampleChannelData("events", 1, false, 0, false, 0)
				require.NoError(t, err)
				assert.Equal(t, r == tc.region, len(got) == 1, r)
			}
		})
	}
}

func TestIoTShadowTarget_ReadsRuleRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		region  string
		wantErr bool
	}{
		{name: "region-with-shadow", region: regionB},
		{name: "region-without-shadow", region: regionA, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplanebackend.NewHandler(iotdataplanebackend.NewInMemoryBackend())
			h.EnableRegions(regionA)

			_, err := h.BackendFor(regionB).UpdateThingShadow("thing", "", []byte(`{"state":{"desired":{"on":true}}}`))
			require.NoError(t, err)

			doc, err := (&iotShadowTarget{handler: h}).GetThingShadow(t.Context(), tc.region, "thing", "")
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, string(doc), `"on":true`)
		})
	}
}

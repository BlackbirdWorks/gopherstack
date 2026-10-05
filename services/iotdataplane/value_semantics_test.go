package iotdataplane_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShadow_NestedUpdatesMergeAndDeltaIsMinimal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		first         string
		second        string
		wantDesired   string
		wantReported  string
		wantDelta     string
		wantNoDeltaOK bool
	}{
		{
			name:         "nested siblings kept",
			first:        `{"state":{"desired":{"a":{"x":1,"y":2}},"reported":{"a":{"x":1,"y":2}}}}`,
			second:       `{"state":{"desired":{"a":{"y":3}}}}`,
			wantDesired:  `{"a":{"x":1,"y":3}}`,
			wantReported: `{"a":{"x":1,"y":2}}`,
			wantDelta:    `{"a":{"y":3}}`,
		},
		{
			name:          "nested null removes member",
			first:         `{"state":{"reported":{"a":{"x":1,"y":2}}}}`,
			second:        `{"state":{"reported":{"a":{"x":null}}}}`,
			wantReported:  `{"a":{"y":2}}`,
			wantNoDeltaOK: true,
		},
		{
			name:          "arrays replaced",
			first:         `{"state":{"reported":{"l":[1,2,3]}}}`,
			second:        `{"state":{"reported":{"l":[9]}}}`,
			wantReported:  `{"l":[9]}`,
			wantNoDeltaOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client, _ := newTestIoTDataPlaneSDKClient(t, newTestHandler(t))

			for _, doc := range []string{tt.first, tt.second} {
				_, err := client.UpdateThingShadow(ctx, &iotdataplanesdk.UpdateThingShadowInput{
					ThingName: aws.String("thing"), Payload: []byte(doc),
				})
				require.NoError(t, err)
			}

			got, err := client.GetThingShadow(ctx, &iotdataplanesdk.GetThingShadowInput{ThingName: aws.String("thing")})
			require.NoError(t, err)

			var doc struct {
				State map[string]any `json:"state"`
			}
			require.NoError(t, json.Unmarshal(got.Payload, &doc))

			if tt.wantDesired != "" {
				assert.JSONEq(t, tt.wantDesired, mustJSON(t, doc.State["desired"]))
			}

			assert.JSONEq(t, tt.wantReported, mustJSON(t, doc.State["reported"]))

			if tt.wantNoDeltaOK {
				assert.NotContains(t, doc.State, "delta")
			} else {
				assert.JSONEq(t, tt.wantDelta, mustJSON(t, doc.State["delta"]))
			}
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}

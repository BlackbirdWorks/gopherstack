package directconnect_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

func TestCreateCFNResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props    map[string]any
		name     string
		typ      string
		wantAttr string
		wantErr  bool
	}{
		{
			name: "connection", typ: directconnect.CFNConnection, wantAttr: "ConnectionArn",
			props: map[string]any{"ConnectionName": "c", "Bandwidth": "1Gbps", "Location": "EqDC2"},
		},
		{
			name: "lag_string_minimum_links", typ: directconnect.CFNLag, wantAttr: "LagArn",
			props: map[string]any{
				"LagName": "l", "ConnectionsBandwidth": "1Gbps", "Location": "EqDC2", "MinimumLinks": "1",
			},
		},
		{
			name: "gateway_string_asn", typ: directconnect.CFNDirectConnectGateway, wantAttr: "DirectConnectGatewayArn",
			props: map[string]any{"DirectConnectGatewayName": "g", "AmazonSideAsn": "64512"},
		},
		{
			name: "unknown_property", typ: directconnect.CFNConnection, wantErr: true,
			props: map[string]any{"ConnectionName": "c", "Bandwidth": "1Gbps", "Location": "EqDC2", "Bogus": 1},
		},
		{
			name: "missing_required", typ: directconnect.CFNConnection, wantErr: true,
			props: map[string]any{"ConnectionName": "c"},
		},
		{name: "unknown_type", typ: "AWS::DirectConnect::Nope", wantErr: true, props: map[string]any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := directconnect.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")

			id, attrs, err := b.CreateCFNResource(tt.typ, tt.props)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, id)
			assert.NotEmpty(t, attrs[tt.wantAttr])

			_, found := b.CFNResourceState(tt.typ, id)
			assert.True(t, found)
		})
	}
}

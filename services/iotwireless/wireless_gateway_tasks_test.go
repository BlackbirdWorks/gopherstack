package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestCreateWirelessGatewayTaskDefinition_ARNUsesRegionAndAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		wantARN   string
	}{
		{
			name:      "default region and account",
			accountID: "000000000000",
			region:    "us-east-1",
			wantARN:   "arn:aws:iotwireless:us-east-1:000000000000:WirelessGatewayTaskDefinition/",
		},
		{
			name:      "eu-west-1 cross-region",
			accountID: "111122223333",
			region:    "eu-west-1",
			wantARN:   "arn:aws:iotwireless:eu-west-1:111122223333:WirelessGatewayTaskDefinition/",
		},
		{
			name:      "ap-southeast-2 cross-region",
			accountID: "999988887777",
			region:    "ap-southeast-2",
			wantARN:   "arn:aws:iotwireless:ap-southeast-2:999988887777:WirelessGatewayTaskDefinition/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()
			def, err := b.CreateWirelessGatewayTaskDefinition(tt.accountID, tt.region, "taskdef", false)
			require.NoError(t, err)
			assert.Greater(t, len(def.ARN), len(tt.wantARN), "ARN too short")
			assert.Equal(t, tt.wantARN, def.ARN[:len(tt.wantARN)],
				"ARN %q does not start with %q", def.ARN, tt.wantARN)
		})
	}
}

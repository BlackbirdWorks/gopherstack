package route53resolver_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestDelegationEndpointAndRule(t *testing.T) {
	t.Parallel()

	ips := []route53resolver.IPAddress{{SubnetID: "subnet-1"}, {SubnetID: "subnet-2"}}

	tests := []struct {
		name      string
		direction string
		protocols []string
		wantErr   bool
	}{
		{name: "delegation_default_protocol", direction: "INBOUND_DELEGATION"},
		{name: "delegation_do53", direction: "INBOUND_DELEGATION", protocols: []string{"Do53"}},
		{name: "delegation_doh_rejected", direction: "INBOUND_DELEGATION", protocols: []string{"DoH"}, wantErr: true},
		{name: "inbound_doh_ok", direction: "INBOUND", protocols: []string{"DoH"}},
		{name: "unknown_direction", direction: "SIDEWAYS", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			ep, err := b.CreateResolverEndpoint(
				context.Background(), "ep", tt.direction, "", ips, nil, "", tt.protocols,
				"", "", "", false, false, false, false,
			)

			if tt.wantErr {
				require.ErrorIs(t, err, route53resolver.ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.direction, ep.Direction)
		})
	}
}

func TestCreateDelegateRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		targets []route53resolver.TargetIP
		wantErr bool
	}{
		{name: "no_targets"},
		{name: "targets_rejected", targets: []route53resolver.TargetIP{{IP: "10.0.0.1", Port: 53}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			r, err := b.CreateResolverRule(
				context.Background(), "r", "example.com", "DELEGATE", "", "", "sub.example.com", tt.targets,
			)

			if tt.wantErr {
				require.ErrorIs(t, err, route53resolver.ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "DELEGATE", r.RuleType)
			assert.Equal(t, "sub.example.com", r.DelegationRecord)
		})
	}
}

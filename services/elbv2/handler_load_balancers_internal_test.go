package elbv2

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseSubnetMappings_SourceNatIpv6Prefix covers gopherstack-xh2a:
// SubnetMapping.SourceNatIpv6Prefix (elasticloadbalancingv2@v1.58.5
// types/types.go:1317, serializers.go:4643 flat key "SourceNatIpv6Prefix")
// must not be silently dropped by parseSubnetMappings.
func TestParseSubnetMappings_SourceNatIpv6Prefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		vals url.Values
		want string
	}{
		{
			name: "prefix set",
			vals: url.Values{
				"SubnetMappings.member.1.SubnetId":            {"subnet-aaa"},
				"SubnetMappings.member.1.SourceNatIpv6Prefix": {"2001:db8:1234:1a00::/80"},
			},
			want: "2001:db8:1234:1a00::/80",
		},
		{
			name: "auto assigned",
			vals: url.Values{
				"SubnetMappings.member.1.SubnetId":            {"subnet-bbb"},
				"SubnetMappings.member.1.SourceNatIpv6Prefix": {"auto_assigned"},
			},
			want: "auto_assigned",
		},
		{
			name: "absent",
			vals: url.Values{
				"SubnetMappings.member.1.SubnetId": {"subnet-ccc"},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := parseSubnetMappings(tt.vals)
			require.Len(t, got, 1)
			assert.Equal(t, tt.want, got[0].SourceNatIpv6Prefix)
		})
	}
}

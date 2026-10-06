package transfer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	"github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_DefaultsAndPartialProtocolDetailsUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update       *types.ProtocolDetails
		name         string
		wantPassive  string
		wantTLS      types.TlsSessionResumptionMode
		wantPassiveU string
		protocols    []types.Protocol
		wantNilPD    bool
	}{
		{
			name:      "sftp has no protocol details",
			protocols: []types.Protocol{types.ProtocolSftp},
			wantNilPD: true,
		},
		{
			name:        "ftps defaults then partial update keeps tls",
			protocols:   []types.Protocol{types.ProtocolFtps, types.ProtocolSftp},
			wantPassive: "AUTO", wantTLS: types.TlsSessionResumptionModeEnforced,
			update:       &types.ProtocolDetails{PassiveIp: aws.String("1.2.3.4")},
			wantPassiveU: "1.2.3.4",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestTransferClient(t, newTestHandler(t))
			in := &transfersdk.CreateServerInput{Protocols: tc.protocols}
			if !tc.wantNilPD {
				in.Certificate = aws.String("arn:aws:acm:us-east-1:123456789012:certificate/x")
			}

			sv, err := c.CreateServer(t.Context(), in)
			require.NoError(t, err)

			d, err := c.DescribeServer(t.Context(), &transfersdk.DescribeServerInput{ServerId: sv.ServerId})
			require.NoError(t, err)
			assert.Equal(t, types.IpAddressTypeIpv4, d.Server.IpAddressType)

			if tc.wantNilPD {
				assert.Nil(t, d.Server.ProtocolDetails)

				return
			}

			assert.Equal(t, tc.wantPassive, aws.ToString(d.Server.ProtocolDetails.PassiveIp))
			assert.Equal(t, tc.wantTLS, d.Server.ProtocolDetails.TlsSessionResumptionMode)

			_, err = c.UpdateServer(
				t.Context(),
				&transfersdk.UpdateServerInput{ServerId: sv.ServerId, ProtocolDetails: tc.update},
			)
			require.NoError(t, err)

			d, err = c.DescribeServer(t.Context(), &transfersdk.DescribeServerInput{ServerId: sv.ServerId})
			require.NoError(t, err)
			assert.Equal(t, tc.wantPassiveU, aws.ToString(d.Server.ProtocolDetails.PassiveIp))
			assert.Equal(t, tc.wantTLS, d.Server.ProtocolDetails.TlsSessionResumptionMode)
		})
	}
}

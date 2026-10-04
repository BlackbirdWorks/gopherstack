package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ProvisionIpamPoolCidrVerificationMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		method  types.VerificationMethod
		wantErr string
	}{
		{"default", "", ""},
		{"remarks_x509", types.VerificationMethodRemarksX509, ""},
		{"dns_token", types.VerificationMethodDnsToken, ""},
		{"unknown_method", types.VerificationMethod("carrier-pigeon"), "InvalidParameterValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMiscClient(t)

			ipam, err := client.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
			require.NoError(t, err)
			pool, err := client.CreateIpamPool(t.Context(), &ec2sdk.CreateIpamPoolInput{
				IpamScopeId:   ipam.Ipam.PrivateDefaultScopeId,
				AddressFamily: types.AddressFamilyIpv4,
			})
			require.NoError(t, err)

			out, err := client.ProvisionIpamPoolCidr(t.Context(), &ec2sdk.ProvisionIpamPoolCidrInput{
				IpamPoolId:         pool.IpamPool.IpamPoolId,
				Cidr:               aws.String("10.30.0.0/16"),
				VerificationMethod: tt.method,
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				cidrs, listErr := client.GetIpamPoolCidrs(t.Context(), &ec2sdk.GetIpamPoolCidrsInput{
					IpamPoolId: pool.IpamPool.IpamPoolId,
				})
				require.NoError(t, listErr)
				assert.Empty(t, cidrs.IpamPoolCidrs, "a rejected request must not provision the CIDR")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "10.30.0.0/16", aws.ToString(out.IpamPoolCidr.Cidr))
		})
	}
}

func TestRealClient_MacSIPModificationTaskCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		creds   *string
		wantErr string
	}{
		{"absent", nil, ""},
		{
			"valid",
			aws.String(`{"internalDiskPassword":"","rootVolumeUsername":"ec2-user","rootVolumepassword":"pw"}`),
			"",
		},
		{"not_json", aws.String("ec2-user:pw"), "InvalidParameterValue"},
		{"missing_root_password", aws.String(`{"rootVolumeUsername":"ec2-user"}`), "InvalidParameterValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newMiscClient(t)

			insts, err := b.RunInstances("ami-mac", "mac2.metal", "", 1)
			require.NoError(t, err)

			out, err := client.CreateMacSystemIntegrityProtectionModificationTask(
				t.Context(), &ec2sdk.CreateMacSystemIntegrityProtectionModificationTaskInput{
					InstanceId:                         aws.String(insts[0].ID),
					MacSystemIntegrityProtectionStatus: types.MacSystemIntegrityProtectionSettingStatusDisabled,
					MacCredentials:                     tt.creds,
				},
			)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, insts[0].ID, aws.ToString(out.MacModificationTask.InstanceId))
		})
	}
}

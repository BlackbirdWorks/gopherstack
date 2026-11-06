package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func TestServiceNic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantIP  string
		in      oci.ServiceNicIn
		wantErr bool
	}{
		{
			name:   "auto-address",
			in:     oci.ServiceNicIn{CompartmentID: testComp, SubnetID: "@subnet", DisplayName: "mt"},
			wantIP: "10.0.1.2",
		},
		{
			name:   "chosen-address",
			in:     oci.ServiceNicIn{CompartmentID: testComp, SubnetID: "@subnet", PrivateIP: "10.0.1.50"},
			wantIP: "10.0.1.50",
		},
		{
			name:    "hostname-needs-dns-label",
			in:      oci.ServiceNicIn{CompartmentID: testComp, SubnetID: "@subnet", HostnameLabel: "files-1"},
			wantErr: true,
		},
		{
			name:    "address-outside-subnet",
			in:      oci.ServiceNicIn{CompartmentID: testComp, SubnetID: "@subnet", PrivateIP: "10.9.9.9"},
			wantErr: true,
		},
		{
			name:    "reserved-address",
			in:      oci.ServiceNicIn{CompartmentID: testComp, SubnetID: "@subnet", PrivateIP: "10.0.1.0"},
			wantErr: true,
		},
		{
			name:    "unknown-subnet",
			in:      oci.ServiceNicIn{CompartmentID: testComp, SubnetID: "ocid1.subnet.oc1.iad.nope"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			v := e.mkVcn(t, "10.0.0.0/16", "nic")
			s := e.mkSubnet(t, v, "10.0.1.0/24")

			if tc.in.SubnetID == "@subnet" {
				tc.in.SubnetID = *s.Id
			}

			nic, err := e.backend.CreateServiceNic(tc.in)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantIP, nic.IPAddress)

			ip, err := e.client.GetPrivateIp(t.Context(), core.GetPrivateIpRequest{PrivateIpId: &nic.PrivateIPID})
			require.NoError(t, err)
			assert.Equal(t, tc.wantIP, *ip.IpAddress)
			assert.Equal(t, nic.VnicID, *ip.VnicId)

			vn, err := e.client.GetVnic(t.Context(), core.GetVnicRequest{VnicId: &nic.VnicID})
			require.NoError(t, err)
			assert.Nil(t, vn.PublicIp, "service VNICs get no public IP")

			e.backend.DeleteServiceNic(nic.VnicID)

			_, err = e.client.GetPrivateIp(t.Context(), core.GetPrivateIpRequest{PrivateIpId: &nic.PrivateIPID})
			require.Error(t, err)
			assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

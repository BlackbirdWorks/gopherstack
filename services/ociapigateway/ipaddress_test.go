package ociapigateway_test

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ociapigateway "github.com/blackbirdworks/gopherstack/services/ociapigateway"
)

type fakeNet struct {
	subnets  map[string]ociapigateway.SubnetInfo
	released []string
	reqs     []ociapigateway.AddressRequest
	mu       sync.Mutex
}

func (f *fakeNet) network() ociapigateway.Network {
	return ociapigateway.Network{
		Subnet: func(id string) (ociapigateway.SubnetInfo, bool) {
			s, ok := f.subnets[id]

			return s, ok
		},
		Allocate: func(r ociapigateway.AddressRequest) (ociapigateway.Allocation, error) {
			f.mu.Lock()
			defer f.mu.Unlock()

			f.reqs = append(f.reqs, r)

			if len(r.ReservedIPIDs) > 0 && r.ReservedIPIDs[0] == "ocid1.publicip.oc1..gone" {
				return ociapigateway.Allocation{}, &ociapigateway.APIError{
					Status: http.StatusNotFound, Code: "NotAuthorizedOrNotFound", Message: "no such public ip",
				}
			}

			n := len(f.reqs)
			a := ociapigateway.Allocation{VnicID: fmt.Sprintf("vnic-%d", n), PrivateIP: fmt.Sprintf("10.0.1.%d", n+1)}

			if r.Public {
				a.PublicIPs = []string{fmt.Sprintf("203.0.113.%d", n)}
			}

			if len(r.ReservedIPIDs) > 0 {
				a.PublicIPs = []string{"203.0.113.200"}
			}

			if r.AssignIPv6 || len(r.IPv6Addresses) > 0 {
				a.IPv6Addresses = []string{"2603:c020::1"}
			}

			return a, nil
		},
		Release: func(id string) {
			f.mu.Lock()
			f.released = append(f.released, id)
			f.mu.Unlock()
		},
	}
}

func TestGatewayAddresses(t *testing.T) {
	t.Parallel()

	const (
		pub  = "ocid1.subnet.oc1..pub"
		priv = "ocid1.subnet.oc1..priv"
		ad   = "ocid1.subnet.oc1..ad"
	)

	tests := []struct {
		mutate  func(*apigateway.CreateGatewayDetails)
		name    string
		wantIPs []string
		status  int
	}{
		{
			name:    "public gets the public ip",
			mutate:  func(d *apigateway.CreateGatewayDetails) { d.SubnetId = new(pub) },
			wantIPs: []string{"203.0.113.1"},
		},
		{
			name: "private gets the subnet ip",
			mutate: func(d *apigateway.CreateGatewayDetails) {
				d.SubnetId, d.EndpointType = new(priv), apigateway.GatewayEndpointTypePrivate
			},
			wantIPs: []string{"10.0.1.2"},
		},
		{
			name: "ipv6 only returns the v6 address",
			mutate: func(d *apigateway.CreateGatewayDetails) {
				d.SubnetId, d.IpMode = new(pub), apigateway.GatewayIpModeIpv6
				d.Ipv6AddressConfiguration = &apigateway.Ipv6AddressConfiguration{Addresses: []string{"2603:c020::1"}}
			},
			wantIPs: []string{"2603:c020::1"},
		},
		{
			name: "dual stack returns both",
			mutate: func(d *apigateway.CreateGatewayDetails) {
				d.SubnetId, d.IpMode = new(pub), apigateway.GatewayIpModeDualStack
			},
			wantIPs: []string{"203.0.113.1", "2603:c020::1"},
		},
		{
			name: "reserved ip is used",
			mutate: func(d *apigateway.CreateGatewayDetails) {
				d.SubnetId = new(pub)
				d.Ipv4AddressConfiguration = &apigateway.Ipv4AddressConfiguration{
					ReservedIpIds: []string{"ocid1.publicip.oc1..r"},
				}
			},
			wantIPs: []string{"203.0.113.200"},
		},
		{
			name: "reserved ip must exist",
			mutate: func(d *apigateway.CreateGatewayDetails) {
				d.SubnetId = new(pub)
				d.Ipv4AddressConfiguration = &apigateway.Ipv4AddressConfiguration{
					ReservedIpIds: []string{"ocid1.publicip.oc1..gone"},
				}
			},
			status: http.StatusNotFound,
		},
		{
			name: "reserved ip needs a public gateway",
			mutate: func(d *apigateway.CreateGatewayDetails) {
				d.SubnetId, d.EndpointType = new(priv), apigateway.GatewayEndpointTypePrivate
				d.Ipv4AddressConfiguration = &apigateway.Ipv4AddressConfiguration{
					ReservedIpIds: []string{"ocid1.publicip.oc1..r"},
				}
			},
			status: http.StatusBadRequest,
		},
		{
			name: "ipv6 config needs an ipv6 mode",
			mutate: func(d *apigateway.CreateGatewayDetails) {
				d.SubnetId = new(pub)
				d.Ipv6AddressConfiguration = &apigateway.Ipv6AddressConfiguration{Addresses: []string{"2603:c020::1"}}
			},
			status: http.StatusBadRequest,
		},
		{
			name: "ipv4 config conflicts with ipv6 mode",
			mutate: func(d *apigateway.CreateGatewayDetails) {
				d.SubnetId, d.IpMode = new(pub), apigateway.GatewayIpModeIpv6
				d.Ipv4AddressConfiguration = &apigateway.Ipv4AddressConfiguration{ReservedIpIds: []string{"x"}}
			},
			status: http.StatusBadRequest,
		},
		{
			name:   "public gateway needs a public subnet",
			mutate: func(d *apigateway.CreateGatewayDetails) { d.SubnetId = new(priv) },
			status: http.StatusBadRequest,
		},
		{
			name:   "subnet must be regional",
			mutate: func(d *apigateway.CreateGatewayDetails) { d.SubnetId = new(ad) },
			status: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			net := &fakeNet{subnets: map[string]ociapigateway.SubnetInfo{
				pub: {Regional: true, Public: true}, priv: {Regional: true}, ad: {Public: true},
			}}
			e.backend.SetNetwork(net.network())

			d := apigateway.CreateGatewayDetails{
				CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
				SubnetId: new(pub), DisplayName: new("gw"),
			}
			tc.mutate(&d)

			resp, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{CreateGatewayDetails: d})
			if tc.status != 0 {
				assert.Equal(t, tc.status, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			e.settle()

			got := e.mkGatewayByID(t, *resp.Id)

			ips := make([]string, 0, len(got.IpAddresses))
			for _, ip := range got.IpAddresses {
				ips = append(ips, *ip.IpAddress)
			}

			assert.Equal(t, tc.wantIPs, ips)

			if d.Ipv4AddressConfiguration != nil {
				assert.Equal(t, d.Ipv4AddressConfiguration.ReservedIpIds, got.Ipv4AddressConfiguration.ReservedIpIds)
			}

			if d.Ipv6AddressConfiguration != nil {
				assert.Equal(t, d.Ipv6AddressConfiguration.Addresses, got.Ipv6AddressConfiguration.Addresses)
			}
		})
	}
}

func TestGatewayAddressesReleasedOnDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	net := &fakeNet{subnets: map[string]ociapigateway.SubnetInfo{testSubnet: {Regional: true, Public: true}}}
	e.backend.SetNetwork(net.network())

	gw := e.mkGateway(t, "gw")
	require.Len(t, gw.IpAddresses, 1)

	_, err := e.gw.DeleteGateway(t.Context(), apigateway.DeleteGatewayRequest{GatewayId: gw.Id})
	require.NoError(t, err)
	assert.Equal(t, []string{"vnic-1"}, net.released)

	snap := e.handler.Snapshot(t.Context())
	e2 := newEnv(t, envOpts{})
	require.NoError(t, e2.handler.Restore(t.Context(), snap))
	assert.Equal(t, *gw.IpAddresses[0].IpAddress, *e2.mkGatewayByID(t, *gw.Id).IpAddresses[0].IpAddress)
}

func TestDuplicateGatewayCode(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkGateway(t, "dup")

	_, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{
		CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
		SubnetId: new(testSubnet), DisplayName: new("dup"),
	})
	require.Error(t, err)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
	assert.Equal(t, "NotAuthorizedOrResourceAlreadyExists", svcErr(t, err).GetCode())
}

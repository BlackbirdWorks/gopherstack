package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVcnDnsResolverAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		lookup       func(string) (string, bool)
		wantResolver string
	}{
		{name: "no dns service", lookup: nil},
		{name: "resolver found", lookup: func(string) (string, bool) { return "ocid1.dnsresolver.oc1..r", true },
			wantResolver: "ocid1.dnsresolver.oc1..r"},
		{name: "resolver missing", lookup: func(string) (string, bool) { return "", false }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			e.backend.SetDNSResolverLookup(tt.lookup)

			vcn := e.mkVcn(t, "10.0.0.0/16", "dns")

			got, err := e.client.GetVcnDnsResolverAssociation(t.Context(),
				core.GetVcnDnsResolverAssociationRequest{VcnId: vcn.Id})
			require.NoError(t, err)
			assert.Equal(t, *vcn.Id, *got.VcnId)
			assert.Equal(t, core.VcnDnsResolverAssociationLifecycleStateProvisioning, got.LifecycleState)

			if tt.wantResolver == "" {
				assert.Nil(t, got.DnsResolverId)
			} else {
				require.NotNil(t, got.DnsResolverId)
				assert.Equal(t, tt.wantResolver, *got.DnsResolverId)
			}

			e.clock.Advance(5 * 1e9)

			got, err = e.client.GetVcnDnsResolverAssociation(t.Context(),
				core.GetVcnDnsResolverAssociationRequest{VcnId: vcn.Id})
			require.NoError(t, err)
			assert.Equal(t, core.VcnDnsResolverAssociationLifecycleStateAvailable, got.LifecycleState)
		})
	}
}

func TestVcnDnsResolverAssociationNotFound(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	_, err := e.client.GetVcnDnsResolverAssociation(t.Context(),
		core.GetVcnDnsResolverAssociationRequest{VcnId: new("ocid1.vcn.oc1..ghost")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

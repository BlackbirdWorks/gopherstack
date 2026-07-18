package route53_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func TestDisassociateVPC_NotAssociated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "vpc_never_associated_returns_not_found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			hz, err := b.CreateHostedZone("example.com", "ref-priv", "", true, "")
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-aaa", "us-east-1"))

			// vpc-bbb was never associated, so the disassociate must be
			// rejected with VPCAssociationNotFound, not the generic
			// InvalidInput this backend previously returned.
			err = b.DisassociateVPCFromHostedZone(hz.ID, "vpc-bbb")
			require.Error(t, err)
			assert.ErrorIs(t, err, route53.ErrVPCAssociationNotFound)
		})
	}
}

func TestDisassociateVPC_LastVPCRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantBody string
		wantCode int
	}{
		{
			name:     "backend_last_vpc_returns_error",
			wantCode: http.StatusBadRequest,
			wantBody: "LastVPCAssociation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("private.example.com", "priv-ref", "", true, "")
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-only", "us-east-1"))

			err = b.DisassociateVPCFromHostedZone(hz.ID, "vpc-only")
			require.ErrorIs(t, err, route53.ErrLastVPCAssociation,
				"removing the last VPC must return ErrLastVPCAssociation")

			_ = tt // wantCode/wantBody verified via HTTP round-trip below
		})
	}
}

func TestDisassociateVPC_LastVPC_HTTPRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create a private zone.
	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>private.example.com</Name>
  <CallerReference>priv-last-vpc-ref</CallerReference>
  <HostedZoneConfig><PrivateZone>true</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	// Associate two VPCs.
	for _, vpcID := range []string{"vpc-alpha", "vpc-beta"} {
		assocBody := `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>` + vpcID + `</VPCId><VPCRegion>us-east-1</VPCRegion></VPC>
</AssociateVPCWithHostedZoneRequest>`
		rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/associatevpc", assocBody)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Disassociate vpc-beta (succeeds — vpc-alpha still present).
	disBody := `<?xml version="1.0" encoding="UTF-8"?>
<DisassociateVPCFromHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-beta</VPCId><VPCRegion>us-east-1</VPCRegion></VPC>
</DisassociateVPCFromHostedZoneRequest>`
	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/disassociatevpc", disBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Attempt to disassociate the only remaining VPC must fail.
	lastDisBody := `<?xml version="1.0" encoding="UTF-8"?>
<DisassociateVPCFromHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-alpha</VPCId><VPCRegion>us-east-1</VPCRegion></VPC>
</DisassociateVPCFromHostedZoneRequest>`
	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/disassociatevpc", lastDisBody)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "LastVPCAssociation")
}

func TestDisassociateVPC_WithMultipleVPCs_Succeeds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "two_vpcs_remove_one"},
		{name: "three_vpcs_remove_one"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("private.example.com", "priv-multi-ref-"+tt.name, "", true, "")
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-keep", "us-east-1"))
			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-remove", "us-east-1"))

			err = b.DisassociateVPCFromHostedZone(hz.ID, "vpc-remove")
			require.NoError(t, err, "removing non-last VPC must succeed")

			assocs, err := b.ListVPCAssociations(hz.ID)
			require.NoError(t, err)
			require.Len(t, assocs, 1)
		})
	}
}

func TestDuplicateVPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
		second  bool
	}{
		{
			name:    "first_associate_ok",
			second:  false,
			wantErr: false,
		},
		{
			name:    "duplicate_vpc_returns_error",
			second:  true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", true, "")
			require.NoError(t, err)

			err = b.AssociateVPCWithHostedZone(hz.ID, "vpc-123", "us-east-1")
			require.NoError(t, err)

			if !tt.second {
				return
			}

			err = b.AssociateVPCWithHostedZone(hz.ID, "vpc-123", "us-east-1")
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, route53.ErrInvalidInput)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDeleteZone_CascadesVPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		deleteZone        bool
		wantVPCAfterCount int
	}{
		{
			name:              "without_delete_vpc_remains",
			deleteZone:        false,
			wantVPCAfterCount: 1,
		},
		{
			name:              "with_delete_cascades_vpc",
			deleteZone:        true,
			wantVPCAfterCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", true, "")
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-abc", "us-east-1"))
			assert.Equal(t, 1, route53.VPCAssociationCount(b))

			if tt.deleteZone {
				require.NoError(t, b.DeleteHostedZone(hz.ID))
			}

			assert.Equal(t, tt.wantVPCAfterCount, route53.VPCAssociationCount(b))
		})
	}
}

func TestExtractOperation_AssociateVPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "associate_vpc",
			path:   "/2013-04-01/hostedzone/ZXXX/associatevpc",
			method: http.MethodPost,
			wantOp: "AssociateVPCWithHostedZone",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestAssociateVPC_RejectsPublicZone(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create public zone.
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-abc</VPCId><VPCRegion>us-east-1</VPCRegion></VPC>
</AssociateVPCWithHostedZoneRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/associatevpc", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPCAssociationAuthorizations_CRUD(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateVPCAssociationAuthorizationRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-cross</VPCId><VPCRegion>us-west-2</VPCRegion></VPC>
</CreateVPCAssociationAuthorizationRequest>`

	// Create authorization.
	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/authorizevpcassociation", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "vpc-cross")

	// List authorizations.
	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/authorizevpcassociation", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "vpc-cross")

	// Delete authorization.
	deleteBody := `<?xml version="1.0" encoding="UTF-8"?>
<DeleteVPCAssociationAuthorizationRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-cross</VPCId></VPC>
</DeleteVPCAssociationAuthorizationRequest>`

	rec = send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID+"/authorizevpcassociation", deleteBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// List again — should be empty.
	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/authorizevpcassociation", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "vpc-cross")
}

func TestDisassociateVPCFromHostedZone(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("private.example.com", "ref", "", true, "")
	require.NoError(t, err)

	// Associate two VPCs so we can remove one without hitting the last-VPC guard.
	require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-xyz", "us-east-1"))
	require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-abc", "us-east-1"))

	// Disassociate one.
	require.NoError(t, b.DisassociateVPCFromHostedZone(hz.ID, "vpc-xyz"))

	// Only the remaining VPC should be present.
	assocs, err := b.ListVPCAssociations(hz.ID)
	require.NoError(t, err)
	require.Len(t, assocs, 1)
}

func TestRoute53_AssociateVPCWithHostedZone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
		useRealZone  bool
	}{
		{
			name:        "associate_vpc_success",
			useRealZone: true,
			body: `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC>
    <VPCId>vpc-12345</VPCId>
    <VPCRegion>us-east-1</VPCRegion>
  </VPC>
</AssociateVPCWithHostedZoneRequest>`,
			wantCode:     http.StatusOK,
			wantContains: []string{"AssociateVPCWithHostedZoneResponse", "INSYNC"},
		},
		{
			name:        "associate_vpc_missing_vpcid",
			useRealZone: true,
			body: `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC>
    <VPCRegion>us-east-1</VPCRegion>
  </VPC>
</AssociateVPCWithHostedZoneRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "associate_vpc_zone_not_found",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-xyz</VPCId></VPC>
</AssociateVPCWithHostedZoneRequest>`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			var path string
			if tt.useRealZone {
				privateZoneXML := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>private.example.com</Name>
  <CallerReference>ref-private-1</CallerReference>
  <HostedZoneConfig>
    <Comment>private test zone</Comment>
    <PrivateZone>true</PrivateZone>
  </HostedZoneConfig>
</CreateHostedZoneRequest>`
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", privateZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)
				zoneID := extractZoneID(t, rec.Body.String())
				path = "/2013-04-01/hostedzone/" + zoneID + "/associatevpc"
			} else {
				path = "/2013-04-01/hostedzone/ZNONEXISTENT/associatevpc"
			}

			got := send(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantCode, got.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, got.Body.String(), s)
			}
		})
	}
}

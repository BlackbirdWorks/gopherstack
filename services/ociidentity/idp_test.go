package ociidentity_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

const samlMetadata = `<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
    xmlns:ds="http://www.w3.org/2000/09/xmldsig#" entityID="https://idp.example/saml">
  <md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <md:KeyDescriptor use="encryption"><ds:KeyInfo><ds:X509Data>
      <ds:X509Certificate>ENCCERT</ds:X509Certificate>
    </ds:X509Data></ds:KeyInfo></md:KeyDescriptor>
    <md:KeyDescriptor use="signing"><ds:KeyInfo><ds:X509Data><ds:X509Certificate>
      MIIC
      SIGNCERT
    </ds:X509Certificate></ds:X509Data></ds:KeyInfo></md:KeyDescriptor>
    <md:SingleSignOnService
      Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://idp.example/post"/>
    <md:SingleSignOnService
      Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example/redirect"/>
  </md:IDPSSODescriptor>
</md:EntityDescriptor>`

func (e *env) mkIDP(t *testing.T, name string) identity.Saml2IdentityProvider {
	t.Helper()

	r, err := e.client.CreateIdentityProvider(t.Context(), identity.CreateIdentityProviderRequest{
		CreateIdentityProviderDetails: identity.CreateSaml2IdentityProviderDetails{
			CompartmentId: new(testTenancy), Name: new(name), Description: new("d"),
			ProductType: identity.CreateIdentityProviderDetailsProductTypeIdcs,
			MetadataUrl: new("https://idp.example/metadata"), Metadata: new(samlMetadata),
		},
	})
	require.NoError(t, err)

	p, ok := r.IdentityProvider.(identity.Saml2IdentityProvider)
	require.True(t, ok)

	return p
}

func TestIdentityProviderCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	p := e.mkIDP(t, "Corp")

	assert.Equal(t, identity.IdentityProviderLifecycleStateActive, p.LifecycleState)
	assert.Equal(t, "MIICSIGNCERT", *p.SigningCertificate, "the signing key descriptor, whitespace removed")
	assert.Equal(t, "https://idp.example/redirect", *p.RedirectUrl)
	assert.Equal(t, "IDCS", *p.ProductType)

	_, err := e.client.CreateIdentityProvider(ctx, identity.CreateIdentityProviderRequest{
		CreateIdentityProviderDetails: identity.CreateSaml2IdentityProviderDetails{
			CompartmentId: new(testTenancy), Name: new("corp"), Description: new("d"),
			ProductType: identity.CreateIdentityProviderDetailsProductTypeAdfs,
			MetadataUrl: new("u"), Metadata: new(samlMetadata),
		},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	got, err := e.client.GetIdentityProvider(ctx, identity.GetIdentityProviderRequest{IdentityProviderId: p.Id})
	require.NoError(t, err)
	require.NotNil(t, got.Etag)
	assert.Equal(t, *p.Id, *got.IdentityProvider.GetId())

	lst, err := e.client.ListIdentityProviders(ctx, identity.ListIdentityProvidersRequest{
		CompartmentId: new(testTenancy), Protocol: identity.ListIdentityProvidersProtocolSaml2,
	})
	require.NoError(t, err)
	require.Len(t, lst.Items, 1)

	_, err = e.client.UpdateIdentityProvider(ctx, identity.UpdateIdentityProviderRequest{
		IdentityProviderId: p.Id, IfMatch: new("stale"),
		UpdateIdentityProviderDetails: identity.UpdateSaml2IdentityProviderDetails{Description: new("x")},
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	newMeta := strings.ReplaceAll(samlMetadata, "https://idp.example/redirect", "https://idp.example/v2")
	up, err := e.client.UpdateIdentityProvider(ctx, identity.UpdateIdentityProviderRequest{
		IdentityProviderId: p.Id, IfMatch: got.Etag,
		UpdateIdentityProviderDetails: identity.UpdateSaml2IdentityProviderDetails{
			Description: new("new"), Metadata: new(newMeta), FreeformAttributes: map[string]string{"clientId": "c"},
		},
	})
	require.NoError(t, err)

	upd := up.IdentityProvider.(identity.Saml2IdentityProvider)
	assert.Equal(t, "new", *upd.Description)
	assert.Equal(t, "https://idp.example/v2", *upd.RedirectUrl)
	assert.Equal(t, map[string]string{"clientId": "c"}, upd.FreeformAttributes)

	_, err = e.client.DeleteIdentityProvider(ctx, identity.DeleteIdentityProviderRequest{IdentityProviderId: p.Id})
	require.NoError(t, err)

	_, err = e.client.GetIdentityProvider(ctx, identity.GetIdentityProviderRequest{IdentityProviderId: p.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestIdentityProviderMetadataValidation(t *testing.T) {
	t.Parallel()

	deep := strings.Repeat("<a>", 40) + strings.Repeat("</a>", 40)
	tests := []struct {
		name     string
		metadata string
	}{
		{"not xml", "this is not xml"},
		{"truncated", samlMetadata[:len(samlMetadata)/2]},
		{"wrong root", `<root/>`},
		{"no idp descriptor", `<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"/>`},
		{"dtd", `<!DOCTYPE x [<!ENTITY e "v">]>` + samlMetadata},
		{
			"too deep",
			`<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata">` + deep + `</md:EntityDescriptor>`,
		},
		{"too big", samlMetadata + strings.Repeat(" ", 300<<10)},
		{"no certificate", strings.ReplaceAll(strings.ReplaceAll(samlMetadata, "SIGNCERT", ""), "MIIC", "")},
		{"empty", " "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			_, err := e.client.CreateIdentityProvider(t.Context(), identity.CreateIdentityProviderRequest{
				CreateIdentityProviderDetails: identity.CreateSaml2IdentityProviderDetails{
					CompartmentId: new(testTenancy), Name: new("n"), Description: new("d"),
					ProductType: identity.CreateIdentityProviderDetailsProductTypeIdcs,
					MetadataUrl: new("https://never-fetched.invalid/"), Metadata: new(tt.metadata),
				},
			})
			require.Error(t, err)
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestIdpGroupMappings(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	p := e.mkIDP(t, "corp")
	g1, g2 := e.mkGroup(t, "admins"), e.mkGroup(t, "devs")

	create := func(name string, groupID *string) (identity.CreateIdpGroupMappingResponse, error) {
		return e.client.CreateIdpGroupMapping(ctx, identity.CreateIdpGroupMappingRequest{
			IdentityProviderId: p.Id,
			IdpGroupName:       new(name), GroupId: groupID,
		})
	}

	_, err := create("idp-admins", new("ocid1.group.oc1..nope"))
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "the IAM group must exist")

	m, err := create("idp-admins", g1.Id)
	require.NoError(t, err)
	assert.Equal(t, *p.Id, *m.IdpId)
	assert.Equal(t, *g1.Id, *m.GroupId)

	_, err = create("idp-admins", g1.Id)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "duplicate pair")

	_, err = create("idp-admins", g2.Id)
	require.NoError(t, err, "one IDP group may map to several IAM groups")

	_, err = e.client.DeleteGroup(ctx, identity.DeleteGroupRequest{GroupId: g1.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "group is mapped")

	lst, err := e.client.ListIdpGroupMappings(ctx, identity.ListIdpGroupMappingsRequest{IdentityProviderId: p.Id})
	require.NoError(t, err)
	assert.Len(t, lst.Items, 2)

	up, err := e.client.UpdateIdpGroupMapping(ctx, identity.UpdateIdpGroupMappingRequest{
		IdentityProviderId: p.Id, MappingId: m.Id, IfMatch: m.Etag,
		IdpGroupName: new("renamed"), GroupId: g2.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *up.IdpGroupName)
	assert.Equal(t, *g2.Id, *up.GroupId)

	_, err = e.client.UpdateIdpGroupMapping(ctx, identity.UpdateIdpGroupMappingRequest{
		IdentityProviderId: p.Id, MappingId: m.Id, IfMatch: m.Etag,
		IdpGroupName: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode(), "etag changed by the update")

	_, err = e.client.DeleteIdpGroupMapping(
		ctx,
		identity.DeleteIdpGroupMappingRequest{IdentityProviderId: p.Id, MappingId: m.Id},
	)
	require.NoError(t, err)

	_, err = e.client.GetIdpGroupMapping(
		ctx,
		identity.GetIdpGroupMappingRequest{IdentityProviderId: p.Id, MappingId: m.Id},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteGroup(ctx, identity.DeleteGroupRequest{GroupId: g1.Id})
	require.NoError(t, err, "the mapping that blocked it is gone")

	_, err = e.client.DeleteIdentityProvider(ctx, identity.DeleteIdentityProviderRequest{IdentityProviderId: p.Id})
	require.NoError(t, err)

	_, err = e.client.ListIdpGroupMappings(ctx, identity.ListIdpGroupMappingsRequest{IdentityProviderId: p.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteGroup(ctx, identity.DeleteGroupRequest{GroupId: g2.Id})
	require.NoError(t, err, "deleting the provider removed its mappings")
}

func TestIdentityProviderSurvivesRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	p := e.mkIDP(t, "persisted")
	g := e.mkGroup(t, "g")

	_, err := e.client.CreateIdpGroupMapping(t.Context(), identity.CreateIdpGroupMappingRequest{
		IdentityProviderId: p.Id,
		IdpGroupName:       new("n"), GroupId: g.Id,
	})
	require.NoError(t, err)

	fresh := oci.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	got, _, err := fresh.GetIdentityProvider(*p.Id)
	require.NoError(t, err)
	assert.Equal(t, "MIICSIGNCERT", got.SigningCertificate)

	mappings, err := fresh.ListIdpGroupMappings(*p.Id, oci.ListOpts{})
	require.NoError(t, err)
	assert.Len(t, mappings.Data, 1)

	err = fresh.DeleteGroup(*g.Id, "")
	assert.Error(t, err, "restored mappings still block group deletion")
}

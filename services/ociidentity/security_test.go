package ociidentity_test

import (
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestVerificationAlwaysDerives(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	known := e.mkUser(t, "known")
	bare := e.mkUser(t, "bare")

	_, err := e.client.CreateOrResetUIPassword(t.Context(), identity.CreateOrResetUIPasswordRequest{UserId: known.Id})
	require.NoError(t, err)

	smtp, err := e.client.CreateSmtpCredential(t.Context(),
		identity.CreateSmtpCredentialRequest{UserId: known.Id, Description: new("s")})
	require.NoError(t, err)

	smtpName := func(user string) string { return user + "@" + oci.TenancyID + ".ab.com" }
	ui := func(user string) func() bool {
		return func() bool {
			_, ok := e.backend.VerifyUIPassword(user, "x")

			return ok
		}
	}
	mail := func(username string) func() bool {
		return func() bool {
			_, ok := e.backend.VerifySMTPCredential(username, "x")

			return ok
		}
	}
	count := e.backend.CountDerivations()

	tests := []struct {
		verify func() bool
		name   string
	}{
		{name: "ui unknown user", verify: ui("nobody")},
		{name: "ui user without password", verify: ui("bare")},
		{name: "ui wrong password", verify: ui("known")},
		{name: "smtp unknown user", verify: mail(smtpName("nobody"))},
		{name: "smtp user without credential", verify: mail(smtpName(*bare.Id))},
		{name: "smtp wrong password", verify: mail(*smtp.Username)},
	}

	for _, tt := range tests {
		before := count.Load()

		assert.False(t, tt.verify(), tt.name)
		assert.Equal(t, int64(1), count.Load()-before, tt.name+": one derivation regardless of who is asked about")
	}
}

func TestSMTPPasswordIsNotPersisted(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	u := e.mkUser(t, "smtpuser")

	r, err := e.client.CreateSmtpCredential(t.Context(), identity.CreateSmtpCredentialRequest{
		UserId: u.Id, Description: new("s"), OpcRetryToken: new("tok"),
	})
	require.NoError(t, err)

	snap := e.backend.Snapshot(t.Context())
	assert.NotContains(t, string(snap), *r.Password)
	assert.Equal(t, 1, e.backend.ReplayHeld())

	lst, err := e.client.ListSmtpCredentials(t.Context(), identity.ListSmtpCredentialsRequest{UserId: u.Id})
	require.NoError(t, err)
	require.Len(t, lst.Items, 1)
}

func TestLegacyPlaintextSMTPIsDigestedOnRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	u := e.mkUser(t, "legacy")

	r, err := e.client.CreateSmtpCredential(t.Context(), identity.CreateSmtpCredentialRequest{
		UserId: u.Id, Description: new("s"),
	})
	require.NoError(t, err)

	digest := regexp.MustCompile(`"salt":"[0-9a-f]+","digest":"[0-9a-f]+"`)
	snap := e.backend.Snapshot(t.Context())
	require.Regexp(t, digest, string(snap))

	legacy := digest.ReplaceAllString(string(snap), `"secret":"`+*r.Password+`"`)
	require.Contains(t, legacy, *r.Password)

	fresh := oci.NewInMemoryBackend()
	fresh.SetClock(e.clock.Now)
	require.NoError(t, fresh.Restore(t.Context(), []byte(legacy)))

	_, ok := fresh.VerifySMTPCredential(*r.Username, *r.Password)
	assert.True(t, ok, "the legacy password still verifies")
	assert.NotContains(t, string(fresh.Snapshot(t.Context())), *r.Password, "and is no longer stored in plaintext")
}

func TestReplaySecretsDoNotLinger(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	u := e.mkUser(t, "lingerer")

	smtp, err := e.client.CreateSmtpCredential(ctx, identity.CreateSmtpCredentialRequest{
		UserId: u.Id, Description: new("s"), OpcRetryToken: new("a"),
	})
	require.NoError(t, err)
	require.Equal(t, 1, e.backend.ReplayHeld())

	_, err = e.client.DeleteSmtpCredential(ctx,
		identity.DeleteSmtpCredentialRequest{UserId: u.Id, SmtpCredentialId: smtp.Id})
	require.NoError(t, err)
	assert.Equal(t, 0, e.backend.ReplayHeld(), "a deleted credential keeps no plaintext")

	_, err = e.client.CreateAuthToken(ctx, identity.CreateAuthTokenRequest{
		UserId: u.Id, Description: new("t"), OpcRetryToken: new("b"),
	})
	require.NoError(t, err)

	for _, tok := range []string{"p1", "p2", "p3"} {
		_, err = e.client.CreateOrResetUIPassword(ctx, identity.CreateOrResetUIPasswordRequest{
			UserId: u.Id, OpcRetryToken: new(tok),
		})
		require.NoError(t, err)
	}

	assert.Equal(t, 2, e.backend.ReplayHeld(), "one auth token and only the latest console password")

	e.clock.Advance(25 * time.Hour)
	e.backend.Sweep()
	assert.Equal(t, 0, e.backend.ReplayHeld(), "expired replay secrets are swept")
}

func TestReplayIsScopedToUser(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	alice, bob := e.mkUser(t, "alice"), e.mkUser(t, "bob")

	a, err := e.client.CreateSmtpCredential(ctx, identity.CreateSmtpCredentialRequest{
		UserId: alice.Id, Description: new("s"), OpcRetryToken: new("shared"),
	})
	require.NoError(t, err)

	b, err := e.client.CreateSmtpCredential(ctx, identity.CreateSmtpCredentialRequest{
		UserId: bob.Id, Description: new("s"), OpcRetryToken: new("shared"),
	})
	require.NoError(t, err)

	assert.NotEqual(t, *a.Id, *b.Id)
	assert.NotEqual(t, *a.Password, *b.Password, "another user's token must never replay this user's secret")
}

func TestSAMLMetadataHostileInput(t *testing.T) {
	t.Parallel()

	const decl = `<?xml version="1.0"?>`

	body := strings.TrimPrefix(samlMetadata, decl)
	laughs := decl + `<!DOCTYPE lolz [<!ENTITY lol "lol"><!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">` +
		`<!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">]>` + body
	external := decl + `<!DOCTYPE x [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>` +
		strings.Replace(body, `entityID="https://idp.example/saml"`, `entityID="&xxe;"`, 1)
	undeclared := strings.Replace(samlMetadata, `entityID="https://idp.example/saml"`, `entityID="&xxe;"`, 1)

	tests := []struct {
		name     string
		metadata string
		wantMsg  string
	}{
		{"billion laughs", laughs, "DTD"},
		{"external entity", external, "DTD"},
		{"undeclared entity", undeclared, "well-formed"},
		{"cert not base64", strings.ReplaceAll(samlMetadata, "SIGNCERT", "!!!!"), "base64"},
		{
			"javascript redirect",
			strings.ReplaceAll(samlMetadata, "https://idp.example/redirect", "javascript:alert(1)"),
			"http or https",
		},
		{
			"relative redirect",
			strings.ReplaceAll(samlMetadata, "https://idp.example/redirect", "/redirect"),
			"http or https",
		},
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
			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Contains(t, se.GetMessage(), tt.wantMsg)
		})
	}
}

func TestUpdateIdentityProviderIsAtomic(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	p := e.mkIDP(t, "atomic")

	_, err := e.client.UpdateIdentityProvider(ctx, identity.UpdateIdentityProviderRequest{
		IdentityProviderId: p.Id,
		UpdateIdentityProviderDetails: identity.UpdateSaml2IdentityProviderDetails{
			Description: new("changed"), MetadataUrl: new(""),
		},
	})
	require.Error(t, err)

	got, err := e.client.GetIdentityProvider(ctx, identity.GetIdentityProviderRequest{IdentityProviderId: p.Id})
	require.NoError(t, err)
	assert.Equal(t, "d", *got.IdentityProvider.(identity.Saml2IdentityProvider).Description,
		"a rejected update must not change anything")
}

func TestNetworkSourceHostileInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		public   []string
		services []string
	}{
		{"ipv6 zone", []string{"fe80::1%eth0"}, nil},
		{"prefix with zone", []string{"fe80::1%eth0/64"}, nil},
		{"too many services", nil, make([]string, 501)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			_, err := e.client.CreateNetworkSource(t.Context(), identity.CreateNetworkSourceRequest{
				CompartmentId: new(testTenancy), Name: new("n"), Description: new("d"),
				PublicSourceList: tt.public, Services: tt.services,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestBulkEditTagValueRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value   any
		name    string
		wantMsg string
	}{
		{name: "enum rejects number", value: 7, wantMsg: "ENUM"},
		{name: "enum rejects object", value: map[string]any{"a": "b"}, wantMsg: "string, number or boolean"},
		{name: "enum rejects other string", value: "bronze", wantMsg: "ENUM"},
		{name: "enum accepts member", value: "gold"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			e.mkTag(t, "Ops", "Tier", identity.EnumTagDefinitionValidator{Values: []string{"gold", "silver"}})
			u := e.mkUser(t, "tagged")

			wrID := e.bulkEdit(t, []identity.BulkEditResource{{Id: u.Id, ResourceType: new("user")}},
				identity.BulkEditOperationDetails{
					OperationType: identity.BulkEditOperationDetailsOperationTypeAddOrSet,
					DefinedTags:   map[string]map[string]any{"Ops": {"Tier": tt.value}},
				})
			e.clock.Advance(3 * time.Second)

			errs, err := e.client.ListTaggingWorkRequestErrors(t.Context(),
				identity.ListTaggingWorkRequestErrorsRequest{WorkRequestId: new(wrID)})
			require.NoError(t, err)

			got, err := e.client.GetUser(t.Context(), identity.GetUserRequest{UserId: u.Id})
			require.NoError(t, err)

			if tt.wantMsg == "" {
				assert.Empty(t, errs.Items)
				assert.Equal(t, "gold", got.DefinedTags["Ops"]["Tier"])

				return
			}

			require.Len(t, errs.Items, 1)
			assert.Contains(t, *errs.Items[0].Message, tt.wantMsg)
			assert.Empty(t, got.DefinedTags, "a rejected value is never stored")
		})
	}
}

func TestBulkEditRespectsPerResourceTagLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ns, err := e.client.CreateTagNamespace(t.Context(), identity.CreateTagNamespaceRequest{
		CompartmentId: new(testTenancy), Name: new("Many"), Description: new("d"),
	})
	require.NoError(t, err)

	tags := map[string]any{}

	for i := range 65 {
		name := "k" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		_, err = e.client.CreateTag(t.Context(), identity.CreateTagRequest{
			TagNamespaceId: ns.Id, Name: new(name), Description: new("d"),
		})
		require.NoError(t, err)

		tags[name] = "v"
	}

	u := e.mkUser(t, "overloaded")
	wrID := e.bulkEdit(t, []identity.BulkEditResource{{Id: u.Id, ResourceType: new("user")}},
		identity.BulkEditOperationDetails{
			OperationType: identity.BulkEditOperationDetailsOperationTypeAddOrSet,
			DefinedTags:   map[string]map[string]any{"Many": tags},
		})
	e.clock.Advance(3 * time.Second)

	wr := e.taggingWR(t, wrID)
	assert.Equal(t, identity.TaggingWorkRequestStatusFailed, wr.Status)

	got, err := e.client.GetUser(t.Context(), identity.GetUserRequest{UserId: u.Id})
	require.NoError(t, err)
	assert.Empty(t, got.DefinedTags)
}

func TestBulkEditRetryTokenIsScopedToCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.mkTag(t, "Ops", "Cost", nil)
	u := e.mkUser(t, "u")
	c := e.mkCompartment(t, testTenancy, "other")

	submit := func(compartment *string) string {
		r, err := e.client.BulkEditTags(t.Context(), identity.BulkEditTagsRequest{
			OpcRetryToken: new("same"),
			CompartmentId: compartment,
			Resources:     []identity.BulkEditResource{{Id: u.Id, ResourceType: new("user")}},
			BulkEditOperations: []identity.BulkEditOperationDetails{{
				OperationType: identity.BulkEditOperationDetailsOperationTypeRemove,
				DefinedTags:   map[string]map[string]any{"Ops": {"Cost": "1"}},
			}},
		})
		require.NoError(t, err)

		return *r.OpcWorkRequestId
	}

	first := submit(new(testTenancy))
	assert.Equal(t, first, submit(new(testTenancy)), "same token and compartment replays")
	assert.NotEqual(t, first, submit(c.Id), "another compartment is a different request")
}

type panicHook struct{}

func (panicHook) holds(string) string { panic("hook exploded") }

func TestPanickingUsageHookDoesNotBreakDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	e.backend.AddCompartmentUsage(panicHook{}.holds)

	c := e.mkCompartment(t, testTenancy, "doomed")

	del, err := e.client.DeleteCompartment(ctx, identity.DeleteCompartmentRequest{CompartmentId: c.Id})
	require.NoError(t, err)

	e.clock.Advance(3 * time.Second)
	e.backend.Sweep()

	wr, err := e.client.GetWorkRequest(ctx, identity.GetWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, identity.WorkRequestStatusSucceeded, wr.Status)
}

func TestSAMLMetadataDepthCap(t *testing.T) {
	t.Parallel()

	const open, closing = `<md:Extensions>`, `</md:Extensions>`

	wrapped := strings.Replace(samlMetadata, `<md:IDPSSODescriptor`, strings.Repeat(open, 40)+`<md:IDPSSODescriptor`, 1)
	wrapped = strings.Replace(wrapped, `</md:IDPSSODescriptor>`,
		`</md:IDPSSODescriptor>`+strings.Repeat(closing, 40), 1)

	e := newEnv(t, false)
	_, err := e.client.CreateIdentityProvider(t.Context(), identity.CreateIdentityProviderRequest{
		CreateIdentityProviderDetails: identity.CreateSaml2IdentityProviderDetails{
			CompartmentId: new(testTenancy), Name: new("deep"), Description: new("d"),
			ProductType: identity.CreateIdentityProviderDetailsProductTypeIdcs,
			MetadataUrl: new("https://never-fetched.invalid/"), Metadata: new(wrapped),
		},
	})
	se := svcErr(t, err)
	assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
	assert.Contains(t, se.GetMessage(), "nested too deeply")
}

func TestIdentityProviderAndMappingLimits(t *testing.T) {
	t.Parallel()

	t.Run("identity providers", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, false)

		for i := range 50 {
			e.mkIDP(t, "idp"+string(rune('a'+i/26))+string(rune('a'+i%26)))
		}

		_, err := e.client.CreateIdentityProvider(t.Context(), identity.CreateIdentityProviderRequest{
			CreateIdentityProviderDetails: identity.CreateSaml2IdentityProviderDetails{
				CompartmentId: new(testTenancy), Name: new("one-too-many"), Description: new("d"),
				ProductType: identity.CreateIdentityProviderDetailsProductTypeIdcs,
				MetadataUrl: new("u"), Metadata: new(samlMetadata),
			},
		})
		assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())
	})

	t.Run("group mappings", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, false)
		p, g := e.mkIDP(t, "corp"), e.mkGroup(t, "g")

		for i := range 250 {
			_, err := e.client.CreateIdpGroupMapping(t.Context(), identity.CreateIdpGroupMappingRequest{
				IdentityProviderId: p.Id, IdpGroupName: new("grp" + string(rune('a'+i/26)) + string(rune('a'+i%26))),
				GroupId: g.Id,
			})
			require.NoError(t, err)
		}

		_, err := e.client.CreateIdpGroupMapping(t.Context(), identity.CreateIdpGroupMappingRequest{
			IdentityProviderId: p.Id, IdpGroupName: new("overflow"), GroupId: g.Id,
		})
		assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())
	})
}

func TestBulkRequestBounds(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.mkTag(t, "Ops", "Cost", nil)

	ops := func(n int) []identity.BulkEditOperationDetails {
		out := make([]identity.BulkEditOperationDetails, n)
		for i := range out {
			out[i] = identity.BulkEditOperationDetails{
				OperationType: identity.BulkEditOperationDetailsOperationTypeRemove,
				DefinedTags:   map[string]map[string]any{"Ops": {"Cost": "1"}},
			}
		}

		return out
	}
	res := func(n int) []identity.BulkEditResource {
		out := make([]identity.BulkEditResource, n)
		for i := range out {
			id := "ocid1.user.oc1..u" + string(rune('a'+i/26)) + string(rune('a'+i%26)) + strings.Repeat("x", i/676)
			out[i] = identity.BulkEditResource{Id: new(id), ResourceType: new("user")}
		}

		return out
	}

	tests := []struct {
		do   func() error
		name string
	}{
		{name: "too many resources", do: func() error {
			_, err := e.client.BulkEditTags(t.Context(), identity.BulkEditTagsRequest{
				CompartmentId: new(testTenancy), Resources: res(1001), BulkEditOperations: ops(1),
			})

			return err
		}},
		{name: "too many operations", do: func() error {
			_, err := e.client.BulkEditTags(t.Context(), identity.BulkEditTagsRequest{
				CompartmentId: new(testTenancy), Resources: res(1), BulkEditOperations: ops(51),
			})

			return err
		}},
		{name: "too many tag definitions", do: func() error {
			ids := make([]string, 1001)
			for i := range ids {
				ids[i] = "ocid1.tag.oc1..t" + strings.Repeat("x", i)
			}

			_, err := e.client.BulkDeleteTags(t.Context(), identity.BulkDeleteTagsRequest{
				TagDefinitionIds: ids,
			})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, "LimitExceeded", svcErr(t, tt.do()).GetCode())
		})
	}
}

func TestBulkEditRetiredTag(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	nsID, _ := e.mkTag(t, "Ops", "Cost", nil)
	u := e.mkUser(t, "u")

	apply := func(op identity.BulkEditOperationDetailsOperationTypeEnum) identity.TaggingWorkRequest {
		tags := map[string]map[string]any{"Ops": {"Cost": "1"}}
		id := e.bulkEdit(t, []identity.BulkEditResource{{Id: u.Id, ResourceType: new("user")}},
			identity.BulkEditOperationDetails{OperationType: op, DefinedTags: tags})
		e.clock.Advance(3 * time.Second)

		return e.taggingWR(t, id)
	}

	assert.Equal(t, identity.TaggingWorkRequestStatusSucceeded,
		apply(identity.BulkEditOperationDetailsOperationTypeAddOrSet).Status)

	_, err := e.client.UpdateTag(t.Context(), identity.UpdateTagRequest{
		TagNamespaceId: new(nsID), TagName: new("Cost"),
		IsRetired: new(true),
	})
	require.NoError(t, err)

	assert.Equal(t, identity.TaggingWorkRequestStatusFailed,
		apply(identity.BulkEditOperationDetailsOperationTypeAddOrSet).Status, "a retired tag cannot be applied")
	assert.Equal(t, identity.TaggingWorkRequestStatusSucceeded,
		apply(identity.BulkEditOperationDetailsOperationTypeRemove).Status, "but it can be removed")

	got, err := e.client.GetUser(t.Context(), identity.GetUserRequest{UserId: u.Id})
	require.NoError(t, err)
	assert.Empty(t, got.DefinedTags)
}

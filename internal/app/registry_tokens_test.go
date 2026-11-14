package app

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

func TestAzureContainerRegistryTokensSDK(t *testing.T) {
	t.Parallel()

	fx := newACRFixture(t)
	ctx := t.Context()
	scopeMaps := fx.f.NewScopeMapsClient()
	tokens := fx.f.NewTokensClient()

	admin, err := fx.regs.ListCredentials(ctx, "sdk-rg", "sdkregistry", nil)
	require.NoError(t, err)

	adminClient := ocitest.Client{
		T: t, Base: fx.acr.URL, Host: "sdkregistry.azurecr.localhost",
		Prepare: func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+
				base64.StdEncoding.EncodeToString([]byte(*admin.Username+":"+*admin.Passwords[0].Value)))
		},
	}
	adminClient.PushImage("samples/app", "v1")
	adminClient.PushImage("private/app", "v1")

	smPoller, err := scopeMaps.BeginCreate(ctx, "sdk-rg", "sdkregistry", "pullsamples", armcontainerregistry.ScopeMap{
		Properties: &armcontainerregistry.ScopeMapProperties{
			Description: new("pull samples"),
			Actions:     []*string{new("repositories/samples/*/content/read")},
		},
	}, nil)
	require.NoError(t, err)

	sm, err := smPoller.PollUntilDone(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "UserDefined", *sm.Properties.Type)

	tokPoller, err := tokens.BeginCreate(ctx, "sdk-rg", "sdkregistry", "puller", armcontainerregistry.Token{
		Properties: &armcontainerregistry.TokenProperties{ScopeMapID: sm.ID},
	}, nil)
	require.NoError(t, err)

	tok, err := tokPoller.PollUntilDone(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, armcontainerregistry.TokenStatusEnabled, *tok.Properties.Status)
	assert.Equal(t, *sm.ID, *tok.Properties.ScopeMapID)

	genPoller, err := fx.regs.BeginGenerateCredentials(ctx, "sdk-rg", "sdkregistry",
		armcontainerregistry.GenerateCredentialsParameters{
			TokenID: tok.ID, Name: new(armcontainerregistry.TokenPasswordNamePassword1),
		}, nil)
	require.NoError(t, err)

	gen, err := genPoller.PollUntilDone(ctx, nil)
	require.NoError(t, err)
	require.Len(t, gen.Passwords, 1)
	assert.Equal(t, "puller", *gen.Username)
	require.NotEmpty(t, *gen.Passwords[0].Value)

	tokClient := ocitest.Client{
		T: t, Base: fx.acr.URL, Host: "sdkregistry.azurecr.localhost",
		Prepare: func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+
				base64.StdEncoding.EncodeToString([]byte("puller:"+*gen.Passwords[0].Value)))
		},
	}

	assert.Equal(t, http.StatusOK, tokClient.Do(http.MethodGet, "/v2/samples/app/manifests/v1", nil, nil).Status)
	assert.Equal(
		t,
		http.StatusUnauthorized,
		tokClient.Do(http.MethodGet, "/v2/private/app/manifests/v1", nil, nil).Status,
	)
	assert.Equal(
		t,
		http.StatusUnauthorized,
		tokClient.Do(http.MethodPost, "/v2/samples/app/blobs/uploads/", nil, nil).Status,
	)

	got, err := tokens.Get(ctx, "sdk-rg", "sdkregistry", "puller", nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.Credentials.Passwords, 1)
	assert.Nil(t, got.Properties.Credentials.Passwords[0].Value)

	upd, err := tokens.BeginUpdate(ctx, "sdk-rg", "sdkregistry", "puller", armcontainerregistry.TokenUpdateParameters{
		Properties: &armcontainerregistry.TokenUpdateProperties{Status: new(armcontainerregistry.TokenStatusDisabled)},
	}, nil)
	require.NoError(t, err)

	disabled, err := upd.PollUntilDone(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, armcontainerregistry.TokenStatusDisabled, *disabled.Properties.Status)
	assert.Equal(
		t,
		http.StatusUnauthorized,
		tokClient.Do(http.MethodGet, "/v2/samples/app/manifests/v1", nil, nil).Status,
	)

	smUpd, err := scopeMaps.BeginUpdate(
		ctx,
		"sdk-rg",
		"sdkregistry",
		"pullsamples",
		armcontainerregistry.ScopeMapUpdateParameters{
			Properties: &armcontainerregistry.ScopeMapPropertiesUpdateParameters{
				Actions: []*string{new("repositories/private/app/content/read")},
			},
		},
		nil,
	)
	require.NoError(t, err)

	_, err = smUpd.PollUntilDone(ctx, nil)
	require.NoError(t, err)

	enable, err := tokens.BeginUpdate(
		ctx,
		"sdk-rg",
		"sdkregistry",
		"puller",
		armcontainerregistry.TokenUpdateParameters{
			Properties: &armcontainerregistry.TokenUpdateProperties{
				Status: new(armcontainerregistry.TokenStatusEnabled),
			},
		},
		nil,
	)
	require.NoError(t, err)

	_, err = enable.PollUntilDone(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, tokClient.Do(http.MethodGet, "/v2/private/app/manifests/v1", nil, nil).Status)

	var names []string

	pager := scopeMaps.NewListPager("sdk-rg", "sdkregistry", nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, item := range page.Value {
			names = append(names, *item.Name)
		}
	}

	assert.Equal(t, []string{"_repositories_admin", "_repositories_pull", "_repositories_push", "pullsamples"}, names)

	inUse, err := scopeMaps.BeginDelete(ctx, "sdk-rg", "sdkregistry", "pullsamples", nil)
	if err == nil {
		_, err = inUse.PollUntilDone(ctx, nil)
	}

	require.Error(t, err)

	delTok, err := tokens.BeginDelete(ctx, "sdk-rg", "sdkregistry", "puller", nil)
	require.NoError(t, err)
	_, err = delTok.PollUntilDone(ctx, nil)
	require.NoError(t, err)

	delSM, err := scopeMaps.BeginDelete(ctx, "sdk-rg", "sdkregistry", "pullsamples", nil)
	require.NoError(t, err)
	_, err = delSM.PollUntilDone(ctx, nil)
	require.NoError(t, err)

	_, err = tokens.Get(ctx, "sdk-rg", "sdkregistry", "puller", nil)
	require.Error(t, err)
}

func TestAzureContainerRegistryImportImageSDK(t *testing.T) {
	t.Parallel()

	fx := newACRFixture(t)
	ctx := t.Context()

	other, err := fx.regs.BeginCreate(ctx, "sdk-rg", "sourceregistry", armcontainerregistry.Registry{
		Location: new("local"), SKU: &armcontainerregistry.SKU{Name: new(armcontainerregistry.SKUNameBasic)},
		Properties: &armcontainerregistry.RegistryProperties{AdminUserEnabled: new(true)},
	}, nil)
	require.NoError(t, err)

	srcReg, err := other.PollUntilDone(ctx, nil)
	require.NoError(t, err)

	srcCreds, err := fx.regs.ListCredentials(ctx, "sdk-rg", "sourceregistry", nil)
	require.NoError(t, err)

	basicFor := func(user, pass string) func(*http.Request) {
		return func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
		}
	}

	ocitest.Client{
		T: t, Base: fx.acr.URL, Host: "sourceregistry.azurecr.localhost",
		Prepare: basicFor(*srcCreds.Username, *srcCreds.Passwords[0].Value),
	}.PushImage("lib/base", "1.0")

	imp, err := fx.regs.BeginImportImage(ctx, "sdk-rg", "sdkregistry", armcontainerregistry.ImportImageParameters{
		Source: &armcontainerregistry.ImportSource{
			ResourceID: srcReg.ID, SourceImage: new("lib/base:1.0"),
		},
		TargetTags: []*string{new("imported/base:prod")},
		Mode:       new(armcontainerregistry.ImportModeNoForce),
	}, nil)
	require.NoError(t, err)
	_, err = imp.PollUntilDone(ctx, nil)
	require.NoError(t, err)

	dstCreds, err := fx.regs.ListCredentials(ctx, "sdk-rg", "sdkregistry", nil)
	require.NoError(t, err)

	dst := ocitest.Client{
		T: t, Base: fx.acr.URL, Host: "sdkregistry.azurecr.localhost",
		Prepare: basicFor(*dstCreds.Username, *dstCreds.Passwords[0].Value),
	}
	assert.Equal(t, http.StatusOK, dst.Do(http.MethodGet, "/v2/imported/base/manifests/prod", nil, nil).Status)

	again, err := fx.regs.BeginImportImage(ctx, "sdk-rg", "sdkregistry", armcontainerregistry.ImportImageParameters{
		Source:     &armcontainerregistry.ImportSource{ResourceID: srcReg.ID, SourceImage: new("lib/base:1.0")},
		TargetTags: []*string{new("imported/base:prod")},
	}, nil)
	if err == nil {
		_, err = again.PollUntilDone(ctx, nil)
	}

	require.Error(t, err)

	missing, err := fx.regs.BeginImportImage(ctx, "sdk-rg", "sdkregistry", armcontainerregistry.ImportImageParameters{
		Source: &armcontainerregistry.ImportSource{ResourceID: srcReg.ID, SourceImage: new("lib/none:1.0")},
	}, nil)
	if err == nil {
		_, err = missing.PollUntilDone(ctx, nil)
	}

	require.Error(t, err)
}

func TestAzureContainerRegistryNetworkRulesSDK(t *testing.T) {
	t.Parallel()

	fx := newACRFixture(t)
	ctx := t.Context()

	update := func(props armcontainerregistry.RegistryPropertiesUpdateParameters) error {
		poller, err := fx.regs.BeginUpdate(ctx, "sdk-rg", "sdkregistry",
			armcontainerregistry.RegistryUpdateParameters{Properties: &props}, nil)
		if err != nil {
			return err
		}

		_, err = poller.PollUntilDone(ctx, nil)

		return err
	}

	status := func() int {
		return ocitest.Client{T: t, Base: fx.acr.URL, Host: "sdkregistry.azurecr.localhost"}.
			Do(http.MethodGet, "/v2/", nil, nil).Status
	}

	assert.Equal(t, http.StatusUnauthorized, status())

	require.NoError(t, update(armcontainerregistry.RegistryPropertiesUpdateParameters{
		NetworkRuleSet: &armcontainerregistry.NetworkRuleSet{
			DefaultAction: new(armcontainerregistry.DefaultActionDeny),
			IPRules: []*armcontainerregistry.IPRule{
				{Action: new(armcontainerregistry.ActionAllow), IPAddressOrRange: new("10.0.0.0/8")},
			},
		},
	}))
	assert.Equal(t, http.StatusForbidden, status())

	require.NoError(t, update(armcontainerregistry.RegistryPropertiesUpdateParameters{
		NetworkRuleSet: &armcontainerregistry.NetworkRuleSet{
			DefaultAction: new(armcontainerregistry.DefaultActionDeny),
			IPRules: []*armcontainerregistry.IPRule{
				{Action: new(armcontainerregistry.ActionAllow), IPAddressOrRange: new("127.0.0.1")},
			},
		},
	}))
	assert.Equal(t, http.StatusUnauthorized, status())

	require.NoError(t, update(armcontainerregistry.RegistryPropertiesUpdateParameters{
		PublicNetworkAccess: new(armcontainerregistry.PublicNetworkAccessDisabled),
	}))
	assert.Equal(t, http.StatusForbidden, status())

	require.NoError(t, update(armcontainerregistry.RegistryPropertiesUpdateParameters{
		PublicNetworkAccess: new(armcontainerregistry.PublicNetworkAccessEnabled),
		NetworkRuleSet: &armcontainerregistry.NetworkRuleSet{
			DefaultAction: new(armcontainerregistry.DefaultActionAllow),
		},
	}))
	assert.Equal(t, http.StatusUnauthorized, status())

	require.Error(t, update(armcontainerregistry.RegistryPropertiesUpdateParameters{
		NetworkRuleSet: &armcontainerregistry.NetworkRuleSet{
			DefaultAction: new(armcontainerregistry.DefaultActionDeny),
			IPRules:       []*armcontainerregistry.IPRule{{IPAddressOrRange: new("not-an-ip")}},
		},
	}))
}

func TestAzureContainerRegistryExchangeUsesEntraIssuer(t *testing.T) {
	t.Parallel()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	backend := acr.NewInMemoryBackend()
	backend.SetContext(t.Context())

	h := acr.NewHandler(backend, acr.DefaultSettings())
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	wireAzureContainerRegistry(map[string]service.Registerable{"AzureARM": armH, "AzureContainerRegistry": h})

	_, err = backend.PutRegistry("exchangereg", false)
	require.NoError(t, err)

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	issued, err := azurearm.IssueToken(armH.Issuer, "https://login.example", "tenant-1", "client-1",
		"https://management.core.windows.net/.default")
	require.NoError(t, err)

	tests := []struct {
		name   string
		token  string
		tenant string
		want   int
	}{
		{name: "emulator token", token: issued.AccessToken, tenant: "tenant-1", want: http.StatusOK},
		{name: "wrong tenant", token: issued.AccessToken, tenant: "tenant-2", want: http.StatusUnauthorized},
		{name: "foreign token", token: "not.a.jwt", tenant: "tenant-1", want: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := url.Values{
				"grant_type": {"access_token"}, "access_token": {tt.token}, "tenant": {tt.tenant},
				"service": {"exchangereg.azurecr.localhost"},
			}.Encode()

			c := ocitest.Client{T: t, Base: srv.URL, Host: "exchangereg.azurecr.localhost"}
			got := c.Do(http.MethodPost, "/oauth2/exchange", []byte(body),
				map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
			assert.Equal(t, tt.want, got.Status, string(got.Body))
		})
	}
}

func TestAzureContainerRegistryArtifactCacheSDK(t *testing.T) {
	t.Parallel()

	fx := newACRFixture(t)
	ctx := t.Context()

	kv := azurekeyvault.NewInMemoryBackend(true)
	fx.acrH.Backend.SetSecretResolver(keyVaultSecretResolver(kv))

	upPoller, err := fx.regs.BeginCreate(ctx, "sdk-rg", "upstreamreg", armcontainerregistry.Registry{
		Location: new("local"), SKU: &armcontainerregistry.SKU{Name: new(armcontainerregistry.SKUNameBasic)},
		Properties: &armcontainerregistry.RegistryProperties{AdminUserEnabled: new(true)},
	}, nil)
	require.NoError(t, err)
	_, err = upPoller.PollUntilDone(ctx, nil)
	require.NoError(t, err)

	upCreds, err := fx.regs.ListCredentials(ctx, "sdk-rg", "upstreamreg", nil)
	require.NoError(t, err)

	ocitest.Client{
		T: t, Base: fx.acr.URL, Host: "upstreamreg.azurecr.localhost",
		Prepare: func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+
				base64.StdEncoding.EncodeToString([]byte(*upCreds.Username+":"+*upCreds.Passwords[0].Value)))
		},
	}.PushImage("ext/app", "v1")

	base := "https://kv.vault.azure.net"
	_, err = kv.SetSecret("kv", base, "user", &azurekeyvault.SetSecretRequest{Value: upCreds.Username})
	require.NoError(t, err)
	_, err = kv.SetSecret("kv", base, "pass", &azurekeyvault.SetSecretRequest{Value: upCreds.Passwords[0].Value})
	require.NoError(t, err)

	loginServer := "upstreamreg.azurecr.localhost:10011"
	csClient, rulesClient := fx.f.NewCredentialSetsClient(), fx.f.NewCacheRulesClient()

	csPoller, err := csClient.BeginCreate(ctx, "sdk-rg", "sdkregistry", "upcreds", armcontainerregistry.CredentialSet{
		Identity: &armcontainerregistry.IdentityProperties{
			Type: new(armcontainerregistry.ResourceIdentityTypeSystemAssigned),
		},
		Properties: &armcontainerregistry.CredentialSetProperties{
			LoginServer: new(loginServer),
			AuthCredentials: []*armcontainerregistry.AuthCredential{{
				Name:                     new(armcontainerregistry.CredentialNameCredential1),
				UsernameSecretIdentifier: new(base + "/secrets/user"),
				PasswordSecretIdentifier: new(base + "/secrets/pass"),
			}},
		},
	}, nil)
	require.NoError(t, err)

	cs, err := csPoller.PollUntilDone(ctx, nil)
	require.NoError(t, err)
	require.NotEmpty(t, *cs.Identity.PrincipalID)
	assert.Equal(t, armcontainerregistry.CredentialHealthStatusHealthy,
		*cs.Properties.AuthCredentials[0].CredentialHealth.Status)

	rulePoller, err := rulesClient.BeginCreate(ctx, "sdk-rg", "sdkregistry", "rule1", armcontainerregistry.CacheRule{
		Properties: &armcontainerregistry.CacheRuleProperties{
			SourceRepository: new(loginServer + "/ext/app"), TargetRepository: new("cached/app"),
			CredentialSetResourceID: cs.ID,
		},
	}, nil)
	require.NoError(t, err)

	_, err = rulePoller.PollUntilDone(ctx, nil)
	require.NoError(t, err)

	dstCreds, err := fx.regs.ListCredentials(ctx, "sdk-rg", "sdkregistry", nil)
	require.NoError(t, err)

	dst := ocitest.Client{
		T: t, Base: fx.acr.URL, Host: "sdkregistry.azurecr.localhost",
		Prepare: func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+
				base64.StdEncoding.EncodeToString([]byte(*dstCreds.Username+":"+*dstCreds.Passwords[0].Value)))
		},
	}
	assert.Equal(t, http.StatusOK, dst.Do(http.MethodGet, "/v2/cached/app/manifests/v1", nil, nil).Status)
	assert.Equal(t, http.StatusNotFound, dst.Do(http.MethodGet, "/v2/other/app/manifests/v1", nil, nil).Status)

	overlap, err := rulesClient.BeginCreate(ctx, "sdk-rg", "sdkregistry", "rule2", armcontainerregistry.CacheRule{
		Properties: &armcontainerregistry.CacheRuleProperties{
			SourceRepository: new(loginServer + "/ext/*"), TargetRepository: new("cached/*"),
		},
	}, nil)
	if err == nil {
		_, err = overlap.PollUntilDone(ctx, nil)
	}

	require.Error(t, err)

	inUse, err := csClient.BeginDelete(ctx, "sdk-rg", "sdkregistry", "upcreds", nil)
	if err == nil {
		_, err = inUse.PollUntilDone(ctx, nil)
	}

	require.Error(t, err)

	delRule, err := rulesClient.BeginDelete(ctx, "sdk-rg", "sdkregistry", "rule1", nil)
	require.NoError(t, err)
	_, err = delRule.PollUntilDone(ctx, nil)
	require.NoError(t, err)

	delCS, err := csClient.BeginDelete(ctx, "sdk-rg", "sdkregistry", "upcreds", nil)
	require.NoError(t, err)
	_, err = delCS.PollUntilDone(ctx, nil)
	require.NoError(t, err)
}

func TestAzureContainerRegistryCustomerManagedKeySDK(t *testing.T) {
	t.Parallel()

	fx := newACRFixture(t)
	ctx := t.Context()

	kv := azurekeyvault.NewInMemoryBackend(true)
	fx.acrH.Backend.SetKeyChecker(keyVaultKeyChecker(kv))

	base := "https://kv.vault.azure.net"
	created, err := kv.CreateKey("kv", base, "regkey", &azurekeyvault.CreateKeyRequest{Kty: "RSA", KeySize: 2048})
	require.NoError(t, err)

	_, err = kv.CreateKey("kv", base, "eckey", &azurekeyvault.CreateKeyRequest{Kty: "EC", Crv: "P-256"})
	require.NoError(t, err)

	identity := &armcontainerregistry.IdentityProperties{
		Type: new(armcontainerregistry.ResourceIdentityTypeUserAssigned),
		UserAssignedIdentities: map[string]*armcontainerregistry.UserIdentityProperties{
			"/subscriptions/s/resourceGroups/sdk-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/id1": {},
		},
	}

	create := func(name, sku, key string, id *armcontainerregistry.IdentityProperties) error {
		poller, createErr := fx.regs.BeginCreate(ctx, "sdk-rg", name, armcontainerregistry.Registry{
			Location: new("local"), SKU: &armcontainerregistry.SKU{Name: (*armcontainerregistry.SKUName)(&sku)},
			Identity: id,
			Properties: &armcontainerregistry.RegistryProperties{
				AdminUserEnabled: new(true),
				Encryption: &armcontainerregistry.EncryptionProperty{
					Status:             new(armcontainerregistry.EncryptionStatusEnabled),
					KeyVaultProperties: &armcontainerregistry.KeyVaultProperties{KeyIdentifier: new(key)},
				},
			},
		}, nil)
		if createErr != nil {
			return createErr
		}

		_, createErr = poller.PollUntilDone(ctx, nil)

		return createErr
	}

	require.Error(t, create("cmkbasic", "Basic", base+"/keys/regkey", identity))
	require.Error(t, create("cmknoid", "Premium", base+"/keys/regkey", nil))
	require.Error(t, create("cmkmissing", "Premium", base+"/keys/absent", identity))
	require.Error(t, create("cmkec", "Premium", base+"/keys/eckey", identity))
	require.NoError(t, create("cmkregistry", "Premium", created.Key.KID, identity))

	got, err := fx.regs.Get(ctx, "sdk-rg", "cmkregistry", nil)
	require.NoError(t, err)
	assert.Equal(t, armcontainerregistry.EncryptionStatusEnabled, *got.Properties.Encryption.Status)
	assert.False(t, *got.Properties.Encryption.KeyVaultProperties.KeyRotationEnabled)

	status := func() int {
		return ocitest.Client{T: t, Base: fx.acr.URL, Host: "cmkregistry.azurecr.localhost"}.
			Do(http.MethodGet, "/v2/", nil, nil).Status
	}

	assert.Equal(t, http.StatusUnauthorized, status())

	_, err = kv.UpdateKey("kv", base, "regkey", "", &azurekeyvault.UpdateKeyRequest{
		Attributes: &azurekeyvault.Attributes{Enabled: new(false)},
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, status())

	_, err = kv.UpdateKey("kv", base, "regkey", "", &azurekeyvault.UpdateKeyRequest{
		Attributes: &azurekeyvault.Attributes{Enabled: new(true)},
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, status())

	off, err := fx.regs.BeginUpdate(ctx, "sdk-rg", "cmkregistry", armcontainerregistry.RegistryUpdateParameters{
		Properties: &armcontainerregistry.RegistryPropertiesUpdateParameters{
			Encryption: &armcontainerregistry.EncryptionProperty{
				Status: new(armcontainerregistry.EncryptionStatusDisabled),
			},
		},
	}, nil)
	if err == nil {
		_, err = off.PollUntilDone(ctx, nil)
	}

	require.Error(t, err)

	on, err := fx.regs.BeginUpdate(ctx, "sdk-rg", "sdkregistry", armcontainerregistry.RegistryUpdateParameters{
		Properties: &armcontainerregistry.RegistryPropertiesUpdateParameters{
			Encryption: &armcontainerregistry.EncryptionProperty{
				Status:             new(armcontainerregistry.EncryptionStatusEnabled),
				KeyVaultProperties: &armcontainerregistry.KeyVaultProperties{KeyIdentifier: &created.Key.KID},
			},
		},
	}, nil)
	if err == nil {
		_, err = on.PollUntilDone(ctx, nil)
	}

	require.Error(t, err)
}

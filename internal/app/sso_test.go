package app

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/ssocreds"
	"github.com/aws/aws-sdk-go-v2/service/sso"
	"github.com/aws/aws-sdk-go-v2/service/ssooidc"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	identitystorebackend "github.com/blackbirdworks/gopherstack/services/identitystore"
	ssoadminbackend "github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

const (
	ssoStartURL       = "https://d-0000000000.awsapps.com/start"
	ssoDeviceGrant    = "urn:ietf:params:oauth:grant-type:device_code"
	ssoRefreshGrant   = "refresh_token"
	ssoAdminAccountID = "000000000000"
	ssoOtherAccountID = "111122223333"
	ssoInlinePolicy   = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"s3:ListAllMyBuckets","Resource":"*"}]}`
)

type ssoStack struct {
	oidc     *ssooidc.Client
	portal   *sso.Client
	sts      func(creds aws.CredentialsProvider) *sts.Client
	srv      *httptest.Server
	ids      *identitystorebackend.InMemoryBackend
	admin    ssoadminbackend.StorageBackend
	instance string
	storeID  string
}

func newSSOStack(t *testing.T) ssoStack {
	t.Helper()

	log := buildLogger("")
	cli := CLI{AccountID: ssoAdminAccountID, Region: "us-east-1"}
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
	)
	require.NoError(t, err)

	byName := serviceByName(services)
	adminH, ok := byName["SsoAdmin"].(*ssoadminbackend.Handler)
	require.True(t, ok)

	idsH, ok := byName["IdentityStore"].(*identitystorebackend.Handler)
	require.True(t, ok)

	instances := adminH.Backend.ListInstances()
	require.NotEmpty(t, instances)

	return ssoStack{
		oidc: ssooidc.NewFromConfig(cfg), portal: sso.NewFromConfig(cfg),
		sts: func(p aws.CredentialsProvider) *sts.Client {
			c := cfg.Copy()
			c.Credentials = p

			return sts.NewFromConfig(c)
		},
		srv: srv, ids: idsH.Backend, admin: adminH.Backend,
		instance: instances[0].InstanceArn, storeID: instances[0].IdentityStoreID,
	}
}

func (s ssoStack) addUser(t *testing.T, name string) string {
	t.Helper()

	u, err := s.ids.CreateUser(
		t.Context(),
		s.storeID,
		&identitystorebackend.CreateUserRequest{UserName: name, DisplayName: name},
	)
	require.NoError(t, err)

	return u.UserID
}

func (s ssoStack) addPermissionSet(t *testing.T, name string) string {
	t.Helper()

	ps, err := s.admin.CreatePermissionSet(s.instance, name, "", "", "", nil)
	require.NoError(t, err)
	require.NoError(t, s.admin.PutInlinePolicyToPermissionSet(s.instance, ps.PermissionSetArn, ssoInlinePolicy))

	return ps.PermissionSetArn
}

func (s ssoStack) assign(t *testing.T, psArn, account, principalID, principalType string) {
	t.Helper()

	_, err := s.admin.CreateAccountAssignment(s.instance, psArn, account, principalID, principalType)
	require.NoError(t, err)
}

type ssoLogin struct {
	clientID, clientSecret, accessToken, refreshToken string
}

func (s ssoStack) approveURL(t *testing.T, verificationComplete, user string) {
	t.Helper()

	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodGet, verificationComplete+"&action=approve&user="+user, http.NoBody,
	)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
}

// deviceLogin runs the `aws sso login` device flow through the real SDK client.
func (s ssoStack) deviceLogin(t *testing.T, user string) ssoLogin {
	t.Helper()

	ctx := t.Context()

	reg, err := s.oidc.RegisterClient(ctx, &ssooidc.RegisterClientInput{
		ClientName: aws.String("botocore-client-gopherstack"), ClientType: aws.String("public"),
		GrantTypes: []string{ssoDeviceGrant, ssoRefreshGrant}, Scopes: []string{"sso:account:access"},
	})
	require.NoError(t, err)

	dev, err := s.oidc.StartDeviceAuthorization(ctx, &ssooidc.StartDeviceAuthorizationInput{
		ClientId: reg.ClientId, ClientSecret: reg.ClientSecret, StartUrl: aws.String(ssoStartURL),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(dev.VerificationUriComplete), "/_gopherstack/sso/device?user_code=")

	poll := &ssooidc.CreateTokenInput{
		ClientId: reg.ClientId, ClientSecret: reg.ClientSecret,
		GrantType: aws.String(ssoDeviceGrant), DeviceCode: dev.DeviceCode,
	}

	_, err = s.oidc.CreateToken(ctx, poll)
	require.ErrorContains(t, err, "AuthorizationPendingException")

	s.approveURL(t, aws.ToString(dev.VerificationUriComplete), user)

	tok, err := s.oidc.CreateToken(ctx, poll)
	require.NoError(t, err)

	return ssoLogin{
		clientID: aws.ToString(reg.ClientId), clientSecret: aws.ToString(reg.ClientSecret),
		accessToken: aws.ToString(tok.AccessToken), refreshToken: aws.ToString(tok.RefreshToken),
	}
}

func writeSSOTokenCache(t *testing.T, l ssoLogin, expiresAt time.Time) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sso-token.json")
	data, err := json.Marshal(map[string]string{
		"accessToken": l.accessToken, "expiresAt": expiresAt.UTC().Format(time.RFC3339),
		"refreshToken": l.refreshToken, "clientId": l.clientID, "clientSecret": l.clientSecret,
		"region": "us-east-1", "startUrl": ssoStartURL,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

// TestSSOLoginEndToEnd runs register -> device authorization -> approval -> token -> portal ->
// ssocreds -> STS through the real SDKs against the composed server.
func TestSSOLoginEndToEnd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		roleName   string
		assignAs   string
		loginAs    string
		useGroup   bool
		expiredTok bool
	}{
		{name: "direct_assignment", roleName: "SSOEndToEndDirect", assignAs: "e2e-alice", loginAs: "e2e-alice"},
		{
			name:     "group_assignment",
			roleName: "SSOEndToEndGroup",
			assignAs: "e2e-bob",
			loginAs:  "e2e-bob",
			useGroup: true,
		},
		{
			name:       "refreshes_expired_token",
			roleName:   "SSOEndToEndRefresh",
			assignAs:   "e2e-carol",
			loginAs:    "e2e-carol",
			expiredTok: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSSOStack(t)
			userID := s.addUser(t, tt.assignAs)
			psArn := s.addPermissionSet(t, tt.roleName)

			if tt.useGroup {
				g, err := s.ids.CreateGroup(
					t.Context(),
					s.storeID,
					&identitystorebackend.CreateGroupRequest{DisplayName: "e2e-admins"},
				)
				require.NoError(t, err)

				_, err = s.ids.CreateGroupMembership(
					t.Context(),
					s.storeID,
					g.GroupID,
					identitystorebackend.MemberID{UserID: userID},
				)
				require.NoError(t, err)

				s.assign(t, psArn, ssoAdminAccountID, g.GroupID, "GROUP")
			} else {
				s.assign(t, psArn, ssoAdminAccountID, userID, "USER")
			}

			login := s.deviceLogin(t, tt.loginAs)

			accounts, err := s.portal.ListAccounts(
				t.Context(),
				&sso.ListAccountsInput{AccessToken: aws.String(login.accessToken)},
			)
			require.NoError(t, err)
			require.Len(t, accounts.AccountList, 1)
			assert.Equal(t, ssoAdminAccountID, aws.ToString(accounts.AccountList[0].AccountId))

			roles, err := s.portal.ListAccountRoles(t.Context(), &sso.ListAccountRolesInput{
				AccessToken: aws.String(login.accessToken), AccountId: aws.String(ssoAdminAccountID),
			})
			require.NoError(t, err)
			require.Len(t, roles.RoleList, 1)
			assert.Equal(t, tt.roleName, aws.ToString(roles.RoleList[0].RoleName))

			expiry := time.Now().Add(time.Hour)
			if tt.expiredTok {
				expiry = time.Now().Add(-time.Hour)
			}

			cache := writeSSOTokenCache(t, login, expiry)
			provider := ssocreds.New(s.portal, ssoAdminAccountID, tt.roleName, ssoStartURL, func(o *ssocreds.Options) {
				o.CachedTokenFilepath = cache
				o.SSOTokenProvider = ssocreds.NewSSOTokenProvider(s.oidc, cache)
			})

			creds, err := aws.NewCredentialsCache(provider).Retrieve(t.Context())
			require.NoError(t, err)
			assert.True(t, creds.CanExpire)

			id, err := s.sts(provider).GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(id.Arn), ":assumed-role/AWSReservedSSO_"+tt.roleName+"_")
			assert.Contains(t, aws.ToString(id.Arn), "/"+tt.loginAs)
			assert.Equal(t, ssoAdminAccountID, aws.ToString(id.Account))

			_, err = s.portal.Logout(t.Context(), &sso.LogoutInput{AccessToken: aws.String(login.accessToken)})
			require.NoError(t, err)

			_, err = s.portal.ListAccounts(
				t.Context(),
				&sso.ListAccountsInput{AccessToken: aws.String(login.accessToken)},
			)
			require.ErrorContains(t, err, "UnauthorizedException")
		})
	}
}

func TestSSOPortalReflectsAssignments(t *testing.T) {
	t.Parallel()

	s := newSSOStack(t)
	aliceID := s.addUser(t, "scope-alice")
	s.addUser(t, "scope-bob")
	psAdmin := s.addPermissionSet(t, "ScopeAdmin")
	psRead := s.addPermissionSet(t, "ScopeRead")

	s.assign(t, psAdmin, ssoAdminAccountID, aliceID, "USER")
	s.assign(t, psRead, ssoOtherAccountID, aliceID, "USER")

	alice := s.deviceLogin(t, "scope-alice")
	bob := s.deviceLogin(t, "scope-bob")

	tests := []struct {
		name         string
		token        string
		wantErr      string
		account      string
		role         string
		wantAccounts []string
	}{
		{
			name:         "alice_accounts",
			token:        alice.accessToken,
			wantAccounts: []string{ssoAdminAccountID, ssoOtherAccountID},
		},
		{name: "bob_has_none", token: bob.accessToken},
		{
			name:    "bob_denied_role",
			token:   bob.accessToken,
			account: ssoAdminAccountID,
			role:    "ScopeAdmin",
			wantErr: "ResourceNotFoundException",
		},
		{
			name:    "alice_wrong_role",
			token:   alice.accessToken,
			account: ssoOtherAccountID,
			role:    "ScopeAdmin",
			wantErr: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.role != "" {
				_, err := s.portal.GetRoleCredentials(t.Context(), &sso.GetRoleCredentialsInput{
					AccessToken: aws.String(tt.token), AccountId: aws.String(tt.account), RoleName: aws.String(tt.role),
				})
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			out, err := s.portal.ListAccounts(t.Context(), &sso.ListAccountsInput{AccessToken: aws.String(tt.token)})
			require.NoError(t, err)

			got := make([]string, 0, len(out.AccountList))
			for _, a := range out.AccountList {
				got = append(got, aws.ToString(a.AccountId))
			}

			assert.ElementsMatch(t, tt.wantAccounts, got)
		})
	}
}

func TestSSOVerificationUnknownUser(t *testing.T) {
	t.Parallel()

	s := newSSOStack(t)
	reg, err := s.oidc.RegisterClient(t.Context(), &ssooidc.RegisterClientInput{
		ClientName: aws.String("c"), ClientType: aws.String("public"),
	})
	require.NoError(t, err)

	dev, err := s.oidc.StartDeviceAuthorization(t.Context(), &ssooidc.StartDeviceAuthorizationInput{
		ClientId: reg.ClientId, ClientSecret: reg.ClientSecret, StartUrl: aws.String(ssoStartURL),
	})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		aws.ToString(dev.VerificationUriComplete)+"&action=approve&user=nobody",
		http.NoBody,
	)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

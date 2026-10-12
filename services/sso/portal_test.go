package sso_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssosdk "github.com/aws/aws-sdk-go-v2/service/sso"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sso"
)

func TestListAccounts(t *testing.T) {
	t.Parallel()

	ents := []sso.Entitlement{
		entitlement("222222222222", "Admin"), entitlement("111111111111", "Admin"),
		entitlement("111111111111", "ReadOnly"), entitlement("333333333333", "Admin"),
	}

	tests := []struct {
		name     string
		max      *int32
		wantIDs  []string
		wantNext bool
	}{
		{name: "all_sorted_and_deduplicated", wantIDs: []string{"111111111111", "222222222222", "333333333333"}},
		{name: "first_page", max: aws.Int32(2), wantIDs: []string{"111111111111", "222222222222"}, wantNext: true},
		{name: "single_item_page", max: aws.Int32(1), wantIDs: []string{"111111111111"}, wantNext: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, ents...)
			out, err := f.client.ListAccounts(t.Context(), &ssosdk.ListAccountsInput{
				AccessToken: aws.String(f.token), MaxResults: tt.max,
			})
			require.NoError(t, err)

			var ids []string

			for _, a := range out.AccountList {
				ids = append(ids, aws.ToString(a.AccountId))
			}

			assert.Equal(t, tt.wantIDs, ids)
			assert.Equal(t, tt.wantNext, out.NextToken != nil)
			assert.Equal(t, "prod", aws.ToString(out.AccountList[0].AccountName))
			assert.Equal(t, "prod@example.com", aws.ToString(out.AccountList[0].EmailAddress))
		})
	}
}

func TestListAccountsPaginatorWalk(t *testing.T) {
	t.Parallel()

	f := newFixture(
		t,
		entitlement(
			"111111111111",
			"Admin",
		),
		entitlement("222222222222", "Admin"),
		entitlement("333333333333", "Admin"),
	)
	p := ssosdk.NewListAccountsPaginator(f.client, &ssosdk.ListAccountsInput{
		AccessToken: aws.String(f.token), MaxResults: aws.Int32(1),
	})

	var ids []string

	for p.HasMorePages() {
		out, err := p.NextPage(t.Context())
		require.NoError(t, err)

		for _, a := range out.AccountList {
			ids = append(ids, aws.ToString(a.AccountId))
		}
	}

	assert.Equal(t, []string{"111111111111", "222222222222", "333333333333"}, ids)
}

func TestListAccountRoles(t *testing.T) {
	t.Parallel()

	ents := []sso.Entitlement{
		entitlement("111111111111", "ReadOnly"), entitlement("111111111111", "Admin"),
		entitlement("111111111111", "Admin"), entitlement("222222222222", "Dev"),
	}

	tests := []struct {
		name      string
		account   string
		wantCode  string
		wantRoles []string
	}{
		{name: "sorted_and_deduplicated", account: "111111111111", wantRoles: []string{"Admin", "ReadOnly"}},
		{name: "other_account", account: "222222222222", wantRoles: []string{"Dev"}},
		{name: "not_assigned", account: "999999999999", wantCode: "ResourceNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, ents...)
			out, err := f.client.ListAccountRoles(t.Context(), &ssosdk.ListAccountRolesInput{
				AccessToken: aws.String(f.token), AccountId: aws.String(tt.account),
			})

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			var roles []string

			for _, r := range out.RoleList {
				roles = append(roles, aws.ToString(r.RoleName))
				assert.Equal(t, tt.account, aws.ToString(r.AccountId))
			}

			assert.Equal(t, tt.wantRoles, roles)
		})
	}
}

func TestGetRoleCredentials(t *testing.T) {
	t.Parallel()

	ents := []sso.Entitlement{entitlement("111111111111", "Admin")}

	tests := []struct {
		name     string
		account  string
		role     string
		wantCode string
	}{
		{name: "assigned", account: "111111111111", role: "Admin"},
		{name: "role_name_case_insensitive", account: "111111111111", role: "admin"},
		{name: "wrong_account", account: "222222222222", role: "Admin", wantCode: "ResourceNotFoundException"},
		{name: "wrong_role", account: "111111111111", role: "Nope", wantCode: "ResourceNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, ents...)
			out, err := f.client.GetRoleCredentials(t.Context(), &ssosdk.GetRoleCredentialsInput{
				AccessToken: aws.String(f.token), AccountId: aws.String(tt.account), RoleName: aws.String(tt.role),
			})

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))
				assert.Empty(t, f.dir.issued, "no credentials are minted for an unassigned role")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "ASIAAdmin", aws.ToString(out.RoleCredentials.AccessKeyId))
			assert.Equal(t, "secret", aws.ToString(out.RoleCredentials.SecretAccessKey))
			assert.Equal(t, "token", aws.ToString(out.RoleCredentials.SessionToken))
			assert.Equal(t, int64(1893456000123), out.RoleCredentials.Expiration, "expiration is epoch milliseconds")
			require.Len(t, f.dir.subjects, 1)
			assert.Equal(t, "alice", f.dir.subjects[0].UserName)
		})
	}
}

func TestLogout(t *testing.T) {
	t.Parallel()

	f := newFixture(t, entitlement("111111111111", "Admin"))

	_, err := f.client.Logout(t.Context(), &ssosdk.LogoutInput{AccessToken: aws.String(f.token)})
	require.NoError(t, err)

	_, err = f.client.ListAccounts(t.Context(), &ssosdk.ListAccountsInput{AccessToken: aws.String(f.token)})
	require.Error(t, err)
	assert.Equal(t, "UnauthorizedException", apiCode(err))

	_, err = f.client.Logout(t.Context(), &ssosdk.LogoutInput{AccessToken: aws.String(f.token)})
	assert.Equal(t, "UnauthorizedException", apiCode(err))
}

func TestInvalidToken(t *testing.T) {
	t.Parallel()

	f := newFixture(t, entitlement("111111111111", "Admin"))
	tok := aws.String("not-a-token")

	calls := map[string]func() error{
		"list_accounts": func() error {
			_, err := f.client.ListAccounts(t.Context(), &ssosdk.ListAccountsInput{AccessToken: tok})

			return err
		},
		"list_roles": func() error {
			_, err := f.client.ListAccountRoles(t.Context(),
				&ssosdk.ListAccountRolesInput{AccessToken: tok, AccountId: aws.String("111111111111")})

			return err
		},
		"get_credentials": func() error {
			_, err := f.client.GetRoleCredentials(t.Context(), &ssosdk.GetRoleCredentialsInput{
				AccessToken: tok, AccountId: aws.String("111111111111"), RoleName: aws.String("Admin"),
			})

			return err
		},
		"logout": func() error {
			_, err := f.client.Logout(t.Context(), &ssosdk.LogoutInput{AccessToken: tok})

			return err
		},
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, "UnauthorizedException", apiCode(call()))
		})
	}
}

func TestDirectoryFailure(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.dir.err = errDirectory

	_, err := f.client.ListAccounts(t.Context(), &ssosdk.ListAccountsInput{AccessToken: aws.String(f.token)})
	require.Error(t, err)
	assert.Equal(t, "InternalFailure", apiCode(err))
}

func TestUnwiredHandler(t *testing.T) {
	t.Parallel()

	client := newClient(t, sso.NewHandler())

	_, err := client.ListAccounts(t.Context(), &ssosdk.ListAccountsInput{AccessToken: aws.String("x")})
	require.Error(t, err)
	assert.Equal(t, "InternalFailure", apiCode(err))
}

func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	const (
		s3Auth  = "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/s3/aws4_request, "
		portal  = "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/awsssoportal/aws4_request"
		unknown = "Unknown"
	)

	tests := []struct {
		name, method, uri, auth, token, wantOp string
		wantMatch                              bool
	}{
		{
			name:      "accounts",
			method:    "GET",
			uri:       "/assignment/accounts?max_result=5",
			token:     "t",
			wantMatch: true,
			wantOp:    "ListAccounts",
		},
		{
			name:      "roles",
			method:    "GET",
			uri:       "/assignment/roles?account_id=1",
			token:     "t",
			wantMatch: true,
			wantOp:    "ListAccountRoles",
		},
		{
			name:      "credentials",
			method:    "GET",
			uri:       "/federation/credentials",
			token:     "t",
			wantMatch: true,
			wantOp:    "GetRoleCredentials",
		},
		{name: "logout", method: "POST", uri: "/logout", token: "t", wantMatch: true, wantOp: "Logout"},
		{
			name:      "portal_signed",
			method:    "POST",
			uri:       "/logout",
			token:     "t",
			auth:      portal,
			wantMatch: true,
			wantOp:    "Logout",
		},
		{name: "s3_object_without_token", method: "GET", uri: "/assignment/accounts", wantOp: "ListAccounts"},
		{
			name:   "s3_signed",
			method: "GET",
			uri:    "/assignment/accounts",
			token:  "t",
			auth:   s3Auth,
			wantOp: "ListAccounts",
		},
		{name: "wrong_method", method: "POST", uri: "/assignment/accounts", token: "t", wantOp: unknown},
		{name: "other_path", method: "GET", uri: "/other", token: "t", wantOp: unknown},
	}

	h := sso.NewHandler()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.uri, http.NoBody)
			if tt.token != "" {
				req.Header.Set("X-Amz-Sso_bearer_token", tt.token)
			}

			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}

			c := echo.New().NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

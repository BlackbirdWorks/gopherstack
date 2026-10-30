package kms_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kms"
)

const (
	grantOwnerAcct    = "000000000000"
	grantGranteeAcct  = "111111111111"
	grantRetirerAcct  = "222222222222"
	grantStrangerAcct = "333333333333"
	grantRoleARN      = "arn:aws:iam::111111111111:role/GranteeRole"
	grantRetirerARN   = "arn:aws:iam::222222222222:user/retirer"
)

func grantPrincipals() map[string]*awsmeta.Principal {
	return map[string]*awsmeta.Principal{
		"owner": {
			Kind: awsmeta.PrincipalKindUser, AccountID: grantOwnerAcct,
			Arn: "arn:aws:iam::000000000000:user/owner",
		},
		"grantee": {
			Kind: awsmeta.PrincipalKindAssumedRole, AccountID: grantGranteeAcct,
			Arn: "arn:aws:sts::111111111111:assumed-role/GranteeRole/s1",
		},
		"retirer": {Kind: awsmeta.PrincipalKindUser, AccountID: grantRetirerAcct, Arn: grantRetirerARN},
		"stranger": {
			Kind: awsmeta.PrincipalKindUser, AccountID: grantStrangerAcct,
			Arn: "arn:aws:iam::333333333333:user/stranger",
		},
	}
}

type grantStack struct {
	clients map[string]*kmssdk.Client
}

func newGrantStack(t *testing.T) *grantStack {
	t.Helper()

	principals := grantPrincipals()
	e := echo.New()
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			meta := awsmeta.FromRequest(req, config.DefaultRegion)
			if p, ok := principals[meta.AccessKeyID]; ok {
				meta.Principal = p
				meta.Account = p.AccountID
			}

			c.SetRequest(req.WithContext(awsmeta.Set(req.Context(), meta)))

			return next(c)
		}
	})

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(kms.NewHandler(kms.NewInMemoryBackend())))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	s := &grantStack{clients: map[string]*kmssdk.Client{}}

	for name := range principals {
		cfg, err := awscfg.LoadDefaultConfig(t.Context(),
			awscfg.WithRegion(config.DefaultRegion),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(name, "x", "")),
		)
		require.NoError(t, err)

		s.clients[name] = kmssdk.NewFromConfig(cfg, func(o *kmssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	}

	return s
}

func requireAccessDenied(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AccessDeniedException")
}

func (s *grantStack) newKeyAndGrant(t *testing.T, ops []types.GrantOperation) (string, string, string) {
	t.Helper()

	k, err := s.clients["owner"].CreateKey(t.Context(), &kmssdk.CreateKeyInput{})
	require.NoError(t, err)

	keyID := aws.ToString(k.KeyMetadata.KeyId)
	g, err := s.clients["owner"].CreateGrant(t.Context(), &kmssdk.CreateGrantInput{
		KeyId: aws.String(keyID), GranteePrincipal: aws.String(grantRoleARN),
		RetiringPrincipal: aws.String(grantRetirerARN), Operations: ops,
	})
	require.NoError(t, err)

	return keyID, aws.ToString(g.GrantId), aws.ToString(g.GrantToken)
}

func TestGrantRetireCallerIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		caller  string
		ops     []types.GrantOperation
		byToken bool
		denied  bool
	}{
		{name: "issuing account", caller: "owner", ops: []types.GrantOperation{"Decrypt"}},
		{name: "retiring principal", caller: "retirer", ops: []types.GrantOperation{"Decrypt"}},
		{
			name: "grantee with retire op", caller: "grantee",
			ops: []types.GrantOperation{"Decrypt", "RetireGrant"},
		},
		{name: "grantee without retire op", caller: "grantee", ops: []types.GrantOperation{"Decrypt"}, denied: true},
		{name: "stranger by id", caller: "stranger", ops: []types.GrantOperation{"Decrypt"}, denied: true},
		{
			name: "stranger by token", caller: "stranger", ops: []types.GrantOperation{"Decrypt"},
			byToken: true, denied: true,
		},
		{name: "retiring principal by token", caller: "retirer", ops: []types.GrantOperation{"Decrypt"}, byToken: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newGrantStack(t)
			keyID, grantID, token := s.newKeyAndGrant(t, tt.ops)

			in := &kmssdk.RetireGrantInput{}
			if tt.byToken {
				in.GrantToken = aws.String(token)
			} else {
				in.KeyId, in.GrantId = aws.String(keyID), aws.String(grantID)
			}

			_, err := s.clients[tt.caller].RetireGrant(t.Context(), in)
			if tt.denied {
				requireAccessDenied(t, err)

				list, lerr := s.clients["owner"].ListGrants(
					t.Context(),
					&kmssdk.ListGrantsInput{KeyId: aws.String(keyID)},
				)
				require.NoError(t, lerr)
				assert.Len(t, list.Grants, 1)

				return
			}

			require.NoError(t, err)

			list, lerr := s.clients["owner"].ListGrants(t.Context(), &kmssdk.ListGrantsInput{KeyId: aws.String(keyID)})
			require.NoError(t, lerr)
			assert.Empty(t, list.Grants)
		})
	}
}

func TestGrantRevokeCreateCallerIdentity(t *testing.T) {
	t.Parallel()

	policyFor := func(acct, action string) string {
		return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam::` +
			grantOwnerAcct + `:root","arn:aws:iam::` + acct + `:root"]},"Action":"` + action + `","Resource":"*"}]}`
	}

	tests := []struct {
		name   string
		op     string
		caller string
		policy string
		denied bool
	}{
		{name: "revoke owner", op: "revoke", caller: "owner"},
		{name: "revoke retiring principal", op: "revoke", caller: "retirer", denied: true},
		{name: "revoke grantee", op: "revoke", caller: "grantee", denied: true},
		{
			name: "revoke cross account allowed by key policy", op: "revoke", caller: "stranger",
			policy: policyFor(grantStrangerAcct, "kms:RevokeGrant"),
		},
		{name: "create owner", op: "create", caller: "owner"},
		{name: "create cross account no policy", op: "create", caller: "stranger", denied: true},
		{
			name: "create cross account allowed by key policy", op: "create", caller: "stranger",
			policy: policyFor(grantStrangerAcct, "kms:CreateGrant"),
		},
		{
			name: "create cross account policy wrong action", op: "create", caller: "stranger",
			policy: policyFor(grantStrangerAcct, "kms:Decrypt"), denied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newGrantStack(t)
			keyID, grantID, _ := s.newKeyAndGrant(t, []types.GrantOperation{"Decrypt"})

			if tt.policy != "" {
				_, err := s.clients["owner"].PutKeyPolicy(t.Context(), &kmssdk.PutKeyPolicyInput{
					KeyId: aws.String(keyID), Policy: aws.String(tt.policy),
				})
				require.NoError(t, err)
			}

			var err error
			if tt.op == "revoke" {
				_, err = s.clients[tt.caller].RevokeGrant(t.Context(), &kmssdk.RevokeGrantInput{
					KeyId: aws.String(keyID), GrantId: aws.String(grantID),
				})
			} else {
				_, err = s.clients[tt.caller].CreateGrant(t.Context(), &kmssdk.CreateGrantInput{
					KeyId: aws.String(keyID), GranteePrincipal: aws.String(grantRoleARN),
					Name: aws.String("second"), Operations: []types.GrantOperation{"Encrypt"},
				})
			}

			if tt.denied {
				requireAccessDenied(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestListRetirableGrantsCallerIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		caller    string
		principal string
		wantCount int
		denied    bool
	}{
		{name: "own account principal", caller: "retirer", principal: grantRetirerARN, wantCount: 1},
		{name: "other account principal", caller: "stranger", principal: grantRetirerARN, denied: true},
		{name: "owner asks for other account", caller: "owner", principal: grantRetirerARN, denied: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newGrantStack(t)
			s.newKeyAndGrant(t, []types.GrantOperation{"Decrypt"})

			out, err := s.clients[tt.caller].ListRetirableGrants(t.Context(), &kmssdk.ListRetirableGrantsInput{
				RetiringPrincipal: aws.String(tt.principal),
			})
			if tt.denied {
				requireAccessDenied(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Grants, tt.wantCount)
		})
	}
}

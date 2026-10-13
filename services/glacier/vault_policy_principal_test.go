package glacier_test

import (
	"bytes"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	glaciertypes "github.com/aws/aws-sdk-go-v2/service/glacier/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func newCallerAwareClient(t *testing.T) (*glaciersdk.Client, *atomic.Value) {
	t.Helper()

	h := glacier.NewHandler(glacier.NewInMemoryBackend())
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	caller := &atomic.Value{}
	caller.Store("")

	e := echo.New()
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if arn, _ := caller.Load().(string); arn != "" {
				ctx := awsmeta.Set(c.Request().Context(), &awsmeta.Metadata{
					Principal: &awsmeta.Principal{Arn: arn, AccountID: testAccountID},
				})
				c.SetRequest(c.Request().WithContext(ctx))
			}

			return next(c)
		}
	})
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return glaciersdk.NewFromConfig(cfg, func(o *glaciersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	}), caller
}

func TestVaultAccessPolicyPrincipalEnforcement(t *testing.T) {
	t.Parallel()

	const (
		alice = "arn:aws:iam::000000000000:user/alice"
		bob   = "arn:aws:iam::000000000000:user/bob"
	)

	tests := []struct {
		name       string
		principal  string
		caller     string
		wantDenied bool
	}{
		{name: "named_principal_denied", principal: `{"AWS":"` + alice + `"}`, caller: alice, wantDenied: true},
		{name: "other_principal_allowed", principal: `{"AWS":"` + alice + `"}`, caller: bob},
		{name: "anonymous_not_named", principal: `{"AWS":"` + alice + `"}`, caller: ""},
		{name: "wildcard_denies_all", principal: `"*"`, caller: bob, wantDenied: true},
		{name: "account_id_principal", principal: `{"AWS":"000000000000"}`, caller: bob, wantDenied: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, caller := newCallerAwareClient(t)
			createLockTestVault(t, client, "pv")

			policy := `{"Statement":[{"Effect":"Deny","Principal":` + tt.principal +
				`,"Action":"glacier:UploadArchive","Resource":"arn:aws:glacier:` + testRegion +
				`:000000000000:vaults/pv"}]}`
			_, err := client.SetVaultAccessPolicy(t.Context(), &glaciersdk.SetVaultAccessPolicyInput{
				AccountId: aws.String("-"), VaultName: aws.String("pv"),
				Policy: &glaciertypes.VaultAccessPolicy{Policy: aws.String(policy)},
			})
			require.NoError(t, err)

			caller.Store(tt.caller)

			_, err = client.UploadArchive(t.Context(), &glaciersdk.UploadArchiveInput{
				AccountId: aws.String("-"), VaultName: aws.String("pv"),
				Body: bytes.NewReader([]byte("data")),
			})

			if tt.wantDenied {
				require.Error(t, err)
				assert.ErrorContains(t, err, "AccessDenied")

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestVaultLockResourceTagCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags       map[string]string
		name       string
		wantDenied bool
	}{
		{name: "tagged_vault_denied", tags: map[string]string{"legal-hold": "true"}, wantDenied: true},
		{name: "other_tag_value_allowed", tags: map[string]string{"legal-hold": "false"}},
		{name: "untagged_allowed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newVaultLockTestClient(t)
			createLockTestVault(t, client, "tagged")
			archiveID := uploadLockTestArchive(t, client, "tagged")

			if tt.tags != nil {
				_, err := client.AddTagsToVault(t.Context(), &glaciersdk.AddTagsToVaultInput{
					AccountId: aws.String("-"), VaultName: aws.String("tagged"), Tags: tt.tags,
				})
				require.NoError(t, err)
			}

			policy := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"glacier:DeleteArchive",` +
				`"Resource":"arn:aws:glacier:` + testRegion + `:000000000000:vaults/tagged",` +
				`"Condition":{"StringEquals":{"glacier:ResourceTag/legal-hold":"true"}}}]}`
			lockID := initiateLockTestPolicy(t, client, "tagged", policy)
			_, err := client.CompleteVaultLock(t.Context(), &glaciersdk.CompleteVaultLockInput{
				AccountId: aws.String("-"), VaultName: aws.String("tagged"), LockId: aws.String(lockID),
			})
			require.NoError(t, err)

			_, err = client.DeleteArchive(t.Context(), &glaciersdk.DeleteArchiveInput{
				AccountId: aws.String("-"), VaultName: aws.String("tagged"), ArchiveId: aws.String(archiveID),
			})

			if tt.wantDenied {
				require.Error(t, err)
				assert.ErrorContains(t, err, "AccessDenied")

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestVaultLockDeniesUploadArchive(t *testing.T) {
	t.Parallel()

	client := newVaultLockTestClient(t)
	createLockTestVault(t, client, "noupload")

	policy := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"glacier:UploadArchive",` +
		`"Resource":"arn:aws:glacier:` + testRegion + `:000000000000:vaults/noupload"}]}`
	lockID := initiateLockTestPolicy(t, client, "noupload", policy)
	_, err := client.CompleteVaultLock(t.Context(), &glaciersdk.CompleteVaultLockInput{
		AccountId: aws.String("-"), VaultName: aws.String("noupload"), LockId: aws.String(lockID),
	})
	require.NoError(t, err)

	_, err = client.UploadArchive(t.Context(), &glaciersdk.UploadArchiveInput{
		AccountId: aws.String("-"), VaultName: aws.String("noupload"), Body: bytes.NewReader([]byte("x")),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "AccessDenied")
}

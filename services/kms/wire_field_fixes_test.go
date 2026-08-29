package kms_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kms"
)

// newTestKMSClient stands up the real aws-sdk-go-v2 KMS client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestKMSClient(t *testing.T, h *kms.Handler) *kmssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return kmssdk.NewFromConfig(cfg, func(o *kmssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func newTestKMSHandler() *kms.Handler {
	return kms.NewHandler(kms.NewInMemoryBackend())
}

// TestDescribeKey_AWSAccountId_RealClient covers a layer-3 bug (gopherstack-g8k9):
// the backend's accountID is already tracked (used to build every key's Arn
// via gopherarn.Build in keys.go) but keyToMetadata never surfaced it as its
// own KeyMetadata.AWSAccountId field. Real field name and presence confirmed
// against kms@v1.55.4 deserializers.go's awsAwsjson11_deserializeDocumentKeyMetadata
// (case "AWSAccountId":). Pre-fix, a real client's KeyMetadata.AwsAccountId
// was always nil regardless of the account the key actually belonged to.
func TestDescribeKey_AWSAccountId_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	created, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
	require.NoError(t, err)
	require.NotNil(t, created.KeyMetadata.AWSAccountId,
		"KeyMetadata.AWSAccountId must round-trip from the backend's tracked account ID; pre-fix it was always nil")
	assert.Equal(t, config.DefaultAccountID, aws.ToString(created.KeyMetadata.AWSAccountId))

	described, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{
		KeyId: created.KeyMetadata.KeyId,
	})
	require.NoError(t, err)
	require.NotNil(t, described.KeyMetadata.AWSAccountId)
	assert.Equal(t, config.DefaultAccountID, aws.ToString(described.KeyMetadata.AWSAccountId))
}

// TestListRetirableGrants_RetiringServicePrincipal_RealClient is a
// write-only-state bug: CreateGrant has always accepted and stored
// GranteeServicePrincipal/RetiringServicePrincipal on the Grant (see
// Grant.RetiringServicePrincipal in models.go), but ListRetirableGrantsInput
// had no RetiringServicePrincipal field at all, and the backend filtered
// solely on RetiringPrincipal -- so a grant whose only retiring principal was
// a service principal could never be found through ListRetirableGrants,
// KMS's only real read path for "which grants can I retire" (RetireGrant
// itself requires a GrantId/GrantToken you'd otherwise have no way to
// discover). Confirmed against aws-sdk-go-v2/service/kms@v1.55.4
// api_op_ListRetirableGrants.go: ListRetirableGrantsInput carries both
// RetiringPrincipal and RetiringServicePrincipal ("You must specify either
// RetiringPrincipal or RetiringServicePrincipal, but not both").
func TestListRetirableGrants_RetiringServicePrincipal_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	created, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
	require.NoError(t, err)

	sourceArn := "arn:aws:cloudtrail:us-east-1:123456789012:trail/example"
	retiringService := "cloudtrail.amazonaws.com"

	_, err = client.CreateGrant(ctx, &kmssdk.CreateGrantInput{
		KeyId:                    created.KeyMetadata.KeyId,
		GranteeServicePrincipal:  aws.String(retiringService),
		RetiringServicePrincipal: aws.String(retiringService),
		Constraints: &kmstypes.GrantConstraints{
			SourceArn: aws.String(sourceArn),
		},
		Operations: []kmstypes.GrantOperation{kmstypes.GrantOperationDecrypt},
	})
	require.NoError(t, err)

	// Decoy: a grant with NEITHER RetiringPrincipal nor RetiringServicePrincipal
	// set. Both have the empty-string zero value, same as an unrecognized
	// RetiringServicePrincipal field would decode to on the request side --
	// this decoy exists so an empty-string-matches-empty-string filter bug
	// can't masquerade as a pass by accidentally including this grant too.
	_, err = client.CreateGrant(ctx, &kmssdk.CreateGrantInput{
		KeyId:            created.KeyMetadata.KeyId,
		GranteePrincipal: aws.String("arn:aws:iam::123456789012:role/decoy-grantee"),
		Operations:       []kmstypes.GrantOperation{kmstypes.GrantOperationDecrypt},
	})
	require.NoError(t, err)

	retirable, err := client.ListRetirableGrants(ctx, &kmssdk.ListRetirableGrantsInput{
		RetiringServicePrincipal: aws.String(retiringService),
	})
	require.NoError(t, err)
	require.Len(
		t, retirable.Grants, 1,
		"ListRetirableGrants filtered by RetiringServicePrincipal must return exactly "+
			"the matching grant, not every grant with an empty RetiringPrincipal",
	)
	assert.Equal(t, retiringService, aws.ToString(retirable.Grants[0].RetiringServicePrincipal))
}

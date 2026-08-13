package codeartifact_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:])
}

// TestPublishPackageVersion_AssetSHA256_RoundTrip drives PublishPackageVersion
// through a real SDK client and proves a client-supplied AssetSHA256 that
// doesn't match the actual asset content is rejected, instead of being
// silently ignored while the server computed and stored its own hash
// (gopherstack-h910). The pinned SDK (codeartifact@v1.41.4) declares no
// MismatchedSha256Exception for this op -- only ValidationException and
// siblings -- so the mismatch surfaces as ValidationException, not a
// dedicated exception type.
func TestPublishPackageVersion_AssetSHA256_RoundTrip(t *testing.T) {
	t.Parallel()

	h := codeartifact.NewHandler(codeartifact.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestCodeArtifactClient(t, h)

	_, err := client.CreateDomain(t.Context(), &casdk.CreateDomainInput{Domain: aws.String("sha-domain")})
	require.NoError(t, err)

	_, err = client.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
		Domain:     aws.String("sha-domain"),
		Repository: aws.String("sha-repo"),
	})
	require.NoError(t, err)

	_, err = client.PublishPackageVersion(t.Context(), &casdk.PublishPackageVersionInput{
		Domain:         aws.String("sha-domain"),
		Repository:     aws.String("sha-repo"),
		Format:         types.PackageFormatGeneric,
		Package:        aws.String("mylib"),
		PackageVersion: aws.String("1.0.0"),
		AssetName:      aws.String("mylib-1.0.0.tgz"),
		AssetSHA256:    aws.String("0000000000000000000000000000000000000000000000000000000000000000"),
		AssetContent:   strings.NewReader("asset-content"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ValidationException")

	out, err := client.PublishPackageVersion(t.Context(), &casdk.PublishPackageVersionInput{
		Domain:         aws.String("sha-domain"),
		Repository:     aws.String("sha-repo"),
		Format:         types.PackageFormatGeneric,
		Package:        aws.String("mylib"),
		PackageVersion: aws.String("1.0.0"),
		AssetName:      aws.String("mylib-1.0.0.tgz"),
		AssetSHA256:    aws.String(sha256Hex("asset-content")),
		AssetContent:   strings.NewReader("asset-content"),
	})
	require.NoError(t, err)
	assert.Equal(t, "1.0.0", aws.ToString(out.Version))
}

// TestPublishPackageVersion_ClientRequiresAssetSHA256 proves the real SDK
// client itself refuses to send PublishPackageVersion without AssetSHA256,
// confirming it is genuinely a required member on the pinned SDK, not an
// assumption (gopherstack-h910).
func TestPublishPackageVersion_ClientRequiresAssetSHA256(t *testing.T) {
	t.Parallel()

	h := codeartifact.NewHandler(codeartifact.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestCodeArtifactClient(t, h)

	_, err := client.PublishPackageVersion(t.Context(), &casdk.PublishPackageVersionInput{
		Domain:         aws.String("sha-domain"),
		Repository:     aws.String("sha-repo"),
		Format:         types.PackageFormatGeneric,
		Package:        aws.String("mylib"),
		PackageVersion: aws.String("1.0.0"),
		AssetName:      aws.String("mylib-1.0.0.tgz"),
		AssetContent:   strings.NewReader("asset-content"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AssetSHA256")
}

package ecr_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// TestDescribeImageScanFindings_AttributesDecodeAsList drives
// DescribeImageScanFindings through the real aws-sdk-go-v2 ecr client.
// ImageScanFinding.Attributes deserializes via
// awsAwsjson11_deserializeDocumentAttributeList, a list of {key, value}
// objects -- gopherstack previously emitted a bare map, which fails every
// real client's decode once any finding carries attributes (BASIC scans
// always seed package_name/package_version).
func TestDescribeImageScanFindings_AttributesDecodeAsList(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	h := ecr.NewHandler(b, nil)
	client := newTestECRClient(t, h)

	b.CreateRepoInternal("scan-repo")
	digest := "sha256:deadbeef00000000000000000000000000000000000000000000000000000"
	b.AddImageInternal("scan-repo", makeImage(digest, "latest"))

	_, err := b.StartImageScan(context.Background(), "scan-repo", ecr.ImageIdentifier{ImageDigest: digest})
	require.NoError(t, err)

	out, err := client.DescribeImageScanFindings(t.Context(), &ecrsdk.DescribeImageScanFindingsInput{
		RepositoryName: aws.String("scan-repo"),
		ImageId:        &ecrtypes.ImageIdentifier{ImageDigest: aws.String(digest)},
	})
	require.NoError(t, err, "real SDK client must decode DescribeImageScanFindings without error")
	require.NotNil(t, out.ImageScanFindings)
	require.NotEmpty(t, out.ImageScanFindings.Findings)

	found := false
	for _, f := range out.ImageScanFindings.Findings {
		if len(f.Attributes) > 0 {
			found = true
			assert.NotEmpty(t, aws.ToString(f.Attributes[0].Key))
		}
	}
	assert.True(t, found, "expected at least one finding with attributes")
}

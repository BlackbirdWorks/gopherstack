package cloudfront_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestXMLDeclarationAppearsExactlyOnce is a regression test: xmlResp used to hand
// bodies that already began with `<?xml version="1.0" encoding="UTF-8"?>` to echo's
// c.XMLBlob, which prepends its own copy of the same declaration, producing two
// back-to-back declarations. An XML declaration is legal only as the very first
// construct in a document, so real parsers (botocore: "Unable to parse response")
// reject the whole body. Asserted on raw response bytes, not a decoded struct --
// decoding round-trips fine even with the doubled declaration since Go's
// encoding/xml (and the smithy-go decoder used by the AWS SDK for Go v2) are
// lenient about it, which is exactly why the doubling went unnoticed here.
func TestXMLDeclarationAppearsExactlyOnce(t *testing.T) {
	t.Parallel()

	const decl = `<?xml version="1.0" encoding="UTF-8"?>`

	tests := []struct {
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:       "list_distributions",
			method:     http.MethodGet,
			path:       "/2020-05-31/distribution",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_distribution_not_found_error_path",
			method:     http.MethodGet,
			path:       "/2020-05-31/distribution/does-not-exist",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list_distributions_by_key_group_id",
			method:     http.MethodGet,
			path:       "/2020-05-31/distributionsByKeyGroupId/kg-1",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXML(t, h, tt.method, tt.path, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			body := rec.Body.String()
			require.True(t, strings.HasPrefix(body, decl), "body must start with the XML declaration: %s", body)
			assert.Equal(t, 1, strings.Count(body, decl),
				"XML declaration must appear exactly once, got body: %s", body)
		})
	}
}

// TestGetDistributionConfig_XMLDeclarationAppearsExactlyOnce covers the one
// xmlResp caller that passes RawConfig straight through: RawConfig is stored
// from either the raw request body (real SDK/smithy-go REST-XML requests never
// carry a leading declaration -- confirmed against aws-sdk-go-v2/service/
// cloudfront@v1.67.4 serializers.go, which drives smithy-go's xml.Encoder with
// no xml.Header write) or from xml.Marshal output (which also never emits one),
// so unlike every other body builder in this package RawConfig never carried
// its own declaration. Before the fix this path accidentally looked correct
// (XMLBlob supplied the sole declaration); after switching xmlResp to write
// bytes verbatim it would have emitted zero declarations without an explicit
// fix at the call site.
func TestGetDistributionConfig_XMLDeclarationAppearsExactlyOnce(t *testing.T) {
	t.Parallel()

	const decl = `<?xml version="1.0" encoding="UTF-8"?>`

	h := newTestHandler(t)
	createRec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
		minimalDistConfig("ref-getconfig-decl", "test", true))
	require.Equal(t, http.StatusCreated, createRec.Code, createRec.Body.String())
	id := extractXMLID(t, createRec.Body.String())
	require.NotEmpty(t, id)

	rec := doXML(t, h, http.MethodGet, "/2020-05-31/distribution/"+id+"/config", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	require.True(t, strings.HasPrefix(body, decl), "body must start with the XML declaration: %s", body)
	assert.Equal(t, 1, strings.Count(body, decl), "XML declaration must appear exactly once, got body: %s", body)
}

// TestListDistributions_RealSDKClient_ParsesSuccessfully drives ListDistributions
// through the real aws-sdk-go-v2 CloudFront client so a doubled XML declaration
// surfaces as a client-side parse failure, not merely a substring mismatch on a
// decoded struct. Reproduces the botocore "Unable to parse response" failure
// against the emulator.
func TestListDistributions_RealSDKClient_ParsesSuccessfully(t *testing.T) {
	t.Parallel()

	h := cloudfront.NewHandler(cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"))
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateDistribution(t.Context(), &cfsdk.CreateDistributionInput{
		DistributionConfig: &types.DistributionConfig{
			CallerReference: aws.String("ref-xml-decl-1"),
			Comment:         aws.String("xml decl test"),
			Enabled:         aws.Bool(true),
			Origins: &types.Origins{
				Quantity: aws.Int32(1),
				Items: []types.Origin{
					{Id: aws.String("origin1"), DomainName: aws.String("example.com")},
				},
			},
			DefaultCacheBehavior: &types.DefaultCacheBehavior{
				TargetOriginId:       aws.String("origin1"),
				ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Distribution)

	out, err := client.ListDistributions(t.Context(), &cfsdk.ListDistributionsInput{})
	require.NoError(t, err, "ListDistributions must parse cleanly through the real SDK client")
	require.NotNil(t, out.DistributionList)
	require.NotEmpty(t, out.DistributionList.Items)
	assert.Equal(t, aws.ToString(created.Distribution.Id), aws.ToString(out.DistributionList.Items[0].Id))
}

// TestGetDistribution_RealSDKClient_ErrorPathParses drives the 404 error path
// through the real SDK client and asserts it surfaces as a typed NoSuchDistribution
// API error rather than a client-side XML parse failure -- confirming the error
// path (cfErrorXML via xmlResp) was in the doubled-declaration blast radius too.
func TestGetDistribution_RealSDKClient_ErrorPathParses(t *testing.T) {
	t.Parallel()

	h := cloudfront.NewHandler(cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"))
	client := newTestCloudFrontClient(t, h)

	_, err := client.GetDistribution(t.Context(), &cfsdk.GetDistributionInput{
		Id: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAsf(t, err, &apiErr, "error path must parse into a typed API error, got: %v", err)
	assert.Equal(t, "NoSuchDistribution", apiErr.ErrorCode())
}

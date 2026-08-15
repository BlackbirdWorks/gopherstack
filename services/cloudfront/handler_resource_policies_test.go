package cloudfront_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResourcePolicy_NotFound verifies GetResourcePolicy 404s when no policy has
// been put for a resource ARN, and succeeds once one has been. All three
// resource-policy ops are POSTs to distinct RPC-style paths carrying ResourceArn
// in the body (api_op_{Get,Put,Delete}ResourcePolicy.go), never a query string.
func TestResourcePolicy_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	const prefix = "/2020-05-31/"
	const arn = "arn:aws:cloudfront::123456789012:distribution/ENOPOLICY"

	getBody := fmt.Sprintf(`<GetResourcePolicyRequest><ResourceArn>%s</ResourceArn></GetResourcePolicyRequest>`, arn)
	getRec := doXML(t, h, http.MethodPost, prefix+"get-resource-policy", []byte(getBody))
	assert.Equal(t, http.StatusNotFound, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "EntityNotFound")

	putBody := fmt.Sprintf(
		`<PutResourcePolicyRequest><PolicyDocument>policy-body-2012-10-17</PolicyDocument>`+
			`<ResourceArn>%s</ResourceArn></PutResourcePolicyRequest>`,
		arn,
	)
	putRec := doXML(t, h, http.MethodPost, prefix+"put-resource-policy", []byte(putBody))
	require.Equal(t, http.StatusOK, putRec.Code)

	getAfterPut := doXML(t, h, http.MethodPost, prefix+"get-resource-policy", []byte(getBody))
	assert.Equal(t, http.StatusOK, getAfterPut.Code)
	assert.Contains(t, getAfterPut.Body.String(), "policy-body-2012-10-17")
}

// TestResourcePolicy_CRUD tests resource policy Put/Get/Delete.
func TestResourcePolicy_CRUD(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const arn = "arn:aws:cloudfront::123456789012:distribution/E1"
	const prefix = "/2020-05-31/"

	putBody := fmt.Sprintf(
		`<PutResourcePolicyRequest><PolicyDocument>{"Version":"2012-10-17"}</PolicyDocument>`+
			`<ResourceArn>%s</ResourceArn></PutResourcePolicyRequest>`,
		arn,
	)
	cfOK(t, h, http.MethodPost, prefix+"put-resource-policy", putBody)

	getBody := fmt.Sprintf(`<GetResourcePolicyRequest><ResourceArn>%s</ResourceArn></GetResourcePolicyRequest>`, arn)
	out := cfOK(t, h, http.MethodPost, prefix+"get-resource-policy", getBody)
	if !strings.Contains(out, "PolicyDocument") {
		t.Errorf("unexpected response: %s", out)
	}

	deleteBody := fmt.Sprintf(
		`<DeleteResourcePolicyRequest><ResourceArn>%s</ResourceArn></DeleteResourcePolicyRequest>`,
		arn,
	)
	cfOK(t, h, http.MethodPost, prefix+"delete-resource-policy", deleteBody)
}

// TestPutResourcePolicy_PolicyDocumentWired is a regression test for
// gopherstack-nfka: the real request root is PutResourcePolicyRequest and the
// policy field is PolicyDocument, not the previous handler's xml:"ResourcePolicy"
// root / xml:"Policy" field. Neither name matched a real client's element names,
// so xml.Unmarshal errored on the whole body (err was discarded) and every real
// PutResourcePolicy call silently stored an empty policy.
func TestPutResourcePolicy_PolicyDocumentWired(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	const arn = "arn:aws:cloudfront::123456789012:distribution/EWIRED"

	const policyMarker = "policy-statement-sid-1-marker"
	putBody := fmt.Sprintf(
		`<PutResourcePolicyRequest><PolicyDocument>%s</PolicyDocument>`+
			`<ResourceArn>%s</ResourceArn></PutResourcePolicyRequest>`,
		policyMarker, arn,
	)
	putRec := doXML(t, h, http.MethodPost, "/2020-05-31/put-resource-policy", []byte(putBody))
	require.Equal(t, http.StatusOK, putRec.Code, putRec.Body.String())
	assert.Contains(t, putRec.Body.String(), arn)

	getBody := fmt.Sprintf(`<GetResourcePolicyRequest><ResourceArn>%s</ResourceArn></GetResourcePolicyRequest>`, arn)
	getRec := doXML(t, h, http.MethodPost, "/2020-05-31/get-resource-policy", []byte(getBody))
	require.Equal(t, http.StatusOK, getRec.Code, getRec.Body.String())
	assert.Contains(t, getRec.Body.String(), policyMarker)
}

// TestResourcePolicy_RealClient drives Put/Get/Delete through the real
// aws-sdk-go-v2 CloudFront client -- the strongest available check that
// gopherstack's request parsing matches the real wire shape byte-for-byte.
func TestResourcePolicy_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)
	const arn = "arn:aws:cloudfront::123456789012:distribution/EREALCLIENT"
	const policy = `{"Version":"2012-10-17","Statement":[]}`

	_, err := client.PutResourcePolicy(t.Context(), &cfsdk.PutResourcePolicyInput{
		ResourceArn:    aws.String(arn),
		PolicyDocument: aws.String(policy),
	})
	require.NoError(t, err)

	got, err := client.GetResourcePolicy(t.Context(), &cfsdk.GetResourcePolicyInput{
		ResourceArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.NotNil(t, got.PolicyDocument)
	assert.JSONEq(t, policy, *got.PolicyDocument)
	require.NotNil(t, got.ResourceArn)
	assert.Equal(t, arn, *got.ResourceArn)

	_, err = client.DeleteResourcePolicy(t.Context(), &cfsdk.DeleteResourcePolicyInput{
		ResourceArn: aws.String(arn),
	})
	require.NoError(t, err)

	_, err = client.GetResourcePolicy(t.Context(), &cfsdk.GetResourcePolicyInput{
		ResourceArn: aws.String(arn),
	})
	require.Error(t, err)
}

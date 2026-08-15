package ecr_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real ECR
// operation, extracted from ecr@v1.60.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "AmazonEC2ContainerRegistry_V20150921.<Op>") and always POSTs to "/" --
// ECR's control plane is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a
// REST-family service there is no path template to get wrong: dispatch is
// entirely by this one header. ExtractOperation and Handler() both derive
// the action the same way (TrimPrefix on
// "AmazonEC2ContainerRegistry_V20150921."), so the class of bug this table
// can catch is a dispatch-table key that doesn't exactly match the real op
// name (typo, wrong case -- ECR is case-sensitive JSON-RPC), not a
// route-template mismatch. This table only exercises the control-plane
// (X-Amz-Target) surface; the separate Docker registry v2 HTTP API
// (isRegistryPath in handler.go) is a distinct, path-routed protocol not
// covered here.
//
// This table covers all 58 real ECR ops, which is also gopherstack's full
// implemented set (h.GetSupportedOperations(), 58/58) as of ecr@v1.60.4 --
// confirmed by diffing both GetSupportedOperations() and the actual
// buildOps() dispatch table (buildCoreOps + buildExtOps) against this exact
// list. Zero mismatches either direction: no dead key, no gap.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AmazonEC2ContainerRegistry_V20150921.`
// and pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"BatchCheckLayerAvailability", "AmazonEC2ContainerRegistry_V20150921.BatchCheckLayerAvailability"},
		{"BatchDeleteImage", "AmazonEC2ContainerRegistry_V20150921.BatchDeleteImage"},
		{"BatchGetImage", "AmazonEC2ContainerRegistry_V20150921.BatchGetImage"},
		{
			"BatchGetRepositoryScanningConfiguration",
			"AmazonEC2ContainerRegistry_V20150921.BatchGetRepositoryScanningConfiguration",
		},
		{"CompleteLayerUpload", "AmazonEC2ContainerRegistry_V20150921.CompleteLayerUpload"},
		{"CreatePullThroughCacheRule", "AmazonEC2ContainerRegistry_V20150921.CreatePullThroughCacheRule"},
		{"CreateRepository", "AmazonEC2ContainerRegistry_V20150921.CreateRepository"},
		{
			"CreateRepositoryCreationTemplate",
			"AmazonEC2ContainerRegistry_V20150921.CreateRepositoryCreationTemplate",
		},
		{"DeleteLifecyclePolicy", "AmazonEC2ContainerRegistry_V20150921.DeleteLifecyclePolicy"},
		{"DeletePullThroughCacheRule", "AmazonEC2ContainerRegistry_V20150921.DeletePullThroughCacheRule"},
		{"DeleteRegistryPolicy", "AmazonEC2ContainerRegistry_V20150921.DeleteRegistryPolicy"},
		{"DeleteRepository", "AmazonEC2ContainerRegistry_V20150921.DeleteRepository"},
		{
			"DeleteRepositoryCreationTemplate",
			"AmazonEC2ContainerRegistry_V20150921.DeleteRepositoryCreationTemplate",
		},
		{"DeleteRepositoryPolicy", "AmazonEC2ContainerRegistry_V20150921.DeleteRepositoryPolicy"},
		{"DeleteSigningConfiguration", "AmazonEC2ContainerRegistry_V20150921.DeleteSigningConfiguration"},
		{
			"DeregisterPullTimeUpdateExclusion",
			"AmazonEC2ContainerRegistry_V20150921.DeregisterPullTimeUpdateExclusion",
		},
		{"DescribeImageReplicationStatus", "AmazonEC2ContainerRegistry_V20150921.DescribeImageReplicationStatus"},
		{"DescribeImages", "AmazonEC2ContainerRegistry_V20150921.DescribeImages"},
		{"DescribeImageScanFindings", "AmazonEC2ContainerRegistry_V20150921.DescribeImageScanFindings"},
		{"DescribeImageSigningStatus", "AmazonEC2ContainerRegistry_V20150921.DescribeImageSigningStatus"},
		{"DescribePullThroughCacheRules", "AmazonEC2ContainerRegistry_V20150921.DescribePullThroughCacheRules"},
		{"DescribeRegistry", "AmazonEC2ContainerRegistry_V20150921.DescribeRegistry"},
		{"DescribeRepositories", "AmazonEC2ContainerRegistry_V20150921.DescribeRepositories"},
		{
			"DescribeRepositoryCreationTemplates",
			"AmazonEC2ContainerRegistry_V20150921.DescribeRepositoryCreationTemplates",
		},
		{"GetAccountSetting", "AmazonEC2ContainerRegistry_V20150921.GetAccountSetting"},
		{"GetAuthorizationToken", "AmazonEC2ContainerRegistry_V20150921.GetAuthorizationToken"},
		{"GetDownloadUrlForLayer", "AmazonEC2ContainerRegistry_V20150921.GetDownloadUrlForLayer"},
		{"GetLifecyclePolicy", "AmazonEC2ContainerRegistry_V20150921.GetLifecyclePolicy"},
		{"GetLifecyclePolicyPreview", "AmazonEC2ContainerRegistry_V20150921.GetLifecyclePolicyPreview"},
		{"GetRegistryPolicy", "AmazonEC2ContainerRegistry_V20150921.GetRegistryPolicy"},
		{"GetRegistryScanningConfiguration", "AmazonEC2ContainerRegistry_V20150921.GetRegistryScanningConfiguration"},
		{"GetRepositoryPolicy", "AmazonEC2ContainerRegistry_V20150921.GetRepositoryPolicy"},
		{"GetSigningConfiguration", "AmazonEC2ContainerRegistry_V20150921.GetSigningConfiguration"},
		{"InitiateLayerUpload", "AmazonEC2ContainerRegistry_V20150921.InitiateLayerUpload"},
		{"ListImageReferrers", "AmazonEC2ContainerRegistry_V20150921.ListImageReferrers"},
		{"ListImages", "AmazonEC2ContainerRegistry_V20150921.ListImages"},
		{"ListPullTimeUpdateExclusions", "AmazonEC2ContainerRegistry_V20150921.ListPullTimeUpdateExclusions"},
		{"ListTagsForResource", "AmazonEC2ContainerRegistry_V20150921.ListTagsForResource"},
		{"PutAccountSetting", "AmazonEC2ContainerRegistry_V20150921.PutAccountSetting"},
		{"PutImage", "AmazonEC2ContainerRegistry_V20150921.PutImage"},
		{"PutImageScanningConfiguration", "AmazonEC2ContainerRegistry_V20150921.PutImageScanningConfiguration"},
		{"PutImageTagMutability", "AmazonEC2ContainerRegistry_V20150921.PutImageTagMutability"},
		{"PutLifecyclePolicy", "AmazonEC2ContainerRegistry_V20150921.PutLifecyclePolicy"},
		{"PutRegistryPolicy", "AmazonEC2ContainerRegistry_V20150921.PutRegistryPolicy"},
		{"PutRegistryScanningConfiguration", "AmazonEC2ContainerRegistry_V20150921.PutRegistryScanningConfiguration"},
		{"PutReplicationConfiguration", "AmazonEC2ContainerRegistry_V20150921.PutReplicationConfiguration"},
		{"PutSigningConfiguration", "AmazonEC2ContainerRegistry_V20150921.PutSigningConfiguration"},
		{
			"RegisterPullTimeUpdateExclusion",
			"AmazonEC2ContainerRegistry_V20150921.RegisterPullTimeUpdateExclusion",
		},
		{"SetRepositoryPolicy", "AmazonEC2ContainerRegistry_V20150921.SetRepositoryPolicy"},
		{"StartImageScan", "AmazonEC2ContainerRegistry_V20150921.StartImageScan"},
		{"StartLifecyclePolicyPreview", "AmazonEC2ContainerRegistry_V20150921.StartLifecyclePolicyPreview"},
		{"TagResource", "AmazonEC2ContainerRegistry_V20150921.TagResource"},
		{"UntagResource", "AmazonEC2ContainerRegistry_V20150921.UntagResource"},
		{"UpdateImageStorageClass", "AmazonEC2ContainerRegistry_V20150921.UpdateImageStorageClass"},
		{"UpdatePullThroughCacheRule", "AmazonEC2ContainerRegistry_V20150921.UpdatePullThroughCacheRule"},
		{
			"UpdateRepositoryCreationTemplate",
			"AmazonEC2ContainerRegistry_V20150921.UpdateRepositoryCreationTemplate",
		},
		{"UploadLayerPart", "AmazonEC2ContainerRegistry_V20150921.UploadLayerPart"},
		{"ValidatePullThroughCacheRule", "AmazonEC2ContainerRegistry_V20150921.ValidatePullThroughCacheRule"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real ECR operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler() does
// not fall through to the "UnknownOperationException" sentinel that a
// dispatch-table key mismatch would produce.
//
// errUnknownAction (handler.go) is wire-typed as "UnknownOperationException",
// which is distinct from every other error type classifyError produces
// (ordinary validation errors map to "InvalidParameterException" instead --
// see classifyError's switch), so it cannot collide with a legitimate error
// on this all-empty-body table. It has exactly one production call site: the
// h.ops map miss in dispatch().
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := ecr.NewInMemoryBackend("000000000000", "us-east-1", "")
			h := ecr.NewHandler(backend, nil)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}

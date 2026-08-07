package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CloudFront_StreamingDistributionLifecycle drives the full
// StreamingDistribution lifecycle through the real SDK: create->get->list->update->delete,
// asserting the round-tripped fields (comment, aliases, trusted signers) are not empty and
// match what was submitted.
func TestIntegration_CloudFront_StreamingDistributionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFrontClient(t)
	ctx := t.Context()

	callerRef := "sd-ref-" + uuid.NewString()[:8]
	comment := "test-streaming-dist-" + uuid.NewString()[:8]

	cfg := &cftypes.StreamingDistributionConfig{
		CallerReference: aws.String(callerRef),
		Comment:         aws.String(comment),
		Enabled:         aws.Bool(true),
		S3Origin: &cftypes.S3Origin{
			DomainName:           aws.String("my-bucket.s3.amazonaws.com"),
			OriginAccessIdentity: aws.String(""),
		},
		Aliases: &cftypes.Aliases{
			Quantity: aws.Int32(1),
			Items:    []string{"videos.example.com"},
		},
		TrustedSigners: &cftypes.TrustedSigners{
			Enabled:  aws.Bool(true),
			Quantity: aws.Int32(1),
			Items:    []string{"123456789012"},
		},
		PriceClass: cftypes.PriceClassPriceClassAll,
	}

	createOut, err := client.CreateStreamingDistribution(ctx, &cloudfront.CreateStreamingDistributionInput{
		StreamingDistributionConfig: cfg,
	})
	require.NoError(t, err, "CreateStreamingDistribution should succeed")
	require.NotNil(t, createOut.StreamingDistribution)

	id := aws.ToString(createOut.StreamingDistribution.Id)
	require.NotEmpty(t, id)
	assert.Equal(t, "Deployed", aws.ToString(createOut.StreamingDistribution.Status))
	etag := aws.ToString(createOut.ETag)
	require.NotEmpty(t, etag)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		getOut, gErr := client.GetStreamingDistribution(
			cleanupCtx,
			&cloudfront.GetStreamingDistributionInput{Id: aws.String(id)},
		)
		if gErr != nil {
			return
		}

		disabled := *cfg
		disabled.Enabled = aws.Bool(false)
		updOut, uErr := client.UpdateStreamingDistribution(cleanupCtx, &cloudfront.UpdateStreamingDistributionInput{
			Id:                          aws.String(id),
			StreamingDistributionConfig: &disabled,
			IfMatch:                     getOut.ETag,
		})
		if uErr != nil {
			return
		}

		_, _ = client.DeleteStreamingDistribution(cleanupCtx, &cloudfront.DeleteStreamingDistributionInput{
			Id:      aws.String(id),
			IfMatch: updOut.ETag,
		})
	})

	// GetStreamingDistribution: real fields must round-trip.
	getOut, err := client.GetStreamingDistribution(ctx, &cloudfront.GetStreamingDistributionInput{Id: aws.String(id)})
	require.NoError(t, err, "GetStreamingDistribution should succeed")
	require.NotNil(t, getOut.StreamingDistribution)
	require.NotNil(t, getOut.StreamingDistribution.StreamingDistributionConfig)
	assert.Equal(t, comment, aws.ToString(getOut.StreamingDistribution.StreamingDistributionConfig.Comment))
	require.NotNil(t, getOut.StreamingDistribution.StreamingDistributionConfig.Aliases)
	assert.Contains(t, getOut.StreamingDistribution.StreamingDistributionConfig.Aliases.Items, "videos.example.com")
	require.NotNil(t, getOut.StreamingDistribution.StreamingDistributionConfig.TrustedSigners)
	assert.Contains(t, getOut.StreamingDistribution.StreamingDistributionConfig.TrustedSigners.Items, "123456789012")
	assert.NotEmpty(t, aws.ToString(getOut.StreamingDistribution.DomainName))

	// ListStreamingDistributions: created distribution must appear with real fields.
	listOut, err := client.ListStreamingDistributions(ctx, &cloudfront.ListStreamingDistributionsInput{})
	require.NoError(t, err, "ListStreamingDistributions should succeed")
	require.NotNil(t, listOut.StreamingDistributionList)

	var found *cftypes.StreamingDistributionSummary
	for i := range listOut.StreamingDistributionList.Items {
		if aws.ToString(listOut.StreamingDistributionList.Items[i].Id) == id {
			found = &listOut.StreamingDistributionList.Items[i]

			break
		}
	}
	require.NotNil(t, found, "created streaming distribution should appear in list")
	assert.Equal(t, comment, aws.ToString(found.Comment))
	assert.NotEmpty(t, aws.ToString(found.DomainName))

	// UpdateStreamingDistribution: comment change must round-trip and ETag must rotate.
	updatedComment := "updated-" + comment
	updateCfg := *cfg
	updateCfg.Comment = aws.String(updatedComment)

	updateOut, err := client.UpdateStreamingDistribution(ctx, &cloudfront.UpdateStreamingDistributionInput{
		Id:                          aws.String(id),
		StreamingDistributionConfig: &updateCfg,
		IfMatch:                     aws.String(etag),
	})
	require.NoError(t, err, "UpdateStreamingDistribution should succeed")
	require.NotNil(t, updateOut.StreamingDistribution)
	assert.Equal(t, updatedComment, aws.ToString(updateOut.StreamingDistribution.StreamingDistributionConfig.Comment))
	newEtag := aws.ToString(updateOut.ETag)
	assert.NotEmpty(t, newEtag)
	assert.NotEqual(t, etag, newEtag)

	// Disable before delete (AWS requires streaming distributions be disabled to delete).
	disabledCfg := updateCfg
	disabledCfg.Enabled = aws.Bool(false)
	disableOut, err := client.UpdateStreamingDistribution(ctx, &cloudfront.UpdateStreamingDistributionInput{
		Id:                          aws.String(id),
		StreamingDistributionConfig: &disabledCfg,
		IfMatch:                     aws.String(newEtag),
	})
	require.NoError(t, err, "disabling before delete should succeed")

	// DeleteStreamingDistribution.
	_, err = client.DeleteStreamingDistribution(ctx, &cloudfront.DeleteStreamingDistributionInput{
		Id:      aws.String(id),
		IfMatch: disableOut.ETag,
	})
	require.NoError(t, err, "DeleteStreamingDistribution should succeed")

	_, err = client.GetStreamingDistribution(ctx, &cloudfront.GetStreamingDistributionInput{Id: aws.String(id)})
	require.Error(t, err, "streaming distribution should be gone after delete")
}

// TestIntegration_CloudFront_TrustStoreLifecycle drives the full TrustStore lifecycle through
// the real SDK: create->get->list->delete, asserting the submitted Name/Comment round-trip.
func TestIntegration_CloudFront_TrustStoreLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFrontClient(t)
	ctx := t.Context()

	name := "test-truststore-" + uuid.NewString()[:8]

	createOut, err := client.CreateTrustStore(ctx, &cloudfront.CreateTrustStoreInput{
		Name: aws.String(name),
		CaCertificatesBundleSource: &cftypes.CaCertificatesBundleSourceMemberCaCertificatesBundleS3Location{
			Value: cftypes.CaCertificatesBundleS3Location{
				Bucket: aws.String("my-bundle-bucket"),
				Key:    aws.String("ca-bundle.pem"),
				Region: aws.String("us-east-1"),
			},
		},
	})
	require.NoError(t, err, "CreateTrustStore should succeed")
	require.NotNil(t, createOut.TrustStore)

	id := aws.ToString(createOut.TrustStore.Id)
	require.NotEmpty(t, id)
	assert.Equal(
		t,
		name,
		aws.ToString(createOut.TrustStore.Name),
		"trust store Name should round-trip from the request",
	)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		getOut, gErr := client.GetTrustStore(cleanupCtx, &cloudfront.GetTrustStoreInput{Identifier: aws.String(id)})
		if gErr != nil {
			return
		}

		_, _ = client.DeleteTrustStore(cleanupCtx, &cloudfront.DeleteTrustStoreInput{
			Id:      aws.String(id),
			IfMatch: getOut.ETag,
		})
	})

	getOut, err := client.GetTrustStore(ctx, &cloudfront.GetTrustStoreInput{Identifier: aws.String(id)})
	require.NoError(t, err, "GetTrustStore should succeed")
	require.NotNil(t, getOut.TrustStore)
	assert.Equal(t, name, aws.ToString(getOut.TrustStore.Name))

	listOut, err := client.ListTrustStores(ctx, &cloudfront.ListTrustStoresInput{})
	require.NoError(t, err, "ListTrustStores should succeed")

	found := false
	for _, ts := range listOut.TrustStoreList {
		if aws.ToString(ts.Id) == id {
			found = true

			assert.Equal(t, name, aws.ToString(ts.Name))
		}
	}
	assert.True(t, found, "created trust store should appear in list")

	_, err = client.DeleteTrustStore(ctx, &cloudfront.DeleteTrustStoreInput{
		Id:      aws.String(id),
		IfMatch: getOut.ETag,
	})
	require.NoError(t, err, "DeleteTrustStore should succeed")

	_, err = client.GetTrustStore(ctx, &cloudfront.GetTrustStoreInput{Identifier: aws.String(id)})
	require.Error(t, err, "trust store should be gone after delete")
}

// TestIntegration_CloudFront_DistributionTenantLifecycle drives the full DistributionTenant
// lifecycle through the real SDK: create->get->list->delete, associated with a real
// distribution, asserting the submitted domain(s) round-trip.
func TestIntegration_CloudFront_DistributionTenantLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFrontClient(t)
	ctx := t.Context()

	// A distribution tenant is associated with a (multi-tenant capable) distribution; create
	// one first so DistributionId refers to something real.
	distOut, err := client.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{
		DistributionConfig: minimalCFDistributionConfig("tenant-ref-"+uuid.NewString()[:8], "tenant-parent-dist"),
	})
	require.NoError(t, err, "CreateDistribution (parent) should succeed")
	distID := aws.ToString(distOut.Distribution.Id)
	require.NotEmpty(t, distID)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		getOut, gErr := client.GetDistribution(cleanupCtx, &cloudfront.GetDistributionInput{Id: aws.String(distID)})
		if gErr != nil {
			return
		}
		_, _ = client.DeleteDistribution(cleanupCtx, &cloudfront.DeleteDistributionInput{
			Id:      aws.String(distID),
			IfMatch: getOut.ETag,
		})
	})

	tenantName := "test-tenant-" + uuid.NewString()[:8]
	domain := uuid.NewString()[:8] + ".tenant.example.com"

	createOut, err := client.CreateDistributionTenant(ctx, &cloudfront.CreateDistributionTenantInput{
		DistributionId: aws.String(distID),
		Name:           aws.String(tenantName),
		Domains: []cftypes.DomainItem{
			{Domain: aws.String(domain)},
		},
	})
	require.NoError(t, err, "CreateDistributionTenant should succeed")
	require.NotNil(t, createOut.DistributionTenant)

	tenantID := aws.ToString(createOut.DistributionTenant.Id)
	require.NotEmpty(t, tenantID)
	require.NotEmpty(
		t, createOut.DistributionTenant.Domains,
		"created tenant should have at least one domain (the one submitted)",
	)
	assert.Equal(t, domain, aws.ToString(createOut.DistributionTenant.Domains[0].Domain))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		getOut, gErr := client.GetDistributionTenant(
			cleanupCtx, &cloudfront.GetDistributionTenantInput{Identifier: aws.String(tenantID)},
		)
		if gErr != nil {
			return
		}
		_, _ = client.DeleteDistributionTenant(cleanupCtx, &cloudfront.DeleteDistributionTenantInput{
			Id:      aws.String(tenantID),
			IfMatch: getOut.ETag,
		})
	})

	getOut, err := client.GetDistributionTenant(
		ctx, &cloudfront.GetDistributionTenantInput{Identifier: aws.String(tenantID)},
	)
	require.NoError(t, err, "GetDistributionTenant should succeed")
	require.NotNil(t, getOut.DistributionTenant)
	require.NotEmpty(t, getOut.DistributionTenant.Domains)
	assert.Equal(t, domain, aws.ToString(getOut.DistributionTenant.Domains[0].Domain))
	assert.Equal(t, distID, aws.ToString(getOut.DistributionTenant.DistributionId))

	listOut, err := client.ListDistributionTenants(ctx, &cloudfront.ListDistributionTenantsInput{})
	require.NoError(t, err, "ListDistributionTenants should succeed")

	found := false
	for _, tn := range listOut.DistributionTenantList {
		if aws.ToString(tn.Id) == tenantID {
			found = true
		}
	}
	assert.True(t, found, "created distribution tenant should appear in list")

	_, err = client.DeleteDistributionTenant(ctx, &cloudfront.DeleteDistributionTenantInput{
		Id:      aws.String(tenantID),
		IfMatch: getOut.ETag,
	})
	require.NoError(t, err, "DeleteDistributionTenant should succeed")

	_, err = client.GetDistributionTenant(ctx, &cloudfront.GetDistributionTenantInput{Identifier: aws.String(tenantID)})
	require.Error(t, err, "distribution tenant should be gone after delete")
}

// TestIntegration_CloudFront_ConnectionGroupAndFunctionLifecycle drives ConnectionGroup
// create->get->delete and ConnectionFunction create->get->publish->delete through the real SDK.
func TestIntegration_CloudFront_ConnectionGroupAndFunctionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFrontClient(t)
	ctx := t.Context()

	// --- ConnectionGroup ---
	cgName := "test-cg-" + uuid.NewString()[:8]

	cgCreateOut, err := client.CreateConnectionGroup(ctx, &cloudfront.CreateConnectionGroupInput{
		Name:    aws.String(cgName),
		Enabled: aws.Bool(true),
	})
	require.NoError(t, err, "CreateConnectionGroup should succeed")
	require.NotNil(t, cgCreateOut.ConnectionGroup)

	cgID := aws.ToString(cgCreateOut.ConnectionGroup.Id)
	require.NotEmpty(t, cgID)
	assert.Equal(t, cgName, aws.ToString(cgCreateOut.ConnectionGroup.Name))
	assert.NotEmpty(t, aws.ToString(cgCreateOut.ConnectionGroup.RoutingEndpoint), "RoutingEndpoint should be generated")
	cgEtag := aws.ToString(cgCreateOut.ETag)
	require.NotEmpty(t, cgEtag)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		getOut, gErr := client.GetConnectionGroup(
			cleanupCtx,
			&cloudfront.GetConnectionGroupInput{Identifier: aws.String(cgID)},
		)
		if gErr != nil {
			return
		}
		_, _ = client.DeleteConnectionGroup(cleanupCtx, &cloudfront.DeleteConnectionGroupInput{
			Id:      aws.String(cgID),
			IfMatch: getOut.ETag,
		})
	})

	cgGetOut, err := client.GetConnectionGroup(ctx, &cloudfront.GetConnectionGroupInput{Identifier: aws.String(cgID)})
	require.NoError(t, err, "GetConnectionGroup should succeed")
	require.NotNil(t, cgGetOut.ConnectionGroup)
	assert.Equal(t, cgName, aws.ToString(cgGetOut.ConnectionGroup.Name))

	// --- ConnectionFunction ---
	cfnName := "test-cfn-" + uuid.NewString()[:8]

	cfnCreateOut, err := client.CreateConnectionFunction(ctx, &cloudfront.CreateConnectionFunctionInput{
		Name:                   aws.String(cfnName),
		ConnectionFunctionCode: []byte("function handler(event) { return event; }"),
		ConnectionFunctionConfig: &cftypes.FunctionConfig{
			Comment: aws.String("integration test connection function"),
			Runtime: cftypes.FunctionRuntimeCloudfrontJs20,
		},
	})
	require.NoError(t, err, "CreateConnectionFunction should succeed")
	require.NotNil(t, cfnCreateOut.ConnectionFunctionSummary)

	cfnID := aws.ToString(cfnCreateOut.ConnectionFunctionSummary.Id)
	require.NotEmpty(t, cfnID)
	assert.Equal(t, cfnName, aws.ToString(cfnCreateOut.ConnectionFunctionSummary.Name))
	assert.Equal(t, cftypes.FunctionStageDevelopment, cfnCreateOut.ConnectionFunctionSummary.Stage)
	cfnEtag := aws.ToString(cfnCreateOut.ETag)
	require.NotEmpty(t, cfnEtag)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		descOut, dErr := client.DescribeConnectionFunction(
			cleanupCtx, &cloudfront.DescribeConnectionFunctionInput{Identifier: aws.String(cfnID)},
		)
		if dErr != nil {
			return
		}
		_, _ = client.DeleteConnectionFunction(cleanupCtx, &cloudfront.DeleteConnectionFunctionInput{
			Id:      aws.String(cfnID),
			IfMatch: descOut.ETag,
		})
	})

	cfnGetOut, err := client.GetConnectionFunction(
		ctx,
		&cloudfront.GetConnectionFunctionInput{Identifier: aws.String(cfnID)},
	)
	require.NoError(t, err, "GetConnectionFunction should succeed")
	assert.Equal(t, "function handler(event) { return event; }", string(cfnGetOut.ConnectionFunctionCode))

	// Publish: DEVELOPMENT -> LIVE.
	publishOut, err := client.PublishConnectionFunction(ctx, &cloudfront.PublishConnectionFunctionInput{
		Id:      aws.String(cfnID),
		IfMatch: aws.String(cfnEtag),
	})
	require.NoError(t, err, "PublishConnectionFunction should succeed")
	require.NotNil(t, publishOut.ConnectionFunctionSummary)
	assert.Equal(t, cftypes.FunctionStageLive, publishOut.ConnectionFunctionSummary.Stage)

	// Delete ConnectionFunction (need current ETag after publish).
	descOut, err := client.DescribeConnectionFunction(
		ctx, &cloudfront.DescribeConnectionFunctionInput{Identifier: aws.String(cfnID)},
	)
	require.NoError(t, err, "DescribeConnectionFunction should succeed")

	_, err = client.DeleteConnectionFunction(ctx, &cloudfront.DeleteConnectionFunctionInput{
		Id:      aws.String(cfnID),
		IfMatch: descOut.ETag,
	})
	require.NoError(t, err, "DeleteConnectionFunction should succeed")

	_, err = client.GetConnectionFunction(ctx, &cloudfront.GetConnectionFunctionInput{Identifier: aws.String(cfnID)})
	require.Error(t, err, "connection function should be gone after delete")

	// Delete ConnectionGroup.
	_, err = client.DeleteConnectionGroup(ctx, &cloudfront.DeleteConnectionGroupInput{
		Id:      aws.String(cgID),
		IfMatch: aws.String(cgEtag),
	})
	require.NoError(t, err, "DeleteConnectionGroup should succeed")

	_, err = client.GetConnectionGroup(ctx, &cloudfront.GetConnectionGroupInput{Identifier: aws.String(cgID)})
	require.Error(t, err, "connection group should be gone after delete")
}

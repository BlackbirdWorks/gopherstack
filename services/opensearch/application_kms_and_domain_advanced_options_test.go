package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateApplication_KmsKeyArn_RealClient covers CreateApplicationInput's
// KmsKeyArn (opensearch@v1.75.4 api_op_CreateApplication.go), which was
// parsed nowhere -- a real client's requested customer-managed KMS key was
// silently dropped. CreateApplicationOutput/GetApplicationOutput both echo
// it back.
func TestCreateApplication_KmsKeyArn_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)

	created, err := client.CreateApplication(t.Context(), &opensearchsdk.CreateApplicationInput{
		Name:      aws.String("kms-app"),
		KmsKeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/test-key"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/test-key", aws.ToString(created.KmsKeyArn),
		"CreateApplication must apply and echo KmsKeyArn, not silently drop it")

	got, err := client.GetApplication(t.Context(), &opensearchsdk.GetApplicationInput{Id: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/test-key", aws.ToString(got.KmsKeyArn),
		"GetApplication must echo the KmsKeyArn applied at CreateApplication")
}

// TestDomain_AdvancedOptions_RealClient covers a reqfielddiff tier-1
// finding: CreateDomainInput.AdvancedOptions and UpdateDomainConfigInput.
// AdvancedOptions (both real, opensearch@v1.75.4 api_op_CreateDomain.go /
// api_op_UpdateDomainConfig.go) were parsed nowhere -- DescribeDomainConfig
// always echoed a hardcoded empty map regardless of what a real client set.
func TestDomain_AdvancedOptions_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)
	ctx := t.Context()

	created, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
		DomainName:      aws.String("adv-opts-dom"),
		AdvancedOptions: map[string]string{"rest.action.multi.allow_explicit_index": "true"},
	})
	require.NoError(t, err)
	assert.Equal(t, "true", created.DomainStatus.AdvancedOptions["rest.action.multi.allow_explicit_index"],
		"CreateDomain must apply and echo AdvancedOptions, not silently drop it")

	_, err = client.UpdateDomainConfig(ctx, &opensearchsdk.UpdateDomainConfigInput{
		DomainName:      aws.String("adv-opts-dom"),
		AdvancedOptions: map[string]string{"indices.fielddata.cache.size": "40"},
	})
	require.NoError(t, err)

	cfg, err := client.DescribeDomainConfig(ctx, &opensearchsdk.DescribeDomainConfigInput{
		DomainName: aws.String("adv-opts-dom"),
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.DomainConfig.AdvancedOptions)
	assert.Equal(t, "40", cfg.DomainConfig.AdvancedOptions.Options["indices.fielddata.cache.size"],
		"UpdateDomainConfig must apply AdvancedOptions, not silently drop it")
}

// TestDirectQueryDataSource_DataSourceAccessPolicy_RealClient covers a
// reqfielddiff tier-1 finding: UpdateDirectQueryDataSourceInput.
// DataSourceAccessPolicy (opensearch@v1.75.4
// api_op_UpdateDirectQueryDataSource.go) was parsed nowhere.
// GetDirectQueryDataSourceOutput echoes it back (unlike the plain
// ListDirectQueryDataSources item type, which has no such member).
func TestDirectQueryDataSource_DataSourceAccessPolicy_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)
	ctx := t.Context()

	dqType := &types.DirectQueryDataSourceTypeMemberCloudWatchLog{
		Value: types.CloudWatchDirectQueryDataSource{
			RoleArn: aws.String("arn:aws:iam::000000000000:role/cwl"),
		},
	}

	_, err := client.AddDirectQueryDataSource(ctx, &opensearchsdk.AddDirectQueryDataSourceInput{
		DataSourceName:         aws.String("dq-policy"),
		DataSourceType:         dqType,
		DataSourceAccessPolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	got, err := client.GetDirectQueryDataSource(ctx, &opensearchsdk.GetDirectQueryDataSourceInput{
		DataSourceName: aws.String("dq-policy"),
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, aws.ToString(got.DataSourceAccessPolicy),
		"AddDirectQueryDataSource must apply DataSourceAccessPolicy, not silently drop it")

	_, err = client.UpdateDirectQueryDataSource(ctx, &opensearchsdk.UpdateDirectQueryDataSourceInput{
		DataSourceName:         aws.String("dq-policy"),
		DataSourceType:         dqType,
		DataSourceAccessPolicy: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`),
	})
	require.NoError(t, err)

	got, err = client.GetDirectQueryDataSource(ctx, &opensearchsdk.GetDirectQueryDataSourceInput{
		DataSourceName: aws.String("dq-policy"),
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`,
		aws.ToString(got.DataSourceAccessPolicy),
		"UpdateDirectQueryDataSource must apply DataSourceAccessPolicy, not silently drop it")
}

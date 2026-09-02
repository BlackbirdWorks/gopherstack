package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestListMigrations_UnknownApplication_ValidationException proves
// ListMigrations reports an unknown ApplicationId as a real typed
// ValidationException, not ResourceNotFoundException. opensearch@v1.75.4
// deserializers.go's awsRestjson1_deserializeOpErrorListMigrations switch
// models AccessDeniedException/DisabledOperationException/InternalException/
// ValidationException only -- no ResourceNotFoundException case exists,
// unlike its GetMigration/StartMigration siblings which do model it.
func TestListMigrations_UnknownApplication_ValidationException(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	_, err := client.ListMigrations(t.Context(), &opensearchsdk.ListMigrationsInput{
		ApplicationId: aws.String("no-such-app"),
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAsf(t, err, &ve, "expected a real ValidationException from the SDK deserializer, got %v", err)
}

// TestAddDataSource_DuplicateName_ValidationException proves AddDataSource
// reports a duplicate data source name as ValidationException, not
// ResourceAlreadyExistsException. opensearch@v1.75.4 deserializers.go's
// awsRestjson1_deserializeOpErrorAddDataSource switch has no
// ResourceAlreadyExistsException case.
func TestAddDataSource_DuplicateName_ValidationException(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("dup-ds-domain"),
	})
	require.NoError(t, err)

	dsType := &types.DataSourceTypeMemberS3GlueDataCatalog{
		Value: types.S3GlueDataCatalog{RoleArn: aws.String("arn:aws:iam::123456789012:role/glue")},
	}

	_, err = client.AddDataSource(ctx, &opensearchsdk.AddDataSourceInput{
		DomainName:     aws.String("dup-ds-domain"),
		Name:           aws.String("mysource"),
		DataSourceType: dsType,
	})
	require.NoError(t, err)

	_, err = client.AddDataSource(ctx, &opensearchsdk.AddDataSourceInput{
		DomainName:     aws.String("dup-ds-domain"),
		Name:           aws.String("mysource"),
		DataSourceType: dsType,
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAsf(t, err, &ve, "expected a real ValidationException from the SDK deserializer, got %v", err)
}

// TestAddDirectQueryDataSource_DuplicateName_ValidationException is
// AddDataSource's sibling for the direct-query-data-source family: same
// unmodeled ResourceAlreadyExistsException bug, confirmed independently
// against awsRestjson1_deserializeOpErrorAddDirectQueryDataSource.
func TestAddDirectQueryDataSource_DuplicateName_ValidationException(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	dsType := &types.DirectQueryDataSourceTypeMemberCloudWatchLog{
		Value: types.CloudWatchDirectQueryDataSource{
			RoleArn: aws.String("arn:aws:iam::123456789012:role/cwl"),
		},
	}

	_, err := client.AddDirectQueryDataSource(ctx, &opensearchsdk.AddDirectQueryDataSourceInput{
		DataSourceName: aws.String("dup-direct-source"),
		DataSourceType: dsType,
	})
	require.NoError(t, err)

	_, err = client.AddDirectQueryDataSource(ctx, &opensearchsdk.AddDirectQueryDataSourceInput{
		DataSourceName: aws.String("dup-direct-source"),
		DataSourceType: dsType,
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAsf(t, err, &ve, "expected a real ValidationException from the SDK deserializer, got %v", err)
}

// TestCreateApplication_DuplicateName_ConflictException proves
// CreateApplication reports a duplicate application name as ConflictException
// -- the code its own deserializer actually models -- not
// ResourceAlreadyExistsException, which CreateApplication's switch has no
// case for at all (opensearch@v1.75.4 deserializers.go).
func TestCreateApplication_DuplicateName_ConflictException(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &opensearchsdk.CreateApplicationInput{
		Name: aws.String("dup-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateApplication(ctx, &opensearchsdk.CreateApplicationInput{
		Name: aws.String("dup-app"),
	})
	require.Error(t, err)

	var ce *types.ConflictException
	require.ErrorAsf(t, err, &ce, "expected a real ConflictException from the SDK deserializer, got %v", err)
}

// TestAddTags_UnknownARN_ValidationException proves AddTags reports an
// unrecognized resource ARN as ValidationException, not
// ResourceNotFoundException -- opensearch@v1.75.4 deserializers.go's
// awsRestjson1_deserializeOpErrorAddTags switch has no
// ResourceNotFoundException case, unlike ListTags.
func TestAddTags_UnknownARN_ValidationException(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	_, err := client.AddTags(t.Context(), &opensearchsdk.AddTagsInput{
		ARN:     aws.String("arn:aws:es:us-east-1:123456789012:domain/no-such-domain"),
		TagList: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAsf(t, err, &ve, "expected a real ValidationException from the SDK deserializer, got %v", err)
}

// TestRemoveTags_UnknownARN_ValidationException is AddTags' sibling for
// RemoveTags -- same unmodeled-ResourceNotFoundException bug, confirmed
// independently against awsRestjson1_deserializeOpErrorRemoveTags.
func TestRemoveTags_UnknownARN_ValidationException(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	_, err := client.RemoveTags(t.Context(), &opensearchsdk.RemoveTagsInput{
		ARN:     aws.String("arn:aws:es:us-east-1:123456789012:domain/no-such-domain"),
		TagKeys: []string{"k"},
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAsf(t, err, &ve, "expected a real ValidationException from the SDK deserializer, got %v", err)
}

// TestGetUpgradeHistory_UnknownDomain_ResourceNotFoundException proves
// GetUpgradeHistory raises a real typed ResourceNotFoundException for a
// nonexistent domain instead of silently returning an empty, fabricated
// success response. opensearch@v1.75.4 deserializers.go's
// awsRestjson1_deserializeOpErrorGetUpgradeHistory switch models
// ResourceNotFoundException.
func TestGetUpgradeHistory_UnknownDomain_ResourceNotFoundException(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	_, err := client.GetUpgradeHistory(t.Context(), &opensearchsdk.GetUpgradeHistoryInput{
		DomainName: aws.String("no-such-domain"),
	})
	require.Error(t, err)

	var nf *types.ResourceNotFoundException
	require.ErrorAsf(t, err, &nf, "expected a real ResourceNotFoundException from the SDK deserializer, got %v", err)
}

// TestGetUpgradeStatus_UnknownDomain_ResourceNotFoundException is
// GetUpgradeHistory's sibling for GetUpgradeStatus -- same swallowed-error
// bug, confirmed independently against
// awsRestjson1_deserializeOpErrorGetUpgradeStatus.
func TestGetUpgradeStatus_UnknownDomain_ResourceNotFoundException(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	_, err := client.GetUpgradeStatus(t.Context(), &opensearchsdk.GetUpgradeStatusInput{
		DomainName: aws.String("no-such-domain"),
	})
	require.Error(t, err)

	var nf *types.ResourceNotFoundException
	require.ErrorAsf(t, err, &nf, "expected a real ResourceNotFoundException from the SDK deserializer, got %v", err)
}

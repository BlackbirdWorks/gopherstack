package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestDescribeActivations_FiltersAndPagination proves DescribeActivations'
// real, optional Filters/MaxResults/NextToken members (api_op_DescribeActivations.go)
// -- previously discarded by a literal struct{} input -- now actually
// change what the real SDK client sees back.
func TestDescribeActivations_FiltersAndPagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	roles := []string{"role-a", "role-b", "role-c"}
	for _, role := range roles {
		_, err := client.CreateActivation(t.Context(), &ssmsdk.CreateActivationInput{
			IamRole: aws.String(role),
		})
		require.NoError(t, err)
	}

	t.Run("filter by IamRole", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeActivations(t.Context(), &ssmsdk.DescribeActivationsInput{
			Filters: []ssmtypes.DescribeActivationsFilter{
				{FilterKey: ssmtypes.DescribeActivationsFilterKeysIamRole, FilterValues: []string{"role-b"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.ActivationList, 1)
		require.Equal(t, "role-b", *out.ActivationList[0].IamRole)
	})

	t.Run("paginates with MaxResults and NextToken", func(t *testing.T) {
		t.Parallel()

		first, err := client.DescribeActivations(t.Context(), &ssmsdk.DescribeActivationsInput{
			MaxResults: aws.Int32(1),
		})
		require.NoError(t, err)
		require.Len(t, first.ActivationList, 1)
		require.NotNil(t, first.NextToken)

		second, err := client.DescribeActivations(t.Context(), &ssmsdk.DescribeActivationsInput{
			MaxResults: aws.Int32(10),
			NextToken:  first.NextToken,
		})
		require.NoError(t, err)
		require.Len(t, second.ActivationList, len(roles)-1)
	})
}

// TestListResourceDataSync_FilterAndPagination proves ListResourceDataSync's
// real, optional SyncType/MaxResults/NextToken members
// (api_op_ListResourceDataSync.go) now actually affect the response.
func TestListResourceDataSync_FilterAndPagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	syncs := []struct {
		name     string
		syncType string
	}{
		{"sync-source-1", "SyncFromSource"},
		{"sync-source-2", "SyncFromSource"},
		{"sync-dest-1", "SyncToDestination"},
	}

	for _, s := range syncs {
		in := &ssmsdk.CreateResourceDataSyncInput{
			SyncName: aws.String(s.name),
			SyncType: aws.String(s.syncType),
		}
		if s.syncType == "SyncFromSource" {
			in.SyncSource = &ssmtypes.ResourceDataSyncSource{
				SourceType:    aws.String("SingleAccountMultiRegions"),
				SourceRegions: []string{"us-east-1"},
			}
		} else {
			in.S3Destination = &ssmtypes.ResourceDataSyncS3Destination{
				BucketName: aws.String("b"),
				Region:     aws.String("us-east-1"),
				SyncFormat: ssmtypes.ResourceDataSyncS3FormatJsonSerde,
			}
		}
		_, err := client.CreateResourceDataSync(t.Context(), in)
		require.NoError(t, err)
	}

	t.Run("filter by SyncType", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListResourceDataSync(t.Context(), &ssmsdk.ListResourceDataSyncInput{
			SyncType: aws.String("SyncToDestination"),
		})
		require.NoError(t, err)
		require.Len(t, out.ResourceDataSyncItems, 1)
		require.Equal(t, "sync-dest-1", *out.ResourceDataSyncItems[0].SyncName)
	})

	t.Run("paginates with MaxResults and NextToken", func(t *testing.T) {
		t.Parallel()

		first, err := client.ListResourceDataSync(t.Context(), &ssmsdk.ListResourceDataSyncInput{
			MaxResults: aws.Int32(1),
		})
		require.NoError(t, err)
		require.Len(t, first.ResourceDataSyncItems, 1)
		require.NotNil(t, first.NextToken)

		second, err := client.ListResourceDataSync(t.Context(), &ssmsdk.ListResourceDataSyncInput{
			MaxResults: aws.Int32(10),
			NextToken:  first.NextToken,
		})
		require.NoError(t, err)
		require.Len(t, second.ResourceDataSyncItems, len(syncs)-1)
	})
}

// TestDescribeInstanceInformation_FilterAndPagination proves
// DescribeInstanceInformation's real, optional Filters/MaxResults/NextToken
// members (api_op_DescribeInstanceInformation.go) now actually affect the
// response.
func TestDescribeInstanceInformation_FilterAndPagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	activationIDs := make([]string, 0, 3)

	for range 3 {
		out, err := client.CreateActivation(t.Context(), &ssmsdk.CreateActivationInput{
			IamRole: aws.String("role"),
		})
		require.NoError(t, err)
		activationIDs = append(activationIDs, *out.ActivationId)
	}

	t.Run("filter by InstanceIds", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeInstanceInformation(t.Context(), &ssmsdk.DescribeInstanceInformationInput{
			Filters: []ssmtypes.InstanceInformationStringFilter{
				{Key: aws.String("InstanceIds"), Values: []string{activationIDs[1]}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.InstanceInformationList, 1)
		require.Equal(t, activationIDs[1], *out.InstanceInformationList[0].InstanceId)
	})

	t.Run("paginates with MaxResults and NextToken", func(t *testing.T) {
		t.Parallel()

		first, err := client.DescribeInstanceInformation(t.Context(), &ssmsdk.DescribeInstanceInformationInput{
			MaxResults: aws.Int32(1),
		})
		require.NoError(t, err)
		require.Len(t, first.InstanceInformationList, 1)
		require.NotNil(t, first.NextToken)

		second, err := client.DescribeInstanceInformation(t.Context(), &ssmsdk.DescribeInstanceInformationInput{
			MaxResults: aws.Int32(10),
			NextToken:  first.NextToken,
		})
		require.NoError(t, err)
		require.Len(t, second.InstanceInformationList, len(activationIDs)-1)
	})
}

// TestListAssociations_FilterAndPagination proves ListAssociations' real,
// optional AssociationFilterList/MaxResults/NextToken members
// (api_op_ListAssociations.go) now actually affect the response.
func TestListAssociations_FilterAndPagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	instanceIDs := []string{"i-aaa", "i-bbb", "i-ccc"}
	for _, id := range instanceIDs {
		_, err := client.CreateAssociation(t.Context(), &ssmsdk.CreateAssociationInput{
			Name:       aws.String("AWS-RunShellScript"),
			InstanceId: aws.String(id),
		})
		require.NoError(t, err)
	}

	t.Run("filter by InstanceId", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListAssociations(t.Context(), &ssmsdk.ListAssociationsInput{
			AssociationFilterList: []ssmtypes.AssociationFilter{
				{Key: ssmtypes.AssociationFilterKeyInstanceId, Value: aws.String("i-bbb")},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.Associations, 1)
		require.Equal(t, "i-bbb", *out.Associations[0].InstanceId)
	})

	t.Run("paginates with MaxResults and NextToken", func(t *testing.T) {
		t.Parallel()

		first, err := client.ListAssociations(t.Context(), &ssmsdk.ListAssociationsInput{
			MaxResults: aws.Int32(1),
		})
		require.NoError(t, err)
		require.Len(t, first.Associations, 1)
		require.NotNil(t, first.NextToken)

		second, err := client.ListAssociations(t.Context(), &ssmsdk.ListAssociationsInput{
			MaxResults: aws.Int32(10),
			NextToken:  first.NextToken,
		})
		require.NoError(t, err)
		require.Len(t, second.Associations, len(instanceIDs)-1)
	})
}

// TestDescribeAutomationExecutions_FilterAndPagination proves
// DescribeAutomationExecutions' real, optional Filters/MaxResults/NextToken
// members (api_op_DescribeAutomationExecutions.go) now actually affect the
// response.
func TestDescribeAutomationExecutions_FilterAndPagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	docs := []string{"Prefix-One", "Prefix-Two", "Other-Doc"}
	for _, doc := range docs {
		_, err := client.StartAutomationExecution(t.Context(), &ssmsdk.StartAutomationExecutionInput{
			DocumentName: aws.String(doc),
		})
		require.NoError(t, err)
	}

	t.Run("filter by DocumentNamePrefix", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeAutomationExecutions(t.Context(), &ssmsdk.DescribeAutomationExecutionsInput{
			Filters: []ssmtypes.AutomationExecutionFilter{
				{Key: ssmtypes.AutomationExecutionFilterKeyDocumentNamePrefix, Values: []string{"Prefix-"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.AutomationExecutionMetadataList, 2)
	})

	t.Run("paginates with MaxResults and NextToken", func(t *testing.T) {
		t.Parallel()

		first, err := client.DescribeAutomationExecutions(t.Context(), &ssmsdk.DescribeAutomationExecutionsInput{
			MaxResults: aws.Int32(1),
		})
		require.NoError(t, err)
		require.Len(t, first.AutomationExecutionMetadataList, 1)
		require.NotNil(t, first.NextToken)

		second, err := client.DescribeAutomationExecutions(t.Context(), &ssmsdk.DescribeAutomationExecutionsInput{
			MaxResults: aws.Int32(10),
			NextToken:  first.NextToken,
		})
		require.NoError(t, err)
		require.Len(t, second.AutomationExecutionMetadataList, len(docs)-1)
	})
}

// TestListOpsMetadata_FilterAndPagination proves ListOpsMetadata's real,
// optional Filters/MaxResults/NextToken members (api_op_ListOpsMetadata.go)
// now actually affect the response.
func TestListOpsMetadata_FilterAndPagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	resourceIDs := []string{"res-aaa", "res-bbb", "res-ccc"}
	for _, id := range resourceIDs {
		_, err := client.CreateOpsMetadata(t.Context(), &ssmsdk.CreateOpsMetadataInput{
			ResourceId: aws.String(id),
		})
		require.NoError(t, err)
	}

	t.Run("filter by ResourceId", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListOpsMetadata(t.Context(), &ssmsdk.ListOpsMetadataInput{
			Filters: []ssmtypes.OpsMetadataFilter{
				{Key: aws.String("ResourceId"), Values: []string{"res-bbb"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.OpsMetadataList, 1)
		require.Equal(t, "res-bbb", *out.OpsMetadataList[0].ResourceId)
	})

	t.Run("paginates with MaxResults and NextToken", func(t *testing.T) {
		t.Parallel()

		first, err := client.ListOpsMetadata(t.Context(), &ssmsdk.ListOpsMetadataInput{
			MaxResults: aws.Int32(1),
		})
		require.NoError(t, err)
		require.Len(t, first.OpsMetadataList, 1)
		require.NotNil(t, first.NextToken)

		second, err := client.ListOpsMetadata(t.Context(), &ssmsdk.ListOpsMetadataInput{
			MaxResults: aws.Int32(10),
			NextToken:  first.NextToken,
		})
		require.NoError(t, err)
		require.Len(t, second.OpsMetadataList, len(resourceIDs)-1)
	})
}

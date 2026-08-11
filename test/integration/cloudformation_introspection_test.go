package integration_test

import (
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cfnResourceTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"MyBucket": {
			"Type": "AWS::S3::Bucket",
			"Properties": {}
		}
	}
}`

// TestIntegration_CloudFormation_ResourceIntrospection tests DescribeStackResource,
// ListStackResources, and DescribeStackResources.
func TestIntegration_CloudFormation_ResourceIntrospection(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudFormationClient(t)
	ctx := t.Context()

	stackName := "introspect-stack-" + uuid.NewString()[:8]

	// CreateStack with a single bucket resource.
	_, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(cfnResourceTemplate),
	})
	require.NoError(t, err)

	// DescribeStackResource – wait for "MyBucket" to reach CREATE_COMPLETE.
	var descResOut *cloudformationsdk.DescribeStackResourceOutput
	require.Eventually(t, func() bool {
		var descErr error
		descResOut, descErr = client.DescribeStackResource(ctx, &cloudformationsdk.DescribeStackResourceInput{
			StackName:         aws.String(stackName),
			LogicalResourceId: aws.String("MyBucket"),
		})

		return descErr == nil && descResOut.StackResourceDetail != nil &&
			descResOut.StackResourceDetail.ResourceStatus == cftypes.ResourceStatusCreateComplete
	}, 10*time.Second, 50*time.Millisecond)

	require.NotNil(t, descResOut.StackResourceDetail)
	assert.Equal(t, "MyBucket", *descResOut.StackResourceDetail.LogicalResourceId)
	assert.Equal(t, "AWS::S3::Bucket", *descResOut.StackResourceDetail.ResourceType)
	assert.Equal(t, "CREATE_COMPLETE", string(descResOut.StackResourceDetail.ResourceStatus))
	assert.NotEmpty(t, *descResOut.StackResourceDetail.PhysicalResourceId)

	// ListStackResources – paginated list of all resources.
	listResOut, err := client.ListStackResources(ctx, &cloudformationsdk.ListStackResourcesInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listResOut.StackResourceSummaries)
	assert.Equal(t, "MyBucket", *listResOut.StackResourceSummaries[0].LogicalResourceId)
	assert.Equal(t, "AWS::S3::Bucket", *listResOut.StackResourceSummaries[0].ResourceType)

	// DescribeStackResources – describe all resources for a stack.
	descResAllOut, err := client.DescribeStackResources(ctx, &cloudformationsdk.DescribeStackResourcesInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, descResAllOut.StackResources)
	assert.Equal(t, "MyBucket", *descResAllOut.StackResources[0].LogicalResourceId)

	// Cleanup.
	_, err = client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
}

// TestIntegration_CloudFormation_CrossStackExports tests ListExports and ListImports.
func TestIntegration_CloudFormation_CrossStackExports(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudFormationClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	exportStackName := "export-stack-" + suffix
	importStackName := "import-stack-" + suffix
	exportName := "integ-shared-bucket-" + suffix

	exportTmpl := `{"AWSTemplateFormatVersion":"2010-09-09",` +
		`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}},` +
		`"Outputs":{"BucketName":{"Value":{"Ref":"MyBucket"},` +
		`"Export":{"Name":"` + exportName + `"}}}}`

	// Create the exporting stack.
	_, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(exportStackName),
		TemplateBody: aws.String(exportTmpl),
	})
	require.NoError(t, err)

	// ListExports – wait for the export to appear.
	var exportsOut *cloudformationsdk.ListExportsOutput
	var foundExport bool
	require.Eventually(t, func() bool {
		var listErr error
		exportsOut, listErr = client.ListExports(ctx, &cloudformationsdk.ListExportsInput{})
		if listErr != nil {
			return false
		}

		for _, exp := range exportsOut.Exports {
			if aws.ToString(exp.Name) == exportName {
				foundExport = true
			}
		}

		return foundExport
	}, 10*time.Second, 50*time.Millisecond, "expected export %q to be present in ListExports", exportName)

	for _, exp := range exportsOut.Exports {
		if aws.ToString(exp.Name) == exportName {
			assert.NotEmpty(t, aws.ToString(exp.Value))
			assert.NotEmpty(t, aws.ToString(exp.ExportingStackId))
		}
	}

	// Create an importing stack that references the export.
	importTmpl := `{"AWSTemplateFormatVersion":"2010-09-09",` +
		`"Resources":{"MyTopic":{"Type":"AWS::SNS::Topic","Properties":{}}},` +
		`"Outputs":{"ImportedBucket":{"Value":{"Fn::ImportValue":"` + exportName + `"}}}}`

	_, err = client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(importStackName),
		TemplateBody: aws.String(importTmpl),
	})
	require.NoError(t, err)

	// ListImports – wait for the importing stack to appear.
	var importsOut *cloudformationsdk.ListImportsOutput
	require.Eventually(t, func() bool {
		var listErr error
		importsOut, listErr = client.ListImports(ctx, &cloudformationsdk.ListImportsInput{
			ExportName: aws.String(exportName),
		})

		return listErr == nil && slices.Contains(importsOut.Imports, importStackName)
	}, 10*time.Second, 50*time.Millisecond)

	assert.Contains(t, importsOut.Imports, importStackName)

	// Cleanup.
	_, err = client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(importStackName)})
	require.NoError(t, err)

	_, err = client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{StackName: aws.String(exportStackName)})
	require.NoError(t, err)

	// After deletion, wait for the export to disappear.
	require.Eventually(t, func() bool {
		exportsOut2, listErr := client.ListExports(ctx, &cloudformationsdk.ListExportsInput{})
		if listErr != nil {
			return false
		}

		for _, exp := range exportsOut2.Exports {
			if aws.ToString(exp.Name) == exportName {
				return false
			}
		}

		return true
	}, 10*time.Second, 50*time.Millisecond, "export should be removed after stack deletion")
}

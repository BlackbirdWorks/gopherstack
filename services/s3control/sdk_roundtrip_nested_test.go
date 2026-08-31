package s3control_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestSDKRoundTrip_JobManifestOperationReport is a typed round-trip test
// (gopherstack-21my) for CreateJob/DescribeJob/ListJobs, the richest nested
// item shape in this service: Manifest{Location,Spec}, a JobOperation union
// member (LambdaInvoke), and Report. This service stores Manifest/Operation/
// Report as the client's own raw inner XML and echoes it back verbatim
// (handler_jobs.go) rather than re-modeling every union member -- this test
// proves that echo actually round-trips through the real SDK's own
// serializer/deserializer pair, not just that the bytes look plausible.
// Seeds two jobs so ListJobs exercises a real multi-item collection and
// each job's Operation is independently derived (jobOperationName) from its
// own raw XML.
func TestSDKRoundTrip_JobManifestOperationReport(t *testing.T) {
	t.Parallel()

	backend := s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion)
	client := newTestS3ControlClient(t, s3control.NewHandler(backend))
	ctx := t.Context()

	manifest := &types.JobManifest{
		Spec: &types.JobManifestSpec{
			Format: types.JobManifestFormatS3BatchOperationsCsv20180820,
			Fields: []types.JobManifestFieldName{types.JobManifestFieldNameBucket, types.JobManifestFieldNameKey},
		},
		Location: &types.JobManifestLocation{
			ObjectArn:       aws.String("arn:aws:s3:::manifest-bucket/manifest.csv"),
			ObjectVersionId: aws.String("manifest-version-1"),
			ETag:            aws.String("manifest-etag-1"),
		},
	}
	report := &types.JobReport{
		Enabled:             true,
		Bucket:              aws.String("arn:aws:s3:::report-bucket"),
		Format:              types.JobReportFormatReportCsv20180820,
		Prefix:              aws.String("reports/lambda-job"),
		ReportScope:         types.JobReportScopeAllTasks,
		ExpectedBucketOwner: aws.String(createTagsTestAccountID),
	}

	lambdaOut, err := client.CreateJob(ctx, &s3csdk.CreateJobInput{
		AccountId:          aws.String(createTagsTestAccountID),
		ClientRequestToken: aws.String("token-lambda"),
		Description:        aws.String("lambda invoke job"),
		Priority:           aws.Int32(3),
		RoleArn:            aws.String("arn:aws:iam::" + createTagsTestAccountID + ":role/batch-ops"),
		Manifest:           manifest,
		Report:             report,
		Operation: &types.JobOperation{
			LambdaInvoke: &types.LambdaInvokeOperation{
				FunctionArn: aws.String("arn:aws:lambda:us-east-1:" + createTagsTestAccountID + ":function:job-fn"),
			},
		},
	})
	require.NoError(t, err)

	copyOut, err := client.CreateJob(ctx, &s3csdk.CreateJobInput{
		AccountId:          aws.String(createTagsTestAccountID),
		ClientRequestToken: aws.String("token-copy"),
		Description:        aws.String("copy job"),
		Priority:           aws.Int32(7),
		RoleArn:            aws.String("arn:aws:iam::" + createTagsTestAccountID + ":role/batch-ops"),
		Manifest:           manifest,
		Report:             &types.JobReport{Enabled: false},
		Operation: &types.JobOperation{
			S3PutObjectCopy: &types.S3CopyObjectOperation{
				TargetResource: aws.String("arn:aws:s3:::dest-bucket"),
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListJobs(ctx, &s3csdk.ListJobsInput{AccountId: aws.String(createTagsTestAccountID)})
	require.NoError(t, err)
	require.Len(t, listOut.Jobs, 2, "both created jobs must round-trip through ListJobs")

	byID := map[string]types.JobListDescriptor{}
	for _, j := range listOut.Jobs {
		byID[aws.ToString(j.JobId)] = j
	}

	lambdaSummary, ok := byID[aws.ToString(lambdaOut.JobId)]
	require.True(t, ok, "lambda job must appear in ListJobs")
	require.Equal(t, types.OperationNameLambdaInvoke, lambdaSummary.Operation)
	require.Equal(t, int32(3), lambdaSummary.Priority)

	copySummary, ok := byID[aws.ToString(copyOut.JobId)]
	require.True(t, ok, "copy job must appear in ListJobs")
	require.Equal(t, types.OperationNameS3PutObjectCopy, copySummary.Operation)
	require.Equal(t, int32(7), copySummary.Priority)

	descOut, err := client.DescribeJob(ctx, &s3csdk.DescribeJobInput{
		AccountId: aws.String(createTagsTestAccountID),
		JobId:     lambdaOut.JobId,
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Job)
	job := descOut.Job

	require.NotNil(t, job.Manifest)
	require.NotNil(t, job.Manifest.Location)
	require.Equal(t, "arn:aws:s3:::manifest-bucket/manifest.csv", aws.ToString(job.Manifest.Location.ObjectArn))
	require.Equal(t, "manifest-version-1", aws.ToString(job.Manifest.Location.ObjectVersionId))
	require.Equal(t, "manifest-etag-1", aws.ToString(job.Manifest.Location.ETag))
	require.NotNil(t, job.Manifest.Spec)
	require.Equal(t, types.JobManifestFormatS3BatchOperationsCsv20180820, job.Manifest.Spec.Format)
	require.Equal(
		t,
		[]types.JobManifestFieldName{types.JobManifestFieldNameBucket, types.JobManifestFieldNameKey},
		job.Manifest.Spec.Fields,
	)

	require.NotNil(t, job.Report)
	require.True(t, job.Report.Enabled)
	require.Equal(t, "arn:aws:s3:::report-bucket", aws.ToString(job.Report.Bucket))
	require.Equal(t, types.JobReportFormatReportCsv20180820, job.Report.Format)
	require.Equal(t, "reports/lambda-job", aws.ToString(job.Report.Prefix))
	require.Equal(t, types.JobReportScopeAllTasks, job.Report.ReportScope)
	require.Equal(t, createTagsTestAccountID, aws.ToString(job.Report.ExpectedBucketOwner))

	require.NotNil(t, job.Operation)
	require.NotNil(t, job.Operation.LambdaInvoke)
	require.Equal(t,
		"arn:aws:lambda:us-east-1:"+createTagsTestAccountID+":function:job-fn",
		aws.ToString(job.Operation.LambdaInvoke.FunctionArn),
	)

	require.Equal(t, "lambda invoke job", aws.ToString(job.Description))
	require.Equal(t, int32(3), job.Priority)
}

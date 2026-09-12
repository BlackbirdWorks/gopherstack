package s3control_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestTypedSlice7RealClient drives s3control's typed-coverage-blind ops
// (gopherstack-n3zi slice 7) through the real aws-sdk-go-v2 s3control
// client, one subtest per named priority family, asserting decoded values.
func TestTypedSlice7RealClient(t *testing.T) {
	t.Parallel()

	t.Run("buckets_outposts", func(t *testing.T) {
		t.Parallel()

		// Real CreateBucketInput has no AccountId member at all (verified
		// against api_op_CreateBucket.go) -- accountIDFromRequest falls back
		// to "default" when the header is absent, so every follow-up call
		// below must address the bucket under that same fallback account.
		const acct = "default"

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		client := newTestS3ControlClient(t, h)

		createOut, err := client.CreateBucket(t.Context(), &s3csdk.CreateBucketInput{
			Bucket:    aws.String("s7-outpost-bucket"),
			OutpostId: aws.String("op-1234"),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(createOut.BucketArn))

		getOut, err := client.GetBucket(t.Context(), &s3csdk.GetBucketInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s7-outpost-bucket", aws.ToString(getOut.Bucket))

		listOut, err := client.ListRegionalBuckets(t.Context(), &s3csdk.ListRegionalBucketsInput{
			AccountId: aws.String(acct),
		})
		require.NoError(t, err)
		require.Len(t, listOut.RegionalBucketList, 1)
		assert.Equal(t, "s7-outpost-bucket", aws.ToString(listOut.RegionalBucketList[0].Bucket))

		_, err = client.PutBucketVersioning(t.Context(), &s3csdk.PutBucketVersioningInput{
			AccountId:               aws.String(acct),
			Bucket:                  aws.String("s7-outpost-bucket"),
			VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
		})
		require.NoError(t, err)

		verOut, err := client.GetBucketVersioning(t.Context(), &s3csdk.GetBucketVersioningInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
		})
		require.NoError(t, err)
		assert.Equal(t, types.BucketVersioningStatusEnabled, verOut.Status)

		_, err = client.PutBucketTagging(t.Context(), &s3csdk.PutBucketTaggingInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
			Tagging: &types.Tagging{
				TagSet: []types.S3Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
			},
		})
		require.NoError(t, err)

		tagOut, err := client.GetBucketTagging(t.Context(), &s3csdk.GetBucketTaggingInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
		})
		require.NoError(t, err)
		require.Len(t, tagOut.TagSet, 1)
		assert.Equal(t, "prod", aws.ToString(tagOut.TagSet[0].Value))

		_, err = client.DeleteBucketTagging(t.Context(), &s3csdk.DeleteBucketTaggingInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
		})
		require.NoError(t, err)

		_, err = client.PutBucketPolicy(t.Context(), &s3csdk.PutBucketPolicyInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
			Policy:    aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.NoError(t, err)

		polOut, err := client.GetBucketPolicy(t.Context(), &s3csdk.GetBucketPolicyInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
		})
		require.NoError(t, err)
		assert.Contains(t, aws.ToString(polOut.Policy), "2012-10-17")

		_, err = client.DeleteBucketPolicy(t.Context(), &s3csdk.DeleteBucketPolicyInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
		})
		require.NoError(t, err)

		_, err = client.PutBucketLifecycleConfiguration(t.Context(), &s3csdk.PutBucketLifecycleConfigurationInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
			LifecycleConfiguration: &types.LifecycleConfiguration{
				Rules: []types.LifecycleRule{
					{Status: types.ExpirationStatusEnabled, ID: aws.String("rule-1")},
				},
			},
		})
		require.NoError(t, err)

		lcOut, err := client.GetBucketLifecycleConfiguration(
			t.Context(),
			&s3csdk.GetBucketLifecycleConfigurationInput{
				AccountId: aws.String(acct), Bucket: aws.String("s7-outpost-bucket"),
			},
		)
		require.NoError(t, err)
		require.Len(t, lcOut.Rules, 1)
		assert.Equal(t, "rule-1", aws.ToString(lcOut.Rules[0].ID))

		_, err = client.DeleteBucketLifecycleConfiguration(
			t.Context(),
			&s3csdk.DeleteBucketLifecycleConfigurationInput{
				AccountId: aws.String(acct), Bucket: aws.String("s7-outpost-bucket"),
			},
		)
		require.NoError(t, err)

		_, err = client.PutBucketReplication(t.Context(), &s3csdk.PutBucketReplicationInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
			ReplicationConfiguration: &types.ReplicationConfiguration{
				Role: aws.String("arn:aws:iam::123456789012:role/replication"),
				Rules: []types.ReplicationRule{
					{
						Bucket: aws.String(
							"arn:aws:s3-outposts:us-east-1:123456789012:outpost/op-1234/accesspoint/src-ap",
						),
						Status: types.ReplicationRuleStatusEnabled,
						Destination: &types.Destination{
							Bucket: aws.String(
								"arn:aws:s3-outposts:us-east-1:123456789012:outpost/op-1234/accesspoint/dest-ap",
							),
						},
					},
				},
			},
		})
		require.NoError(t, err)

		replOut, err := client.GetBucketReplication(
			t.Context(),
			&s3csdk.GetBucketReplicationInput{AccountId: aws.String(acct), Bucket: aws.String("s7-outpost-bucket")},
		)
		require.NoError(t, err)
		require.NotNil(t, replOut.ReplicationConfiguration)
		require.Len(t, replOut.ReplicationConfiguration.Rules, 1)
		assert.Equal(
			t,
			"arn:aws:iam::123456789012:role/replication",
			aws.ToString(replOut.ReplicationConfiguration.Role),
		)

		_, err = client.DeleteBucketReplication(
			t.Context(),
			&s3csdk.DeleteBucketReplicationInput{AccountId: aws.String(acct), Bucket: aws.String("s7-outpost-bucket")},
		)
		require.NoError(t, err)

		_, err = client.DeleteBucket(t.Context(), &s3csdk.DeleteBucketInput{
			AccountId: aws.String(acct),
			Bucket:    aws.String("s7-outpost-bucket"),
		})
		require.NoError(t, err)
	})

	t.Run("public_access_block", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion))
		client := newTestS3ControlClient(t, h)

		_, err := client.PutPublicAccessBlock(t.Context(), &s3csdk.PutPublicAccessBlockInput{
			AccountId: aws.String(createTagsTestAccountID),
			PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
				BlockPublicAcls: aws.Bool(true),
			},
		})
		require.NoError(t, err)

		getOut, err := client.GetPublicAccessBlock(t.Context(), &s3csdk.GetPublicAccessBlockInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.PublicAccessBlockConfiguration)
		assert.True(t, aws.ToBool(getOut.PublicAccessBlockConfiguration.BlockPublicAcls))

		_, err = client.DeletePublicAccessBlock(t.Context(), &s3csdk.DeletePublicAccessBlockInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
	})

	t.Run("object_lambda", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion))
		client := newTestS3ControlClient(t, h)

		olConfig := &types.ObjectLambdaConfiguration{
			SupportingAccessPoint: aws.String("arn:aws:s3:us-east-1:123456789012:accesspoint/base-ap"),
			TransformationConfigurations: []types.ObjectLambdaTransformationConfiguration{
				{
					Actions: []types.ObjectLambdaTransformationConfigurationAction{
						types.ObjectLambdaTransformationConfigurationActionGetObject,
					},
					ContentTransformation: &types.ObjectLambdaContentTransformationMemberAwsLambda{
						Value: types.AwsLambdaTransformation{
							FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn"),
						},
					},
				},
			},
		}

		_, err := client.CreateAccessPointForObjectLambda(t.Context(), &s3csdk.CreateAccessPointForObjectLambdaInput{
			AccountId:     aws.String(createTagsTestAccountID),
			Name:          aws.String("s7-olap"),
			Configuration: olConfig,
		})
		require.NoError(t, err)

		getOut, err := client.GetAccessPointForObjectLambda(t.Context(), &s3csdk.GetAccessPointForObjectLambdaInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-olap"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s7-olap", aws.ToString(getOut.Name))

		cfgOut, err := client.GetAccessPointConfigurationForObjectLambda(
			t.Context(),
			&s3csdk.GetAccessPointConfigurationForObjectLambdaInput{
				AccountId: aws.String(createTagsTestAccountID),
				Name:      aws.String("s7-olap"),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, cfgOut.Configuration)
		assert.Equal(
			t,
			"arn:aws:s3:us-east-1:123456789012:accesspoint/base-ap",
			aws.ToString(cfgOut.Configuration.SupportingAccessPoint),
		)

		newConfig := &types.ObjectLambdaConfiguration{
			SupportingAccessPoint: aws.String("arn:aws:s3:us-east-1:123456789012:accesspoint/base-ap"),
			TransformationConfigurations: []types.ObjectLambdaTransformationConfiguration{
				{
					Actions: []types.ObjectLambdaTransformationConfigurationAction{
						types.ObjectLambdaTransformationConfigurationActionGetObject,
					},
					ContentTransformation: &types.ObjectLambdaContentTransformationMemberAwsLambda{
						Value: types.AwsLambdaTransformation{
							FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn2"),
						},
					},
				},
			},
		}

		_, err = client.PutAccessPointConfigurationForObjectLambda(
			t.Context(),
			&s3csdk.PutAccessPointConfigurationForObjectLambdaInput{
				AccountId:     aws.String(createTagsTestAccountID),
				Name:          aws.String("s7-olap"),
				Configuration: newConfig,
			},
		)
		require.NoError(t, err)

		cfgOut, err = client.GetAccessPointConfigurationForObjectLambda(
			t.Context(),
			&s3csdk.GetAccessPointConfigurationForObjectLambdaInput{
				AccountId: aws.String(createTagsTestAccountID),
				Name:      aws.String("s7-olap"),
			},
		)
		require.NoError(t, err)
		gotTransform := cfgOut.Configuration.TransformationConfigurations[0].ContentTransformation
		fn, ok := gotTransform.(*types.ObjectLambdaContentTransformationMemberAwsLambda)
		require.True(t, ok, "ContentTransformation must decode as the AwsLambda union member")
		assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:fn2", aws.ToString(fn.Value.FunctionArn))

		_, err = client.PutAccessPointPolicyForObjectLambda(
			t.Context(),
			&s3csdk.PutAccessPointPolicyForObjectLambdaInput{
				AccountId: aws.String(createTagsTestAccountID),
				Name:      aws.String("s7-olap"),
				Policy:    aws.String(`{"Version":"2012-10-17","Statement":[]}`),
			},
		)
		require.NoError(t, err)

		polOut, err := client.GetAccessPointPolicyForObjectLambda(
			t.Context(),
			&s3csdk.GetAccessPointPolicyForObjectLambdaInput{
				AccountId: aws.String(createTagsTestAccountID),
				Name:      aws.String("s7-olap"),
			},
		)
		require.NoError(t, err)
		assert.Contains(t, aws.ToString(polOut.Policy), "2012-10-17")

		statusOut, err := client.GetAccessPointPolicyStatusForObjectLambda(
			t.Context(),
			&s3csdk.GetAccessPointPolicyStatusForObjectLambdaInput{
				AccountId: aws.String(createTagsTestAccountID),
				Name:      aws.String("s7-olap"),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, statusOut.PolicyStatus)

		_, err = client.DeleteAccessPointPolicyForObjectLambda(
			t.Context(),
			&s3csdk.DeleteAccessPointPolicyForObjectLambdaInput{
				AccountId: aws.String(createTagsTestAccountID),
				Name:      aws.String("s7-olap"),
			},
		)
		require.NoError(t, err)

		listOut, err := client.ListAccessPointsForObjectLambda(
			t.Context(),
			&s3csdk.ListAccessPointsForObjectLambdaInput{AccountId: aws.String(createTagsTestAccountID)},
		)
		require.NoError(t, err)
		require.Len(t, listOut.ObjectLambdaAccessPointList, 1)
		assert.Equal(t, "s7-olap", aws.ToString(listOut.ObjectLambdaAccessPointList[0].Name))

		_, err = client.DeleteAccessPointForObjectLambda(t.Context(), &s3csdk.DeleteAccessPointForObjectLambdaInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-olap"),
		})
		require.NoError(t, err)
	})

	t.Run("access_point_scope_and_directory", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion))
		client := newTestS3ControlClient(t, h)

		_, err := client.CreateAccessPoint(t.Context(), &s3csdk.CreateAccessPointInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-scope-ap"),
			Bucket:    aws.String("s7-scope-bucket"),
		})
		require.NoError(t, err)

		_, err = client.PutAccessPointScope(t.Context(), &s3csdk.PutAccessPointScopeInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-scope-ap"),
			Scope: &types.Scope{
				Permissions: []types.ScopePermission{types.ScopePermissionGetObject},
				Prefixes:    []string{"logs/"},
			},
		})
		require.NoError(t, err)

		scopeOut, err := client.GetAccessPointScope(t.Context(), &s3csdk.GetAccessPointScopeInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-scope-ap"),
		})
		require.NoError(t, err)
		require.NotNil(t, scopeOut.Scope)
		assert.Equal(t, []string{"logs/"}, scopeOut.Scope.Prefixes)
		require.Len(t, scopeOut.Scope.Permissions, 1)
		assert.Equal(t, types.ScopePermissionGetObject, scopeOut.Scope.Permissions[0])

		_, err = client.DeleteAccessPointScope(t.Context(), &s3csdk.DeleteAccessPointScopeInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-scope-ap"),
		})
		require.NoError(t, err)

		dirOut, err := client.ListAccessPointsForDirectoryBuckets(
			t.Context(),
			&s3csdk.ListAccessPointsForDirectoryBucketsInput{AccountId: aws.String(createTagsTestAccountID)},
		)
		require.NoError(t, err)
		assert.NotNil(t, dirOut.AccessPointList)
	})

	t.Run("multi_region_access_points", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion))
		client := newTestS3ControlClient(t, h)

		createOut, err := client.CreateMultiRegionAccessPoint(t.Context(), &s3csdk.CreateMultiRegionAccessPointInput{
			AccountId:   aws.String(createTagsTestAccountID),
			ClientToken: aws.String("s7-mrap-token"),
			Details: &types.CreateMultiRegionAccessPointInput{
				Name: aws.String("s7-mrap"),
				Regions: []types.Region{
					{Bucket: aws.String("s7-mrap-bucket-east")},
				},
			},
		})
		require.NoError(t, err)
		requestTokenARN := aws.ToString(createOut.RequestTokenARN)
		assert.NotEmpty(t, requestTokenARN)

		descOut, err := client.DescribeMultiRegionAccessPointOperation(
			t.Context(),
			&s3csdk.DescribeMultiRegionAccessPointOperationInput{
				AccountId:       aws.String(createTagsTestAccountID),
				RequestTokenARN: aws.String(requestTokenARN),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, descOut.AsyncOperation)
		assert.Equal(t, requestTokenARN, aws.ToString(descOut.AsyncOperation.RequestTokenARN))

		getOut, err := client.GetMultiRegionAccessPoint(t.Context(), &s3csdk.GetMultiRegionAccessPointInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-mrap"),
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.AccessPoint)
		assert.Equal(t, "s7-mrap", aws.ToString(getOut.AccessPoint.Name))

		listOut, err := client.ListMultiRegionAccessPoints(t.Context(), &s3csdk.ListMultiRegionAccessPointsInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
		require.Len(t, listOut.AccessPoints, 1)

		_, err = client.PutMultiRegionAccessPointPolicy(t.Context(), &s3csdk.PutMultiRegionAccessPointPolicyInput{
			AccountId:   aws.String(createTagsTestAccountID),
			ClientToken: aws.String("s7-mrap-policy-token"),
			Details: &types.PutMultiRegionAccessPointPolicyInput{
				Name:   aws.String("s7-mrap"),
				Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
			},
		})
		require.NoError(t, err)

		polOut, err := client.GetMultiRegionAccessPointPolicy(t.Context(), &s3csdk.GetMultiRegionAccessPointPolicyInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-mrap"),
		})
		require.NoError(t, err)
		require.NotNil(t, polOut.Policy)
		require.NotNil(t, polOut.Policy.Established)
		assert.Contains(t, aws.ToString(polOut.Policy.Established.Policy), "2012-10-17")

		statusOut, err := client.GetMultiRegionAccessPointPolicyStatus(
			t.Context(),
			&s3csdk.GetMultiRegionAccessPointPolicyStatusInput{
				AccountId: aws.String(createTagsTestAccountID),
				Name:      aws.String("s7-mrap"),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, statusOut.Established)

		// This backend keys MRAP routing config by the plain access point Name
		// (not a full ARN); the Mrap URI-label parameter is a bare string as
		// far as the real client is concerned, so passing the Name directly
		// round-trips through the real client the same way an ARN would.
		mrapArn := "s7-mrap"

		_, err = client.SubmitMultiRegionAccessPointRoutes(t.Context(), &s3csdk.SubmitMultiRegionAccessPointRoutesInput{
			AccountId: aws.String(createTagsTestAccountID),
			Mrap:      aws.String(mrapArn),
			RouteUpdates: []types.MultiRegionAccessPointRoute{
				{Bucket: aws.String("s7-mrap-bucket-east"), TrafficDialPercentage: aws.Int32(100)},
			},
		})
		require.NoError(t, err)

		routesOut, err := client.GetMultiRegionAccessPointRoutes(
			t.Context(),
			&s3csdk.GetMultiRegionAccessPointRoutesInput{
				AccountId: aws.String(createTagsTestAccountID),
				Mrap:      aws.String(mrapArn),
			},
		)
		require.NoError(t, err)
		require.Len(t, routesOut.Routes, 1)
		assert.Equal(t, "s7-mrap-bucket-east", aws.ToString(routesOut.Routes[0].Bucket))
		assert.EqualValues(t, 100, aws.ToInt32(routesOut.Routes[0].TrafficDialPercentage))

		_, err = client.DeleteMultiRegionAccessPoint(t.Context(), &s3csdk.DeleteMultiRegionAccessPointInput{
			AccountId:   aws.String(createTagsTestAccountID),
			ClientToken: aws.String("s7-mrap-delete-token"),
			Details: &types.DeleteMultiRegionAccessPointInput{
				Name: aws.String("s7-mrap"),
			},
		})
		require.NoError(t, err)
	})

	t.Run("jobs_extras", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion))
		client := newTestS3ControlClient(t, h)

		createOut, err := client.CreateJob(t.Context(), &s3csdk.CreateJobInput{
			AccountId:          aws.String(createTagsTestAccountID),
			ClientRequestToken: aws.String("s7-job-token"),
			Operation: &types.JobOperation{
				LambdaInvoke: &types.LambdaInvokeOperation{
					FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:s7-fn"),
				},
			},
			Priority: aws.Int32(1),
			Report:   &types.JobReport{Enabled: false},
			RoleArn:  aws.String("arn:aws:iam::123456789012:role/batch-ops"),
		})
		require.NoError(t, err)
		jobID := aws.ToString(createOut.JobId)

		_, err = client.PutJobTagging(t.Context(), &s3csdk.PutJobTaggingInput{
			AccountId: aws.String(createTagsTestAccountID),
			JobId:     aws.String(jobID),
			Tags:      []types.S3Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
		})
		require.NoError(t, err)

		tagOut, err := client.GetJobTagging(t.Context(), &s3csdk.GetJobTaggingInput{
			AccountId: aws.String(createTagsTestAccountID),
			JobId:     aws.String(jobID),
		})
		require.NoError(t, err)
		require.Len(t, tagOut.Tags, 1)
		assert.Equal(t, "prod", aws.ToString(tagOut.Tags[0].Value))

		_, err = client.DeleteJobTagging(t.Context(), &s3csdk.DeleteJobTaggingInput{
			AccountId: aws.String(createTagsTestAccountID),
			JobId:     aws.String(jobID),
		})
		require.NoError(t, err)

		tagOut, err = client.GetJobTagging(t.Context(), &s3csdk.GetJobTaggingInput{
			AccountId: aws.String(createTagsTestAccountID),
			JobId:     aws.String(jobID),
		})
		require.NoError(t, err)
		assert.Empty(t, tagOut.Tags)

		prioOut, err := client.UpdateJobPriority(t.Context(), &s3csdk.UpdateJobPriorityInput{
			AccountId: aws.String(createTagsTestAccountID),
			JobId:     aws.String(jobID),
			Priority:  9,
		})
		require.NoError(t, err)
		assert.EqualValues(t, 9, prioOut.Priority)

		statusOut, err := client.UpdateJobStatus(t.Context(), &s3csdk.UpdateJobStatusInput{
			AccountId:          aws.String(createTagsTestAccountID),
			JobId:              aws.String(jobID),
			RequestedJobStatus: types.RequestedJobStatusCancelled,
			StatusUpdateReason: aws.String("s7 test cancel"),
		})
		require.NoError(t, err)
		assert.Equal(t, types.JobStatusCancelled, statusOut.Status)
		assert.Equal(t, "s7 test cancel", aws.ToString(statusOut.StatusUpdateReason))
	})

	t.Run("storage_lens", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion))
		client := newTestS3ControlClient(t, h)

		_, err := client.PutStorageLensConfiguration(t.Context(), &s3csdk.PutStorageLensConfigurationInput{
			AccountId: aws.String(createTagsTestAccountID),
			ConfigId:  aws.String("s7-sl-cfg"),
			StorageLensConfiguration: &types.StorageLensConfiguration{
				Id:           aws.String("s7-sl-cfg"),
				IsEnabled:    true,
				AccountLevel: &types.AccountLevel{BucketLevel: &types.BucketLevel{}},
			},
		})
		require.NoError(t, err)

		getOut, err := client.GetStorageLensConfiguration(t.Context(), &s3csdk.GetStorageLensConfigurationInput{
			AccountId: aws.String(createTagsTestAccountID),
			ConfigId:  aws.String("s7-sl-cfg"),
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.StorageLensConfiguration)
		assert.True(t, getOut.StorageLensConfiguration.IsEnabled)

		_, err = client.PutStorageLensConfigurationTagging(t.Context(), &s3csdk.PutStorageLensConfigurationTaggingInput{
			AccountId: aws.String(createTagsTestAccountID),
			ConfigId:  aws.String("s7-sl-cfg"),
			Tags:      []types.StorageLensTag{{Key: aws.String("env"), Value: aws.String("prod")}},
		})
		require.NoError(t, err)

		slTagOut, err := client.GetStorageLensConfigurationTagging(
			t.Context(),
			&s3csdk.GetStorageLensConfigurationTaggingInput{
				AccountId: aws.String(createTagsTestAccountID),
				ConfigId:  aws.String("s7-sl-cfg"),
			},
		)
		require.NoError(t, err)
		require.Len(t, slTagOut.Tags, 1)
		assert.Equal(t, "prod", aws.ToString(slTagOut.Tags[0].Value))

		_, err = client.DeleteStorageLensConfigurationTagging(
			t.Context(),
			&s3csdk.DeleteStorageLensConfigurationTaggingInput{
				AccountId: aws.String(createTagsTestAccountID),
				ConfigId:  aws.String("s7-sl-cfg"),
			},
		)
		require.NoError(t, err)

		_, err = client.DeleteStorageLensConfiguration(t.Context(), &s3csdk.DeleteStorageLensConfigurationInput{
			AccountId: aws.String(createTagsTestAccountID),
			ConfigId:  aws.String("s7-sl-cfg"),
		})
		require.NoError(t, err)

		_, err = client.CreateStorageLensGroup(t.Context(), &s3csdk.CreateStorageLensGroupInput{
			AccountId: aws.String(createTagsTestAccountID),
			StorageLensGroup: &types.StorageLensGroup{
				Name:   aws.String("s7-sl-group"),
				Filter: &types.StorageLensGroupFilter{MatchAnyPrefix: []string{"logs/"}},
			},
		})
		require.NoError(t, err)

		groupOut, err := client.GetStorageLensGroup(t.Context(), &s3csdk.GetStorageLensGroupInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-sl-group"),
		})
		require.NoError(t, err)
		require.NotNil(t, groupOut.StorageLensGroup)
		assert.Equal(t, []string{"logs/"}, groupOut.StorageLensGroup.Filter.MatchAnyPrefix)

		_, err = client.UpdateStorageLensGroup(t.Context(), &s3csdk.UpdateStorageLensGroupInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-sl-group"),
			StorageLensGroup: &types.StorageLensGroup{
				Name:   aws.String("s7-sl-group"),
				Filter: &types.StorageLensGroupFilter{MatchAnyPrefix: []string{"archive/"}},
			},
		})
		require.NoError(t, err)

		groupOut, err = client.GetStorageLensGroup(t.Context(), &s3csdk.GetStorageLensGroupInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-sl-group"),
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"archive/"}, groupOut.StorageLensGroup.Filter.MatchAnyPrefix)

		_, err = client.DeleteStorageLensGroup(t.Context(), &s3csdk.DeleteStorageLensGroupInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String("s7-sl-group"),
		})
		require.NoError(t, err)
	})

	t.Run("access_grants", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion))
		client := newTestS3ControlClient(t, h)

		_, err := client.CreateAccessGrantsInstance(t.Context(), &s3csdk.CreateAccessGrantsInstanceInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)

		getInstOut, err := client.GetAccessGrantsInstance(t.Context(), &s3csdk.GetAccessGrantsInstanceInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(getInstOut.AccessGrantsInstanceArn))

		listInstOut, err := client.ListAccessGrantsInstances(t.Context(), &s3csdk.ListAccessGrantsInstancesInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
		require.Len(t, listInstOut.AccessGrantsInstancesList, 1)
		assert.Equal(
			t,
			aws.ToString(getInstOut.AccessGrantsInstanceArn),
			aws.ToString(listInstOut.AccessGrantsInstancesList[0].AccessGrantsInstanceArn),
		)

		prefixOut, err := client.GetAccessGrantsInstanceForPrefix(
			t.Context(),
			&s3csdk.GetAccessGrantsInstanceForPrefixInput{
				AccountId: aws.String(createTagsTestAccountID),
				S3Prefix:  aws.String("s3://s7-bucket/prefix/"),
			},
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			aws.ToString(getInstOut.AccessGrantsInstanceArn),
			aws.ToString(prefixOut.AccessGrantsInstanceArn),
		)

		_, err = client.AssociateAccessGrantsIdentityCenter(
			t.Context(),
			&s3csdk.AssociateAccessGrantsIdentityCenterInput{
				AccountId:         aws.String(createTagsTestAccountID),
				IdentityCenterArn: aws.String("arn:aws:sso:::instance/ssoins-s7"),
			},
		)
		require.NoError(t, err)

		getInstOut, err = client.GetAccessGrantsInstance(t.Context(), &s3csdk.GetAccessGrantsInstanceInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:sso:::instance/ssoins-s7", aws.ToString(getInstOut.IdentityCenterInstanceArn))

		_, err = client.DissociateAccessGrantsIdentityCenter(
			t.Context(),
			&s3csdk.DissociateAccessGrantsIdentityCenterInput{AccountId: aws.String(createTagsTestAccountID)},
		)
		require.NoError(t, err)

		_, err = client.PutAccessGrantsInstanceResourcePolicy(
			t.Context(),
			&s3csdk.PutAccessGrantsInstanceResourcePolicyInput{
				AccountId: aws.String(createTagsTestAccountID),
				Policy:    aws.String(`{"Version":"2012-10-17","Statement":[]}`),
			},
		)
		require.NoError(t, err)

		policyOut, err := client.GetAccessGrantsInstanceResourcePolicy(
			t.Context(),
			&s3csdk.GetAccessGrantsInstanceResourcePolicyInput{AccountId: aws.String(createTagsTestAccountID)},
		)
		require.NoError(t, err)
		assert.Contains(t, aws.ToString(policyOut.Policy), "2012-10-17")

		_, err = client.DeleteAccessGrantsInstanceResourcePolicy(
			t.Context(),
			&s3csdk.DeleteAccessGrantsInstanceResourcePolicyInput{AccountId: aws.String(createTagsTestAccountID)},
		)
		require.NoError(t, err)

		locOut, err := client.CreateAccessGrantsLocation(t.Context(), &s3csdk.CreateAccessGrantsLocationInput{
			AccountId:     aws.String(createTagsTestAccountID),
			LocationScope: aws.String("s3://s7-bucket/"),
			IAMRoleArn:    aws.String("arn:aws:iam::123456789012:role/access-grants"),
		})
		require.NoError(t, err)
		locationID := aws.ToString(locOut.AccessGrantsLocationId)

		getLocOut, err := client.GetAccessGrantsLocation(t.Context(), &s3csdk.GetAccessGrantsLocationInput{
			AccountId:              aws.String(createTagsTestAccountID),
			AccessGrantsLocationId: aws.String(locationID),
		})
		require.NoError(t, err)
		assert.Equal(t, "s3://s7-bucket/", aws.ToString(getLocOut.LocationScope))

		_, err = client.UpdateAccessGrantsLocation(t.Context(), &s3csdk.UpdateAccessGrantsLocationInput{
			AccountId:              aws.String(createTagsTestAccountID),
			AccessGrantsLocationId: aws.String(locationID),
			IAMRoleArn:             aws.String("arn:aws:iam::123456789012:role/access-grants-v2"),
		})
		require.NoError(t, err)

		getLocOut, err = client.GetAccessGrantsLocation(t.Context(), &s3csdk.GetAccessGrantsLocationInput{
			AccountId:              aws.String(createTagsTestAccountID),
			AccessGrantsLocationId: aws.String(locationID),
		})
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:iam::123456789012:role/access-grants-v2", aws.ToString(getLocOut.IAMRoleArn))

		listLocOut, err := client.ListAccessGrantsLocations(t.Context(), &s3csdk.ListAccessGrantsLocationsInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
		require.Len(t, listLocOut.AccessGrantsLocationsList, 1)

		grantOut, err := client.CreateAccessGrant(t.Context(), &s3csdk.CreateAccessGrantInput{
			AccountId:              aws.String(createTagsTestAccountID),
			AccessGrantsLocationId: aws.String(locationID),
			Grantee: &types.Grantee{
				GranteeType:       types.GranteeTypeIam,
				GranteeIdentifier: aws.String("arn:aws:iam::123456789012:role/s7-reader"),
			},
			Permission: types.PermissionRead,
		})
		require.NoError(t, err)
		grantID := aws.ToString(grantOut.AccessGrantId)

		getGrantOut, err := client.GetAccessGrant(t.Context(), &s3csdk.GetAccessGrantInput{
			AccountId:     aws.String(createTagsTestAccountID),
			AccessGrantId: aws.String(grantID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.PermissionRead, getGrantOut.Permission)

		listGrantsOut, err := client.ListAccessGrants(t.Context(), &s3csdk.ListAccessGrantsInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
		require.Len(t, listGrantsOut.AccessGrantsList, 1)

		callerOut, err := client.ListCallerAccessGrants(t.Context(), &s3csdk.ListCallerAccessGrantsInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
		assert.NotNil(t, callerOut.CallerAccessGrantsList)

		dataAccessOut, err := client.GetDataAccess(t.Context(), &s3csdk.GetDataAccessInput{
			AccountId:  aws.String(createTagsTestAccountID),
			Target:     aws.String("s3://s7-bucket/prefix/object.txt"),
			Permission: types.PermissionRead,
		})
		require.NoError(t, err)
		assert.Equal(t, "s3://s7-bucket/prefix/object.txt", aws.ToString(dataAccessOut.MatchedGrantTarget))

		_, err = client.DeleteAccessGrant(t.Context(), &s3csdk.DeleteAccessGrantInput{
			AccountId:     aws.String(createTagsTestAccountID),
			AccessGrantId: aws.String(grantID),
		})
		require.NoError(t, err)

		_, err = client.DeleteAccessGrantsLocation(t.Context(), &s3csdk.DeleteAccessGrantsLocationInput{
			AccountId:              aws.String(createTagsTestAccountID),
			AccessGrantsLocationId: aws.String(locationID),
		})
		require.NoError(t, err)

		_, err = client.DeleteAccessGrantsInstance(t.Context(), &s3csdk.DeleteAccessGrantsInstanceInput{
			AccountId: aws.String(createTagsTestAccountID),
		})
		require.NoError(t, err)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion))
		client := newTestS3ControlClient(t, h)

		_, err := client.CreateStorageLensGroup(t.Context(), &s3csdk.CreateStorageLensGroupInput{
			AccountId: aws.String(createTagsTestAccountID),
			StorageLensGroup: &types.StorageLensGroup{
				Name:   aws.String("s7-tag-group"),
				Filter: &types.StorageLensGroupFilter{MatchAnyPrefix: []string{"a/"}},
			},
		})
		require.NoError(t, err)

		groupARN := "arn:aws:s3:" + createTagsTestRegion + ":" + createTagsTestAccountID + ":storage-lens-group/s7-tag-group"

		_, err = client.TagResource(t.Context(), &s3csdk.TagResourceInput{
			AccountId:   aws.String(createTagsTestAccountID),
			ResourceArn: aws.String(groupARN),
			Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
		})
		require.NoError(t, err)

		listOut, err := client.ListTagsForResource(t.Context(), &s3csdk.ListTagsForResourceInput{
			AccountId:   aws.String(createTagsTestAccountID),
			ResourceArn: aws.String(groupARN),
		})
		require.NoError(t, err)
		require.Len(t, listOut.Tags, 1)
		assert.Equal(t, "prod", aws.ToString(listOut.Tags[0].Value))

		_, err = client.UntagResource(t.Context(), &s3csdk.UntagResourceInput{
			AccountId:   aws.String(createTagsTestAccountID),
			ResourceArn: aws.String(groupARN),
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		listOut, err = client.ListTagsForResource(t.Context(), &s3csdk.ListTagsForResourceInput{
			AccountId:   aws.String(createTagsTestAccountID),
			ResourceArn: aws.String(groupARN),
		})
		require.NoError(t, err)
		assert.Empty(t, listOut.Tags)
	})
}

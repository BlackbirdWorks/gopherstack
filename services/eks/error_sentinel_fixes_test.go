package eks_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

func newSentinelTestHandler(t *testing.T) *eks.Handler {
	t.Helper()

	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	return eks.NewHandler(backend)
}

// TestCreateAddon_InvalidResolveConflicts_InvalidParameterException proves
// this service's global validation sentinel reports the real
// InvalidParameterException code. eks@v1.90.4's types/errors.go has no
// "InvalidParameterValueException" type at all -- it does not exist anywhere
// in the pinned SDK -- yet that is the code every EKS op that fails
// client-side validation through this handler's shared ErrValidation
// sentinel used to emit.
func TestCreateAddon_InvalidResolveConflicts_InvalidParameterException(t *testing.T) {
	t.Parallel()

	h := newSentinelTestHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("sentinel-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &types.VpcConfigRequest{},
	})
	require.NoError(t, err)

	_, err = client.CreateAddon(ctx, &ekssdk.CreateAddonInput{
		ClusterName:      aws.String("sentinel-cluster"),
		AddonName:        aws.String("vpc-cni"),
		ResolveConflicts: types.ResolveConflicts("BOGUS"),
	})
	require.Error(t, err)

	var ipe *types.InvalidParameterException
	require.ErrorAsf(t, err, &ipe, "expected a real InvalidParameterException from the SDK deserializer, got %v", err)
}

// TestCreateFargateProfile_UnknownCluster_InvalidParameterException proves
// CreateFargateProfile reports an unknown cluster name via a code its own
// deserializer models. eks@v1.90.4 deserializers.go's
// awsRestjson1_deserializeOpErrorCreateFargateProfile switch has no
// ResourceNotFoundException case at all.
func TestCreateFargateProfile_UnknownCluster_InvalidParameterException(t *testing.T) {
	t.Parallel()

	h := newSentinelTestHandler(t)
	client := newTestEKSClient(t, h)

	_, err := client.CreateFargateProfile(t.Context(), &ekssdk.CreateFargateProfileInput{
		ClusterName:         aws.String("no-such-cluster"),
		FargateProfileName:  aws.String("fp1"),
		PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
	})
	require.Error(t, err)

	var ipe *types.InvalidParameterException
	require.ErrorAsf(t, err, &ipe, "expected a real InvalidParameterException from the SDK deserializer, got %v", err)
}

// TestCreateFargateProfile_Duplicate_InvalidParameterException is
// CreateFargateProfile's sibling finding: its own deserializer also has no
// ResourceInUseException case, so a duplicate profile name cannot be
// reported that way either.
func TestCreateFargateProfile_Duplicate_InvalidParameterException(t *testing.T) {
	t.Parallel()

	h := newSentinelTestHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("fp-dup-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &types.VpcConfigRequest{},
	})
	require.NoError(t, err)

	in := &ekssdk.CreateFargateProfileInput{
		ClusterName:         aws.String("fp-dup-cluster"),
		FargateProfileName:  aws.String("dup-fp"),
		PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
	}
	_, err = client.CreateFargateProfile(ctx, in)
	require.NoError(t, err)

	_, err = client.CreateFargateProfile(ctx, in)
	require.Error(t, err)

	var ipe *types.InvalidParameterException
	require.ErrorAsf(t, err, &ipe, "expected a real InvalidParameterException from the SDK deserializer, got %v", err)
}

// TestCreateCapability_UnknownCluster_InvalidParameterException proves
// CreateCapability reports an unknown cluster name via a code its own
// deserializer models. eks@v1.90.4 deserializers.go's
// awsRestjson1_deserializeOpErrorCreateCapability switch has no
// ResourceNotFoundException case.
func TestCreateCapability_UnknownCluster_InvalidParameterException(t *testing.T) {
	t.Parallel()

	h := newSentinelTestHandler(t)
	client := newTestEKSClient(t, h)

	_, err := client.CreateCapability(t.Context(), &ekssdk.CreateCapabilityInput{
		ClusterName:             aws.String("no-such-cluster"),
		CapabilityName:          aws.String("cap1"),
		Type:                    types.CapabilityTypeAck,
		RoleArn:                 aws.String("arn:aws:iam::123456789012:role/cap"),
		DeletePropagationPolicy: types.CapabilityDeletePropagationPolicyRetain,
	})
	require.Error(t, err)

	var ipe *types.InvalidParameterException
	require.ErrorAsf(t, err, &ipe, "expected a real InvalidParameterException from the SDK deserializer, got %v", err)
}

// TestCreateNodegroup_UnknownCluster_InvalidParameterException proves
// CreateNodegroup reports an unknown cluster name via a code its own
// deserializer models. eks@v1.90.4 deserializers.go's
// awsRestjson1_deserializeOpErrorCreateNodegroup switch has no
// ResourceNotFoundException case.
func TestCreateNodegroup_UnknownCluster_InvalidParameterException(t *testing.T) {
	t.Parallel()

	h := newSentinelTestHandler(t)
	client := newTestEKSClient(t, h)

	_, err := client.CreateNodegroup(t.Context(), &ekssdk.CreateNodegroupInput{
		ClusterName:   aws.String("no-such-cluster"),
		NodegroupName: aws.String("ng1"),
		NodeRole:      aws.String("arn:aws:iam::123456789012:role/node"),
		Subnets:       []string{"subnet-1"},
	})
	require.Error(t, err)

	var ipe *types.InvalidParameterException
	require.ErrorAsf(t, err, &ipe, "expected a real InvalidParameterException from the SDK deserializer, got %v", err)
}

// TestTagResource_UnknownARN_NotFoundException proves TagResource reports an
// unrecognized ARN with the actual code its own deserializer models.
// eks@v1.90.4 deserializers.go's awsRestjson1_deserializeOpErrorTagResource
// switch models only BadRequestException/NotFoundException -- an entirely
// different exception family from the rest of this service's ops (which use
// ResourceNotFoundException/InvalidParameterException/etc).
func TestTagResource_UnknownARN_NotFoundException(t *testing.T) {
	t.Parallel()

	h := newSentinelTestHandler(t)
	client := newTestEKSClient(t, h)

	_, err := client.TagResource(t.Context(), &ekssdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:eks:us-east-1:123456789012:cluster/no-such-cluster"),
		Tags:        map[string]string{"k": "v"},
	})
	require.Error(t, err)

	var nf *types.NotFoundException
	require.ErrorAsf(t, err, &nf, "expected a real NotFoundException from the SDK deserializer, got %v", err)
}

// TestUntagResource_UnknownARN_NotFoundException is TagResource's sibling
// for UntagResource -- same wrong-exception-family bug, confirmed
// independently against awsRestjson1_deserializeOpErrorUntagResource.
func TestUntagResource_UnknownARN_NotFoundException(t *testing.T) {
	t.Parallel()

	h := newSentinelTestHandler(t)
	client := newTestEKSClient(t, h)

	_, err := client.UntagResource(t.Context(), &ekssdk.UntagResourceInput{
		ResourceArn: aws.String("arn:aws:eks:us-east-1:123456789012:cluster/no-such-cluster"),
		TagKeys:     []string{"k"},
	})
	require.Error(t, err)

	var nf *types.NotFoundException
	require.ErrorAsf(t, err, &nf, "expected a real NotFoundException from the SDK deserializer, got %v", err)
}

// TestListTagsForResource_UnknownARN_NotFoundException is TagResource's
// sibling for ListTagsForResource -- same wrong-exception-family bug,
// confirmed independently against
// awsRestjson1_deserializeOpErrorListTagsForResource.
func TestListTagsForResource_UnknownARN_NotFoundException(t *testing.T) {
	t.Parallel()

	h := newSentinelTestHandler(t)
	client := newTestEKSClient(t, h)

	_, err := client.ListTagsForResource(t.Context(), &ekssdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:eks:us-east-1:123456789012:cluster/no-such-cluster"),
	})
	require.Error(t, err)

	var nf *types.NotFoundException
	require.ErrorAsf(t, err, &nf, "expected a real NotFoundException from the SDK deserializer, got %v", err)
}

// TestTagResource_TooManyTags_BadRequestException proves TagResource reports
// a tag-limit validation failure with the real BadRequestException code
// (see TestTagResource_UnknownARN_NotFoundException's deserializer note) --
// not "InvalidParameterException", which TagResource's own switch also does
// not model.
func TestTagResource_TooManyTags_BadRequestException(t *testing.T) {
	t.Parallel()

	h := newSentinelTestHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	out, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("tag-limit-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &types.VpcConfigRequest{},
	})
	require.NoError(t, err)

	tags := make(map[string]string, 51)
	for i := range 51 {
		tags[fmt.Sprintf("k%d", i)] = "v"
	}

	_, err = client.TagResource(ctx, &ekssdk.TagResourceInput{
		ResourceArn: out.Cluster.Arn,
		Tags:        tags,
	})
	require.Error(t, err)

	var br *types.BadRequestException
	require.ErrorAsf(t, err, &br, "expected a real BadRequestException from the SDK deserializer, got %v", err)
}

package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDeregisterImage_RealClient covers handler_images.go's handleDeregisterImage,
// which on success returned (nil, nil) -- an untyped nil "any". xml.Marshal(nil)
// serializes to zero bytes, so the wire response was just the XML declaration
// with no DeregisterImageResponse root element, no requestId, and no Return
// field at all (real DeregisterImageOutput.Return is *bool, always true --
// api_op_DeregisterImage.go:89). The pre-fix response also used the wrong
// error code for a nonexistent AMI: ErrInvalidParameter (InvalidParameterValue)
// instead of the sentinel every sibling not-found path in this file already
// uses, ErrImageNotFound (InvalidAMIID.NotFound, the real AWS code).
func TestDeregisterImage_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	img, err := b.RegisterImage("test-ami", "test image", "x86_64")
	require.NoError(t, err)

	out, err := client.DeregisterImage(t.Context(), &ec2sdk.DeregisterImageInput{
		ImageId: &img.ImageID,
	})
	require.NoError(t, err, "pre-fix this call's response had no Return field/root element")
	require.NotNil(t, out.Return, "pre-fix Return was always nil: the response body was empty")
	assert.True(t, *out.Return)

	_, err = client.DeregisterImage(t.Context(), &ec2sdk.DeregisterImageInput{
		ImageId: aws.String("ami-does-not-exist"),
	})
	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidAMIID.NotFound", apiErr.ErrorCode(),
		"pre-fix this returned the generic InvalidParameterValue instead of the real AMI-not-found code")
}

// TestReportInstanceStatus_AllInstancesValidated_RealClient covers
// handler_instances.go's handleReportInstanceStatus, which only ever read
// InstanceId.1 and ignored InstanceId.2+ entirely -- so a real client
// reporting status for a batch of instances (ReportInstanceStatusInput.Instances
// is a required list, serialized flat as InstanceId.N per
// ec2@v1.319.1 serializers.go:91277) silently succeeded even when a later
// instance in the batch didn't exist, instead of real AWS's per-request
// InvalidInstanceID.NotFound.
func TestReportInstanceStatus_AllInstancesValidated_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	insts, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	_, err = client.ReportInstanceStatus(t.Context(), &ec2sdk.ReportInstanceStatusInput{
		Instances:   []string{insts[0].ID, "i-doesnotexist"},
		ReasonCodes: []types.ReportInstanceReasonCodes{types.ReportInstanceReasonCodesOther},
		Status:      types.ReportStatusTypeOk,
	})
	require.Error(t, err, "pre-fix the second, nonexistent instance ID was never even read")
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidInstanceID.NotFound", apiErr.ErrorCode())
}

// TestModifyInstanceCreditSpecification_Batch_RealClient covers
// handler_instances.go's handleModifyInstanceCreditSpecification, which only
// ever read InstanceCreditSpecification.1 and silently dropped every other
// entry in the (required, unbounded) InstanceCreditSpecifications list
// (ec2@v1.319.1 serializers.go:87727). A real client modifying a fleet's
// credit option in one batch call only got the first instance changed, with
// no error and no UnsuccessfulInstanceCreditSpecifications reporting for the
// rest.
func TestModifyInstanceCreditSpecification_Batch_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	insts, err := b.RunInstances("ami-test", "t3.micro", "", 2)
	require.NoError(t, err)

	out, err := client.ModifyInstanceCreditSpecification(
		t.Context(),
		&ec2sdk.ModifyInstanceCreditSpecificationInput{
			InstanceCreditSpecifications: []types.InstanceCreditSpecificationRequest{
				{InstanceId: &insts[0].ID, CpuCredits: aws.String("unlimited")},
				{InstanceId: &insts[1].ID, CpuCredits: aws.String("unlimited")},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.SuccessfulInstanceCreditSpecifications, 2,
		"pre-fix only the first InstanceCreditSpecification entry was ever read")

	specs := b.DescribeInstanceCreditSpecifications([]string{insts[0].ID, insts[1].ID})
	require.Len(t, specs, 2)
	for _, s := range specs {
		assert.Equal(t, "unlimited", s.CPUCredits)
	}

	// A nonexistent instance in the batch is reported per-item, not fatal to
	// the whole request.
	out2, err := client.ModifyInstanceCreditSpecification(
		t.Context(),
		&ec2sdk.ModifyInstanceCreditSpecificationInput{
			InstanceCreditSpecifications: []types.InstanceCreditSpecificationRequest{
				{InstanceId: aws.String("i-doesnotexist"), CpuCredits: aws.String("unlimited")},
			},
		},
	)
	require.NoError(t, err)
	require.Empty(t, out2.SuccessfulInstanceCreditSpecifications)
	require.Len(t, out2.UnsuccessfulInstanceCreditSpecifications, 1)
	assert.Equal(t, "i-doesnotexist", *out2.UnsuccessfulInstanceCreditSpecifications[0].InstanceId)
}

// TestDescribeInstanceTypeOfferings_Filters_RealClient covers
// handler_instances.go's handleDescribeInstanceTypeOfferings, which ignored
// its vals url.Values entirely (took `_ url.Values`) -- so the real
// "instance-type"/"location" Filters (api_op_DescribeInstanceTypeOfferings.go
// DescribeInstanceTypeOfferingsInput.Filters doc comment) were silently
// discarded and every call returned the full static offering list regardless
// of what a real client asked to filter on.
func TestDescribeInstanceTypeOfferings_Filters_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	unfiltered, err := client.DescribeInstanceTypeOfferings(
		t.Context(), &ec2sdk.DescribeInstanceTypeOfferingsInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, unfiltered.InstanceTypeOfferings)

	filtered, err := client.DescribeInstanceTypeOfferings(
		t.Context(),
		&ec2sdk.DescribeInstanceTypeOfferingsInput{
			Filters: []types.Filter{{
				Name:   aws.String("instance-type"),
				Values: []string{"t3.micro"},
			}},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, filtered.InstanceTypeOfferings,
		"pre-fix filters were ignored, but this alone can't tell fixed from broken")
	assert.Less(t, len(filtered.InstanceTypeOfferings), len(unfiltered.InstanceTypeOfferings),
		"pre-fix the instance-type filter was silently ignored and every offering came back")
	for _, o := range filtered.InstanceTypeOfferings {
		assert.Equal(t, types.InstanceType("t3.micro"), o.InstanceType)
	}
}

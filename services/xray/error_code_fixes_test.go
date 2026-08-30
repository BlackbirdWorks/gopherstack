package xray_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	xraytypes "github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/stretchr/testify/require"
)

// TestCreateGroup_AlreadyExists_RealClient drives CreateGroup through the
// real client with a GroupName that already exists. gopherstack previously
// emitted "GroupAlreadyExistsException" -- that type names no shape anywhere
// in this SDK (checked types/errors.go and every
// awsRestjson1_deserializeOpError* switch in deserializers.go). CreateGroup's
// own deserializer (awsRestjson1_deserializeOpErrorCreateGroup) models only
// InvalidRequestException and ThrottledException, so InvalidRequestException
// is the correct code (gopherstack-101r).
func TestCreateGroup_AlreadyExists_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestXRayClient(t)

	_, err := client.CreateGroup(t.Context(), &xraysdk.CreateGroupInput{
		GroupName: aws.String("dup-group"),
	})
	require.NoError(t, err)

	_, err = client.CreateGroup(t.Context(), &xraysdk.CreateGroupInput{
		GroupName: aws.String("dup-group"),
	})
	require.Error(t, err)

	var ir *xraytypes.InvalidRequestException
	require.ErrorAs(t, err, &ir, "expected a real InvalidRequestException from the SDK deserializer")
}

// TestCreateSamplingRule_AlreadyExists_RealClient drives CreateSamplingRule
// through the real client with a RuleName that already exists. gopherstack
// previously emitted "RuleAlreadyExistsException" -- absent from this SDK
// entirely. CreateSamplingRule's own deserializer
// (awsRestjson1_deserializeOpErrorCreateSamplingRule) models
// InvalidRequestException, RuleLimitExceededException, and
// ThrottledException; InvalidRequestException is the correct code for a
// duplicate name (gopherstack-101r).
func TestCreateSamplingRule_AlreadyExists_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestXRayClient(t)

	newRule := func() *xraytypes.SamplingRule {
		return &xraytypes.SamplingRule{
			RuleName:      aws.String("dup-rule"),
			ResourceARN:   aws.String("*"),
			ServiceName:   aws.String("*"),
			ServiceType:   aws.String("*"),
			Host:          aws.String("*"),
			HTTPMethod:    aws.String("*"),
			URLPath:       aws.String("*"),
			FixedRate:     0.05,
			Priority:      aws.Int32(100),
			ReservoirSize: 1,
			Version:       aws.Int32(1),
		}
	}

	_, err := client.CreateSamplingRule(t.Context(), &xraysdk.CreateSamplingRuleInput{SamplingRule: newRule()})
	require.NoError(t, err)

	_, err = client.CreateSamplingRule(t.Context(), &xraysdk.CreateSamplingRuleInput{SamplingRule: newRule()})
	require.Error(t, err)

	var ir *xraytypes.InvalidRequestException
	require.ErrorAs(t, err, &ir, "expected a real InvalidRequestException from the SDK deserializer")
}

// TestCreateSamplingRule_InvalidPriority_RealClient drives CreateSamplingRule
// through the real client with a Priority outside the documented 1-9999
// range. gopherstack previously emitted "InvalidSamplingRuleException" --
// absent from this SDK entirely (same 0-of-N pattern as GroupAlreadyExistsException
// above). InvalidRequestException is the correct code (gopherstack-101r).
func TestCreateSamplingRule_InvalidPriority_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestXRayClient(t)

	_, err := client.CreateSamplingRule(t.Context(), &xraysdk.CreateSamplingRuleInput{
		SamplingRule: &xraytypes.SamplingRule{
			RuleName:      aws.String("bad-priority-rule"),
			ResourceARN:   aws.String("*"),
			ServiceName:   aws.String("*"),
			ServiceType:   aws.String("*"),
			Host:          aws.String("*"),
			HTTPMethod:    aws.String("*"),
			URLPath:       aws.String("*"),
			FixedRate:     0.05,
			Priority:      aws.Int32(99999),
			ReservoirSize: 1,
			Version:       aws.Int32(1),
		},
	})
	require.Error(t, err)

	var ir *xraytypes.InvalidRequestException
	require.ErrorAs(t, err, &ir, "expected a real InvalidRequestException from the SDK deserializer")
}

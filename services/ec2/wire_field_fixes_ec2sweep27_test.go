package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestGetLaunchTemplateData_InstanceFields_RealClient covers
// handleGetLaunchTemplateData, which pre-fix only ever populated ImageId and
// InstanceType on the response, discarding KeyName, SecurityGroupIds,
// DisableApiTermination, DisableApiStop, EbsOptimized, and
// InstanceInitiatedShutdownBehavior even though the source instance tracks
// all of them (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentResponseLaunchTemplateData matches
// "keyName", "securityGroupIdSet", "disableApiTermination",
// "disableApiStop", "ebsOptimized", and
// "instanceInitiatedShutdownBehavior"), so a real client always saw these as
// empty/zero despite HTTP 200/err==nil.
//
// It also covers a second bug: DisableApiTermination/DisableApiStop/
// EbsOptimized are *bool on ResponseLaunchTemplateData
// (ec2@v1.319.1 types/types.go), so an explicit false must decode as a
// non-nil pointer to false, not nil -- the "defaults" case below proves the
// wire response serializes false rather than omitting the element.
//
//nolint:tparallel // subtests are sequential by design, see the loop comment below
func TestGetLaunchTemplateData_InstanceFields_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	insts, err := b.RunInstances("ami-sweep27", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, insts, 1)
	instanceID := insts[0].ID

	require.NoError(t, b.SetInstanceLaunchConfig(instanceID, "sweep27-key", []string{"sg-sweep27"}))

	tests := []struct {
		setup func(t *testing.T)
		name  string
		want  struct {
			disableAPITermination bool
			disableAPIStop        bool
			ebsOptimized          bool
		}
	}{
		{
			name:  "defaults",
			setup: func(*testing.T) {},
		},
		{
			name: "explicit true",
			setup: func(t *testing.T) {
				t.Helper()

				// Real ModifyInstanceAttribute accepts exactly one attribute
				// per call, so each boolean needs its own request.
				_, attrErr := client.ModifyInstanceAttribute(t.Context(), &ec2sdk.ModifyInstanceAttributeInput{
					InstanceId:            &instanceID,
					DisableApiTermination: &types.AttributeBooleanValue{Value: aws.Bool(true)},
				})
				require.NoError(t, attrErr)

				_, attrErr = client.ModifyInstanceAttribute(t.Context(), &ec2sdk.ModifyInstanceAttributeInput{
					InstanceId:     &instanceID,
					DisableApiStop: &types.AttributeBooleanValue{Value: aws.Bool(true)},
				})
				require.NoError(t, attrErr)

				_, attrErr = client.ModifyInstanceAttribute(t.Context(), &ec2sdk.ModifyInstanceAttributeInput{
					InstanceId:   &instanceID,
					EbsOptimized: &types.AttributeBooleanValue{Value: aws.Bool(true)},
				})
				require.NoError(t, attrErr)
			},
			want: struct {
				disableAPITermination bool
				disableAPIStop        bool
				ebsOptimized          bool
			}{disableAPITermination: true, disableAPIStop: true, ebsOptimized: true},
		},
	}

	// Sequential: "explicit true" mutates instanceID's attributes, which
	// "defaults" must observe unmodified first.
	for _, tc := range tests { //nolint:paralleltest // sequential by design, see comment above
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(t)

			out, getErr := client.GetLaunchTemplateData(t.Context(), &ec2sdk.GetLaunchTemplateDataInput{
				InstanceId: &instanceID,
			})
			require.NoError(t, getErr)
			require.NotNil(t, out.LaunchTemplateData)

			data := out.LaunchTemplateData
			require.NotNil(t, data.KeyName, "KeyName nil - pre-fix never populated")
			assert.Equal(t, "sweep27-key", *data.KeyName)
			assert.Equal(
				t, []string{"sg-sweep27"}, data.SecurityGroupIds,
				"SecurityGroupIds empty - pre-fix never populated",
			)

			require.NotNil(t, data.DisableApiTermination, "DisableApiTermination nil - false must serialize, not omit")
			assert.Equal(t, tc.want.disableAPITermination, *data.DisableApiTermination)

			require.NotNil(t, data.DisableApiStop, "DisableApiStop nil - false must serialize, not omit")
			assert.Equal(t, tc.want.disableAPIStop, *data.DisableApiStop)

			require.NotNil(t, data.EbsOptimized, "EbsOptimized nil - false must serialize, not omit")
			assert.Equal(t, tc.want.ebsOptimized, *data.EbsOptimized)
		})
	}
}
